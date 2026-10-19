# 04 数据规范与数据字典

版本 HL-DOC-1.0.0；数据策略 `retail-clean-v1`；数据schema `1.0`；FROZEN。

## 1. 来源与不可变性

唯一真实数据为UCI Online Retail.xlsx，541,909条来源交易明细，覆盖2010-12-01至2011-12-09，币种GBP、CC BY 4.0。保存下载URL、下载UTC时间、文件名、字节数、SHA-256、行数、工作表。以工作表第一行为字段标题，其余非空行计为raw_rows；仅末尾全空Excel行不算数据，内部全空行作为拒绝记录。

同一source_sha256与policy_version组合只允许一个成功或进行中batch；失败批次复用batch_id重试attempt。原始文件不可编辑。追加新文件构成新批次，1.0不支持把多个文件自动当增量合并。来源信息见[15](15-evidence.md)。

## 2. 原字段与规范化

| 原字段 | 规范字段/类型 | 规则 |
|---|---|---|
| InvoiceNo | invoice_no/string≤32 | trim、ASCII大写，保留C前缀；空/超长拒绝 |
| StockCode | stock_code/string≤32 | trim、ASCII大写；空/超长拒绝，不丢前导零 |
| Description | description/string≤512 | 空置NULL；换行/制表符变空格，去NUL，超长截断并计warning |
| Quantity | quantity/int64 | 必须整数且非0，溢出拒绝 |
| InvoiceDate | invoice_at/local datetime | 从Excel日期值解析，文本仅接受明确配置的来源格式，不猜测日/月顺序；失败拒绝 |
| UnitPrice | unit_price_pence/int64 | Decimal转便士，HALF_UP保留2位；价格≤0拒绝，舍入计数 |
| CustomerID | customer_id/nullable string≤32 | Excel整数型ID转换无“.0”的十进制字符串；禁止四舍五入非整数；空或非法视无客户并计warning，不因此删除合法销售 |
| Country | country/string≤64 | trim、连续空格压缩，空为UNKNOWN；超长拒绝，大小写保留 |

额外源字段：source_row_number（从2起、int64）、row_id（SHA-256十六进制，源文件hash与行号的UTF-8 `hash:row`组合计算）、batch_id、schema_version。

描述不同不会产生新商品ID；展示描述取该stock_code在正向数据最后一条非空描述，按invoice_at降序、source_row_number降序选取，若全空则显示stock_code。

## 3. 分类与金额

分类优先级：无法解析/越界→REJECTED；quantity=0或unit_price_pence≤0→REJECTED；其他字段合法且invoice_no以C开头或quantity<0→RETURN；其余→SALE。每条来源行恰好落入一个主分类。C前缀但数量为正属于RETURN并附warning `CANCEL_POSITIVE_QUANTITY`；数量为负但无C前缀附 `NEGATIVE_WITHOUT_CANCEL`。不假定退货能匹配原单。

SALE的quantity_abs=quantity，signed_amount_pence=quantity*unit_price_pence；RETURN的quantity_abs=abs(quantity)，signed_amount_pence=−quantity_abs*unit_price_pence。乘积必须落入有符号int64，否则REJECTED。保留原quantity用于审计，RETURN不参与正向订单数、RFM和推荐。

正向销售额gross=sum(SALE金额)；退货额returns=−sum(RETURN signed金额)，为非负数；net=gross−returns。正向销量与退货数量分别统计，禁止混写成“总销量”。订单数仅SALE的不同invoice_no；每个invoice_no必须只有一个来源日期，矛盾时整个batch质量门禁失败，不悄悄拆单。每单有不同country/customer_id的矛盾也使质量门禁失败（NULL与唯一非NULL并存允许，订单级采用唯一非NULL）。

`average_order_value=gross/order_count`，在API展示时HALF_UP两位；分母0返回NULL。SALE有效客户数为customer_id非NULL的不同标识数。

重复疑似：以八个标准化来源字段完全相同定义，记录重复组与超额行数，**保留原交易行**；缺少可靠明细唯一键，不能武断删除。只有重导同文件的批次幂等消除重复写入。

## 4. 推荐资格与 RFM

推荐商品限定stock_code匹配ASCII正则 `^[0-9]{5,6}[A-Z]{0,2}$` 且SALE中出现。其他编码计为non_merchandise，不进入模型，但其合法金额仍进入销售。customer_id非NULL的SALE进入客户购买数据；描述缺失不影响推荐资格。

同invoice中商品去重后做共现。购物篮大于100种不同商品时，仅为共现生成按stock_code字节序最小的100种，记录截断订单/商品数；销售、RFM、模型购买交互仍保留全部合格商品。订单共现不能跨订单配对。

RFM基准日=本batch有效SALE最大来源日期+1日；R为该基准日减客户最后SALE日期的天数，F为客户不同正向订单数，M为正向金额便士。分数使用全体有效客户分布：q(x)=min(5,1+floor(5*小于x的人数/N))；R_score=6−q(R)，F/M_score=q(F/M)，同值同分。segment依次匹配：CORE（R≥4,F≥4,M≥4）、AT_RISK（R≤2且F≥4）、NEW（R≥4且F≤2）、POTENTIAL（R≥3且不属于前述）、OTHER。这里R/F/M指分数；RFM快照不随页面销售筛选重算。

## 5. HDFS 路径与文件协议

统一根`/retail/v1`。所有batch目录名为`batch_id=<ID>`，run目录为`run_id=<UUID>`。原始区`raw/batch_id=…/source.xlsx`；转换区`normalized/batch_id=…`；清洗区`clean/batch_id=…/run_id=…`；特征区`features/batch_id=…/run_id=…`；结果区`results/batch_id=…/run_id=…`；模型归档`models/model_run_id=…`；发布清单`releases/release_id=…`。引用必须带完整路径和hash，不用“latest”目录指向可变结果。

TSV协议：UTF-8，无BOM，无header，LF换行，字段无真实tab/CR/LF/NUL；缺失值字面量`\N`，字符串恰为该值时按反斜线转义为`\\N`，其余反斜线也双写。解码时先判断原始token是否恰为`\N`（是则NULL），其他token再还原反斜线转义；不能在解码后把真实字符串误判为NULL。schema写入相邻manifest；MapReduce使用TextInputFormat，每行一条。金额以便士整数，不出现千分位。来源时间格式`YYYY-MM-DDTHH:mm:ss`，无时区后缀。

| 数据集 | 每行字段（严格按顺序） | 唯一粒度 |
|---|---|---|
| normalized | row_id,source_row_number,invoice_no,stock_code,description,quantity,invoice_at,unit_price_pence,customer_id,country,parse_error | 源行；错误行原值放拒绝审计附件，字段可NULL |
| clean | row_id,source_row_number,invoice_no,stock_code,description,quantity,quantity_abs,invoice_at,source_date,unit_price_pence,signed_amount_pence,customer_id,country,record_class,is_merchandise | 源行，仅SALE/RETURN |
| rejected | row_id,source_row_number,primary_reason | 源行；primary_reason按本节固定优先级 |
| warnings | row_id,warning_code | 行与warning组合，可多个 |
| sales_cube | source_date,country,stock_code,gross_pence,return_pence,sale_quantity,return_quantity | 日期×国家×商品 |
| order_fact | invoice_no,source_date,country,customer_id,gross_pence | 正向订单 |
| product_order_fact | invoice_no,stock_code,source_date,country,gross_pence,quantity | 正向订单×商品，包含非商品编码的合法销售 |
| rfm | customer_id,last_purchase,recency_days,frequency,monetary_pence,r_score,f_score,m_score,segment | 客户 |
| baskets | invoice_no,customer_id,invoice_at,stock_code | 订单×合格商品，customer_id可NULL；最早来源行的时间；同订单已校验同日 |
| interactions | customer_id,stock_code,order_count,last_purchase | 客户×合格商品，订单去重计数 |
| item_pairs | item_i,item_j,co_count | i<j，无向商品对 |
| item_frequency | stock_code,basket_count | 同一截断篮子全集中的单品订单频次，另在manifest记录篮子总数N |
| similarity | stock_code,related_stock_code,similarity,co_count,confidence,lift,rank | 有向相似商品，最多10 |
| recommendations | model_run_id,customer_id,stock_code,rank,score,reason_code,is_fallback | 模型×客户×商品，最多10 |

所有模型split、索引映射与评价产物格式见07，数据库与API名称映射见06/08。相似度、分数浮点以17位有效数字序列化，拒绝NaN/Infinity。

manifest必须记录schema、基线、batch/run/model关联、输入输出路径与SHA-256列表、记录数、分类计数、参数hash、代码commit、开始结束UTC时间、application_id、状态及`_SUCCESS`。只有成功且manifest核验通过的run可被下游选择。文件夹命名之外的字段schema版本也必须校验。

## 6. 数据质量门禁

raw_rows=SALE+RETURN+REJECTED；warnings不参与此加总。转换parse_error的行必须进入REJECTED并保留row_id。无SALE、未知schema、跨来源文件污染、重复row_id、订单身份矛盾、汇总溢出、金额不守恒均为失败，不能发布。REJECTED/raw_rows>5%阻止正常发布；来源确有超过阈值时只能记录分析并经变更流程调整，不能隐藏异常。

拒绝primary_reason固定优先级：PARSE_ERROR→MISSING_INVOICE→INVALID_INVOICE_LENGTH→MISSING_STOCK_CODE→INVALID_STOCK_LENGTH→INVALID_QUANTITY→ZERO_QUANTITY→INVALID_DATETIME→INVALID_PRICE→NONPOSITIVE_PRICE→INVALID_COUNTRY_LENGTH→AMOUNT_OVERFLOW。每行只选首个原因，详细字段错误附在审计报告。warning枚举：MISSING_CUSTOMER_ID、INVALID_CUSTOMER_ID、MISSING_DESCRIPTION、DESCRIPTION_TRUNCATED、PRICE_ROUNDED、CANCEL_POSITIVE_QUANTITY、NEGATIVE_WITHOUT_CANCEL。疑似重复、非商品与大篮子截断是单独质量指标，不混进拒绝率。

quality指标键固定：RAW_ROWS、SALE_ROWS、RETURN_ROWS、REJECTED_ROWS、各拒绝原因名加`_ROWS`、各warning名加`_ROWS`、DUPLICATE_EXCESS_ROWS、NON_MERCHANDISE_SALE_ROWS、TRUNCATED_BASKETS、TRUNCATED_BASKET_ITEMS；大小写原样保存。批次报告与数据库data_quality按batch_id保留，即使尚未发布也可由任务页查询。

合成数据仅置`benchmarks/synthetic`，清单带synthetic=true、生成规则和seed；禁止进入真实评价或正式release。1.0性能扩展规模为原始数据5倍、10倍，须先通过09磁盘预算检查。
