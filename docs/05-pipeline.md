# 05 离线数据处理与作业设计

版本 HL-DOC-1.0.0，FROZEN。CLI名称为后续必须实现的接口，本文件不提供执行程序。

## 1. DAG 与责任

执行顺序：导入→标准化→J01清洗→J02订单事实与校验→J03销售立方体→J04商品订单→J05客户RFM→J06购物篮/交互→J07共现→J08相似商品→实验切分→四算法评价→四算法FULLFIT→J09客户推荐→发布。J03—J08虽然部分数据依赖可并行，7.4GiB部署下必须串行重任务。

| 作业 | Mapper的键/值 | Reducer与后续步骤 | 输出与检查 |
|---|---|---|---|
| J01 Clean | 源row_id→规范化字段 | 校验与SALE/RETURN/REJECTED分流；幂等row_id检查 | clean/rejected/warnings；分类Counters守恒 |
| J02 Orders | invoice_no→clean SALE字段 | 验证单日/国家/非空客户唯一，合并金额；身份矛盾失败 | order_fact，订单数与客户数基准 |
| J03 Sales | (date,country,stock_code)→金额/数量 | sum可Combiner，汇总gross/return/quantity | sales_cube；与clean金额相等 |
| J04 ProductOrders | (invoice_no,stock_code)→日期/国家/金额 | 同单商品聚合，保留含非商品编码的正向销售 | product_order_fact；支持商品不同订单数 |
| J05 RFM | customer_id→order_fact | max日期、count订单、sum金额；第二阶段按指标聚合频率分布、前缀累计、小表广播打分 | rfm；不把明细行数当F；评分不能收集全客户到一台机 |
| J06 Baskets | (invoice_no,stock_code)→SALE合格商品 | 同订单商品去重；保留所有商品的购买关联，再按customer/item聚合不同订单数 | baskets、interactions、customer_seen |
| J07 Pairs | invoice_no→去重合格商品 | 确定截断后枚举无向pair；第二阶段pair计数与单品篮子频次 | item_pairs、item_frequency、eligible_basket_count |
| J08 Similarity | 商品对→共现与频次 | 连接频次，过滤co_count<3，计算sim/confidence/lift；每item局部Top10再合并 | similarity，不出现自关联 |
| J09 Recommend | 由模型打分器输出(customer,item)→score | 合并、去已购、排序Top10、POP补足；输出模型与理由 | recommendations；算法详情见07 |

J09的ALS/NCF评分由各框架批处理完成，统一TopK与过滤契约；不强迫神经推理放进Java Reducer。POP、ItemCF聚合和候选生成必须能处理分区文件，不读取全部真实明细到网页进程。

共现频次n_i和总篮子N都基于同一截断后的商品篮子全集（包括单品篮子），训练/验证/服务各自输入窗口独立计算。n_ij为同篮子包含两商品的次数；similarity=n_ij/sqrt(n_i*n_j)，confidence(i→j)=n_ij/n_i，lift=n_ij*N/(n_i*n_j)。item对两方向分别输出，最终按similarity降序、co_count降序、stock_code字节序升序取Top10。

## 2. 聚合正确性

Combiner只处理满足结合交换性的sum/count/min/max，不计算局部平均再平均。订单distinct采用先(invoice,dimension)去重、再dimension聚合；商品订单数量不能直接从sales_cube行数推导。RFM使用订单事实；跨行溢出也要失败。Reducer不能把全部输入迭代器无限收集；只保留必要状态，TopK使用有界结构。篮子上限只作用共现，不能污染销售/RFM/ALS/NCF。

MapReduce JAR编译目标Java8，Hadoop3.3.5依赖provided，第三方库冲突检查。默认reducers=2，极小样例reducers=1；分区数与输入split记录到manifest。小样例可先LocalJobRunner调试，验收必须在YARN完成上述业务MR作业，不能只提供WordCount证据。

## 3. 时间窗口特征

EVAL训练输入仅来源日期<2011-09-01，validation为2011-09-01至2011-10-31，test为2011-11-01至数据结束。窗口在J06/J07前过滤原始clean正向记录，不能先计算全量共现/热门/频次再切训练表。FULLFIT另生成完整历史特征，路径和model_run严格隔离。

每个split的manifest包含cutoff、原始行hash引用、合格用户/商品映射hash；评估不消费FULLFIT路径。RFM用于分析展示，不作为1.0模型输入，防止全量RFM无意泄漏测试时间。

## 4. CLI 契约

统一离线入口名`retailctl`，实现阶段可用Python模块包装，但对外子命令和语义固定如下。所有写入均需要基线版本、配置文件路径；配置只含非敏感参数，凭据读环境变量。公共选项：`--config`必填，`--baseline HL-DOC-1.0.0`必填，`--dry-run`仅验证依赖和计划、不提交/写数据。

| 子命令 | 必填输入 | 结果 | 幂等与失败 |
|---|---|---|---|
| ingest | --source-file、--source-url | batch_id及归档路径 | 相同hash/policy复用batch；冲突拒绝 |
| pipeline | --batch-id | J01—J08按顺序完成 | 复用校验通过的步骤，失败步骤新attempt |
| evaluate | --batch-id、--experiment-id | 四算法EVAL模型、metrics、预选champion | 同ID配置不得变，测试封存后不得调参 |
| train | --batch-id、--experiment-id、--algorithm、--device | FULLFIT模型 | NCF正式设备必须cuda；ALS/POP/ITEMCF设备cpu |
| recommend | --model-run-id | 全量服务Top10与覆盖统计 | 已成功产物复用，不能覆盖 |
| publish | --batch-id、--experiment-id、--model-run-ids（四个） | release_id和原子current切换 | 同release键重试只验证，不重复导入 |
| rollback | --release-id、--reason | 当前指针变更审计 | 目标必须READY，产物完整 |
| benchmark | --experiment-id、--model-run-id | CPU/GPU对照记录 | 使用EVAL NCF所选配置与固定训练数据 |
| verify | --release-id | 校验报告、通过或失败 | 只读，不修改版本 |
| status | --run-id 或 --batch-id | 当前任务与来源证据 | 不按超时自动重启 |
| report | --experiment-id、--release-id、--output-dir | 指标表与答辩报告材料 | 不重新训练或改指标 |

train的返回值为model_run_id，不能根据“最新目录”猜模型。evaluate可内部产生多候选EVAL模型，但只有按07规则选出的每算法结果关联experiment最终结果。

CLI统一退出语义：0成功；2参数/schema/配置错误；3依赖/环境门禁不满足；4运行失败；5锁占用或原任务仍运行；6数据或产物校验失败。标准输出提供ID与机器可读摘要；标准错误记录脱敏错误；执行记录结构由实现阶段落盘，不在文档阶段创建程序文件。

## 5. 状态、提交与恢复

task_run状态：PENDING→RUNNING→SUCCEEDED/FAILED/CANCELLED；终态不可改。重试产生新run_id并关联parent_run_id和attempt；失败产物不作下游输入。batch状态REGISTERED→PROCESSING→SUCCEEDED/FAILED；失败重试可回PROCESSING，但已成功产物不覆盖，逻辑batch仍同一来源。

先创建任务记录，再提交YARN并立即记录application ID。提交与记录之间崩溃要按应用名中的run_id查询YARN核对，不能直接重复提交。写入新run目录，作业成功后校验、manifest完成，最后登记SUCCEEDED。HDFS存在`_SUCCESS`但数据库未更新时，恢复程序重新验证hash和application终态后补录，不直接相信目录名。

ingest的raw与normalized虽按batch而非run命名，也必须先写同父目录带run_id的临时目录，完成hash/行数核验后以HDFS rename发布到固定路径。目标已存在时只核验并复用，禁止覆盖；数据库记录失败时按source hash和临时run清单恢复。

禁止自动format HDFS、删除原始数据、kill未知进程或清空数据库作为重试步骤。失败临时结果保留7天供调查，清理须遵守10中的引用和存活检查。
