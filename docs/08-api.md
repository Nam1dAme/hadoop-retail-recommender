# 08 API 与应用服务契约

版本 HL-DOC-1.0.0，API `/api/v1`，FROZEN。协议HTTP/JSON UTF-8；FastAPI只读，Streamlit仅通过该接口取数。实现时可生成OpenAPI，但本文是协议依据，不在本轮生成程序或API定义代码。

## 1. 公共请求和响应

GET无请求body。业务端点可带release_id，省略时服务在请求开始解析current一次；UI必须显式携带固定release。release为READY可查，即使不是current；BUILDING不允许查，RETIRED返回410。标识值需URL编码，customer_id/stock_code为1..32字符且不可含路径分隔符或控制字符，其他ID按00校验。

每次响应带X-Request-ID（服务生成UUID）、Content-Type application/json，业务成功封装顶层字段：data（本端点结构）、meta（request_id、schema_version=1.0、release_id、batch_id）；无release上下文的控制查询后两者可NULL。列表data为object，包含items数组、page、page_size、total、has_next；空列表total=0且items为空，不能返回404。

错误封装error.code、error.message、error.details（参数名到原因，可空），以及meta.request_id；不得返回堆栈/凭据/SQL。未知query参数按422拒绝，框架默认异常必须适配本封装。额外响应字段只允许schema兼容扩展。

金额均为两位小数十进制字符串，计数整数，分数/比率有限JSON number，未知值NULL。source_date与审计时间分别为ISO日期与带Z UTC；所有业务响应携带currency=GBP的金额对象。bool为JSON布尔，不以0/1字符串替代。

分页page默认1且≥1，page_size默认20且1..100，列表page超过末页返回空。除明确说明，日期参数start_date/end_date省略则使用release全范围，均包含边界，start>end返回422；超出数据时间范围合法，按交集返回。country精确匹配，未知国家值产生空数据。排序枚举固定，不接受任意列名或SQL片段。

## 2. 端点清单

| GET路径 | 参数（公共release/page除外） | data内容与排序 |
|---|---|---|
| /health/live | 无 | status=UP、service_version；仅进程存活，不读数据库 |
| /health/ready | 无 | status=READY或NOT_READY；检查数据库连接与迁移匹配；无release仍可READY |
| /releases/current | 无 | release_id,batch_id,experiment_id,champion_algorithm,source_start,source_end,currency,published_at,baseline_version |
| /releases | page,page_size | items同current字段加status，只列READY；published_at降序/release_id升序 |
| /summary | start_date,end_date,country | currency,gross_amount,return_amount,net_amount,order_count,customer_count,average_order_value,sale_quantity,return_quantity |
| /sales/trend | start_date,end_date,country,granularity=DAY或MONTH（默认DAY） | points数组：period_start,gross_amount,return_amount,net_amount,order_count；currency；日期升序，无交易日期补0，查询与release日期交集超过400个DAY点返回422 |
| /sales/countries | start_date,end_date,country，sort=NET_DESC或GROSS_DESC（默认NET_DESC） | items：country,currency,gross_amount,return_amount,net_amount,order_count；金额降序/country字节序升序 |
| /products | start_date,end_date,country,stock_code可选，sort=GROSS_DESC或QUANTITY_DESC（默认GROSS_DESC） | items：stock_code,description,currency,gross_amount,return_amount,net_amount,sale_quantity,return_quantity,order_count,is_merchandise；主排序降序/stock_code升序 |
| /customers | segment可选，customer_id可选 | items：customer_id,last_purchase,as_of_date,recency_days,frequency,monetary,currency,r_score,f_score,m_score,segment；customer_id升序 |
| /customers/segments | 无 | as_of_date,items：segment,customer_count；固定CORE,AT_RISK,NEW,POTENTIAL,OTHER顺序，缺类补0 |
| /products/{stock_code}/similar | k默认10，范围1..10 | stock_code,description,items：related_stock_code,description,rank,similarity,co_count,confidence,lift；rank升序 |
| /customers/{customer_id}/recommendations | algorithm可选取四算法，省略用champion；k默认10，1..10 | customer_id,known_customer,algorithm,model_run_id,eligible_candidates,returned_count,fallback_used,items；推荐项字段见下 |
| /batches | page,page_size | batch_id,status,source_name,source_sha256,policy_version,raw_rows,created_at,updated_at；created_at降序/batch_id升序，无release要求 |
| /batches/{batch_id}/quality | 无 | batch_id,status,metrics数组(metric_name,metric_value,description),report_available；成功或失败批次都可查询可用报告 |
| /runs | batch_id可选，status可选，page,page_size | items：run_id,batch_id,task_name,attempt,status,started_at,finished_at,application_id,error_code,error_message；created_at降序/run_id升序 |
| /runs/{run_id} | 无 | 上述字段加parent_run_id,heartbeat_at,output_manifest_path；日志仅返回是否可用，不暴露本机任意文件读接口 |
| /experiments | batch_id可选，page,page_size | experiment_id,batch_id,status,split_policy_version,champion_algorithm,created_at,finished_at；created_at降序/experiment_id升序 |
| /experiments/{experiment_id}/metrics | split=VALIDATION或TEST（默认TEST），cohort=WARM或ALL_ELIGIBLE（默认WARM） | experiment_id,split,cohort,k=10,candidate_policy_version,items：algorithm,model_run_id,user_count,catalog_count,window_user_count,precision_at_k,recall_at_k,ndcg_at_k,coverage,cold_user_count,cold_item_count,cold_interaction_count,excluded_user_count；四算法固定顺序 |
| /models/{model_run_id}/epochs | 无 | model_run_id,purpose,algorithm,items：epoch,train_loss,validation_ndcg,elapsed_seconds,peak_gpu_mib；epoch升序；非NCF返回空数组 |
| /experiments/{experiment_id}/benchmarks | 无 | experiment_id,items：benchmark_id,model_run_id,device,repeat_index,batch_size,epochs,warmup_steps,train_seconds,samples_per_second,peak_memory_mib,hardware,torch_version,cuda_runtime,driver_version；summary见下 |

无release控制端点（batches/runs/experiments/models/health）拒绝release_id参数，以资源自身关系作为上下文，不误用current。可分页端点使用列表封装；非分页的points/items数组仍位于各data对象内，不添加虚假page字段。

推荐项：stock_code、description可空、rank、score、reason_code、reason_text、is_fallback。model_run_id始终为请求algorithm的FULLFIT模型；fallback项理由为FALLBACK_POPULAR，不能把model_run_id换成POP导致整条响应算法含糊。eligible_candidates=|C_full−H_full|；known_customer按customer_rfm判断。未知客户返回200、known_customer=false、热门兜底；已存在但全目录已购返回200和空数组。fallback_used为实际返回项存在fallback，未知客户无候选时为false并returned_count=0。POP自身使用POPULAR，不算fallback。

benchmark summary：cpu_median_seconds、gpu_median_seconds、speedup、complete_repeats（每端是否各3次）、comparison_valid。不足各3次或样本/config hash不同，summary数值为NULL，comparison_valid=false；不得给错误加速比。

## 3. 错误字典

| HTTP | code | 情况 |
|---|---|---|
| 404 | NO_ACTIVE_RELEASE | 首次无current，业务查询和current端点 |
| 404 | RESOURCE_NOT_FOUND | 不存在的release/batch/run/model/experiment/商品；未知客户推荐例外 |
| 409 | RELEASE_NOT_READY | 请求BUILDING/FAILED release |
| 410 | RELEASE_RETIRED | 指定版本已退休，提示用户切版本 |
| 409 | RESULT_NOT_READY | 实验尚未封存指定指标或GPU对照未完整；任务页仍能查状态 |
| 422 | INVALID_PARAMETER | ID/日期/分页/k/枚举/未知参数不合法 |
| 503 | DEPENDENCY_UNAVAILABLE | 数据库不可达或迁移不匹配，ready端点同样503 |
| 500 | INTERNAL_ERROR | 未分类内部错误，附request_id，日志脱敏 |

API请求超时5秒，服务SQL超时2秒；错误不重试写操作（本API无写入）。短暂GET失败UI允许手动重试。列表total为过滤后的条目数，分页相同值有稳定ID排序，READY数据不可变因此不会重复翻页。

## 4. 安全和兼容

仅监听127.0.0.1:8000；不启用公网CORS通配，不含认证端点。服务没有提交作业/重试/发布/上传/删除接口。SQL参数绑定、过滤枚举白名单、数据库reader最小权限，防路径遍历和SQL注入。将来如需对外部署必须另做认证与权限设计，不可直接把监听改0.0.0.0视为上线。
