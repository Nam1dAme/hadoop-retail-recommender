# 14 需求、设计、实施与验收追踪

版本 HL-DOC-1.0.0，FROZEN。右侧测试表示未来必须提供的证据，当前不是PASS声明。

| 需求 | 设计依据 | 实施任务 | 验收 |
|---|---|---|---|
| FR-01 导入/原始归档/幂等 | 04来源与文件协议，05 ingest，06 data_batch | W02/W03 | T-01 |
| FR-02 清洗/质量 | 04分类与门禁，05 J01/J02，06 data_quality | W03/W04 | T-02/T-03 |
| FR-03 销售分析 | 04金额，05 J03/J04，06业务表，08 summary/sales/products | W04/W09/W10 | T-04/T-15/T-16 |
| FR-04 RFM | 04 RFM规则，05 J05，06 customer_rfm，02客户页 | W04/W09/W10 | T-05/T-16 |
| FR-05 商品关联 | 05 J07/J08，06 item_similarity，07 ITEMCF | W04/W06 | T-06/T-11 |
| FR-06 四算法客户推荐 | 07四算法/FULLFIT，06 release_model/recommendation，08推荐接口 | W05/W06/W07 | T-08/T-09/T-11 |
| FR-07 NCF GPU与CPU对照 | 07 NCF/benchmark，09 GPU门禁，06 epoch/benchmark | W01/W06/W07 | T-12/T-13 |
| FR-08 时间评价与指标 | 07 split/cohort/指标，06 model_metric，08实验API | W05/W06 | T-08/T-10 |
| FR-09 原子release | 03协议，06发布表，10发布/回滚 | W02/W08 | T-14/T-19 |
| FR-10 七页/API | 02页面，08全部HTTP契约，03版本固定 | W09/W10 | T-15/T-16 |
| FR-11 状态/错误/任务 | 05 CLI/状态，06 task_run，10观测 | W02/W04/W08 | T-07/T-14/T-20 |
| FR-12 部署/恢复/报告 | 09环境，10备份，11演示，15来源 | W01/W11 | T-18/T-19/T-20 |
| NF-01 正确一致 | 04口径，06约束，03发布，08响应 | W03/W04/W08 | T-02/T-04/T-05/T-10/T-14 |
| NF-02 查询性能 | 01阈值，06索引，10压测方法 | W09/W11 | T-18 |
| NF-03 资源适配 | 09预算，03重任务锁，07分块 | W01/W07/W11 | T-12/T-13/T-18 |
| NF-04 可复现 | 00标识，04manifest，07种子/映射，09依赖锁 | W01/W05/W06 | T-01/T-08/T-09/T-13 |
| NF-05 可运维 | 05恢复，06事务，10备份/清理 | W02/W08/W11 | T-14/T-19 |
| NF-06 本机安全 | 08只读/输入，09配置，10权限 | W01/W09 | T-15/T-17 |
| NF-07 可观测 | 05任务，06控制表，10日志 | W02/W04/W06/W08 | T-07/T-12/T-14/T-20 |
| NF-08 文档可审计 | 00治理、12 Agent规则、本矩阵、16审查与hash | W01—W11 | T-20及G-DOC |

## 数据流追踪

| 产物 | 生产者 | 消费者 | 关联键 |
|---|---|---|---|
| source/normalized | ingest | J01 | source_sha256,batch_id,source_row_number |
| clean/order/cube/RFM | J01—J05 | 特征/发布 | batch_id,run_id,row_id/invoice/customer |
| baskets/interactions | J06 | 共现/四算法 | batch_id,窗口,customer/item |
| pairs/similarity | J07/J08 | ITEMCF/商品页 | batch_id,窗口,item pair |
| EVAL模型/metrics | evaluate | 报告/champion/FULLFIT参数 | experiment_id,model_run_id,split hash |
| FULLFIT模型/recs | train/recommend | publish/API | model_run_id,mapping hash |
| release | publish | API/UI/rollback | release_id,四model_run_id,batch_id |
| CPU/GPU结果 | benchmark | G-GPU/实验页 | experiment_id,EVAL model_run_id,config/dataset hash |

如果实现增加字段、算法或任务，须先判断是否改变冻结语义；不是通过矩阵新增一行就可以绕过00变更流程。
