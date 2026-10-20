# W04 Hadoop 业务计算

依据冻结基线 HL-DOC-1.0.0 的 04/05/11，实现 Java 8 / Hadoop 3.3.5 的 J01—J08。运行入口为 `retailctl pipeline`，重任务串行，默认两个 Reducer；RFM 评分为广播频率表的 Mapper 作业。Hadoop 依赖为 provided，业务 JAR 不打包 Hadoop 运行时。

## 构建与执行

先启动 HDFS、YARN 和 MariaDB，完成 W03 真实导入。环境加载脚本不启动服务。以项目目录为当前目录：

```bash
source scripts/dev-env.sh
mvn -Dmaven.repo.local="$PWD/var/cache/maven" -f mapreduce/pom.xml package

uv run --locked python -m orchestration.cli \
  --config config/development.toml --baseline HL-DOC-1.0.0 \
  --db-defaults-file var/secrets/writer.cnf --dry-run \
  pipeline --batch-id B20261019194025b4f65854
```

移除 `--dry-run` 执行。可用 `--reducers 1` 运行极小样例，正式默认 2。相同参数重跑核验并复用成功阶段；成功阶段的参数、输入或 JAR hash 不同则拒绝覆盖。`--dry-run` 验证已导入数据及依赖，不提交 YARN 应用或新建任务。

## 作业与产物

| 物理阶段 / task_name | 输入与输出 | 实现要点 |
|---|---|---|
| J01 | normalized、来源 warnings → clean、rejected、warnings | 校验来源 row_id、拒绝重复 row_id，精确 int64 分类与金额，保留疑似重复交易 |
| J02 | clean → order_fact | 正向订单汇总，跨日/国家/有效客户冲突失败，NULL 与唯一非空客户允许共存 |
| J03 | clean → sales_cube | 日期×国家×商品的销售/退货金额及数量分别汇总 |
| J04 | clean → product_order_fact | 正向订单×商品聚合，包含非商品编码的合法销售 |
| J05base | order_fact → rfm_base | 按有效客户汇总末次日期、不同订单数、金额 |
| J05distribution | rfm_base → rfm_scores | 按 R/F/M 值分组计频并前缀累计，同值同分，不收集全体客户评分 |
| J05 | rfm_base、rfm_scores → rfm | 广播频率评分表，基准日为全体 SALE 最大日期+1，包括匿名销售 |
| J06baskets | clean、order_fact → baskets | Secondary sort 流式去重订单商品，采用订单身份和最早来源行的时间 |
| J06interactions | clean → interactions、customer_seen | 只取有效客户实际 SALE，按客户商品去重订单；不受共现截断影响 |
| J07baskets | baskets → occurrences | 每单仅保留 UTF-8 字节序最小 100 种商品供共现，计截断指标，单品篮子参与总篮子数 |
| J07 | occurrences → item_pairs、item_frequency | 汇总无向商品对及同一截断篮子全集的单品频次 |
| J08 | item_pairs、item_frequency → similarity | 共现至少 3 次，计算相似度/置信度/lift，使用有界堆生成每商品 Top10 |
| pipeline | 上述结果 → pipeline_summary、quality.json | Python/SQLite 独立验算业务结果，成功后冻结最终质量指标并标记批次成功 |

前十二个阶段分别对应真实 YARN 应用，最后的 pipeline 为独立验收控制任务，不冒充 YARN 作业。rfm_base、rfm_scores、occurrences、customer_seen、pipeline_summary 为内部 schema；对外结果 schema 与冻结 04 完全一致。频率小表广播上限 32 MiB，超限失败，避免无界收集。

## 任务与目录

每个物理阶段先建立 `task_run`，以 `retail-<task_name>-<run_id>` 提交应用并立即登记 application ID。DB 命名锁覆盖整条流水线，十秒更新任务心跳。进程死活通过 PID 加启动时间核对；提交与登记间崩溃通过精确应用名查 YARN，观察失败或应用仍活时拒绝重跑。

J01 固定在 `/retail/v1/clean/batch_id=<ID>/run_id=<UUID>/`；其余依阶段放 features/ 或 results/ 下同样的 batch/run 目录。应用先写同父 `.run_id=<UUID>.tmp`；应用成功、schema/hash/行数与来源核验完毕后写 manifest 并 rename 到最终目录。每个 bundle 包含分数据集 TSV、`_SUCCESS`、manifest.json、config.json、environment.lock、business.jar、source-code.zip、execution.json。最终 pipeline 另含 quality.json。

manifest 的 datasets 逐项声明 schema、分区文件 hash/行数；execution 保存应用状态、实际 Map/Reduce 计数、输入 split 数、CPU/GC 和 YARN memory/vcore seconds。任务记录本地 driver.log 位置，日志和审计 SQLite 在 var/pipeline 下，不入 Git。空数据集保留空 part 文件及 schema，阈值空态不伪造商品对。

成功输出不可覆盖。确认旧进程不活且原应用终态后，最终 bundle 已发布但 DB 未登记则校验并补录同一 run；仅有临时输出则保留调查，原任务 FAILED，新 attempt 关联 parent。失败临时数据至少保留七天，不自动删除或格式化 HDFS。失锁后停止本进程提交并请求终止本次精确关联的应用；未确认终态的记录保留，下一次仍需核实。

## 验收

```bash
uv run --locked python -m pytest -q --tb=short
uv run --locked python scripts/verify-w04.py \
  --config config/development.toml --db-defaults-file var/secrets/writer.cnf \
  --batch-id B20261019194025b4f65854 --output var/w04/verification.json
```

LocalJobRunner 测试覆盖冻结手算样例、销售筛选、RFM 同分、阈值空态、超过 100 种商品的截断、相似度公式、订单冲突和聚合溢出；故障测试使用隔离真实 MariaDB，不作为正式 YARN 证据。独立验算以磁盘 SQLite 重算每个销售分组、订单/商品订单、客户 RFM、完整交互、截断篮子、商品对及 Top10，不只比较总行数。

正式验收须核对真实 UCI 批次的所有阶段及 YARN 应用终态、完整 manifest 和独立结果；`verify-w04.py` 返回 0 且 status=PASS 才通过。W04 不训练模型、不发布 release、不实现 API 或页面；W05 需按时间窗口从 clean 重新构建特征，不得把全历史共现结果用于 EVAL 训练。
