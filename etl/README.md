# W03 原始导入与标准化

依据冻结基线 HL-DOC-1.0.0 的 04/05/06/11。输入是唯一真实数据源 [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail) 的原始 XLSX（Chen, D., 2015，CC BY 4.0，doi:10.24432/C5BW33）。下载时间、来源、文件名、字节数、SHA-256、工作表及源行数保存在来源审计 JSON，原始文件不改写。

## 使用

先按 config/development.toml 启动 HDFS，并完成 W02 schema/角色配置。导入使用 writer，迁移账号不供任务或应用共享；凭据文件只从显式路径读取。

```bash
source scripts/dev-env.sh
uv run --locked python -m orchestration.cli \
  --config config/development.toml --baseline HL-DOC-1.0.0 \
  --db-defaults-file var/secrets/writer.cnf --dry-run \
  ingest --source-file 'var/data/source/Online Retail.xlsx' \
  --source-url 'https://archive.ics.uci.edu/static/public/352/online%2Bretail.zip' \
  --source-metadata var/data/source/source.json
```

移除 `--dry-run` 执行真实导入。重复执行会重新核验已归档内容，返回原 batch_id/run_id 和 reused=true，不追加批次或覆盖文件。source-file/source-metadata 必须解析到显式 work_dir 内；原数据与报告不进入 Git。真实导入只接受指向 UCI 352 的 HTTPS 来源和与源文件实际 hash/字节数匹配的来源元数据，记录真实观测的下载 UTC 时间，不能用运行时间冒充未知下载时间。

source.json 至少包含下面的字段，值须来自实际下载记录；不要把示意值用于真实导入：

```json
{
  "source_url": "https://archive.ics.uci.edu/static/public/352/online%2Bretail.zip",
  "source_sha256": "实际 XLSX 的 64 位小写 SHA-256",
  "source_bytes": 23715344,
  "downloaded_at": "实际下载完成的 UTC ISO 8601 时间，以 Z 结尾"
}
```

源工作簿有多个工作表时必须 `--sheet` 明确选择。Excel 原生日期原样保留 source_local，不转北京时间；文本日期只有显式指定 `--source-date-format` 时才解析，例如 `%d/%m/%Y %H:%M`。同一文件及 policy 的已有批次不能用不同标准化配置悄悄重算；不一致返回校验失败，已有目录保持不变。

## 查看已导入数据

交易明细保存在 HDFS；MariaDB 当前只保存来源、批次、任务和质量指标，尚无销售聚合或推荐结果。以下使用已验收的实际批次：

```bash
cd /home/hadoop/hadoop-lab
source scripts/dev-env.sh
retail_data='/retail/v1/normalized/batch_id=B20261019194025b4f65854'
hdfs dfs -ls "$retail_data"
hdfs dfs -head "$retail_data/normalized.tsv"
hdfs dfs -cat "$retail_data/quality.json"
```

`-head` 预览开头 1 KiB，可能在行中间截断。normalized.tsv 无表头，列依次为 row_id、source_row_number、invoice_no、stock_code、description、quantity、invoice_at、unit_price_pence、customer_id、country、parse_error。`\N` 表示 NULL，单价单位为便士，除以 100 才是 GBP。文件保留全部来源记录，包含拒绝行；分类和拒绝原因须结合质量报告及 rejected-audit.jsonl 查看。

需要本地完整查看时，可下载到未占用的目标路径；原始 Excel 位于 `var/data/source/Online Retail.xlsx`。

```bash
mkdir -p var/view
hdfs dfs -get "$retail_data/normalized.tsv" var/view/normalized.tsv
mariadb --defaults-file="$PWD/var/secrets/reader.cnf"
```

登录后的只读查询使用显式授权列，避免对受列权限限制的表执行 `SELECT *`：

```sql
SELECT batch_id, status, raw_rows FROM data_batch;
SELECT run_id, task_name, status, attempt FROM task_run;
SELECT metric_name, metric_value FROM data_quality
WHERE batch_id = 'B20261019194025b4f65854'
ORDER BY metric_name;
```

若连接 `localhost:9000` 被拒绝，先用 `jps -l` 和 `ss -ltn '( sport = :9000 )'` 检查 NameNode 及监听端口。`source scripts/dev-env.sh` 只加载环境变量；确认整个本地集群已停止后，用 `bash scripts/start-hadoop-host.sh` 启动。已有部分守护进程时先检查 `var/logs/hadoop/`，不要重复启动；不执行 NameNode format，不需要重新导入。实际故障与恢复证据见 [2026-10-20 运维记录](../docs/execution/2026-10-20-hadoop-restart.md)。

## 模块与产物

- normalize.py：字段规范化、精确 Decimal HALF_UP 转便士、int64 范围、SALE/RETURN/REJECTED 预检和固定原因/warning。
- tsv.py：UTF-8、无 BOM/表头、LF、反斜线转义，NULL 与真实 `\N` 字符串可区分。
- workbook.py：流式 Excel、固定八列表头、公式/Excel 错误审计、内部空行拒绝、末尾空行排除；不执行公式或宏。
- audit.py：磁盘 SQLite 索引统计疑似重复和订单身份，保留重复交易，输出分类计数、warning、拒绝原值及质量门禁；不把全体明细收集到内存。
- storage.py：在同父 run 临时目录上传、逐文件 hash/字节校验、最后 HDFS rename；拒绝覆盖固定目录。
- ingest.py：批次幂等、任务控制、来源归档、manifest 生成与恢复。源代码未提交时记录 dirty 和具体源码 hash，不将旧 HEAD 冒充新实现。
- orchestration/runtime.py：PID 加进程启动时间与连接命名锁检查，长任务持续写十秒心跳，连接失效停止继续操作。

正式 HDFS 路径：

```text
/retail/v1/raw/batch_id=<ID>/source.xlsx
/retail/v1/raw/batch_id=<ID>/source.json
/retail/v1/normalized/batch_id=<ID>/normalized.tsv
/retail/v1/normalized/batch_id=<ID>/warnings.tsv
/retail/v1/normalized/batch_id=<ID>/rejected-audit.jsonl
/retail/v1/normalized/batch_id=<ID>/order-conflicts.tsv
/retail/v1/normalized/batch_id=<ID>/quality.json
/retail/v1/normalized/batch_id=<ID>/config.json
/retail/v1/normalized/batch_id=<ID>/environment.lock
/retail/v1/normalized/batch_id=<ID>/source-code.zip
/retail/v1/normalized/batch_id=<ID>/manifest.json
/retail/v1/normalized/batch_id=<ID>/_SUCCESS
```

manifest 主输出 normalized.tsv 一行对应一条来源记录，拒绝行保留 row_id/source_row_number 和可解析字段，原值及全部字段错误放 rejected-audit.jsonl，主原因按冻结优先级选取；不静默删行。warnings/审计/配置是独立 artifacts，不计入主输出记录总数。XLSX 输入显式声明 format=XLSX 与 sheet，其 rows 根据实际工作表源行核验，不能用 ZIP 二进制换行数代替 Excel 行数。所有输入、输出及附件均核对 SHA-256；文件 schema 和关联 ID 也要一致。

## 状态、恢复与范围

成功导入令 ingest task=SUCCEEDED，batch 保持 PROCESSING，等待 W04 整条业务 pipeline 完成。预检质量失败会保存来源、标准化文件、报告与计数，batch=FAILED、CLI 返回 6；不能仅因文件已写入就允许正式发布。没有 SALE、拒绝率超过 5%、订单身份冲突和汇总 int64 溢出会失败。

同文件失败重试复用 batch_id，产生新 run_id/attempt，保留 parent；尚活的 PID/启动时间或匹配 run_id 的 Hadoop Java 句柄使重跑返回 5，心跳过期不等于失败。输出已原子发布但 DB 未登记时，确认原 owner 不活后，重新核验原 manifest 和所有文件再补录原任务，不创建第二套业务数据。成功产物缺失、hash 不符或关联漂移会报错，不通过覆盖或删除“修复”。临时失败目录至少保留 7 天，按 10 的引用/存活规则后续清理。

W03 的金额/分类统计是输入质量预检，未写 sales_cube、order_fact、RFM 或推荐结果；W04 仍须在真实 YARN 上独立执行 J01—J08 和最终业务校验。TRUNCATED_BASKETS/TRUNCATED_BASKET_ITEMS 由 W04 共现阶段计算，本轮不伪造这些指标。没有触发模型训练、服务发布或页面。

测试使用独立真实 MariaDB 和隔离文件根做事务/故障注入；synthetic=true 不允许写正式库或正式 HDFS 根，FileStorage 不允许作为真实 HDFS 的替代。真实 UCI 导入和 HDFS hash/manifest/幂等验收另有执行证据。

新增尝试会把执行源码打包到 source-code.zip 并在 manifest 中记录其 hash，期间检测源码是否改变。2026-10-20 首次真实执行早于该补充功能，其精确源码已按原 manifest 校验后归档，最终执行记录另保存可复原该执行版本的补丁；正式 HDFS 既有产物保持不可变。预检复核后补充了标准化 NULL 的重复比较边界、全部字段错误记录与运行失败退出码，不改变本次真实数据的分类与统计；全部真实拒绝行为 NONPOSITIVE_PRICE，原审计主原因已完整解释该文件的拒绝。

独立验收（不写业务数据库）：

```bash
uv run --locked python scripts/verify-w03.py \
  --config config/development.toml --db-defaults-file var/secrets/writer.cnf \
  --batch-id '<实际 batch_id>' --output var/w03/verification.json
```

该命令逐个核对 HDFS 文件 hash/字节、DB/manifest/来源关联、参数 hash 和质量计数，并独立扫描 TSV 字段、源行号、row_id 及重复统计。返回 0 / status=PASS 才表示核验通过。首次导入、复用导入已经分别完整核对 XLSX 工作表实际源行数；独立扫描再次检查原文件 SHA 和标准化行追溯。CLI 退出码为 0 成功、2 参数/配置错误、3 依赖不满足、4 运行失败、5 锁或原任务存活、6 数据/产物校验失败。
