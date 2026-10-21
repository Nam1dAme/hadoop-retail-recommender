# 发布与恢复（W08）

先 `source scripts/dev-env.sh`，以下命令均在项目根目录执行。Hadoop 和 MariaDB 需已启动；凭据使用私有的 `var/secrets/writer.cnf` / `reader.cnf`，不把密码写入命令行。

## 发布

```bash
python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 \
  --db-defaults-file var/secrets/writer.cnf publish \
  --batch-id B20261019194025b4f65854 \
  --experiment-id acea8dc1-3b09-43b8-ac79-09571cfcca53 \
  --model-run-ids 6efb8848-73bf-49ab-8d91-1cfab8d142de fe66f65a-7511-4b85-9e6e-d0cda9281ac2 \
    e6963920-5782-4fa3-9d52-3ecef14b4bc1 e2507953-39db-49a4-9738-9a4d66d6fef3
```

四个 ID 的顺序不限，必须恰好包含 POP、ITEMCF、ALS、NCF 四个同批次、同实验的成功 FULLFIT 模型。代码需先提交，保证发布清单能指向确定的源码。`--dry-run` 是全局参数，放在 `publish` 前，仅输出计划。

发布前重新核对原始 XLSX、上游 manifest、完整购买历史、模型与映射、TEST 封存、指标及 CPU/CUDA 对照。使用 W04 的独立关系验算重新检查业务结果，将九张表投影成带哈希的 TSV。金额用整数便士计算，再用 Decimal 转英镑。

导入先创建 BUILDING 版本，按 2,000 行分批提交。校验完整表内容、引用、金额、购买排除和 Top10 后，在同一短事务中写 READY、current 和审计事件。失败保留 FAILED 记录及现场，旧 current 不变。进程中断后再执行同一命令，会确认旧进程已经结束，为未完成导入创建新 attempt 和新 release；已成功版本只读验证并复用，不能覆盖，也不会自动切回已经回滚掉的版本。

## 查看和校验

先从命令输出取得 `release_id`，或查询：

```bash
mariadb --defaults-file="$PWD/var/secrets/reader.cnf" \
  -e 'SELECT * FROM current_release; SELECT release_id,status,published_at FROM data_release;'
```

固定一个版本查询，避免多条查询期间切换版本：

```sql
SET @r = (SELECT release_id FROM current_release WHERE singleton_id=1);
SELECT SUM(gross_amount) AS gross, SUM(return_amount) AS returns,
       SUM(gross_amount-return_amount) AS net FROM sales_cube WHERE release_id=@r;
SELECT segment,COUNT(*) FROM customer_rfm WHERE release_id=@r GROUP BY segment;
SELECT algorithm,customer_id,stock_code,`rank`,score,reason_code
FROM customer_recommendation WHERE release_id=@r
ORDER BY algorithm,customer_id,`rank` LIMIT 40;
```

`verify` 使用 reader 只读账号，校验 HDFS 依赖及九张数据库表的全部排序后行内容：

```bash
python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 \
  --db-defaults-file var/secrets/reader.cnf verify --release-id "$RELEASE_ID"
```

## 回滚

```bash
python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 \
  --db-defaults-file var/secrets/writer.cnf rollback --release-id "$TARGET_RELEASE_ID" \
  --reason '说明恢复旧版本的原因'
```

目标必须是产物完整的 READY 版本。先完整核验，再原子切换并记录操作者、原因及 from/to。当前版本不删除，仍能用显式 ID 查询。首次发布只有一个 READY 版本时，没有另一个生产版本可供回滚；回滚机制使用两个版本的隔离测试验证。

## 备份和隔离恢复

```bash
python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 \
  --db-defaults-file var/secrets/writer.cnf backup --release-id "$RELEASE_ID" \
  --output-dir /path/to/new-backup-directory

python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 \
  --db-defaults-file var/secrets/reader.cnf restore \
  --backup-dir /path/to/new-backup-directory --output-dir "$PWD/var/new-recovery" \
  --hdfs-root /retail/recovery/new-drill
```

备份目录和恢复目录必须是新路径。备份包含 MariaDB 一致性 dump、原始 XLSX、所有被引用 HDFS 文件、模型及映射、环境锁、配置、冻结文档哈希和 `git archive` 源码。`backup.json` 列出文件大小和 SHA-256；`physical_storage_gate` 单独说明是否为不同的 Windows 物理盘。另一个盘符不等于另一块物理盘，本机副本不能冒充抗磁盘故障备份。

恢复工具校验归档后，把 HDFS 文件上传至全新的 `/retail/recovery/<名称>`，启动独立、仅 socket 监听的 MariaDB，恢复完整 dump。只读测试账号核对同 release 的全部金额、行数和推荐顺序/分数，结束后停止测试数据库；文件和 HDFS 恢复现场保留。清单中的原路径不改写，通过显式恢复根映射读取，原哈希得以保留。生产连接只用于持有共用任务锁，恢复 SQL 仅发送给测试实例，原 HDFS 根不覆盖。

模块：`publication.py` 负责来源核对与 CLI 流程；`release_data.py` 负责九表导出及精确比对；`releases.py` 负责导入、切换及审计；`recovery.py` 负责备份与隔离恢复。API 和展示页面属于 W09/W10。

## 只读报告导出（W11）

`report --experiment-id --release-id --output-dir` 从同一个数据库只读快照导出选定 EVAL 指标、FULLFIT 绑定、实际训练历史、六次 CPU/CUDA 对照、销售总量和运行记录，附 CSV 与 SHA-256 清单。使用 reader 凭据和新输出目录，不重新训练或写入数据库。可选 `--acceptance-file` 附上同版本的最终验收记录；未附记录时完整项目验收状态为 `NOT_EVALUATED`。

启动、更新数据、报告命令、性能实验和五分钟演示见 [交付说明](../docs/delivery.md)。
