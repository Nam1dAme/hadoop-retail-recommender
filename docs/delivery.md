# 运行、验收与五分钟演示

适用基线 HL-DOC-1.0.0。冻结规范保持原样；实际结果与未通过条件以 `docs/execution/` 的最新 W11 记录为准。

2026-10-25 用户明确要求“不要管外部备份问题，继续其他部分”。本轮完成软件、本机运行与演示材料的交付，并提供独立的 [GPU 优化实验方案](gpu-optimization-plan.md)；外部存储不再作为本轮阻塞项。历史正式门禁状态保留，新的检查与交付范围见 [本轮记录](execution/2026-10-25-handoff.md)。

## 1. 启动现有系统

在普通 WSL 终端，以 hadoop 用户运行。以下命令不清库、不格式化 HDFS、不修改驱动。

```bash
cd /home/hadoop/hadoop-lab
source scripts/dev-env.sh
uv sync --locked
jps -l
ss -ltn | rg ':(3306|8000|8501|9000|8088) '
```

MariaDB 服务需已启动。数据库登录使用对应角色的私有文件，例如：

```bash
mariadb --defaults-file="$PWD/var/secrets/reader.cnf" \
  -e 'SELECT * FROM current_release; SELECT release_id,status FROM data_release;'
```

首次配置角色与迁移见 `migrations/README.md`。凭据文件必须由当前用户拥有、权限 0600，库名与 `--config` 一致；密码不写入命令行或 Git。日常 API、查询和报告使用 reader；离线导入、训练、发布和备份使用 writer。

Hadoop 所有守护进程均停止时，执行 `bash scripts/start-hadoop-host.sh`。该脚本先检查旧元数据、备份 NameNode 元数据，再启动 HDFS/YARN 并执行环境 WordCount。若部分守护进程仍在运行，先查看对应日志与端口，仅启动缺少的进程，不重复运行全套启动脚本。

```bash
hdfs dfsadmin -report
yarn node -list
```

应至少有一个 Live DataNode 和一个 RUNNING NodeManager。业务处理的验收证据来自 W04 的 J01—J08；WordCount 只证明环境可用。

另外两个终端分别启动 API 和页面；已监听时先检查健康接口，避免重复启动：

```bash
source scripts/dev-env.sh
uv run --locked python -m service --config config/development.toml \
  --db-defaults-file var/secrets/reader.cnf
```

```bash
source scripts/dev-env.sh
uv run --locked python -m app --config config/development.toml
```

Windows 浏览器建议访问 <http://127.0.0.1:8501>。API 健康地址为 <http://127.0.0.1:8000/api/v1/health/ready>。页面进程会清除继承的数据库凭据，仅通过本机只读 HTTP 接口查询。

本机 Windows 复查中，IPv4 UI/API 均返回200；IPv6 `::1` 的 WSL 转发超时，部分 PowerShell/.NET 客户端使用 `localhost` 时未及时回退IPv4。因此检查命令建议明确使用 `127.0.0.1`，浏览器采用正常IPv4回退时 `localhost` 也可访问。该差异不需要将服务改成全网监听：

```powershell
curl.exe --noproxy "*" http://127.0.0.1:8501/_stcore/health
curl.exe --noproxy "*" http://127.0.0.1:8000/api/v1/health/ready
```

## 2. 五分钟演示路线

| 时间 | 操作与讲解 | 可复核证据 |
|---|---|---|
| 0:00—0:40 | 打开“数据质量与任务”，展示来源、批次、分类计数、拒绝与 warning；说明退货保留、重复疑似不擅自删除 | 原始文件 SHA-256、质量守恒、UCI 出处 |
| 0:40—1:10 | 查看一条成功的 J01—J08 任务详情，展示 application ID 和终态；需要时打开 YARN 8088 | 真实业务 YARN 历史与不可变 manifest；RM 重启后的历史以归档证据为准 |
| 1:10—2:00 | 在“总览/销售分析”筛选 France 和日期，再看客户分层；解释 RFM 属于全批次快照 | 金额、订单 distinct、筛选结果、固定 release |
| 2:00—2:50 | 在“客户推荐”选择客户并切换四算法，再输入未知客户看热门兜底；在“商品关联”查看共现次数 | Top10、去已购、理由、兜底标识、关联公式 |
| 2:50—3:40 | 打开“模型实验”，展示四算法 TEST 指标、NCF 训练曲线、真实 CPU/CUDA 对照 | EVAL/FULLFIT 分开；GPU 当前较慢照实解释 |
| 3:40—4:20 | 展示页面固定的 release、CLI 只读核验和报告导出；解释新数据需经过离线处理与发布 | 版本清单、四模型绑定、九表行摘要 |
| 4:20—5:00 | 展示隔离恢复与两版本回滚测试、W11 性能结果及最终门禁表 | 精确金额/推荐/哈希一致；外部备份条件单列 |

页面中的历史训练曲线和耗时是实际运行的归档证据。重复执行成功的 `train`、`benchmark` 命令会校验并复用已有产物，不代表此刻又进行一次 GPU 训练。需要现场观察计算时，可执行新的隔离实验并同步观察 `nvidia-smi`；这不作为五分钟演示的必需步骤。

当前正式库仅一个 READY release 时，生产环境没有另一个版本可供回滚。回滚正确性由隔离数据库的双版本测试验证；演示时展示实际测试结果和恢复记录，避免为展示改动生产 current。

## 3. 更新数据的流程

本系统展示已发布快照的动态查询结果，不接收实时订单流。刷新页面重新查询同一 release；筛选不会触发训练。网页不提供上传、训练、发布按钮。

新 XLSX 由 CLI `ingest` 导入，依次完成 `pipeline`、`prepare-experiment`、`evaluate`、四算法 `train`、四算法 `recommend`、`benchmark`、`publish` 和 `verify`，最后在页面明确切换当前版本。输入必须覆盖冻结时间窗口，否则需要另行修改评价协议，不能直接套用旧 split。相同文件与相同策略会复用原批次。

具体参数见 `etl/README.md`、`mapreduce/README.md`、`ml/README.md` 和 `orchestration/README.md`。重任务必须串行，失败保留旧发布版本。

## 4. 导出报告

```bash
source scripts/dev-env.sh
uv run --locked python -m orchestration.cli \
  --config config/development.toml --baseline HL-DOC-1.0.0 \
  --db-defaults-file var/secrets/reader.cnf report \
  --experiment-id acea8dc1-3b09-43b8-ac79-09571cfcca53 \
  --release-id R20261021104550ac6985a5 \
  --output-dir var/my-delivery-report
```

输出目录必须尚不存在。输出 `report.md`、完整 `report.json`、`metrics.csv`、`benchmarks.csv` 和文件哈希清单。命令只读数据库，不训练、不重新评价、不修改发布版本。选定 EVAL 指标、FULLFIT 推荐、真实/合成用途明确分开；配置不一致或不足六次的 CPU/GPU 对照不计算加速比。

默认 `project_acceptance=NOT_EVALUATED`，防止把“成功导出”误认为“整项目验收通过”。可用 `--acceptance-file` 附上 `verify-w11.py` 生成、ID 匹配的验收记录；报告保留该记录的哈希与未通过项。

## 5. 复核 W11

先运行接口压测，再运行 MapReduce 扩展实验；两者都持有项目重任务锁，不能与训练或其他重任务并行。

```bash
uv run --locked python scripts/benchmark-api.py \
  --config config/development.toml --db-defaults-file var/secrets/reader.cnf \
  --output-dir var/new-api-performance

uv run --locked python scripts/benchmark-mr.py \
  --config config/development.toml --db-defaults-file var/secrets/writer.cnf \
  --batch-id B20261019194025b4f65854 --output-dir var/new-mr-performance
```

接口压测对 13 类销售/客户/关联/四算法推荐查询，分别预热 10 次，再以 5 并发执行 100 次，记录全部请求、P50/P95、错误率和代表性查询计划；失败请求也计入耗时统计。全库 reader 可见数据在前后保持一致。

MR 扩展选取真实 J03 销售汇总作业，在原始 clean 数据量及 5 倍、10 倍复制输入上各运行三次。扩展前重新核实 Windows VHDX 所在卷余量，按预计临时峰值两倍加 5GiB 检查。每次记录真实 application ID、输入 split、reducer、Shuffle、CPU/GC、任务峰值内存和 WSL 内存采样，并独立逐项核对整数金额/数量。该实验代表 J03 的扩展特性，不声称为十二个物理阶段的全流水线扩展测试。

扩展文件保留原键和顺序，5 倍、10 倍副本仅位于 `benchmarks/synthetic`。它们不会创建新正式 batch、不会改变推荐指标或发布数据。OS/HDFS 缓存不强制清空，各次按实际缓存条件记录；不能把重复测量称为冷启动测试。

全量回归和现有运行验证命令：

```bash
uv run --locked python -m pytest -q --junitxml=var/w11/pytest.xml
uv run --locked python scripts/verify-w09.py --config config/development.toml \
  --db-defaults-file var/secrets/reader.cnf --output var/w11/http.json
PLAYWRIGHT_BROWSERS_PATH="$PWD/var/cache/playwright" \
  uv run --locked python scripts/verify-w10.py --config config/development.toml \
  --output var/w11/browser.json --screenshots var/w11/screenshots
```

`verify-w11.py` 汇总既有 W01—W10 实测证据及本轮新证据，逐项追踪 FR-01—12、NF-01—08、T-01—20 和九个门禁。它是可审计的证据索引，各结果仍需结合对应专项验证器的覆盖范围理解；缺文件、失败、未执行均不能计为通过。

## 6. 备份、恢复与未通过条件

备份与恢复命令见 `orchestration/README.md`。MariaDB dump、源文件、四模型、映射、依赖清单及代码归档需要一起保存；恢复使用全新 HDFS 测试根和 socket-only MariaDB，生产库不执行恢复 SQL。

同一块物理硬盘上的 C、D、E 盘及 WSL 内复制不满足外部备份要求。暂时没有外部存储时，可以完成本机完整恢复演练，并保持 `EXTERNAL_BACKUP=NOT_RUN`、`G-RECOVERY=NOT_RUN`、`project_complete=false`。获得独立物理盘或外部存储后，在该位置重新备份、验证拓扑、执行隔离恢复，再运行最终验收和报告导出。

停止系统前，先确认重任务终态，再停止自己启动的 UI/API；确需停止集群时，依次停止 NodeManager/ResourceManager、HDFS。进程退出或超时都不能代替 YARN 终态检查。
