# Hadoop Lab

基于 Hadoop 与 CUDA 神经协同过滤的电商分析及推荐系统。唯一真实数据源为 [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail)，CC BY 4.0，金额 GBP、来源本地时间；已完成真实导入、Hadoop业务计算、四算法离线评价、全历史服务推荐、CPU/CUDA 对照及真实发布与本机恢复演练；API 与页面按后续阶段实现。

设计依据：[docs/README.md](docs/README.md)，冻结基线 `HL-DOC-1.0.0`。2026-10-19 起按新用户指令建立工程基础，保留冻结文档历史；实测进度见 [执行记录](docs/execution/2026-10-19-foundation.md)、[故障排查记录](docs/execution/2026-10-19-troubleshooting.md)、[uv 迁移记录](docs/execution/2026-10-19-uv-migration.md)。

2026-10-20 已复查当前会话：PyPI/PyTorch/Maven/GitHub HTTPS、Git 远程读取、真实 CUDA 矩阵计算和反向传播正常。W02 已实现完整 001 迁移、角色配置、批次/任务/实验控制和 manifest 核验；33 项测试通过，其中数据库测试使用独立 MariaDB 10.11.14 实例。正式 HDFS `/retail/v1` 命名空间已建立。现有 MariaDB 已按用户授权完成正式 001 迁移；migration/writer/reader 三账号最小权限均已实测通过，W02 及 G-ENV 对应的数据库门禁已完成，详见 [W02 执行记录](docs/execution/2026-10-20-w02.md) 和 [迁移使用说明](migrations/README.md)。

W03 已完成：真实 UCI 文件 541,909 行已原样归档 HDFS 并生成标准化 TSV、来源审计、拒绝/warning 报告及 manifest。首次导入与再次导入复用同一批次/任务，独立扫描验证全部源行追溯和 5,268 条重复超额行；重复交易均保留。W03 验收时导入任务 SUCCEEDED、批次 PROCESSING，后续已由 W04 完成业务流水线。历史验收见 [W03 执行记录](docs/execution/2026-10-20-w03.md)，命令及模块说明见 [导入说明](etl/README.md)。

W04 已完成：J01—J08 的十二个真实 YARN 应用全部成功，独立验算通过 37,830 项检查；原批次现为 SUCCEEDED。产出 19,960 个正向订单、4,338 个客户的 RFM，以及销售、完整购买交互和相似商品结果。重复执行复用同一批任务，完整回归 86 项通过，W03 兼容验收通过。构建/使用见 [MapReduce 说明](mapreduce/README.md)，真实应用、金额及源码证据见 [W04 执行记录](docs/execution/2026-10-20-w04.md) 和 [完整验收 JSON](docs/execution/w04-verification-2026-10-20.json)。

W05 已完成实验数据与评价协议验收：训练窗 3,314 位客户、3,388 个候选商品、162,426 对购买交互；五个 TRAIN 窗口 YARN 应用成功，独立从 clean 重建两套映射、交互、热门和评价标签均一致。说明见 [实验与模型使用说明](ml/README.md)，证据见 [W05 执行记录](docs/execution/2026-10-20-w05.md)。

W06 已完成：POP/ItemCF、四组ALS、两组真实CUDA NCF共八个EVAL候选成功，四算法TEST评价完成并封存。验证选出的默认算法为ALS（rank32/reg0.1），NCF选定lr0.001、第15轮checkpoint；最终回归105项通过，独立模型/负采样/封存验收及同实验幂等复查均通过。一次POP归档失败及重试记录完整保留。详见 [W06 执行记录](docs/execution/2026-10-21-w06.md) 和 [完整验收 JSON](docs/execution/w06-verification-2026-10-21.json)。

W07 已完成：CPU/CUDA各三次真实NCF对照、四个FULLFIT模型与173,520条服务推荐通过独立验收，每算法覆盖全部4,338位RFM客户。CPU/CUDA训练中位数分别为13.34/25.25秒，加速比0.5285，本次固定规模下GPU更慢；成功命令全部复用原产物且相关控制表不变，最终回归116项通过。详见 [W07 执行记录](docs/execution/2026-10-21-w07.md) 和 [完整验收 JSON](docs/execution/w07-verification-2026-10-21.json)。

W08 已实现并完成真实首发与本机恢复验收：版本 `R20261021104550ac6985a5` 为 READY，九张业务表共 1,328,466 行；重复发布及恢复前后生产库全部 23 张表哈希一致，最终回归 133 项通过。按用户选择使用本机副本演练，独立物理盘备份门禁尚未通过。命令见 [发布与恢复说明](orchestration/README.md)，证据见 [W08 执行记录](docs/execution/2026-10-21-w08.md) 和 [完整验收 JSON](docs/execution/w08-verification-2026-10-21.json)。

Python 版本、虚拟环境和依赖统一由 uv 管理：`.python-version` 固定 3.12，`pyproject.toml` 为唯一依赖声明，`uv.lock` 为已解析生成的完整锁。PyTorch 仅从官方 cu124 索引解析；其他依赖默认 PyPI。旧 pip-tools 输入已移除。

首次在普通 WSL、hadoop 用户终端执行：

```bash
bash scripts/bootstrap-host.sh
```

该脚本依次核实宿主盘空间、执行 `uv lock` / `uv sync --locked`、运行 CUDA 与 ALS 冒烟、Maven package、保存环境报告；任何步骤失败立即停止。它不启动 Hadoop 服务、不创建数据库/迁移、不修改驱动。只有这些自动阶段全部通过才生成 completed.json，数据库权限及服务等门禁仍需另行验收。

Hadoop 服务须独立启动，`source scripts/dev-env.sh` 只加载环境变量。`bash scripts/start-hadoop-host.sh` 用于普通 WSL 中启动已停止的本地集群并运行 WordCount 环境冒烟；会先检查进程、备份元数据，保留已有 HDFS 数据。发现已有 Hadoop 进程时会拒绝重复启动。首次验收见 [Hadoop 续检记录](docs/execution/2026-10-19-hadoop-startup.md)；2026-10-20 因服务未运行导致连接被拒绝，恢复后导入目录可读且 YARN WordCount 成功，见 [重启与排障记录](docs/execution/2026-10-20-hadoop-restart.md)。数据查看命令见 [导入说明](etl/README.md#查看已导入数据)。

当前标准 `.git` 指针已接通 `.git-local`，普通 Git 可使用现有提交历史；该元数据目录已忽略，不应清理。远程仓库为 [hadoop-retail-recommender](https://github.com/Nam1dAme/hadoop-retail-recommender)，本地 origin 以 `git remote -v` 为准。`scripts/connect-git.sh` 可在缺少标准入口时建立指针，遇到其他 `.git` 会拒绝覆盖。

W01 已归档至 `f62b3cc`；清理及网络/GPU 复查和推送流程见 [收尾记录](docs/execution/2026-10-19-cleanup.md)。普通 WSL 执行 `bash scripts/push-host.sh https://github.com/Nam1dAme/hadoop-retail-recommender.git`，通过实测检查后提交本次报告并推送 main，最后核对远程 commit。

用户配置提交作者后已创建首次提交 `02d564b`。只读诊断脚本可用于对比普通 WSL 与代理会话的网络/GPU/服务访问差异，采集成功不代表 G-ENV 通过。

首次锁定、同步成功后，日常在项目根目录使用：

```bash
source scripts/dev-env.sh
uv sync --locked
uv run --locked python -m pytest
uv run --locked python scripts/check-environment.py --config config/development.toml --output var/artifacts/environment.json
uv run --locked python scripts/diagnose-environment.py --output var/artifacts/host-diagnostics.json
git status
```

添加或调整依赖使用 `uv add` / `uv remove`，随后检查 pyproject.toml 与 uv.lock 的变更。测试工具位于 dev 依赖组，默认同步安装。Python 选择/建环境使用 `uv python` / `uv venv`；不再调用 pip 或 pip-compile 管理本项目。项目缓存位于 `var/cache/uv`。

安装需要网络和 VHDX 宿主卷至少 15 GiB 余量；已实测 VHDX 位于 E 盘且空间门禁通过。完整脚本每次重新核实空间。当前会话的网络、GPU 可见性、MariaDB 登录及角色权限均已通过实测。uv.lock 已生成，依赖同步、兼容性检查与真实 CUDA 计算通过，见 uv 迁移记录中的原始报告。

CUDA 脚本要求真实 CUDA 张量矩阵乘法和反向传播；ALS 脚本用隔离小样本在本地文件系统做 fit/save/load，不代表业务模型验收。首次 ALS 的保存路径问题已修复，真实重跑、Maven package 及 Hadoop WordCount 均通过，W01 工程基础检查通过。W02 数据库角色/迁移已验收，详见 W02 执行记录；业务系统仍需后续任务实现。配置说明见 [config/README.md](config/README.md)。

数据库凭据由相应角色进程显式注入，示例在 `.env.example`。需要本地文件时用 `umask 077; cp .env.example .env`，填写后仅在相应进程的 shell 显式加载；不要让 UI 继承 writer/migration 凭据。

| 目录 | 职责 |
|---|---|
| docs/、docs/execution/ | 冻结规范、独立执行证据 |
| config/、scripts/ | 非敏感配置、依赖输入/锁、环境工具 |
| etl/、mapreduce/ | 标准化导入、Java8/Hadoop provided 构建 |
| ml/ | 四算法、时间切分、评价与 GPU 对照 |
| orchestration/ | 配置、retailctl、任务编排及后续发布 |
| service/、app/ | FastAPI 只读接口、Streamlit API 展示 |
| migrations/、tests/ | 有序迁移、自动化测试及隔离 fixtures |
| var/ | 忽略的运行数据、缓存、日志与导出 |

当前 W01—W07 已完成。W08—W11 的发布与恢复、API、页面及全链路验收按后续阶段实施。实现及测试、执行证据与运维文档按独立主题归档，真实输入和运行数据保留在 Git 忽略的 var/ 与 HDFS。
