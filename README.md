# Hadoop Lab

基于 Hadoop 与 CUDA 神经协同过滤的电商分析及推荐系统。唯一真实数据源为 [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail)，CC BY 4.0，金额 GBP、来源本地时间；当前尚未下载数据或实现业务链路。

设计依据：[docs/README.md](docs/README.md)，冻结基线 `HL-DOC-1.0.0`。2026-10-19 起按新用户指令建立工程基础，保留冻结文档历史；实测进度见 [执行记录](docs/execution/2026-10-19-foundation.md)，Git 作者信息补充后的结果见 [故障排查记录](docs/execution/2026-10-19-troubleshooting.md)。

在项目根目录使用：

```bash
python3.12 -m venv .venv       # 当前已创建；新检出时执行
source scripts/dev-env.sh
python scripts/check-environment.py --config config/development.toml --output var/artifacts/environment.json
python -m unittest discover -s tests -v
./scripts/git.sh status
python scripts/diagnose-environment.py --output var/artifacts/host-diagnostics.json
```

环境检查在依赖或人工门禁未完成时返回 3，配置错误返回 2。机器可读 JSON 包含采集时间、配置文件 hash、环境变量覆盖后的非敏感有效配置及其 hash、逐项事实；TCP 可连只证明端口可达。命令失败不会推断系统服务已停止，也不会自动启动、清理或重跑任务。激活脚本会在新检出时自动创建被 Git 忽略的运行目录。

当前会话的根 `.git` 是只读挂载，标准 `git init` 已失败。已在 `.git-local/` 建立主分支为 main 的本地仓库，`scripts/git.sh` 显式指定元数据及工作目录；普通 `git status` 在本会话仍不可用。该目录已忽略，不是运行缓存，不应清理；当前未配置远程。回到 `.git` 可写的普通 WSL 后，确认 `.git` 仍为空且 `.git-local` 历史完整，再由操作者迁回标准位置；不要同时初始化第二份历史。

用户配置提交作者后已创建首次提交 `02d564b`。只读诊断脚本可用于对比普通 WSL 与代理会话的网络/GPU/服务访问差异，采集成功不代表 G-ENV 通过。

依赖安装需要网络及已核实的 VHDX 宿主卷至少 15 GiB 余量。满足后：

```bash
source scripts/dev-env.sh
./scripts/lock-dependencies.sh
python scripts/smoke-cuda.py
SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python" python scripts/smoke-als.py
mvn -Dmaven.repo.local="$PWD/var/cache/maven" -f mapreduce/pom.xml package
```

普通 WSL 可执行 `bash scripts/bootstrap-host.sh`，自动读取 Windows 发行版注册表及实际 VHDX 宿主卷空间，通过门禁后串行运行上述安装/冒烟/构建步骤，并保留日志和 JSON。空间不足或任何一步失败即停止；全部自动阶段通过后生成 completed.json，服务与数据库权限等验收仍需另行完成。

CUDA 脚本要求真实 CUDA 张量矩阵乘法和反向传播；ALS 脚本用隔离小样本做 fit/save/load，不代表业务模型验收。上述 GPU、ALS、完整依赖锁、Maven package 和服务检查仍受当前会话限制，不能宣称 W01 或 G-ENV 全部完成。配置说明见 [config/README.md](config/README.md)。

数据库凭据由相应角色进程显式注入，示例在 `.env.example`。需要本地文件时用 `umask 077; cp .env.example .env`，填写后仅在相应进程的 shell 显式加载；不要让 UI 继承 writer/migration 凭据。

| 目录 | 职责 |
|---|---|
| docs/、docs/execution/ | 冻结规范、独立执行证据 |
| config/、scripts/ | 非敏感配置、依赖输入/锁、环境工具 |
| etl/、mapreduce/ | 标准化导入、Java8/Hadoop provided 构建 |
| ml/ | 四算法、时间切分、评价与 GPU 对照 |
| orchestration/ | 配置、未来 retailctl/DAG/发布 |
| service/、app/ | FastAPI 只读接口、Streamlit API 展示 |
| migrations/、tests/ | 有序迁移、自动化测试及隔离 fixtures |
| var/ | 忽略的运行数据、缓存、日志与导出 |

当前只实现工程配置与环境工具，各业务目录按 W02—W11 后续开发，不提供占位接口或模拟训练结果。
