# Hadoop Lab

基于 Hadoop 与 CUDA 神经协同过滤的电商分析及推荐系统。唯一真实数据源为 [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail)，CC BY 4.0，金额 GBP、来源本地时间；当前尚未下载数据或实现业务链路。

设计依据：[docs/README.md](docs/README.md)，冻结基线 `HL-DOC-1.0.0`。2026-10-19 起按新用户指令建立工程基础，保留冻结文档历史；实测进度见 [执行记录](docs/execution/2026-10-19-foundation.md)、[故障排查记录](docs/execution/2026-10-19-troubleshooting.md)、[uv 迁移记录](docs/execution/2026-10-19-uv-migration.md)。

Python 版本、虚拟环境和依赖统一由 uv 管理：`.python-version` 固定 3.12，`pyproject.toml` 为唯一依赖声明，`uv.lock` 为已解析生成的完整锁。PyTorch 仅从官方 cu124 索引解析；其他依赖默认 PyPI。旧 pip-tools 输入已移除。

首次在普通 WSL、hadoop 用户终端执行：

```bash
bash scripts/bootstrap-host.sh
```

该脚本依次核实宿主盘空间、执行 `uv lock` / `uv sync --locked`、运行 CUDA 与 ALS 冒烟、Maven package、保存环境报告；任何步骤失败立即停止。它不启动 Hadoop 服务、不创建数据库/迁移、不修改驱动。只有这些自动阶段全部通过才生成 completed.json，数据库权限及服务等门禁仍需另行验收。

本机 Hadoop 服务已启动，活 DataNode、RUNNING NodeManager 及真实 YARN WordCount 均通过，见 [Hadoop 续检记录](docs/execution/2026-10-19-hadoop-startup.md)。`bash scripts/start-hadoop-host.sh` 用于普通 WSL 中启动已停止的本地集群并运行 WordCount 环境冒烟；会先检查进程、备份元数据，保留已有 HDFS 数据。发现已有 Hadoop 进程时会拒绝重复启动。

当前标准 `.git` 指针已接通 `.git-local`，普通 Git 可使用现有提交历史；该元数据目录已忽略，不应清理。当前未配置远程。`scripts/connect-git.sh` 可在缺少标准入口时建立指针，遇到其他 `.git` 会拒绝覆盖。

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

安装需要网络和 VHDX 宿主卷至少 15 GiB 余量；已实测 VHDX 位于 E 盘且空间门禁通过。完整脚本每次重新核实空间。普通 WSL 的网络、GPU 可见性和 MariaDB 握手已通过，当前代理会话仍存在访问隔离。uv.lock 已生成，依赖同步、兼容性检查与真实 CUDA 计算通过，见 uv 迁移记录中的原始报告。

CUDA 脚本要求真实 CUDA 张量矩阵乘法和反向传播；ALS 脚本用隔离小样本在本地文件系统做 fit/save/load，不代表业务模型验收。首次 ALS 的保存路径问题已修复，真实重跑、Maven package 及 Hadoop WordCount 均通过，W01 工程基础检查通过。W02 数据库角色/迁移尚待验收，G-ENV 保持未完成；不代表业务系统已实现。配置说明见 [config/README.md](config/README.md)。

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
