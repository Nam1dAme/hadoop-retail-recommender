# 配置与依赖

`development.toml` 是显式加载的非敏感配置，环境变量可覆盖对应配置项；字段名称与默认值来自 docs/09。不会从任意当前目录自动发现配置或读取 `.env`。凭据只在调用 `database_credentials()` 时读取，并拒绝缺失值及 root。数据库角色权限仍需 W02 实测。

按用户后续指令，Python 管理统一使用 uv。根 `.python-version` 固定 3.12，根 `pyproject.toml` 是唯一依赖声明，原 requirements 输入已从当前版本移除，历史由 Git 保存。完整传递依赖锁使用 uv 原生根 `uv.lock`，不手写、不伪造。原业务版本范围保持 HL-DOC-1.0.0；运行记录写 docs/execution。

PyTorch 仅声明一次 `torch==2.6.0+cu124`，通过 tool.uv.sources 绑定 explicit=true 的官方 cu124 索引；其他依赖默认 PyPI，解析平台限定 Linux x86_64/Python3.12。pytest/HTTPX 为 dev 组，默认安装。没有 torchvision/torchaudio，也不将 pip/pip-tools 作为业务依赖。

`scripts/lock-dependencies.sh` 使用 `uv lock`、`uv sync --locked`、`uv pip check`；uv 是管理器，`uv pip` 子命令不依赖旧 pip 程序。缓存为 var/cache/uv；包清单、Python/平台/uv 版本及锁/项目文件 hash 保存在 var/artifacts/uv-*。同步会让项目 venv 与锁一致，移除旧解析流程遗留的无关包。完整锁成功生成后提交 Git。

`hadoop/` 是项目独立的配置目录：core-site、hdfs-site 从已有安装原样复制，保留 hdfs://localhost:9000、副本 1、NameNode/DataNode 数据路径；随附原版日志、调度器、安全策略、metrics 配置；新 mapred/yarn/hadoop-env 按 docs/09 配置 Java8、YARN shuffle/classpath 和内存预算。原配置副本在忽略目录 `var/artifacts/hadoop-config-before/`。`source scripts/dev-env.sh` 设置 HADOOP_CONF_DIR；如外部已经设置该变量，会保留其值，检查结果以实际配置为准。

覆盖配置不改变正在运行的守护进程；普通 WSL 必须先检查现有进程、端口和 HDFS 元数据备份，再决定启动缺少的服务或有计划地应用配置。当前已通过真实 YARN WordCount 环境冒烟，业务作业另行验收，禁止 format HDFS。项目启动脚本的 daemon 日志为 var/logs/hadoop，PID 为 var/tmp/hadoop-pids；后续手动管理同一批进程需沿用这两个变量和项目 HADOOP_CONF_DIR。
