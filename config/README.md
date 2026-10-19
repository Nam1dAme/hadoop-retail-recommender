# 配置与依赖

`development.toml` 是显式加载的非敏感配置，环境变量可覆盖对应配置项；字段名称与默认值来自 docs/09。不会从任意当前目录自动发现配置或读取 `.env`。凭据只在调用 `database_credentials()` 时读取，并拒绝缺失值及 root。数据库角色权限仍需 W02 实测。

`requirements.in` 仅为基线版本范围，**不是解析成功的锁**。在网络及宿主磁盘条件满足后运行 `scripts/lock-dependencies.sh`，生成带传递依赖、索引及 SHA-256 的 `requirements-linux-py312.lock`，安装并执行 pip check。完整锁生成后应提交 Git，运行平台与 pip inspect 另存执行证据。torch 固定官方 `2.6.0+cu124`，不安装 torchvision/torchaudio。

`hadoop/` 是项目独立的配置目录：core-site、hdfs-site 从已有安装原样复制，保留 hdfs://localhost:9000、副本 1、NameNode/DataNode 数据路径；随附原版日志、调度器、安全策略、metrics 配置；新 mapred/yarn/hadoop-env 按 docs/09 配置 Java8、YARN shuffle/classpath 和内存预算。原配置副本在忽略目录 `var/artifacts/hadoop-config-before/`。`source scripts/dev-env.sh` 设置 HADOOP_CONF_DIR；如外部已经设置该变量，会保留其值，检查结果以实际配置为准。

覆盖配置不改变正在运行的守护进程；普通 WSL 必须先检查现有进程、端口和 HDFS 元数据备份，再决定启动缺少的服务或有计划地应用配置。当前尚未通过 YARN 作业验收，禁止 format HDFS。
