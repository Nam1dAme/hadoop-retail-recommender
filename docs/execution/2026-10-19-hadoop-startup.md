# Hadoop 服务环境续检

引用 HL-DOC-1.0.0；用户已授权继续工程基础与环境排障。业务 MapReduce、数据库迁移不在本轮实现范围。

## 原因与证据

uv bootstrap 已完成，但宿主环境报告确认 HDFS 9000/9870、YARN 8088 连接被拒绝。随后用户在普通 WSL 采集 `jps -lv` 和 `ss -ltnp`；只有 Jps Java 进程，没有 Hadoop 守护进程，对应端口无监听。因此当前直接原因是 Hadoop 服务未启动；不能再把宿主端口拒绝连接归因于代理会话的 socket 限制。

既有 NameNode、DataNode、SecondaryNameNode 元数据存在；NameNode/DataNode cluster ID 均为 `CID-6366f1f0-ddf1-4fa8-8d56-090f6dd3588b`。旧日志最后写于 10 月 11 日，没有本次服务启动失败的新日志。启动前进程证据见 [宿主采集文本](hadoop-processes-before-2026-10-19.txt)，原始采集位于 `var/artifacts/hadoop-host-processes.txt`。

## 本轮操作

`bash scripts/start-hadoop-host.sh` 仅供普通 WSL、hadoop 用户执行。采用 [Hadoop 3.3.5 官方 daemon 入口](https://hadoop.apache.org/docs/r3.3.5/hadoop-project-dist/hadoop-common/ClusterSetup.html#Hadoop_Startup)，使用项目 `config/hadoop`，日志和 PID 放在项目 var 目录。

脚本检查已有 Hadoop 进程及端口，遇到已有进程拒绝重复启动；校验既有 name/data 路径和 cluster ID，备份 NameNode/SecondaryNameNode 元数据及 DataNode VERSION 到独立 run。备份不含全部 DataNode block，因此不冒充完整数据灾备。保留原数据目录，不执行 format。顺序启动 NameNode、DataNode、SecondaryNameNode、ResourceManager、NodeManager，等待活 DataNode、RUNNING NodeManager 和安全模式退出。

WordCount 使用独立 `/user/hadoop/environment-smoke-*` 根和两行标记为 fixture 的输入，不写业务 `/retail/v1`。核对 `_SUCCESS`、hadoop=2/spark=1/uv=1，以及真实 YARN application 的 Final-State=SUCCEEDED 后才生成 completed.json。保留产物与运行中的服务；若超时或失败，先检查日志和现有进程，不重复运行启动脚本。

本会话已通过 Shell 语法、3 段内嵌 Python 语法及 diff 空白检查。真实服务启动和 WordCount 结果等待普通 WSL 运行，不能用静态检查计为通过。

## 普通 WSL 最终结果

Run `var/artifacts/hadoop-host-20261019T141233Z-26860` 于 2026-10-19T14:13:44Z 通过。元数据备份保存在该 run 的 hdfs-metadata-before.tar.gz；守护进程仍运行。确认 1 个活 DataNode、1 个 RUNNING NodeManager、安全模式 OFF，WordCount 输出 hadoop=2、spark=1、uv=1。

YARN application `application_1792419165578_0001`：State=FINISHED、Final-State=SUCCEEDED。原始证据：[冒烟结果](hadoop-smoke-2026-10-19.json)、[DataNode 报告](hdfs-report-2026-10-19.txt)、[NodeManager 报告](yarn-nodes-2026-10-19.txt)、[安全模式](hdfs-safemode-2026-10-19.txt)、[应用终态](yarn-application-2026-10-19.txt)。这关闭了本轮环境服务门禁，不代表 J01—J08 业务作业已实现。G-ENV 仍等待 W02 数据库角色/迁移。

后续手动管理这批进程，在普通 WSL 项目根目录先设置与启动时相同的环境：

```bash
source scripts/dev-env.sh
export HADOOP_CONF_DIR="$PWD/config/hadoop"
export HADOOP_LOG_DIR="$PWD/var/logs/hadoop"
export HADOOP_PID_DIR="$PWD/var/tmp/hadoop-pids"
```

需要停止时，先确认没有运行中的 MR/Spark/NCF 任务，再依次执行 `yarn --daemon stop nodemanager`、`yarn --daemon stop resourcemanager`、`hdfs --daemon stop secondarynamenode`、`hdfs --daemon stop datanode`、`hdfs --daemon stop namenode`。本轮没有执行停止操作。
