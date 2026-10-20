# Hadoop 连接被拒绝与服务恢复

## 原因与恢复

2026-10-20，用户执行 HDFS 查询时收到 `Call From LAPTOP-OMM0KTB5/127.0.1.1 to localhost:9000 ... Connection refused`。现场 `jps -l` 仅显示 Jps，`ss -ltn '( sport = :9000 or sport = :9870 )'` 无监听。直接原因是 Hadoop 服务未运行；本次没有证据判断进程此前为何停止。

执行 `bash scripts/start-hadoop-host.sh`。脚本在确认集群停止后校验既有 NameNode/DataNode cluster ID、备份元数据，再启动 HDFS 和 YARN；未格式化、删除或重新导入业务数据。实际 run：`var/artifacts/hadoop-host-20261020T063252Z-22376`，完整日志和启动前元数据备份保留在该目录，不进入 Git。

## 验证结果

- NameNode、活 DataNode 和 RUNNING NodeManager 就绪，HDFS 安全模式 OFF。
- `hdfs dfs -ls /retail/v1/normalized/batch_id=B20261019194025b4f65854` 成功，列出原有 9 个文件，包括 normalized.tsv（87,388,902 bytes）、manifest.json 与 `_SUCCESS`。
- 真实 YARN WordCount 应用 `application_1792477984440_0001` 最终 SUCCEEDED；hadoop=2、spark=1、uv=1。
- 脚本退出码 0，完成时间 UTC `2026-10-20T06:34:08.023561+00:00`。机器可读结果见 [完成记录](hadoop-restart-2026-10-20.json)，服务保持运行。

这是环境恢复检查，不能作为 W04 业务 MapReduce 已实现的证据；业务数据完整性沿用 [W03 独立验收](w03-verification-2026-10-20.json)，本次仅额外确认目录可读。

## 后续使用

```bash
cd /home/hadoop/hadoop-lab
source scripts/dev-env.sh
jps -l
ss -ltn '( sport = :9000 )'
```

`source` 只配置当前终端，不启动服务。重启电脑或 WSL 后先检查运行状态；确认本地集群全部停止时执行：

```bash
bash scripts/start-hadoop-host.sh
```

等待脚本成功后使用 [查看数据命令](../../etl/README.md#查看已导入数据)。如果只有部分服务在运行，先检查 `var/logs/hadoop/` 与已有 PID；启动脚本会拒绝重复启动，不能通过 format 或删除数据目录处理连接故障。
