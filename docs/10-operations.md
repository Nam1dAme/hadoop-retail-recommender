# 10 运维、安全与恢复

版本 HL-DOC-1.0.0，FROZEN。

## 1. 运行记录

每条应用日志至少包含UTC时间、level、component、request_id/run_id、batch_id、model_run_id/release_id（无关联可空）、event、duration_ms、error_code。按天滚动，单文件建议50MiB、本机保留14天；所有最终验收日志/指标/hash单独归档课程周期。不得记录密码、完整数据库连接串、环境变量全集或原始客户明细到错误堆栈。

数据任务记录输入/输出行数、reject/warning计数、YARN application ID、Map/Reduce个数、Shuffle字节、耗时和失败重试；模型记录epoch/loss/评价、内存/显存口径；API记录状态与耗时但不记录凭据。page/query客户标识在访问日志中可截断或hash，调试记录需本机受限。

任务10秒心跳，60秒未更新仅标记“心跳过期待核验”，不直接判FAILED。核验05中的运行句柄；YARN仍RUNNING即等待，采样超时不是终态。普通本机PID需要进程启动时间匹配，不能kill复用PID。

## 2. 权限与本机安全

只读API/UI均回环监听；reader只SELECT所需表，writer负责导入/控制，migration单独授权。HDFS运行用户hadoop；原始和模型目录禁止其他用户写。凭据保存在用户私有环境文件、权限600；Git忽略运行数据、venv、checkpoint、日志、数据库dump、私钥和本地环境文件。公开数据来源必须在README/报告标注，不把CustomerID关联现实个人。

路径参数只允许配置根目录内经过解析的路径；禁止把API输入拼成shell、SQL或HDFS路径。报告文件不执行公式、外部URL或脚本；XLSX只读取单元格值不执行宏。NCF只加载本项目可信state_dict，校验hash后加载；不加载陌生pickle模型。

迁移和部署不得默认清库、format HDFS、覆盖CUDA驱动、变更用户默认Java或全局pip。没有用户任务授权的外部发布和消息发送不属于项目流程。

## 3. 发布、回滚和审计

发布器核验SUCCEEDED批次与experiment、四个FULLFIT模型和完整指标/GPU门禁；正式release拒绝CPU生成的NCF模型或缺GPU对照的实验。先BUILDING导入，校验完成再事务切current，并写release_event。更新current与写审计事件必须同一事务。两个并发publish中仅先获得锁且通过校验者可成功，另一方重新读取current后返回冲突，不覆盖未知新版本。

回滚目标READY且非当前时检查数据和manifest完整，记录reason、操作者和from/to。回滚不重训、不删除当前版本，原current仍可通过显式ID查询。API客户端固定release不会因回滚中途混读。

## 4. 备份和恢复

副本1的HDFS不是备份。每次正式发布保留MariaDB一致性逻辑备份、原始XLSX、HDFS产物manifest、四算法模型与映射、环境锁、代码commit、参数、基线hash。备份写独立Windows物理盘或外部存储；只在同一个VHDX中复制不算抗磁盘故障备份。空间不足时至少完成小样本恢复演练并明确正式备份门禁未通过。

恢复演练：在隔离测试数据库恢复迁移和dump；校验文件hash与外键；恢复HDFS归档至新测试根；使用只读测试配置查询同release；比较金额、计数、推荐项/顺序；达到一致后才声明通过。不能在生产演示库直接做破坏性演练。指标与embedding可有浮点容差，已存推荐与金额应精确一致。

数据丢失容忍目标为最近一次成功发布以来的未发布工作（本系统人工离线批处理），恢复时间作为演练测量值，不承诺未测试的分钟级SLA。

## 5. 清理和故障处理

| 情况 | 处置 | 禁止行为 |
|---|---|---|
| HDFS不可连 | 看端口/日志/DataNode状态，保留name/data，确认权限与磁盘 | 重新format |
| YARN容器失败 | 看application diagnostics、资源与classpath，原终态确认后新attempt | 不停重提形成并发 |
| GPU不可见 | 普通WSL/Windows分别核查驱动及设备门禁 | 改模型设备标签或在WSL装Linux显示驱动 |
| OOM/磁盘不足 | 停止新重任务，检查并发、评分块和空间，保留证据 | 杀未知进程、删除原始数据 |
| DB导入中断 | FAILED或BUILDING版本不暴露，旧current继续查询 | 在READY表中补写部分结果 |
| API无current | 显示空态；用CLI完成首个release | 返回伪造演示数据 |
| 旧发布错误 | 校验目标后rollback到READY版本 | 在线重训替代回滚 |

清理FAILED任务临时目录前至少7天保留、确认无活句柄、无成功manifest引用、无release/model依赖；先dry-run列目标再删除，记录操作者。READY release至少保留当前与前两个，且发布后30天；均满足才可退休。标RETIRED时保留控制记录与manifest，先确认current不指向它，业务子表按FK逆序清理。数据/模型归档如被任一保留release或experiment引用不得删除。

## 6. 性能实验

本机API性能用5并发、每类核心查询预热10次后执行100次，记录P50/P95、错误率、查询计划和设备信息，目标见PRD。测试不与训练重任务同时进行。MR输入原始/5倍/10倍，分别3次，记录缓存状态、reducers、输入split、总耗时；真实与synthetic结果分表，不用复制数据后的推荐提升说明效果。CPU/GPU对照严格见07。
