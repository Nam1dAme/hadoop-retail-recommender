# 09 环境、版本与 WSL 部署

版本 HL-DOC-1.0.0，FROZEN。当前阶段只记录部署设计，不安装依赖或启动服务。

## 1. 目标版本与依赖管理

| 层 | 基线 | 决策 |
|---|---|---|
| OS | WSL2、Ubuntu24.04 x86_64 | 当前已具备，Windows负责GPU驱动 |
| Hadoop/Java | Hadoop3.3.5、OpenJDK8u504、Maven3.8.7 | 保留已有安装，Java编译source/target8 |
| Python | 3.12，项目独立venv | 禁止sudo pip和修改系统Python包 |
| 数据库 | MariaDB10.11系列，当前10.11.14 | 库retail_analytics，不能假定是Oracle MySQL8 |
| CUDA Toolkit | 已安装12.4.0、nvcc12.4.99 | `/usr/local/cuda`指向12.4，不擅自升级 |
| PyTorch | 2.6.0，官方cu124构建 | CPU调试与GPU使用同版本；不安装torchvision/torchaudio（本项目不需要） |
| ALS | PySpark3.5.7、local[2]、Java8 | 官方Python3.8+；必须在Python3.12真实跑ALS冒烟验证 |
| Web | FastAPI0.115系列、Pydantic2系列、Uvicorn0.34系列、Streamlit1.40系列 | 实现开始解析相互兼容patch并锁定，不自动跨minor升级 |
| 数据与展示 | NumPy1.26.4、pandas2.2.3、openpyxl3.1.5、PyMySQL1.1系列、Plotly5.24系列、PyYAML6系列 | 按Python3.12解析；完整transitive锁必须提交 |
| 验证工具 | pytest8系列、HTTPX0.28系列 | 锁定兼容patch，冻结不意味着当前已安装 |

选历史可复现版本是项目基线，不宣称是最新或所有GPU都兼容。PyTorch cu124的GPU架构/驱动实测是G-ENV门禁；若显卡不被该构建支持，要按00发起版本变更，不能silent换CPU。官方历史版本页明确提供torch2.6.0 CUDA12.4构建，见15。依赖锁在安装解析成功后记录包版本、来源索引、平台、Python和hash，离线重现依此执行。网络不可用不能填造锁文件。

## 2. 当前实测与前置门禁

已观察到Hadoop/Java/Maven/Python、MariaDB、CUDA安装；项目框架尚未安装；当前会话nvidia-smi被操作系统阻止且无/dev/dxg。不能在此推断Windows没有GPU，也不能标记GPU可用。hadoop用户CUDA配置位于`~/.config/cuda/env.sh`，`.bashrc`已加载，动态库已注册，无需追加Toolkit stubs到运行路径。

G-ENV必须收集：普通WSL的GPU型号/显存/compute capability、Windows驱动、nvidia-smi结果、Toolkit nvcc版本、torch版本和torch.version.cuda、torch.cuda.is_available、真实CUDA张量乘法和反向传播成功。仅nvcc编译通过不够。驱动能力显示版本可高于本机Toolkit，两者不必相同。WSL内不要安装Linux NVIDIA显示驱动，遵守[NVIDIA WSL说明](https://docs.nvidia.com/cuda/wsl-user-guide/index.html)。

普通WSL以hadoop用户执行现有`bash ~/cuda-check/check.sh`可复核基础CUDA；它不是项目最终训练验收。默认WSL用户nam1dame与项目用户不同，要统一使用hadoop，避免改错shell配置。

## 3. 资源预算

WSL当前可见约7.4GiB内存/2GiB swap、32逻辑CPU。Hadoop守护进程建议每个堆512MiB起步，YARN容器预算3072MiB、vcores4；MR AM512MiB（堆384）、Map768MiB（堆512）、Reduce1024MiB（堆768）、scheduler最小256MiB/最大3072MiB。以实际RSS和日志验证，不把Java堆总和当进程总内存。

Spark driver2GiB、local[2]；不与MR/NCF重计算同时运行。GPU NCF初始batch1024、workers0，CPU线程4，训练前可停API/UI以释放内存；不能未经记录减batch后还声称CPU/GPU同配置。OOM时先排除并发与块大小，再调评分分块（不改变模型语义）；如batch仍需改变，记录实验新配置并CPU/GPU同时使用，更新执行记录，默认基线比较仍未完成。

WSL虚拟盘显示927GiB可用不代表宿主空间；当前C盘仅约14GiB。必须确认VHDX所在盘可用空间，原始数据、PyTorch安装、Maven缓存和模型至少预算15GiB可用；5倍/10倍性能扩展另按预计临时峰值2倍加5GiB余量。未通过磁盘门禁不生成合成数据，不自动迁移发行版或清理用户文件。

## 4. 配置项契约

| 项 | 默认/格式 | 敏感与约束 |
|---|---|---|
| RETAIL_DB_HOST/PORT/NAME | 127.0.0.1 / 3306 / retail_analytics | 不允许UI读取writer凭据 |
| RETAIL_DB_USER/PASSWORD | 无默认，按角色注入 | 敏感，禁止入库/日志/Git |
| RETAIL_HDFS_URI | hdfs://localhost:9000 | 保留已有core-site配置 |
| RETAIL_HDFS_ROOT | /retail/v1 | 同04，不改已有name/data目录 |
| RETAIL_WORK_DIR | /home/hadoop/hadoop-lab/var | Linux文件系统，源码/运行数据分开 |
| RETAIL_API_URL | http://127.0.0.1:8000/api/v1 | Streamlit唯一数据入口 |
| RETAIL_API_HOST/PORT | 127.0.0.1 / 8000 | 只读回环 |
| RETAIL_UI_HOST/PORT | 127.0.0.1 / 8501 | 回环，Windows浏览器localhost访问 |
| JAVA_HOME | /usr/lib/jvm/java-8-openjdk-amd64 | Spark/Hadoop子进程均明确设置 |
| HADOOP_HOME/CONF_DIR | /usr/local/hadoop、其etc/hadoop | bin/sbin入PATH，HADOOP_MAPRED_HOME同HOME |
| CUDA_HOME/CUDA_PATH | /usr/local/cuda | WSL工具路径/usr/lib/wsl/lib入PATH |

配置分项目非敏感配置和本地受限环境文件；CLI --config显式加载，不能自动从任意工作目录捡配置。启动缺必需配置立即失败，禁止尝试root空密码。

## 5. 部署顺序与验收

1. Windows VSCode安装WSL扩展，连接发行版，以hadoop用户打开Linux目录。先确认现有HDFS元数据与备份，禁止format。
2. 配置Hadoop环境、hadoop-env显式JAVA_HOME，mapred.framework=yarn、application classpath和YARN shuffle/env whitelist，预算按本文件。配置备份后修改。
3. 普通终端先看jps和端口，已有守护进程不重复启动；按NameNode、DataNode、SecondaryNameNode、ResourceManager、NodeManager顺序启动缺少的服务。可直接使用各自daemon入口，不强依赖SSH免密。
4. 通过`hdfs dfsadmin -report`检查一个可用DataNode、`yarn node -list`检查可用NodeManager；WordCount只作环境冒烟，业务作业另验收。NameNode9870、ResourceManager8088。
5. 验证MariaDB、创建角色库和迁移，venv解析依赖锁；先API数据库ready、再启动UI，无release为正常空态。
6. GPU门禁通过后以小数据完成NCF前向/反向、ALS保存加载、MR业务样例；然后真实数据全链路。
7. 先四算法EVAL、测试封存，再FULLFIT和推荐，CPU/GPU对照完成，发布并核验API页面。

停止顺序：停止新任务→确认重任务终态→UI/API→数据库（若需）→NodeManager/ResourceManager→HDFS。重启WSL会影响全部服务，不作为无条件修复步骤。任何系统服务安装、发行版迁移、驱动修改在后续单独执行，本轮文档任务不执行。
