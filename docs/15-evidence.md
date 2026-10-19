# 15 案例来源与环境证据

版本 HL-DOC-1.0.0；记录日期2026-10-18；FROZEN。

## 1. 本地案例依据

来源文件位于`/home/hadoop/lab-doc`，不复制PDF到docs。此前使用已有Ghostscript提取正文阅读；嵌入图片未逐张核验，不能声称从截图验证了原项目实际性能。

| 材料 | 相关章节与内容 | 本项目采用与调整 |
|---|---|---|
| 大数据分析综合案例.pdf | 9.1—9.5，电影管理/评分、HDFS、关系库、Spark MLlib ALS、网页推荐 | 保留数据—存储—模型—结果—展示闭环；将显式评分改购买隐式反馈，离线训练 |
| 案例/付文博组-面向领域实践小组实验报告.pdf | 2开发环境、3数据、4.2/4.5架构及算法，5结果，7体会 | 借鉴分层、混合推荐、任务展示；改用可核验数据，保留神经训练但用NCF替代GNN |

第二份报告3.4列约2万节点、8.6万边、数万微博，后文体会又提近百万数据，口径不一致；本项目不据此推断实际百万数据或集群性能。原代码片段不能视为可运行工程。本项目NCF是新的领域设计，并非声称复刻原报告GNN。

## 2. 外部一手资料（查阅日2026-10-18）

| 来源 | 支持的决定 |
|---|---|
| [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail) | 数据541,909行、时间、字段、GBP、取消单C前缀、22.6MB、CC BY4.0 |
| [Hadoop3.3.5 Single Node](https://hadoop.apache.org/docs/r3.3.5/hadoop-project-dist/hadoop-common/SingleCluster.html) | HDFS伪分布式、MapReduce on YARN、shuffle/classpath、9870/8088端口 |
| [Hadoop3.3.5 MapReduce Tutorial](https://hadoop.apache.org/docs/r3.3.5/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html) | 键值、Combiner、作业输入输出、YARN执行 |
| [Spark3.5.7安装](https://spark.apache.org/docs/3.5.7/api/python/getting_started/install.html) | PySpark支持Python3.8及以上；实际组合仍需冒烟 |
| [Spark3.5.7 ALS](https://spark.apache.org/docs/3.5.7/ml-collaborative-filtering.html) | 显式/隐式ALS、参数、整数ID及cold start语义 |
| [NCF原论文](https://arxiv.org/abs/1708.05031) | GMF与MLP结合的推荐建模思路；本项目结构/评价为自身冻结协议 |
| [PyTorch历史版本](https://pytorch.org/get-started/previous-versions/) | torch2.6.0官方cu124构建存在；不意味着本机GPU已通过 |
| [CUDA12.4安装指南](https://docs.nvidia.com/cuda/archive/12.4.1/cuda-installation-guide-linux/index.html) | 编译工具、运行库、环境路径；Ubuntu24.04不在该版本原生Linux资格表，实际WSL组合须实测 |
| [NVIDIA CUDA on WSL](https://docs.nvidia.com/cuda/wsl-user-guide/index.html) | Windows提供GPU驱动接口，WSL内不装Linux显示驱动 |
| [Microsoft WSL与VSCode](https://learn.microsoft.com/en-us/windows/wsl/tutorials/wsl-vscode) | Windows编辑器/WSL扩展与Linux工具链工作方式 |
| [Microsoft WSL文件互操作](https://learn.microsoft.com/en-us/windows/dev-environment/wsl-interop) | Linux项目存Linux文件系统的性能建议 |
| [Microsoft WSL配置](https://learn.microsoft.com/en-us/windows/wsl/wsl-config) | .wslconfig、网络与资源配置 |
| [Microsoft WSL磁盘](https://learn.microsoft.com/en-us/windows/wsl/disk-space) | VHDX虚拟空间与宿主存储区别 |

## 3. 环境实测摘要与证据强度

- 文件/命令直接证实：Ubuntu24.04.3、WSL2内核6.6.87.2；Hadoop绝对路径返回3.3.5；Java8u504；Maven3.8.7；Python3.12.3；MariaDB10.11.14安装包；CUDA nvcc12.4.99，Toolkit路径12.4。
- 配置文件直接证实：core-site默认hdfs://localhost:9000、副本1、已有name/data VERSION；此前mapred/yarn配置为空。CUDA用户环境脚本已存在并配置PATH/CUDA_HOME；库搜索ldconfig已登记。
- 当前文档轮次再次检查：torch、pyspark、pandas、FastAPI、Streamlit、PyMySQL、openpyxl在当前Python环境未安装；系统NumPy可导入。
- 当前文档轮次再次检查：无/dev/dxg，WSL nvidia-smi返回“GPU access blocked by the operating system”。这只能证明本工具会话不可访问设备，不能证明普通WSL故障或硬件不存在。
- 既有CUDA检查记录`/home/hadoop/cuda-check/README.md`记载CUDA编译成功，运行返回driver API=0与错误35。不能用编译成功替代GPU执行成功。
- 先前free/df/nproc：7.4GiB RAM、2GiB swap、32逻辑CPU；C盘约14GiB可用；宿主VHDX位置未核验。
- 隔离环境中未看到Hadoop/数据库服务不构成“服务未运行”的充分证据；普通WSL需复核。

当前项目`/home/hadoop/hadoop-lab`初始为空且目录不可写。已将原空目录保存在`/home/hadoop/.hadoop-lab.empty-original-20261018-182124`，在原路径创建可写目录，仅新增docs中的文档；没有删除原文件或创建业务实现。

## 4. 推断与承诺的界线

“适合该规模”“预计可CPU调试”“资源预算”是设计判断；“实际GPU可用”“NCF比ALS好”“GPU更快”“P95达标”必须有运行证据。本基线不承诺这些实验结论，提供验证协议并要求如实报告。
