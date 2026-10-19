# uv 管理迁移与 PyTorch 来源修复

日期：2026-10-19。依据用户新指令“后面统一使用 uv 对 Python 进行管理”；当前 uv 实测 0.13.1。引用基线 HL-DOC-1.0.0，冻结规范未改。

## 失败原因

宿主 run `host-bootstrap-20261019T132346Z-20754` 与 `host-bootstrap-20261019T132445Z-20880` 均已经终态失败：日志显示官方 CUDA wheel 已命中缓存（768.4 MB），随后对独立 `torch==2.6.0+cu124` 声明在仅 PyPI 的索引中查询，返回 DistributionNotFound。旧配置同时保留通用版本声明和平台 direct URL 声明，没有统一来源，是上一轮修复的缺陷；不能把错误解释为官方 cu124 包不存在或本机 GPU 不支持。

## 调整记录 CR-UV-20261019

- 提出依据：用户本轮明确指定 uv；目标为 FR-12、NF-04/08。
- 调整内容：静态 pyproject.toml 为唯一依赖声明；.python-version 固定 3.12；uv.lock 为原生完整锁，按 uv 规范放根目录；旧 pip-tools 输入移除；所有环境管理/启动文档切换 uv。此前系统 Python/项目 venv 已有 3.12.3，uv 可选择并管理现有兼容解释器，无需修改系统包。
- 来源：torch 唯一声明 `==2.6.0+cu124`，绑定官方专用索引，explicit=true；通用包默认 PyPI。支持平台固定 Linux x86_64，运行与开发依赖分别声明，默认同步 dev。setuptools 构建依赖锁定为已实测的 75.9.1。
- 影响：依赖管理与锁位置改变，不改变业务版本要求、金额/时间口径、表/API/schema、四算法/GPU 范围。没有数据库迁移。
- 迁移方式：uv sync --locked 同步现有独立 .venv，按锁移除旧工具残留；不删除历史下载缓存。后续添加包使用 uv add，不混用 pip/pip-tools。
- 回滚：Git 保留旧流程及文件，可按提交历史恢复；不会自动重跑原失败任务。用户已要求后续统一 uv，当前不执行回滚。

实现依据：[uv 官方 PyTorch 集成](https://docs.astral.sh/uv/guides/integration/pytorch/)、[项目同步](https://docs.astral.sh/uv/concepts/projects/sync/)、[平台限定](https://docs.astral.sh/uv/concepts/projects/config/#limited-resolution-environments)。

## bootstrap-host.sh 的范围

1. 检查 uv 与 Python3.12，选择/建立项目 venv；确认普通 WSL socket 能力。
2. 只读读取 Windows 注册表的 WSL VHDX 路径与宿主卷可用字节，门禁至少 15 GiB。
3. uv lock 解析完整依赖与 hash；uv sync --locked 安装；uv pip check 验证依赖，保存版本/平台/锁 hash。
4. 串行执行真实 CUDA matmul/backward、ALS fit-save-load 和 Maven package。
5. 保存环境报告；全自动阶段通过后才写 completed.json。它不启动系统服务、不创建数据库迁移、不修改驱动或 DNS；G-ENV 不能只凭 completed.json 宣称全部完成。

真实网络解析、安装与 GPU/ALS 通过普通 WSL 实测，当前代理会话用于读取共享产物和执行离线验证。以下按实际 run 记录结果。

## 当前验证

通过 `uv run --no-project --python 3.12 python -m unittest discover -s tests -v` 实际执行 17 项测试，全部通过；包含 4 项原生锁静态检查测试，覆盖错误 CPU 版本、错误索引、缺少/非法 hash 和缺失/坏格式文件。该组使用隔离解析 fixture，不是软件安装证据。Python 编译与全部 Shell 文件语法检查通过。

`uv python find --show-version 3.12` 返回 3.12.3。迁移初期离线解析因缺少 numpy 缓存失败；后续普通 WSL run 已完成联网解析和安装，见下方真实结果。普通 Git status 已正常工作，标准入口已接通原提交历史。

脚本现在打印 1—5 阶段编号，并在依赖阶段成功后，将 uv.lock、pyproject.toml、uv 环境元数据与包清单快照复制到本次 run 目录，后续运行不会覆盖该次证据。

本轮提交时发现，标准 `.git` 指针接通后，当前代理会话把真实 `.git-local` 元数据挂载为只读；git add/commit 创建 index.lock 被拒绝。因此本轮迁移修改已保存、测试，但尚未提交，最后既有 commit 仍为 d313151。普通 WSL 可以使用该仓库；uv 环境报告同时记录 code_commit 与 code_dirty，不把未提交脚本冒充该 commit 的内容。

## 普通 WSL 首次 uv 安装结果与 ALS 保存修复

Run：`var/artifacts/host-bootstrap-20261019T134950Z-23422`。磁盘门禁通过，uv 解析 84 个包并完成同步，`uv pip check` 报告全部兼容。原生 `uv.lock` 已真实生成，SHA-256 为 `48eee008149d7925d592b530d8629420ac3993eec3becc0013db0e1c0128dbac`；静态检查确认 torch 版本为 2.6.0+cu124、来源为官方 cu124 索引，所有 registry 包的制品具备 SHA-256。

真实 CUDA 矩阵乘法和反向传播通过：NVIDIA GeForce RTX 4060 Laptop GPU，8585216000 字节显存，compute capability 8.9，torch runtime 12.4。保存的原始报告见 [CUDA](cuda-smoke-2026-10-19.json)、[uv 环境及源码状态](uv-environment-2026-10-19.json)、[安装包清单](uv-packages-2026-10-19.json)。

随后 ALS 已完成拟合和初次预测，但在 `model.save` 失败：项目 `core-site.xml` 的 `fs.defaultFS=hdfs://localhost:9000` 被 Spark 继承，未带 scheme 的本地绝对路径被解释为 HDFS 路径；当时该端点连接被拒绝。这不是 PyTorch 索引问题，也不是 ALS/Python 版本不兼容的证据。该 run 的 als-smoke.json 为空、没有 completed.json，Maven 尚未执行。

修复限定在 local[2] 冒烟：设置 `spark.hadoop.fs.defaultFS=file:///` 并断言实际配置，模型保存和加载均使用显式 file URI。项目 HDFS 配置保持原样，真实 HDFS/YARN 门禁仍需单独核验。依据 [Spark 3.5.7 官方配置说明](https://spark.apache.org/docs/3.5.7/configuration.html#custom-hadoophive-configuration)。

修复后本会话 `uv lock --check --offline`、`uv pip check`、`uv sync --locked --offline --check` 均通过，同步检查确认无需修改现有环境；pytest 17 项通过，修改脚本的 Python 语法、全部 Shell 语法及 git diff --check 通过。ALS 修复后的真实保存加载与后续 Maven 构建等待普通 WSL 重跑，不能以这些离线检查替代。

普通 WSL 后续 run `host-bootstrap-20261019T140446Z-24975` 已通过 ALS 真实重跑，使用 Python 3.12.3、Java 1.8.0_504、Spark 3.5.7 和 file:///，6 条保存前后预测一致。见 [ALS 原始报告](als-smoke-2026-10-19.json)。Maven 已开始首次依赖下载，构建结果尚待核验。业务库实际导入检查同时通过，冻结清单 18 个文件 hash 仍全部匹配。

## 本次 bootstrap 最终结果

同一 run 于 2026-10-19T14:08:51Z 完成：Maven 报告 BUILD SUCCESS，耗时 3 分 18 秒，生成工程骨架 JAR；该 JAR 无业务 class，不能算业务 MapReduce 或 YARN 作业通过。见 [Maven 构建摘要及制品 hash](maven-package-2026-10-19.json)、[脚本完成原始记录](bootstrap-completed-2026-10-19.json)、[普通 WSL 环境原始报告](environment-2026-10-19-uv.json)。

环境报告的工具版本、14 个依赖版本、4 个 Hadoop XML 配置、Java 环境、GPU 可见性、uv 和依赖锁均 PASS。普通 WSL 对 9000、9870、8088 的探测均为 ConnectionRefusedError（errno 111）；hdfs report 也明确连接被拒绝，YARN node list 超时。此次可以确认目标服务端点不可用，不能再归因于代理 socket 隔离。既有 Hadoop 日志最后记录在 10 月 11 日，无本次启动失败的新日志，尚需宿主进程清单判断守护进程状态；NameNode/DataNode 元数据仍在，不能 format。MariaDB 3306 TCP 可连接，但不代表业务角色/迁移验收。

原始 environment.json 的 manual_gates 是检查器固定提示清单，不会跨文件合并证据。磁盘、真实 CUDA、ALS 已分别有本次成功产物；仍未关闭的是 HDFS/YARN 活节点及 WordCount、W02 数据库角色/迁移。G-ENV 继续为 false。本轮修改和 uv.lock 尚未提交，需在普通 WSL 完成 Git 提交；代理元数据只读限制不改变已经通过的安装/计算结果。

后续普通 WSL 已完成 Hadoop 启动、活节点和真实 YARN WordCount，详见 [Hadoop 续检结果](2026-10-19-hadoop-startup.md)。W01 工程基础检查通过；当前 G-ENV 剩余 W02 数据库角色/迁移验收。uv 运行报告中的 code_dirty=true 是当时事实，最终代码归档以 Git 迁移提交为准，不能回写报告为 clean。
