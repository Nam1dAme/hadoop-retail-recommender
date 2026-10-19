# W01 工程基础执行记录

引用冻结基线：HL-DOC-1.0.0。日期：2026-10-19。当前任务是新用户授权的“阅读 docs、验证开发环境、建立工程目录、虚拟环境和 Git 仓库等工程基础”，覆盖此前文档阶段“不创建代码”的时间限定。原 19 份设计文档未修改，本记录不属于冻结清单。

状态：**工程骨架已建立；W01/G-ENV 尚未全部完成**。目标 FR-12、NF-03/04/06/08。没有运行真实数据、业务 MapReduce、四算法训练、数据库迁移或应用服务。

## 已完成及证据

| 项目 | 实际结果 | 证据 |
|---|---|---|
| 阅读设计 | README、00—16、清单全部阅读；按契约保留四算法、GPU、release 等后续要求 | 原 docs 文件与本记录 |
| 冻结完整性 | 清单中的 18 文件 SHA-256 全部匹配 | [环境 JSON](environment-2026-10-19.json) frozen_baseline |
| 工程目录 | config、etl、mapreduce、ml、service、app、orchestration、migrations、tests、var；补充 scripts | 根 README、实际目录、Git index；业务目录只有职责边界，未冒充实现 |
| Python 环境 | 项目 `.venv`，Python 3.12.3，pip 24.0，不继承系统包 | `.venv/pyvenv.cfg`、环境 JSON python_venv |
| 开发工具 | OpenJDK 1.8.0_504、Maven 3.8.7、Hadoop 3.3.5、MariaDB 客户端 10.11.14、nvcc 12.4.99 | 环境 JSON 逐项命令输出 |
| 配置加载 | 显式 --config TOML；契约环境变量覆盖；未知字段、错误端口、非回环监听、缺失凭据/root 账号立即拒绝 | orchestration/settings.py、tests/test_settings.py |
| Hadoop 配置 | 项目内独立 HADOOP_CONF_DIR；保留原 fs.defaultFS、name/data；Java8、YARN framework/shuffle/classpath、文档内存预算 | config/hadoop；原配置备份 var/artifacts/hadoop-config-before；环境 JSON |
| Java 基础 | javac source/target8，加载 Hadoop Configuration/Job，读到正确 HDFS/YARN；class major=52 | [Java 检查 JSON](java-smoke-2026-10-19.json)，源码保留于 var/tmp/java-smoke |
| Maven 配置 | Hadoop 3.3.5 provided；source/target8；明确插件版本；离线 validate 成功 | mapreduce/pom.xml；命令见下 |
| Git | `.git-local` 为 non-bare 仓库，HEAD=refs/heads/main，工作目录为项目根，未配置远程；工程文件加入 index | `scripts/git.sh status`、`scripts/git.sh symbolic-ref HEAD` |
| 忽略与敏感信息 | venv、var、.env、本地配置、私钥、模型、缓存被忽略；示例文件无实际凭据 | .gitignore、.env.example、git check-ignore |

初始现有 Hadoop name/data 的 `current/VERSION` 均存在。没有 format、改写原配置、调整系统服务或重置数据库密码。项目配置只有在相应新进程使用它时才生效，不能视为现有服务已按新配置运行。

当前 Ubuntu 24.04.3 / WSL2 x86_64、约 7.4 GiB RAM / 2 GiB swap。Linux df 显示约 915 GiB 可用；/mnt/c 显示约 15 GiB（四舍五入）。**VHDX 所在宿主卷未确认，不能据此通过 >=15 GiB 磁盘门禁**。

## 实际检查

| 命令 | 结果与边界 |
|---|---|
| `source scripts/dev-env.sh` | 激活 venv，设置项目 Hadoop 配置、Java/CUDA 路径和项目 pip 缓存；不改 shell 全局配置 |
| `python -m unittest discover -s tests -v` | 9 tests，全部 OK；仅覆盖配置和凭据边界 |
| `python -m compileall -q etl ml service app orchestration scripts tests` | 成功 |
| `bash -n scripts/dev-env.sh scripts/git.sh scripts/lock-dependencies.sh` | 成功 |
| `mvn -o -Dmaven.repo.local="$PWD/var/cache/maven" -f mapreduce/pom.xml validate` | BUILD SUCCESS；只证明项目模型可加载 |
| `mvn -o -Dmaven.repo.local="$PWD/var/cache/maven" -f mapreduce/pom.xml package` | 失败：本地缺 maven-resources-plugin:3.3.1，离线无法下载；**未证明 Maven 完整构建成功** |
| `.venv/bin/python -m pip check` | No broken requirements found；空业务环境的元数据检查，不证明依赖已安装 |
| `python scripts/check-environment.py --config config/development.toml --output docs/execution/environment-2026-10-19.json` | 返回 3，未满足项明确列出，g_env_complete=false |
| `python scripts/smoke-cuda.py` / `python scripts/smoke-als.py` | 各返回 3，torch/pyspark 未安装；未执行真实计算 |
| `scripts/git.sh check-ignore .venv/bin/python .env config/local.env var/logs/example.log .git-local/HEAD model.pt` | 这些路径均被忽略 |

Java 检查使用已安装 Hadoop 的 classpath，构造小型工程环境类，编译、运行与 javap 均返回 0；这不替代 Maven 依赖解析或 YARN 业务作业。运行时 native-hadoop 加载警告允许回退内置 Java 类，不是 CUDA 成功证据。

## 未完成与直接原因

1. **依赖安装与完整锁未完成。** `requirements.in` 保存基线范围，不能冒充锁。pip 查询未取得版本；urllib 请求官方 PyPI 返回 DNS 解析失败。当前会话创建 localhost TCP socket 返回 PermissionError/EPERM；网络与普通 WSL 不等价。没有生成虚假的 requirements.lock，没有改装系统 Python。准备了网络可用时使用的锁生成、hash 校验安装、pip check/inspect 脚本。
2. **GPU 尚不可验证。** 没有 /dev/dxg，nvidia-smi 报 GPU access blocked by the operating system。型号、显存、compute capability、Windows 驱动未知，不推断宿主没有 GPU。CUDA Toolkit 已安装不等于张量计算成功。
3. **服务运行未核实。** HDFS report 报 socket Operation not permitted；YARN node list 采集超时；localhost 9000/9870/8088/3306 探测被权限拒绝。不能据此宣布服务停止或重启任何任务。MariaDB 客户端版本不证明服务器/角色/迁移已通过；迁移由 W02 实现后关闭相关门禁。
4. **宿主磁盘门禁未通过。** PowerShell 互操作返回 WSL vsock socket failed，无法核实发行版注册表 BasePath 与 VHDX 所在盘。未下载大依赖或生成合成数据。
5. **标准根 Git 元数据目录不可写。** findmnt 显示根 `.git` 为只读 tmpfs；`git init --initial-branch=main .` 返回 Read-only file system。在允许写入的 `.git-local` 建立了真实仓库，使用项目 wrapper。没有尝试解除挂载、改权限或越过环境限制。
6. **首次 commit 尚未创建。** `git var GIT_AUTHOR_IDENT` 明确报 Author identity unknown。已向用户请求姓名/邮箱；未填造身份、未改全局 Git 配置。暂无代码 commit 的运行只可算开发试跑，不能正式发布。

## 后续复核顺序

在普通 WSL 使用 hadoop 用户打开本目录，先确认工程文件及 `.git-local` 历史仍存在；本机 Git wrapper 可直接使用。用户提供身份后仅设置仓库局部 user.name/user.email，检查暂存文件后创建首次 commit。`.git` 若已可写，人工确认它仍为空后再迁移元数据，避免产生两条独立历史。

1. Windows 查询发行版注册表 BasePath、实际 VHDX 所在盘与可用字节；确认至少 15 GiB 余量。保留磁盘证据，不做自动迁移或清理。
2. `source scripts/dev-env.sh`；核对当前 jps/端口、原 HDFS 元数据备份、现有守护进程使用的 HADOOP_CONF_DIR。按 docs/09 启动缺少的服务，禁止 format。执行 `hdfs dfsadmin -report`、`yarn node -list` 并确认活节点，再进行隔离 WordCount 冒烟。
3. 在无其他 MR/Spark/NCF 重任务时运行 `./scripts/lock-dependencies.sh`。成功后核查并提交完整 transitive/hash 锁及平台执行记录；当前脚本仅是待执行流程。
4. `python scripts/smoke-cuda.py`；同时保存 nvidia-smi 型号/显存/Windows 驱动。固定 cu124 构建不支持硬件时走 00 变更，不能静默换 CPU。
5. `SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python" python scripts/smoke-als.py`，证明 Python3.12/Java8/Spark3.5.7 真实 fit-save-load。
6. 联网完成 Maven package；W02 再做 MariaDB 账号、迁移与服务 ready。重新运行环境检查，将新证据写入新的 execution 文件，不改冻结历史。

以上项目未实测通过前，W01/G-ENV 保持未完成；工程基础完成也不代表整个项目的 FR/NF/T-01—20 完成。

## 同日续检与新检出修复

重新检查实际状态：根 `.git` 仍是只读 tmpfs；TCP socket 创建仍返回 EPERM；/dev/dxg 不存在；venv 中 torch、pyspark、pip-tools、setuptools 均未安装；依赖锁缺失；Git 作者信息仍未配置。没有可继续轮询的安装或训练进程。

发现并修复新检出时的目录问题：`var/` 不进入 Git，激活脚本现在会创建项目内 cache/pip、artifacts、tmp、logs；依赖脚本单独执行也会创建所需缓存和证据目录。在 /tmp 新建独立 Python venv，从另一个工作目录 source 脚本，确认 VIRTUAL_ENV/HADOOP_CONF_DIR 指向该测试项目且所需目录均创建成功。Shell 语法、Python 编译及暂存区空白检查通过。

环境报告新增有效非敏感配置及其 SHA-256，覆盖环境变量对文件默认值的修改，原始配置文件 hash 继续保留。新实际采集结果见 [续检 JSON](environment-2026-10-19-recheck.json)；此前证据原样保留。以上修复不解决网络、GPU、宿主盘与提交身份门禁，W01/G-ENV 继续保持未完成。
