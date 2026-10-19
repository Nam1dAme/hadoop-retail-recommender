# Git 提交与环境故障排查

基线：HL-DOC-1.0.0。日期：2026-10-19。用户补充 Git 作者信息后继续排查；冻结文档未修改。

## 已解决

作者信息已通过 `git var GIT_AUTHOR_IDENT` 验证，首次提交 `02d564b12464914ed18e6b8e7e3306de3c41e498` 已成功创建，包含前期工程基础及全部冻结规范。没有改写用户身份或全局 Git 配置。仓库仍通过 `scripts/git.sh` 访问 `.git-local`。

## 故障分层与证据

实测 JSON：[diagnostics-2026-10-19.json](diagnostics-2026-10-19.json)。

| 现象 | 本轮实测 | 结论及下一步 |
|---|---|---|
| pip 查询不到包、HTTPS 失败 | AF_INET/AF_INET6 创建均返回 EPERM；访问 PyPI、PyTorch cu124、Maven Central 均报解析失败 | 当前命令执行环境已在网络套接字层受限。解析错误不足以证明宿主 DNS 损坏；先比较普通 WSL，不能靠更换依赖版本解决 |
| MariaDB 不可连 | 本地 mysqld.sock 存在；AF_UNIX 可创建，但 connect 返回 EPERM；客户端 error 2002、errno=1 | 失败发生在握手/认证前，不是本次密码错误证据。不能据 socket 存在推断服务存活，也不能据受限连接失败重启数据库 |
| GPU 不可见 | /dev/dxg、/dev/nvidia0 不存在；WSL nvidia-smi 返回 operating system blocked；ldconfig 已找到 WSL libcuda/libnvidia-ml | 用户态驱动库存在，当前会话缺少可访问的 GPU 接口。Windows 显卡/驱动及普通 WSL 状态仍未知，不能断言驱动过旧 |
| 标准 git init 失败 | findmnt 显示 `.git` 为 ro tmpfs；元数据路径权限 555 | 当前挂载限制真实存在，不是提交作者配置问题；`.git-local` 已能提交，不能用 chmod 解除只读挂载 |
| Maven package 失败 | 前次离线包构建缺 resources 插件，本次确认 Maven 缓存中无 JAR，官方仓库网络不可达 | 插件/依赖缓存未建立，尚未取得 Java 源码编译失败证据。已有 javac/Hadoop 本地冒烟及 Maven validate 通过，完整 package 仍需联网 |

进程还显示 NoNewPrivs=1、Seccomp=2、Seccomp_filters=2，说明存在系统调用过滤；仅这些字段不足以还原每条过滤规则。对本次环境已知的网络、文件写入权限约束与 EPERM/只读挂载结果一致。[OpenAI 官方隔离说明](https://learn.chatgpt.com/docs/sandboxing)解释子进程命令继承隔离边界；[权限说明](https://learn.chatgpt.com/docs/permissions)说明命令联网和网页搜索使用不同访问路径，因此网页工具可访问文档不代表 pip 能联网。

没有尝试修改 DNS、重置数据库凭据、安装 Linux NVIDIA 驱动、解除挂载或停止 WSL。现有证据支持先验证执行环境访问能力。

## 可重复的只读诊断

新增 `scripts/diagnose-environment.py`：分别探测 IPv4/IPv6/Unix socket、官方依赖源 HTTP、MariaDB greeting、GPU、Git 挂载及 commit、少量包版本；不读取环境变量全集，不输出密码或 Git 作者邮箱。两种 IP socket 都被拒绝时跳过 HTTP，避免重复把策略拒绝报成依赖源故障。数据库只读初始 greeting，绝不发出认证、SQL 或服务控制命令。

在普通 WSL、hadoop 用户终端执行：

```bash
cd /home/hadoop/hadoop-lab
source scripts/dev-env.sh
python scripts/diagnose-environment.py --output var/artifacts/host-diagnostics.json
```

脚本退出 0 仅表示采集完成；g_env_complete 固定 false，不把诊断当成环境验收。对比时：若普通 WSL socket 创建及 HTTP 成功，当前会话限制得到进一步印证；若普通 WSL socket 可创建但 HTTPS 解析失败，再排查宿主 DNS/代理；若 GPU 普通 WSL 也不可见，再收集 Windows nvidia-smi、WSL 版本和发行版状态。

普通 WSL 报告不会自动提交（位于 var/）；可将命令输出贴回，或由本项目读取该 JSON。VHDX 所在宿主卷仍需至少 15 GiB 可用的直接证据，Linux df 不能代替。

## 普通 WSL 对照结果

用户已在普通 WSL 运行诊断，结果保存到 var/artifacts/host-diagnostics.json，并归档为 [host-diagnostics-2026-10-19.json](host-diagnostics-2026-10-19.json)。实际证据：IPv4/IPv6/Unix socket 均可创建，PyPI/PyTorch/Maven 均 HTTP 200，MariaDB greeting 显示 10.11.14，/dev/dxg 存在，nvidia-smi 成功，显示 GeForce RTX 4060 系列、8188 MiB 显存及 Windows 驱动 566.07。nvidia-smi 表格截断了设备全名，完整型号/compute capability 留待 torch 冒烟采集；驱动显示 CUDA 12.7 是支持能力，不改变已安装 Toolkit 12.4 基线。

这些对照证据将网络/数据库连接/GPU 访问失败定位到当前代理执行会话；宿主 WSL 在这些检查中正常。数据库账号权限、迁移、真实 CUDA 算子和 ALS 仍未因此验收。

新增 `scripts/check-host-storage.ps1` 只读查询 Windows 注册表中各 WSL 发行版 BasePath、VHDX 文件与所属宿主卷可用字节。`scripts/bootstrap-host.sh` 在普通 WSL 调用它并匹配当前 WSL_DISTRO_NAME；未确认 VHDX 或不足 15 GiB 时立即停止，不下载依赖。通过后串行执行依赖锁/安装、CUDA、ALS、Maven package 和环境采集，证据保存到 var/artifacts/host-bootstrap-UTC时间-PID，全部自动阶段通过后才生成 completed.json。不会启动系统服务、迁移数据库、重启 WSL、清理用户文件或修改驱动。

诊断分类的 4 项测试与原配置 9 项测试全部通过。测试中模拟握手/HTTP 状态仅验证分类，不替代上述普通 WSL 实测。Bootstrap 已通过 Bash 语法检查，并在当前受限会话按预期拒绝执行；PowerShell 宿主盘检查、安装与真实框架计算仍待普通 WSL 执行：

```bash
cd /home/hadoop/hadoop-lab
bash scripts/bootstrap-host.sh
```

若磁盘门禁失败，保留 host-storage.json，先确认空间安排再继续；脚本不自动释放空间。当前依赖未安装，W01/G-ENV 未通过，工程任务未宣告全部完成。
