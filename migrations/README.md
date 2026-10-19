# W02 存储与控制库

依据冻结基线 [06](../docs/06-database.md)。`001_initial.up.sql` 建立全部 22 张控制/结果表，迁移器额外建立 `schema_migration`；`current_release` 初始为空。所有业务外键 RESTRICT，业务标识二进制排序，金额 DECIMAL(20,2)，审计时间由应用显式写 UTC。

在项目根目录加载 `scripts/dev-env.sh` 后，统一入口为 `uv run --locked python -m orchestration.cli`（即 retailctl 的 Python 模块包装）。公共 `--config` 和 `--baseline` 必填；`--dry-run` 放子命令前。

```bash
source scripts/dev-env.sh
uv run --locked python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 --dry-run storage-init
uv run --locked python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 storage-init
```

HDFS 仅建立 `/retail/v1` 下七个命名空间，新增目录权限 750；已有目录只检查所有者和写权限。使用显式配置的 URI/Java/Hadoop 配置，不 format 或改写已有业务产物。

首次管理部署在显式获授权的管理 shell 执行：

```bash
# admin 的值替换为实际管理账号；RETAIL_DB_ADMIN_PASSWORD 由已有受限文件加载。
# 若使用 socket 认证，附加 --admin-socket /run/mysqld/mysqld.sock。
uv run --locked python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 db-provision --admin-user admin
```

命令拒绝已存在的库、账号及凭据文件，不修改原密码。它生成三个 localhost 专用账号，凭据只写 `var/secrets/{migration,writer,reader}.env`（目录 700、文件 600）；先给 migration 授权、备份和迁移，再给 writer/reader 最小权限。MariaDB DDL/GRANT 不能整体回滚，中途失败需保留已创建对象与私有文件，核对后手动续接；不自动删库、删用户或重复生成密码。reader 仅能查询结果及允许的控制字段，不能读取源 URL、进程 PID、本机日志路径和模型配置。writer 没有 DDL、GRANT 或迁移历史写权限。UI 不加载任何数据库凭据。

后续迁移只在独立离线 migration 进程中运行：

```bash
source var/secrets/migration.env
uv run --locked python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 db-status
uv run --locked python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 --dry-run db-migrate
uv run --locked python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 db-migrate
```

迁移先取得 schema 锁和 `retail-heavy-job` 连接锁，再核对有序历史、正反向文件联合 SHA-256 和表清单。执行前调用 `mariadb-dump --single-transaction`，dump 及校验凭据均为 600，备份路径与 SHA-256 单独记录于 `var/backups`；失败不执行 DDL。数据库中的 checksum 算法为 `SHA256(up_bytes + NUL + down_bytes)`。已应用文件不得修改。迁移器使用专用连接并清除旧读快照；部分 DDL 与历史不一致时拒绝重跑，需检查并从备份恢复。

`db-migrate --direction down` 仅供空库演练，拒绝含任何批次/业务数据的库；先备份再逆序删除最新版本涉及的表，保留迁移历史表。正式数据恢复、版本发布和 release 回滚属于 W08，不能用此命令代替。

批次、任务、实验接口位于 `orchestration/control.py`：来源 hash/策略幂等；失败任务重试保留 parent/attempt；终态不可改；心跳过期仅提示，外部句柄未经核实不改终态。成功任务/模型必须验证 manifest 和内容；实验严格关联四种 EVAL 模型，验证后选定，TEST 开始后不再选参，封存后禁止指标写入。模型训练/评价计算由 W05—W07 实现。

manifest 的 JSON 字段模型见 `orchestration/manifests.py`。run 声明 dataset 与按 04 顺序的 schema、inputs/outputs（path/sha256/rows）、分类计数、application_id 和 `_SUCCESS` 路径；model 声明 07 要求的身份、参数/映射/环境 hash 与完整文件清单。验证默认拒绝 synthetic、测试路径、非 SUCCEEDED、未知版本、关联漂移、路径越界/符号链接逃逸和内容 hash/行数/字节数不符。HDFS 通过真实 `hdfs dfs -cat/-test` 流式核验，目录型 Spark 模型需要展开列出文件。

```bash
uv run --locked python -m pytest -q
```

测试启动自己的 MariaDB 10.11 socket 实例，真实创建隔离角色/库并验证 PK/FK/CHECK、权限、迁移、回退重建、dump 恢复、命名锁和控制状态机；实例进程终止后删除自身临时目录。无 MariaDB 二进制时数据库测试 SKIP，不能计为通过。测试模型文件与数值只用于控制协议验证，不是模型训练或真实数据实验结果。正式库部署状态以 [W02 执行记录](../docs/execution/2026-10-20-w02.md) 为准。

## 已有数据库与受限客户端配置（当前部署方式）

用户已创建 `retail_analytics`，`codex` 为离线 migration 账号。使用显式私有 MariaDB `[client]` 文件接入，不将密码复制到代码或输出；文件必须归当前用户所有且无组/其他用户权限，文件中的 database 必须匹配项目配置。

```bash
source scripts/dev-env.sh
uv run --locked python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 --db-defaults-file "$HOME/.config/agent-db/client.cnf" db-status
uv run --locked python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 --db-defaults-file "$HOME/.config/agent-db/client.cnf" db-migrate
```

现有 migration 账号没有 CREATE USER/转授权权限时，先运行以下命令准备角色安装文件；重复运行会拒绝覆盖现有凭据，已生成时直接使用原文件。

```bash
uv run --locked python -m orchestration.cli --config config/development.toml --baseline HL-DOC-1.0.0 --db-defaults-file "$HOME/.config/agent-db/client.cnf" db-prepare-roles
```

`var/secrets/install-roles.sql` 和 `writer/reader.cnf`、`writer/reader.env` 均为 600。SQL 只创建两个新账号及表/列权限，不改变已有账号密码；同名用户存在会报错，不自动重置。由管理员在本机终端执行：

```bash
sudo mariadb < /home/hadoop/hadoop-lab/var/secrets/install-roles.sql
```

writer 另有 schema_migration 的 SELECT 权限用于版本检查，没有其写权限。reader 仅能读取协议允许的控制列和业务表。应用和任务分别显式使用相应角色凭据，UI 不持有任何数据库凭据。正式验收命令：

```bash
uv run --locked python scripts/verify-w02.py \
  --config config/development.toml \
  --migration-defaults-file "$HOME/.config/agent-db/client.cnf" \
  --writer-defaults-file var/secrets/writer.cnf \
  --reader-defaults-file var/secrets/reader.cnf \
  --output var/w02/deployment-current.json
```

退出 0 且 `database_gate_complete=true` 才表示真实三账号、迁移版本、23 张 InnoDB 表、最小表/列授权及拒绝写入等权限探针通过。权限探针不创建新对象，允许的 UPDATE 为同值更新并回滚；验证不在正式库保留模拟业务数据。读取控制状态时不再需要共享迁移账号密码给应用。
