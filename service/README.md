# W09 只读 API

依据冻结基线 `HL-DOC-1.0.0` 的 [08 API 契约](../docs/08-api.md)，实现全部 20 个 GET 端点。服务读取 W08 已发布的 MariaDB 结果，不执行 Hadoop、模型训练、上传、发布或回滚；W10 页面尚未接入。

## 启动与查看

在项目根目录使用现有 reader 凭据启动，终端保持运行：

```bash
source scripts/dev-env.sh
uv run --locked python -m service \
  --config config/development.toml \
  --db-defaults-file var/secrets/reader.cnf
```

仅监听 `127.0.0.1:8000`。Windows 浏览器可打开 [交互接口文档](http://localhost:8000/docs)，它是 FastAPI 的接口调试页，不是 W10 业务页面。按 `Ctrl+C` 停止服务。

不提供凭据文件时，必须在该进程中显式设置 reader 对应的 `RETAIL_DB_USER/PASSWORD`。启动会核验凭据、权限及数据库迁移；writer/migration 或带写权限的账号会被拒绝。不要给 API 使用离线任务的凭据文件。

```bash
curl -sS http://127.0.0.1:8000/api/v1/health/ready
curl -sS http://127.0.0.1:8000/api/v1/releases/current
curl -sS --get http://127.0.0.1:8000/api/v1/summary \
  --data-urlencode 'release_id=R20261021104550ac6985a5' \
  --data-urlencode 'country=United Kingdom'
curl -sS --get http://127.0.0.1:8000/api/v1/customers/17850/recommendations \
  --data-urlencode 'release_id=R20261021104550ac6985a5' \
  --data-urlencode 'algorithm=ALS' \
  --data-urlencode 'k=5'
```

示例 release_id 是 W08 首发版本；实际使用先读取 `/releases/current`，再将其返回的 ID 固定携带在全部业务请求中。缺省 ID 每个请求只解析一次 current；准备新版本不会改变正在执行的查询。READY 的旧版本仍可查询。

## 已有 W02 部署的列权限补充

W09 要展示“日志/质量报告是否已登记”，reader 需要额外读取 `task_run.log_path` 与 `data_batch.quality_report_path` 两列。服务 SQL 只投影 `IS NOT NULL`，返回 `log_available/report_available` 布尔值，不返回原始路径，不读取对应文件。布尔值表示数据库存在登记，不是文件系统可用性探测。

新建隔离库或新部署时，`orchestration.provision` 已包含这两列的 SELECT。现有安装由具有转授权权限的管理员执行一次：

```bash
sudo mariadb < /home/hadoop/hadoop-lab/scripts/w09-reader-grants.sql
```

该脚本不改变密码、数据、表结构或写权限。既有 001 迁移及冻结文档保持原样。reader 仍不能读取来源 URL、进程 PID/启动身份等未授权列。

## 查询与错误语义

- 业务数据来自不可变 release；每次数据库请求使用独立的 REPEATABLE READ / READ ONLY 事务。任务、批次和实验端点独立于 current，拒绝 `release_id` 参数。
- 金额以两位小数字符串返回，客单价使用正向销售额除正向订单数并 HALF_UP 舍入；无订单时为 NULL。日期和国家筛选在 cube 与订单查询中一致；国家、客户和商品按精确值匹配，包括尾随空格差异。
- 商品订单数独立查询，不把订单明细与销售 cube 直接相乘；榜单先分页，再按当前页商品统计 distinct 订单，避免全量订单明细聚合超时。
- 推荐读取对应算法的 FULLFIT 结果；未知客户使用热门兜底，响应仍保留所选算法的 FULLFIT model_run_id。已购完整目录返回空列表；POP 自身不标为兜底。模型分数不能跨算法比较。
- 实验指标只读取 `experiment_model` 选定的 EVAL 模型，不读取最新候选或 FULLFIT 指标。TEST 未封存、指定四算法指标不齐、完全没有 benchmark 结果时返回 `RESULT_NOT_READY`。
- 已有部分 benchmark 记录时，返回现有记录与无效摘要，便于观察进度；缺少每端三次、样本/config hash 或比较参数不一致时，三个摘要数值均为 NULL，`comparison_valid=false`。此处遵循 08 的 benchmark summary 专项规则；无任何记录才返回 409。
- 成功和错误响应均有 UUID `X-Request-ID`；验证错误、404/405、依赖故障及内部异常使用统一 JSON 封装。ready 失败为 503，并额外带 `data.status=NOT_READY`。原始 worker 错误文本不外传，保留错误码及 run_id 排查入口。
- 拒绝未知/重复参数和 GET 请求体；排序仅接受固定枚举。每条 SQL 至多两秒，请求总预算五秒；超时返回 503，后台只读查询由 SQL/socket 时限结束并关闭连接，不重试写入。
- 默认不启用 CORS；不信任代理转发头；关闭包含原始 URL 的访问日志。诊断日志只记录 request_id、错误码和异常类型，避免记录 SQL、凭据或堆栈。

## 验证

契约、生命周期与权限测试使用会话启动的独立真实 MariaDB，数据不进入正式库或 HDFS；其中手算样例只是查询 fixture，不绕过发布质量门禁。

```bash
source scripts/dev-env.sh
uv run --locked python -m pytest tests/test_api.py tests/test_database.py tests/test_database_defaults.py -q
```

服务启动后，在另一个终端执行真实 HTTP 验证：

```bash
source scripts/dev-env.sh
uv run --locked python scripts/verify-w09.py \
  --config config/development.toml \
  --db-defaults-file var/secrets/reader.cnf \
  --output var/artifacts/w09-verification.json
```

验证器不导入 API 查询实现，使用独立 SQL 对照 20 个端点、四算法推荐、EVAL 指标、训练曲线和六次 benchmark。它比较前后 23 张表全部 reader 可见列的逐行摘要；未授权的控制列不在摘要范围内。另含一次 5 并发、15 请求的列表/推荐短测，用于 W09 查询回归，不替代 W11 全套 T-18 性能与资源验收。该过程不在正式库创建 fixture 或重训模型。
