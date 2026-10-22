# W10 零售观察

中文 Streamlit 工作台，依据 `HL-DOC-1.0.0` 的 [页面规范](../docs/02-ux.md) 与 [API 契约](../docs/08-api.md)。七页全部通过 W09 HTTP 接口读取真实结果；不直连 MariaDB、不读 HDFS、不启动训练或发布。

## 启动

先在项目根目录启动只读 API（已经运行时不用重复启动）：

```bash
source scripts/dev-env.sh
uv run --locked python -m service \
  --config config/development.toml \
  --db-defaults-file var/secrets/reader.cnf
```

另一个终端启动页面：

```bash
source scripts/dev-env.sh
uv run --locked python -m app --config config/development.toml
```

Windows 浏览器打开 **http://localhost:8501**。API 与 UI 均仅监听回环地址；分别在各自终端按 `Ctrl+C` 停止。UI 启动器会从子进程环境移除项目数据库凭据及常用数据库密码变量，只传入显式项目配置路径；不要给页面传 writer/migration 凭据。

重新安装项目依赖使用 `uv sync --locked`。PyArrow 固定为 17.0.0，兼容项目的 NumPy 1.26.4；未升级 NumPy、PyTorch、CUDA 或 Spark。此前锁中的 PyArrow 26.0.0 在实际表格渲染时报“requires NumPy 2.0 or newer”，本轮已在真实导入与 DataFrame 展示中修复。背景依据见 [Arrow 17 安装与依赖说明](https://arrow.apache.org/docs/17.0/python/install.html#dependencies)，实际兼容性以本项目测试为准。

## 页面与演示路线

| 页面 | 可以现场操作的内容 |
|---|---|
| 总览 | 日期/国家应用与清空、精确金额与客单价、日趋势、Top20 交易项；图表与数据表切换 |
| 销售分析 | 日/月粒度、国家排行、销售额/销量排序、商品与国家明细分页 |
| 客户分层 | 五类分层、RFM 基准日、客户编号查找、分页，选择一行跳转客户推荐 |
| 商品关联 | 商品编码与 K、真实共购关系图、相似度/共现数/置信度/提升度表，404 与无关联空态 |
| 客户推荐 | 客户编号、四算法、K、原始排序与说明、未知客户热门兜底、候选耗尽与不足 K |
| 模型实验 | 实验编号/算法/评价窗口/人群，四算法指标、EVAL NCF 曲线、三次 CPU/CUDA 对照、FULLFIT 模型与曲线 |
| 数据质量与任务 | 批次/状态/run_id，分类守恒、缺失/重复/拒绝/截断指标、实际任务链、尝试/耗时/YARN 应用与脱敏错误 |

五分钟演示可从质量与任务开始，查看已完成的真实 YARN 记录，再到总览/销售与 RFM，最后比较推荐与模型实验。页面展示的是已有任务与训练记录；现场启动新作业须使用离线 CLI，不以页面刷新冒充重新训练。

## 版本与交互语义

- 页面首次读取 current 后固定 release。切页、改筛选、分页都不自动切换版本；顶部同时标识版本、批次、来源日期、GBP、source_local 和默认算法。
- “切换到当前版本”明确重新固定版本，并恢复各页筛选/客户/商品选择。网络失败保持原固定上下文并显示错误，不在后台悄悄混入新版本。
- 表单先编辑、再点“应用”；“清空”恢复该页默认条件。控件初始化与已应用条件分开存储，避免二次编辑因默认值变化而被覆盖。
- RFM 为完整历史快照，不跟随销售日期/国家重算。模型实验的 EVAL 指标与 FULLFIT 服务结果分开，模型编号按所选实验/固定版本关联。
- 金额以 Decimal 两位格式显示，图形坐标才转浮点；无分母显示“—”。空筛选不回退全量，推荐不足不补造已购或重复商品。
- 查询结果只在当前渲染内复用，不保留旧图表充当成功。只缓存固定版本的筛选词表/初始编号，切版本即清理；失败时展示请求 ID 和“重试查询”，不会重试作业。
- 无 release 时业务页禁用提交，但实验/任务仍可查看控制数据。版本退休提示显式切换；结果未完成显示未就绪，不补齐虚构曲线或加速比。
- 所有商品描述在 HTML 中转义；不存在真实姓名、头像、商品图片、点击/转化率或社交关系等来源中没有的数据。

可直接打开 `http://localhost:8501/?page=experiments` 等入口。slug 依次为 `overview/sales/customers/associations/recommendations/experiments/quality`。页面使用自定义七项导航；业务代码放在 `app/views/`，避免 Streamlit 把模块文件名自动显示成额外页面。

## 设计与文件

视觉方向与编码前复核见 [W10 设计记录](../docs/execution/2026-10-22-w10-design.md)。冷白/雾蓝工作区、连续指标带和蓝色销售时间轴；琥珀区分退货，青碧区分客户与验证序列，状态同时提供文字。字体通过本地 WOFF2 加载，不依赖外部 CDN；版权与子集摘要见 [assets](assets/README.md)。

- `client.py`：唯一 HTTP 取数入口、5 秒请求时限、固定版本检查与安全错误。
- `state.py`：版本切换、筛选/分页状态、精确金额、任务最新尝试汇总。
- `components.py`、`assets/theme.css`：表单、排版、空态/错误、可访问表格与响应式样式。
- `charts.py`：交互趋势、排行、分层、共购、质量指标与训练/benchmark 图。
- `views/`：七页按销售、客户、模型、任务四类职责分组。
- `main.py`、`__main__.py`：会话与七页导航、无数据库凭据的本机启动入口。

旧 `app/demo/` 是上一轮静态设计试稿，不是此应用的数据源或运行入口。

## 验证

UI 交互测试使用 Streamlit AppTest，经 HTTP transport 桥接 W09 TestClient，实际查询独立的 MariaDB 测试库。它不在正式数据库注入数据；覆盖版本变化/退休、无 release、错误恢复、两次筛选、空集、未知客户与模型不完整。

```bash
source scripts/dev-env.sh
uv run --locked python -m pytest tests/test_ui.py -q
```

真实浏览器测试需要一次安装开发用 Chromium（不影响运行 UI 所需依赖）：

```bash
source scripts/dev-env.sh
export PLAYWRIGHT_BROWSERS_PATH="$PWD/var/cache/playwright"
uv run --locked playwright install chromium --only-shell
```

API/UI 启动后执行：

```bash
PLAYWRIGHT_BROWSERS_PATH="$PWD/var/cache/playwright" \
uv run --locked python scripts/verify-w10.py \
  --config config/development.toml \
  --output var/w10/browser-verification.json
```

验证真实浏览器中的七页与交互，截图桌面/手机页面，并检查 1920/1440/1024/768/390/360 像素的布局。额外检查筛选按钮与输入控件底边及高度一致、侧栏收起后不占用正文宽度、正文在可用区域内居中，以及侧栏重新展开后的布局。GPU 禁用渲染，无新训练、发布或数据库写入。此验证属于 T-16，不替代 W11 性能、扩展规模与交付演示验收。

2026-10-22 布局修复的 13 项页面交互测试和 272 项浏览器检查均通过，见 [布局修复记录](../docs/execution/2026-10-22-ui-layout.md)。
