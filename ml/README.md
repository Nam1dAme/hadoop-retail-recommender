# 实验数据与推荐模型

引用冻结基线 HL-DOC-1.0.0 的 04/05/07/11/12。W05 负责时间切分、ID 映射、共同候选/cohort、指标和测试封存的输入；W06 负责四算法训练、保存加载及验证选参。

## W05 实验数据

```bash
source scripts/dev-env.sh
uv run --locked --no-sync python -m orchestration.cli \
  --config config/development.toml --baseline HL-DOC-1.0.0 \
  --db-defaults-file var/secrets/writer.cnf \
  prepare-experiment --batch-id B20261019194025b4f65854 \
  --experiment-id <UUID>
```

首次先生成并保存一个 UUID；同一实验所有后续调用使用该 ID。公共选项 `--dry-run` 放在子命令前，仅检查依赖和计划。正式准备要求工作树干净，且已经构建当前 Java JAR。

训练窗来源日期 <2011-09-01；验证窗 [2011-09-01,2011-11-01)；测试窗自 2011-11-01 起。TRAIN 在 clean/order Mapper 中先过滤，再独立运行 J06baskets/J06interactions/J07baskets/J07/J08 五个真实 YARN 应用。完整 W04 篮子另作磁盘 SQLite 独立切分，并逐对核对 TRAIN YARN 交互。共现的 100 商品截断不影响完整正反馈或历史。

EVAL/FULLFIT 各有自己的 mapping.tsv、positive_interactions.tsv、popularity.tsv、baskets.tsv；映射从 0 连续 int32，按 UTF-8 字节序。匿名订单可给已有目录商品贡献热门频次，但不扩充目录。VALIDATION/TEST 分别保存 truth.tsv 和 cohorts.json，包含共同用户、目录和候选哈希、冷启动及排除计数。候选哈希使用目录、cohort 用户及每用户已购排除的规范编码，避免展开用户商品笛卡尔矩阵。测试标签不作为训练和负采样输入。

HDFS 正式产物位于 features/batch_id=…/run_id=…；数据文件采用 `eval.mapping.tsv` 等带用途前缀的名称，manifest 给出完整路径和 hash。每个模型还必须保存自身 mapping.tsv 副本，不能跨用途复用索引。重复准备校验成功产物后返回同一 run；失败 attempt 保留，恢复先检查原进程与 YARN 句柄。

`ml/evaluation.py` 统一分块全目录评分、已购过滤、Top10、热门补足和宏平均指标。主模型列表完成后再追加兜底；未知用户直接热门。短列表 Precision 的分母仍为 10。空 cohort 或空目录报错，不能用零分冒充完成。

## W06 当前实现边界

`ml/models.py` 提供 POP/ItemCF 规则保存加载、Spark ALS 隐式训练及模型保存加载、分块因子评分。`ml/sampling.py` 仅从 TRAIN 目录减 TRAIN 已购采样，seed+epoch 决定样本和顺序。正式 NCF、评价编排、四算法真实训练与 T-09/11/12 的完成状态，以后续执行记录为准。W07 的 CPU/GPU 三次对照、FULLFIT 和全量服务推荐不由 W05 完成。
