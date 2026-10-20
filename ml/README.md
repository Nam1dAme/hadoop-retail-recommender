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

## W06 四算法评价

```bash
uv run --locked --no-sync python -m orchestration.cli \
  --config config/development.toml --baseline HL-DOC-1.0.0 \
  --db-defaults-file var/secrets/writer.cnf \
  evaluate --batch-id B20261019194025b4f65854 --experiment-id <同一UUID>
```

`ml/models.py` 提供 POP/ItemCF 规则保存加载、Spark ALS 隐式训练及模型保存加载、分块因子评分。`ml/sampling.py` 仅从 TRAIN 目录减 TRAIN 已购采样，seed+epoch 决定样本和顺序。`ml/ncf.py` 实现固定 NeuMF、FP32 CUDA 正式训练、验证早停、可信 state_dict 保存加载；每 epoch 记录实际 loss、验证 NDCG、耗时和峰值分配显存，保留实际采样文件。

`orchestration/training.py` 串行运行 POP/ItemCF 各一组、ALS 四组、NCF 两组。主进程持有数据库逻辑锁并持续心跳，独立子进程执行训练；未知存活句柄阻止重提。候选按验证 WARM NDCG/Recall/规模/网格顺序选参，champion 再按固定算法优先级决胜。选择清单先持久化，随后四算法使用共同 TEST cohort 统一评价并封存；同配置重试保留失败记录，不覆盖成功模型或已写指标。

模型包位于 models/model_run_id=…，包括 config.json、mapping.tsv、环境、硬件、输入、源码快照和训练产物。模型 metrics.json 保存模型验收时的验证指标；后续 TEST 是独立、不可变的实验任务附件，由实验报告关联，数据库汇集每个选定模型的四行 VALIDATION/TEST 指标。HDFS 批量上传后重新下载并核验全部原路径/hash，避免每个小文件单独启动 JVM；快照缓存仅在本次调用内使用。

W05 独立验收：`python scripts/verify-w05.py --config … --db-defaults-file … --experiment-id … --output …`，直接从 clean SALE 重建两套映射、购买交互、热门和窗口标签。W06 独立验收使用相同参数的 `scripts/verify-w06.py`，检查八个候选、实际 Spark 参数、CUDA 图与训练历史、每 epoch 负样本、验证选择和封存指标。真实训练及 T-08/09/10/11/12 的完成状态，以 execution 记录为准。W07 的 CPU/GPU 三次对照、FULLFIT 和全量服务推荐仍独立实施。
