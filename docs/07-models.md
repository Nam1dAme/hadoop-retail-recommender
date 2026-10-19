# 07 推荐模型、训练与 GPU 实验

版本 HL-DOC-1.0.0，FROZEN。四种方法均为必做，不能用占位随机推荐替代ALS或NCF。

## 1. 共同训练数据与目录

正反馈为04定义的SALE、有效customer_id及合格商品，同客户同商品聚合不同订单数c_ui。退货不作负反馈；未购买只可作采样负例，不能声称是真实不喜欢。商品目录C为训练窗口内至少有一个有效客户购买过的合格商品；匿名购买不单独扩充C。用户集合U为训练窗口有至少一个合格购买的有效客户，购买历史H_u包含该窗口全部已购合格商品。

训练ID映射：customer_id与stock_code按UTF-8字节序排序，从0起连续int32；0是合法ID，不保留作未知。ALS要求int32；原始业务ID不直接强转或hash截断。映射文件字段entity_type、business_id、index；每个模型保存完整副本及hash。未知ID直接兜底，不用随机embedding。EVAL和FULLFIT映射独立，禁止跨模型复用索引。

文件特征：positive_interactions字段user_index,item_index,order_count,last_purchase；原始ID映射独立。无额外用户画像、类别、文本embedding输入；RFM只用于分析。负样本文件含user_index,item_index,label、epoch、seed；EVAL评估对全目录打分，不生成评价负采样文件。

## 2. 时间划分与评价

固定split `time-split-v1`：TRAIN来源日期<2011-09-01；VALIDATION=[2011-09-01,2011-11-01)；TEST=[2011-11-01,数据结束]。按来源日期过滤，一张订单不能跨split；原始文件不覆盖该时间范围则不能直接套用此基线，须变更协议。

四算法EVAL只在TRAIN训练。验证与测试时均使用TRAIN历史，**不在TEST前把验证购买偷偷加入模型或历史**。参数、阈值、默认算法只按VALIDATION选择；TEST在配置冻结后统一计算一次并封存。失败重试可重算同hash配置，必须保留失败记录；看过TEST后不再据其调参。

每个用户候选C_u=C−H_u；相关集合G_u为该评价窗口真实购买商品与C_u的交集（去重）。G_u为空不计排名指标，计入excluded_user_count及原因；不能把已购重复购买强行算推荐命中。WARM为U内且G_u非空的用户，ALL_ELIGIBLE包含窗口内有G_u的所有客户，未知用户走热门兜底。两种cohort四算法共用用户、候选、真实标签，禁止只为某模型过滤难例。candidate_policy_version固定full-catalog-v1。

K固定10，列表不足10时未填充位置按未命中。Precision@10=hits/10；Recall@10=hits/|G_u|；DCG@10=sum(rank命中/ log2(rank+1))，IDCG为min(10,|G_u|)个理想命中，NDCG=DCG/IDCG。先逐用户再宏平均。Coverage=该cohort推荐商品并集大小/|C|。无评价用户或C为空实验失败，不写0冒充已完成。window_user_count为该窗口有效客户且合格购买的不同用户数；cold_user_count为其中不在U的人数；cold_item_count为窗口合格购买中不在C的不同商品数；cold_interaction_count为这些目录外不同(customer,item)对数；excluded_user_count为窗口内G_u为空的人数。以上窗口质量计数在两cohort行重复保存以便解释，不属于cohort的排名分母。

全目录评分按用户与商品分块并维护TopK，禁止把全用户×全商品矩阵一次放进显存。主模型候选按score降序、stock_code字节序升序稳定排序；不足K时POP补足按热门顺序追加，不能因POP数值较大把兜底插到主模型结果之前。最终rank为唯一输出顺序，score保持其来源分数不作跨方法比较。confidence等分析解释不参与统一排序平局规则。

## 3. 四算法定义

### POP

商品分数=TRAIN内包含该商品的不同订单数（含匿名订单，但商品必须已在C中），按分数降序/商品ID升序。用户过滤已购后取Top10；为其他方法提供按同一窗口统计的补足序列。FULLFIT用全历史同规则重新生成，不复用EVAL热门榜。

### ITEMCF

相似度来自05 J07/J08，co_count≥3，邻居最多10。用户种子为按last_purchase降序、stock_code升序最近20个不同已购商品；候选分数=sum(log(1+c_ui)*similarity(i,j))。过滤已购，分数>0的候选排序，不足10以POP补足。商品详情页显示FULLFIT共现结果，页面“关联”允许出现已购商品，因为该页不是客户推荐。

### ALS

Spark3.5.7 ML ALS、implicitPrefs=true、nonnegative=true、rating=log(1+c_ui)、seed=42、maxIter=10、alpha=10。验证网格rank∈{16,32}、regParam∈{0.01,0.1}，共4组，其余参数不变。coldStartStrategy=drop只用于避免预测NaN，不等于解决冷启动；未知客户需POP。运行local[2]、driver memory2GiB、shuffle partitions4，独立于YARN资源池且不与MR/NCF重叠。

原始rating表示强度而非星级。按同一C_u分块评分，禁止把recommendForAllUsers的原始Top10不经已购过滤直接入库。模型序列化保存Spark元数据、因子、映射hash与参数。

### NCF（NeuMF）

参考[Neural Collaborative Filtering](https://arxiv.org/abs/1708.05031)的GMF+MLP组合；本项目不用预训练和复杂论文扩展，不宣称完全复现论文指标。

| 部件 | 固定设计 |
|---|---|
| GMF | 用户/商品各16维embedding，逐元素相乘得到16维 |
| MLP | 用户/商品各16维embedding，拼接32维，线性32→64→32→16，各隐藏层ReLU |
| 融合 | 拼接GMF16与MLP16得到32维，线性32→1输出logit |
| 损失 | BCEWithLogitsLoss，mean reduction；训练不先sigmoid |
| 优化 | Adam，weight_decay=1e-5；学习率验证候选{0.001,0.0005} |
| 初始化 | embedding N(0,0.01)，线性Xavier uniform，bias=0 |
| 训练 | 每个不同(user,item)每epoch一个正例，4个均匀负例；batch_size=1024；最多20epoch |
| 早停 | 验证WARM NDCG@10，连续3epoch无>1e-6提升停止；保留最优epoch |
| 精度 | FP32，关闭AMP和TF32，基线不使用自定义CUDA算子 |
| 随机性 | Python/NumPy/PyTorch CPU/CUDA seed=42，num_workers=0，记录确定性设置与库版本 |

负采样对每个用户从C−H_u有放回采4个；同一batch可重复负例。用户已买完整C则没有合法负例，从NCF训练样本中剔除并记录；不得采到已购正例，validation/test购买不用于训练采样排除，避免使用未来信息。epoch使用seed+epoch确定序列，CPU/GPU实验复用完全相同样本文件与batch顺序。训练样本没有合法负例或可评价WARM用户则训练门禁失败，不能用测试集绕过。

正式训练必须device=CUDA且张量、模型实际位于cuda；禁止静默回退CPU。score存logit用于排序，不宣传为校准购买概率；页面不跨算法比较分数绝对值。checkpoint只保存可信来源state_dict、结构配置、映射hash，不执行未知来源pickle。

## 4. 模型选择与服务训练

每算法选择验证WARM NDCG最高配置；差≤1e-6时依次按Recall高、参数规模小、预定义网格顺序决胜。experiment的champion同样按验证NDCG/Recall；再平局优先POP、ITEMCF、ALS、NCF。NCF未胜出不影响其训练验收，但默认服务算法可为简单基线。测试结果只作报告，不改变champion。

各算法FULLFIT在batch全部合法历史重新训练；ALS沿用验证选定超参与10迭代；NCF沿用选定学习率、结构和EVAL最优epoch数，不用全量未来评价早停。FULLFIT无VALIDATION/TEST指标，关联原experiment以展示其EVAL对照。POP/ItemCF也生成model_run与可追溯参数。

推荐对象覆盖全量customer_rfm客户；有模型历史者打分过滤全量已购，再按FULLFIT POP补足；无合格历史客户直接兜底。C_full为全量已知客户购买过的合格商品；不能推荐仅匿名客户买过的商品。所有已购、未知、重复、自关联和非目录商品过滤。每算法每客户最多10条，目录耗尽允许不足。

## 5. CUDA 与 CPU/GPU 对照

正式GPU验收要求09门禁通过。benchmark使用选定的EVAL NCF结构、学习率与TRAIN样本，另起独立初始化副本，固定5epoch、不早停、batch1024、FP32、相同seed、采样和顺序；不修改正式模型或重新选参。预热20个step后重置模型、优化器和随机状态，再计时；不足20step循环使用训练batch只作预热。

CPU/CUDA各3次独立重复，CPU线程固定4，GPU单卡。GPU计时开始/结束均cuda synchronize。主时间包括训练循环中的数据传输与forward/backward/optimizer，不含数据读取、负采样文件生成和预热；另记录端到端时间到实验附件。吞吐=5epoch实际训练样本处理总数/训练秒数。加速比=CPU三次中位数/GPU三次中位数。记录型号、显存、驱动、torch.version.cuda、Toolkit、操作系统、峰值显存及CPU内存口径。

峰值GPU内存为计时前reset_peak_memory_stats后torch.cuda.max_memory_allocated，单位MiB，不等于nvidia-smi整卡占用；CPU峰值为训练进程RSS高水位，单独标注，不与GPU分配内存相减比较。训练计时不含每epoch全目录验证，因为benchmark不早停；正式训练历史另记录含验证的epoch耗时。

小模型GPU可能更慢，必须如实报告，不以一定speedup>1作验收。CPU/GPU不要求浮点权重完全相等；两侧loss有限且实际下降、排序指标有效、负采样和超参一致。可额外做AMP实验，但不能替代这组固定FP32对照，也不能改变1.0发布模型。

## 6. 产物契约

每个model_run目录包含manifest.json、config.json、mapping.tsv、environment.lock、hardware.json和inputs.json。NCF模型为model.pt（仅state_dict）；ALS为spark_model/目录；POP/ITEMCF为rules.json及其特征文件hash引用。NCF训练历史为training_history.tsv；EVAL另有metrics.json。数据库与HDFS都记录同model_run_id，不允许只存一份无法解释来源的权重文件。这些是未来产物的文件契约，本轮不创建这些文件。

manifest.json必含schema_version、baseline_version、model_run_id、batch_id、experiment_id、algorithm、purpose、status、created_at、finished_at、code_commit、config_sha256、mapping_sha256、environment_sha256、inputs（path/sha256/rows数组）、artifacts（path/sha256/bytes数组）。config.json保存本节完整训练/评分参数；inputs.json保存窗口边界、正反馈/候选/用户hash及行数；hardware.json保存device、型号、显存、驱动/运行库/Toolkit版本、线程和确定性开关。metrics.json为对应06 model_metric行的数组，training_history.tsv字段epoch,train_loss,validation_ndcg,elapsed_seconds,peak_gpu_mib（FULLFIT验证字段NULL）。mapping.tsv字段entity_type,business_id,index，遵守04 TSV规则。

训练历史至少epoch、train_loss、validation_ndcg（EVAL）、elapsed_seconds、peak_gpu_mib；测试封存包括共同用户和候选集合hash。正式GPU训练结果不得来自CPU调试产物改名。
