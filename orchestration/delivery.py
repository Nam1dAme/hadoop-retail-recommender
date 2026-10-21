"""Read-only experiment/report export; never trains or changes release state."""
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from statistics import median
import subprocess

from orchestration.manifests import ManifestError
from orchestration.provision import READ_COLUMNS


ROOT = Path(__file__).resolve().parents[1]


def benchmark_summary(rows):
    identities = {(r['device'], r['repeat_index']) for r in rows}
    expected = {(d, i) for d in ('CPU', 'CUDA') for i in (1, 2, 3)}
    valid = len(rows) == 6 and identities == expected
    for key in ('dataset_sha256','config_sha256','batch_size','epochs','seed','warmup_steps'):
        valid = valid and len({r[key] for r in rows}) == 1
    valid = valid and all(math.isfinite(r['train_seconds']) and r['train_seconds'] > 0 for r in rows)
    if not valid:
        return dict(comparison_valid=False, cpu_median_seconds=None, gpu_median_seconds=None, speedup=None)
    cpu = median(r['train_seconds'] for r in rows if r['device']=='CPU')
    gpu = median(r['train_seconds'] for r in rows if r['device']=='CUDA')
    return dict(comparison_valid=True, cpu_median_seconds=cpu, gpu_median_seconds=gpu, speedup=cpu/gpu)


def report(connection, config, experiment_id, release_id, directory, *, dry_run=False, acceptance_file=None):
    def query(sql, params=()):
        with connection.cursor() as q:
            q.execute(sql, params)
            return list(q.fetchall())

    connection.rollback()
    with connection.cursor() as q:
        q.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY')
    try:
        releases = query('SELECT * FROM data_release WHERE release_id=%s', (release_id,))
        if not releases or releases[0]['status'] != 'READY' or releases[0]['experiment_id'] != experiment_id:
            raise ManifestError('Report requires a matching READY release and experiment')
        release = releases[0]
        experiment = query('SELECT '+READ_COLUMNS['experiment']+' FROM experiment WHERE experiment_id=%s', (experiment_id,))[0]
        if experiment['status'] != 'SUCCEEDED' or not experiment['test_sealed_at']:
            raise ManifestError('Report requires a successful TEST-sealed experiment')
        batch = query('SELECT '+READ_COLUMNS['data_batch']+' FROM data_batch WHERE batch_id=%s', (release['batch_id'],))[0]
        model_columns = ','.join('m.'+c for c in READ_COLUMNS['model_run'].split(','))
        models = query('SELECT '+model_columns+''' FROM experiment_model e JOIN model_run m USING(model_run_id)
            WHERE e.experiment_id=%s ORDER BY m.algorithm''', (experiment_id,))
        fullfit = query('SELECT '+model_columns+''' FROM release_model r JOIN model_run m USING(model_run_id)
            WHERE r.release_id=%s ORDER BY m.algorithm''', (release_id,))
        if ({m['algorithm'] for m in models} != {'POP','ITEMCF','ALS','NCF'} or len(models) != 4
                or {m['algorithm'] for m in fullfit} != {'POP','ITEMCF','ALS','NCF'} or len(fullfit) != 4
                or any(m['purpose']!='EVAL' or m['status']!='SUCCEEDED' for m in models)
                or any(m['purpose']!='FULLFIT' or m['status']!='SUCCEEDED' for m in fullfit)):
            raise ManifestError('Report model bindings are incomplete')
        metrics = query('''SELECT m.algorithm,v.* FROM experiment_model e JOIN model_run m USING(model_run_id)
            JOIN model_metric v USING(model_run_id) WHERE e.experiment_id=%s ORDER BY m.algorithm,v.split_name,v.cohort''', (experiment_id,))
        ncf = next(m['model_run_id'] for m in models if m['algorithm']=='NCF')
        benchmarks = query('SELECT * FROM gpu_benchmark WHERE model_run_id=%s ORDER BY device,repeat_index', (ncf,))
        history = query('''SELECT m.algorithm,m.purpose,h.* FROM model_epoch h JOIN model_run m USING(model_run_id)
            WHERE h.model_run_id IN (%s,%s) ORDER BY m.purpose,h.epoch''',
                        (ncf, next(m['model_run_id'] for m in fullfit if m['algorithm']=='NCF')))
        totals = query('''SELECT SUM(gross_amount) gross_gbp,SUM(return_amount) returns_gbp,
            SUM(gross_amount-return_amount) net_gbp,SUM(sale_quantity) sale_quantity,SUM(return_quantity) return_quantity
            FROM sales_cube WHERE release_id=%s''', (release_id,))[0]
        counts = dict(orders=query('SELECT COUNT(*) n FROM sale_order WHERE release_id=%s',(release_id,))[0]['n'],
                      customers=query('SELECT COUNT(*) n FROM customer_rfm WHERE release_id=%s',(release_id,))[0]['n'])
        recommendation_counts = query('SELECT algorithm,COUNT(*) rows_count,COUNT(DISTINCT customer_id) customers FROM customer_recommendation WHERE release_id=%s GROUP BY algorithm ORDER BY algorithm', (release_id,))
        tasks = query('SELECT '+READ_COLUMNS['task_run']+' FROM task_run WHERE batch_id=%s ORDER BY created_at,run_id', (release['batch_id'],))
        events = query('SELECT * FROM release_event WHERE from_release_id=%s OR to_release_id=%s ORDER BY created_at', (release_id,release_id))
    finally:
        connection.rollback()
    result = dict(schema_version='1.0', baseline='HL-DOC-1.0.0', generated_at=datetime.now(timezone.utc).isoformat(),
                  code_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                  environment_sha256=hashlib.sha256((ROOT/'uv.lock').read_bytes()).hexdigest(),
                  release=release, experiment=experiment, batch=batch, eval_models=models, fullfit_models=fullfit,
                  metrics=metrics, benchmarks=benchmarks, benchmark_summary=benchmark_summary(benchmarks),
                  training_history=history, sales=totals, counts=counts, recommendation_counts=recommendation_counts,
                  tasks=tasks, release_events=events, source_license='CC BY 4.0',
                  evaluation_scope='Only selected EVAL models have VALIDATION/TEST metrics; FULLFIT recommendations use all historical data.',
                  synthetic_scope='MR scaling copies are performance-only and excluded from these real-data metrics and release.',
                  project_acceptance='NOT_EVALUATED', limitations=[
                      'Historical UCI transactions, offline batches; refresh queries an immutable release, not live order ingestion.',
                      'Single WSL host and HDFS replication 1; external physical-disk backup must be verified separately.',
                      'No clicks, categories or explicit dislikes; sampled negatives are not observed negative feedback.',
                      'CPU/CUDA results apply only to the recorded model, precision, data and hardware; GPU can be slower.',
                      'One production READY release currently cannot demonstrate rollback to a different production release; use isolated two-version tests.'
                  ])
    if acceptance_file:
        acceptance_path = Path(acceptance_file).resolve(strict=True)
        acceptance = json.loads(acceptance_path.read_text())
        if acceptance.get('release_id') != release_id or acceptance.get('experiment_id') != experiment_id:
            raise ManifestError('Acceptance evidence belongs to another release/experiment')
        result['acceptance_evidence'] = dict(path=str(acceptance_path), sha256=hashlib.sha256(acceptance_path.read_bytes()).hexdigest(), report=acceptance)
        result['project_acceptance'] = acceptance['status']
    if dry_run:
        return dict(dry_run=True, experiment_id=experiment_id, release_id=release_id,
                    output_directory=str(directory), selected_models=len(models), metric_rows=len(metrics))
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    (directory/'report.json').write_text(json.dumps(result,default=str,ensure_ascii=False,indent=2)+'\n')
    with (directory/'metrics.csv').open('w',newline='') as stream:
        writer = csv.DictWriter(stream,fieldnames=list(metrics[0]) if metrics else ['algorithm'])
        writer.writeheader(); writer.writerows(metrics)
    with (directory/'benchmarks.csv').open('w',newline='') as stream:
        fields = ['device','repeat_index','train_seconds','samples_per_second','peak_memory_mib','hardware']
        writer = csv.DictWriter(stream,fieldnames=fields,extrasaction='ignore')
        writer.writeheader(); writer.writerows(benchmarks)
    lines = ['# 零售分析与推荐：真实运行报告','',
             f'- 发布版本：`{release_id}`；批次：`{release["batch_id"]}`；实验：`{experiment_id}`。',
             f'- 数据：[UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail)，CC BY 4.0；源 SHA-256：`{batch["source_sha256"]}`。',
             f'- 环境锁：`{result["environment_sha256"]}`；报告代码：`{result["code_commit"]}`。',
             f'- 完整项目验收状态：`{result["project_acceptance"]}`；生成报告本身不代表全门禁通过。','',
             '## 销售与推荐','',
             f'正向销售 £{totals["gross_gbp"]:,.2f}，退货 £{totals["returns_gbp"]:,.2f}，净额 £{totals["net_gbp"]:,.2f}。',
             f'正向订单 {counts["orders"]:,}，客户 {counts["customers"]:,}；默认算法 {experiment["champion_algorithm"]}。','',
             '## 四算法 TEST 指标（WARM）','',
             '| 算法 | Precision@10 | Recall@10 | NDCG@10 | Coverage@10 |',
             '|---|---:|---:|---:|---:|']
    for row in metrics:
        if row['split_name']=='TEST' and row['cohort']=='WARM':
            lines.append('| '+row['algorithm']+' | '+' | '.join(f'{row[k]:.6f}' for k in ('precision_at_k','recall_at_k','ndcg_at_k','coverage'))+' |')
    summary = result['benchmark_summary']
    lines += ['', '## 实际 GPU 对照','']
    if summary['comparison_valid']:
        lines.append(f'CPU/CUDA 五轮训练中位数 {summary["cpu_median_seconds"]:.3f}/{summary["gpu_median_seconds"]:.3f} 秒；CPU÷GPU 为 {summary["speedup"]:.4f}×。')
    else:
        lines.append('对照记录不足或配置不一致，不生成加速比。')
    lines += ['', '## 口径与局限','',
              '指标来自选定 EVAL 模型；FULLFIT 使用全历史生成服务推荐，不是 TEST 预测。合成扩展只衡量性能。',
              '页面为已发布数据的动态查询；更新数据需 CLI 完成导入、计算、训练、发布后明确切换版本。',
              '本机恢复不替代独立物理盘备份。单节点系统不提供跨机器容灾保证。',
              '本次 GPU 比 CPU 慢；单机、固定配置实验不推导商业推荐效果。','',
              '完整指标、硬件、任务、曲线和版本关联见同目录 report.json；部署和演示见 docs/delivery.md。','']
    (directory/'report.md').write_text('\n'.join(lines))
    files = {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(directory.iterdir())}
    (directory/'manifest.json').write_text(json.dumps(dict(files=files,release_id=release_id,experiment_id=experiment_id),indent=2)+'\n')
    return dict(status='PASS', scope='READ_ONLY_REPORT_EXPORT', project_acceptance=result['project_acceptance'],
                release_id=release_id, experiment_id=experiment_id, output_directory=str(directory), files=files)
