"""T18/T20: real isolated MariaDB export, evidence boundaries and thresholds."""
import hashlib
import json

import pytest

from orchestration.api_performance import percentile, summarize, snapshot
from orchestration.delivery import benchmark_summary, report
from orchestration.manifests import ManifestError
from test_api import api, release_fixture


def test_performance_counts_tail_latency_and_every_error():
    values = [dict(seconds=.1,ok=True) for _ in range(95)] + [dict(seconds=2.1,ok=True) for _ in range(5)]
    assert summarize(values)['status']=='PASS'  # nearest-rank P95 is sample 95
    values[0]['ok']=False
    assert summarize(values)['status']=='FAIL'  # exactly 1% is not <1%
    values[0]['ok']=True
    values[1]['seconds']=3
    assert summarize(values)['status']=='FAIL'
    assert percentile([1,2,3,4,5],.95)==5


def test_report_exports_selected_models_without_mutating_database(api,tmp_path):
    c=api['factory']()
    try:
        before=snapshot(c)
        target=tmp_path/'report'
        dry=report(c,{},api['eid'],api['rid'],target,dry_run=True)
        assert dry['selected_models']==4 and not target.exists()
        result=report(c,{},api['eid'],api['rid'],target)
        assert snapshot(c)==before
        assert result['scope']=='READ_ONLY_REPORT_EXPORT' and result['project_acceptance']=='NOT_EVALUATED'
        body=json.loads((target/'report.json').read_text())
        assert len(body['eval_models'])==len(body['fullfit_models'])==4
        assert {m['purpose'] for m in body['eval_models']}=={'EVAL'}
        assert {m['purpose'] for m in body['fullfit_models']}=={'FULLFIT'}
        assert body['benchmark_summary']['comparison_valid']
        assert '/private/secret.log' not in (target/'report.md').read_text()
        for name,digest in result['files'].items():
            assert hashlib.sha256((target/name).read_bytes()).hexdigest()==digest
        with pytest.raises(FileExistsError):
            report(c,{},api['eid'],api['rid'],target)
        with pytest.raises(ManifestError):
            report(c,{},'another-experiment',api['rid'],tmp_path/'invalid')
        assert not (tmp_path/'invalid').exists()
    finally:
        c.close()


def test_report_rejects_partial_or_inconsistent_cpu_gpu_comparison(api):
    with api['c'].cursor() as q:
        q.execute('SELECT * FROM gpu_benchmark WHERE model_run_id=%s',(api['ncf'],))
        rows=q.fetchall()
    assert benchmark_summary(rows)['comparison_valid']
    assert benchmark_summary(rows[:-1])['speedup'] is None
    rows[0]['dataset_sha256']='different'
    assert benchmark_summary(rows)['speedup'] is None


def test_report_cannot_attach_another_versions_acceptance(api,tmp_path):
    evidence=tmp_path/'acceptance.json'
    evidence.write_text(json.dumps(dict(status='PASS',release_id='other',experiment_id=api['eid'])))
    with pytest.raises(ManifestError):
        report(api['c'],{},api['eid'],api['rid'],tmp_path/'report',acceptance_file=evidence)
    assert not (tmp_path/'report').exists()


def test_report_rejects_unsubstantiated_overall_pass(api,tmp_path):
    evidence=tmp_path/'acceptance.json'
    evidence.write_text(json.dumps(dict(status='PASS',release_id=api['rid'],experiment_id=api['eid'])))
    with pytest.raises(ManifestError):
        report(api['c'],{},api['eid'],api['rid'],tmp_path/'report',acceptance_file=evidence)
    assert not (tmp_path/'report').exists()
