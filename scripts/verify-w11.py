#!/usr/bin/env python3
"""Join historical real-run evidence with fresh W11 checks; missing gates stay open.

This is an evidence index, not a replacement for the referenced runtime verifiers.
Only a fully passing requirement/gate set may produce overall PASS.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
from xml.etree import ElementTree

from orchestration.recovery import verify_backup, physical_backup_gate


ROOT = Path(__file__).resolve().parents[1]


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--evidence-dir',required=True,type=Path)
    p.add_argument('--backup-dir',required=True,type=Path)
    p.add_argument('--output',required=True,type=Path)
    a=p.parse_args()
    evidence={}
    payloads={}

    def inspect(name,path,predicate):
        try:
            raw=path.read_bytes()
            value=json.loads(raw)
            passed=bool(predicate(value))
            payloads[name]=value
            state='NOT_RUN' if value.get('status')=='RUNNING' else 'PASS' if passed else 'FAIL'
            evidence[name]=dict(path=str(path.relative_to(ROOT)),sha256=hashlib.sha256(raw).hexdigest(),status=state)
        except FileNotFoundError:
            evidence[name]=dict(path=str(path),status='NOT_RUN')
        except (KeyError,ValueError,TypeError):
            evidence[name]=dict(path=str(path),status='FAIL',reason='Evidence schema/predicate mismatch')

    historical=ROOT/'docs/execution'
    inspect('W02',historical/'w02-acceptance-2026-10-20.json',lambda d:d['w02_complete'] and d['database_gate_complete'] and d['account_separation'])
    inspect('W03',historical/'w03-verification-2026-10-20.json',lambda d:d['w03_complete'] and not d['remaining_w03_requirements'])
    inspect('W04',historical/'w04-verification-2026-10-20.json',lambda d:d['status']=='PASS' and d['w04_complete'] and d['verification']['status']=='PASS')
    inspect('W05',historical/'w05-verification-2026-10-20.json',lambda d:d['status']=='PASS')
    inspect('W06',historical/'w06-verification-2026-10-21.json',lambda d:d['verification']['status']=='PASS' and len(d['cuda_training'])==2)
    inspect('W07',historical/'w07-verification-2026-10-21.json',lambda d:d['verification']['status']=='PASS' and len(d['verification']['benchmarks'])==6)
    inspect('W08',historical/'w08-verification-2026-10-21.json',lambda d:d['status']=='PASS_LOCAL_RECOVERY' and d['recovery']['status']=='PASS')
    directory=a.evidence_dir.resolve()
    inspect('API_PERFORMANCE',directory/'api/api-performance.json',lambda d:d['status']=='PASS' and d['database_unchanged'] and
            len(d['results'])==13 and all(r['concurrency']==5 and r['warmup_requests']==10 and r['requests']==100 and
            r['p95_seconds']<=2 and r['error_rate']<.01 and r['status']=='PASS' for r in d['results']))
    inspect('MR_PERFORMANCE',directory/'mr-final/mr-performance.json',lambda d:d['status']=='PASS' and d['disk_budget']['passed'] and
            len(d['results'])==9 and {(r['factor'],r['repeat']) for r in d['results']}=={(f,i) for f in (1,5,10) for i in (1,2,3)} and
            all(r['status']=='PASS' and r['application']['finalStatus']=='SUCCEEDED' and r['exact_integer_values_verified'] and
                r['synthetic']==(r['factor']>1) and r['application']['job_counters']['MAP_INPUT_RECORDS']==d['base_rows']*r['factor'] for r in d['results']))
    inspect('RELEASE',directory/'release-verify.json',lambda d:d['status']=='PASS' and len(d['tables'])==9)
    inspect('RECOVERY',directory/'recovery/recovery.json',lambda d:d['status']=='PASS' and d['isolated_server_stopped'] and d['verification']['status']=='PASS')
    inspect('HTTP',directory/'http.json',lambda d:d['status']=='PASS' and d['check_count']>=265 and d['before_tables']==d['after_tables'])
    inspect('BROWSER',directory/'browser.json',lambda d:d['status']=='PASS' and d['check_count']>=153 and not d['javascript_errors'])
    inspect('CUDA',directory/'cuda.json',lambda d:d['status']=='PASS' and d['matmul_and_backward'])

    def environment_ok(d):
        for c in d['checks']:
            if c['check']=='hdfs_datanodes':
                if not re.search(r'Live datanodes \([1-9][0-9]*\)',c['evidence']['output']):return False
            elif c['check']=='yarn_nodes':
                if not re.search(r'Total Nodes:\s*[1-9][0-9]*',c['evidence']['output']) or 'RUNNING' not in c['evidence']['output']:return False
            elif c['status']!='PASS':return False
        return True
    inspect('ENVIRONMENT',directory/'environment.json',environment_ok)
    inspect('REPORT',directory/'report/report.json',lambda d:len(d['eval_models'])==4 and len(d['fullfit_models'])==4 and
            len(d['metrics'])==16 and d['benchmark_summary']['comparison_valid'] and
            all(m['purpose']=='EVAL' for m in d['eval_models']) and all(m['purpose']=='FULLFIT' for m in d['fullfit_models']))
    frozen={name:hashlib.sha256((ROOT/'docs'/name).read_bytes()).hexdigest()==digest for name,digest in
            re.findall(r'\[([^\]]+\.md)\]\([^)]*\) \| `([0-9a-f]{64})`',(ROOT/'docs/BASELINE-MANIFEST.md').read_text())}
    evidence['FROZEN']=dict(status='PASS' if len(frozen)==18 and all(frozen.values()) else 'FAIL',files=frozen)
    try:
        raw=(directory/'pytest.xml').read_bytes()
        suites=ElementTree.fromstring(raw).findall('.//testsuite')
        counts={k:sum(int(s.attrib.get(k,0)) for s in suites) for k in ('tests','failures','errors','skipped')}
        evidence['REGRESSION']=dict(status='PASS' if counts['tests']>=195 and counts['failures']==counts['errors']==counts['skipped']==0 else 'FAIL',
                                    path=str(directory/'pytest.xml'),sha256=hashlib.sha256(raw).hexdigest(),**counts)
    except FileNotFoundError:
        evidence['REGRESSION']=dict(status='NOT_RUN')
    # Verify every archived byte again; historical existence is insufficient.
    try:
        archive=verify_backup(a.backup_dir)
        topology=physical_backup_gate(a.backup_dir)
        evidence['BACKUP_INTEGRITY']=dict(status='PASS',files=len(archive['files']),path=str(a.backup_dir),
                                        manifest_sha256=hashlib.sha256((a.backup_dir/'backup.json').read_bytes()).hexdigest())
        evidence['EXTERNAL_BACKUP']=dict(status='PASS' if topology['passed'] else 'NOT_RUN',**topology)
    except (OSError,ValueError,RuntimeError) as exc:
        evidence['BACKUP_INTEGRITY']=dict(status='FAIL',reason=type(exc).__name__)
        evidence['EXTERNAL_BACKUP']=dict(status='NOT_RUN')

    rid=payloads.get('RELEASE',{}).get('release_id')
    eid=payloads.get('API_PERFORMANCE',{}).get('experiment_id')
    # Evidence from a different release/experiment must never close this gate.
    for name in ('API_PERFORMANCE','RECOVERY','HTTP','BROWSER'):
        if name in payloads and (payloads[name].get('release_id')!=rid or
                payloads[name].get('experiment_id',eid)!=eid):
            evidence[name]['status']='FAIL'
            evidence[name]['reason']='Release/experiment mismatch'
    if 'RECOVERY' in payloads and 'RELEASE' in payloads:
        if payloads['RECOVERY']['verification']['tables']!=payloads['RELEASE']['tables']:
            evidence['RECOVERY']['status']='FAIL'
            evidence['RECOVERY']['reason']='Restored contents differ from the current release'

    def outcome(names):
        states=[evidence[n]['status'] for n in names]
        return 'FAIL' if 'FAIL' in states else 'NOT_RUN' if 'NOT_RUN' in states else 'PASS'

    dependencies={
        'T-01':['W03'],'T-02':['W03'],'T-03':['W03'],
        'T-04':['W04'],'T-05':['W04'],'T-06':['W04'],'T-07':['W04'],
        'T-08':['W05','W06'],'T-09':['W06','W07'],'T-10':['W05','W06'],
        'T-11':['W06','W07'],'T-12':['W06','W07','CUDA'],'T-13':['W07'],
        'T-14':['W08','RELEASE','REGRESSION'],'T-15':['HTTP'],'T-16':['BROWSER'],
        'T-17':['HTTP','REGRESSION'],'T-18':['API_PERFORMANCE','MR_PERFORMANCE'],
        'T-19':['W08','RELEASE','RECOVERY','BACKUP_INTEGRITY','REGRESSION'],
        'T-20':['REPORT','FROZEN','REGRESSION'],
    }
    tests={key:dict(status=outcome(names),evidence=names) for key,names in dependencies.items()}
    requirements={}
    for identifier, test_ids in re.findall(r'^\| ((?:FR|NF)-[0-9]{2})[^|]*\|[^|]*\|[^|]*\|([^|]+)\|', (ROOT/'docs/14-traceability.md').read_text(),re.M):
        refs=sorted(set(re.findall(r'T-[0-9]{2}',test_ids)))
        states=[tests[t]['status'] for t in refs]
        if identifier=='NF-08':states.append(evidence['FROZEN']['status'])
        if identifier in ('FR-12','NF-05'):states.append(evidence['EXTERNAL_BACKUP']['status'])
        requirements[identifier]=dict(status='FAIL' if 'FAIL' in states else 'NOT_RUN' if 'NOT_RUN' in states else 'PASS',
                                      tests=refs, extra_evidence=['EXTERNAL_BACKUP'] if identifier in ('FR-12','NF-05') else [])
    gate_dependencies={
        'G-DOC':['FROZEN','REPORT'],'G-ENV':['W02','ENVIRONMENT','CUDA'],
        'G-DATA':['W03','RELEASE'],'G-MR':['W04','MR_PERFORMANCE'],
        'G-MODEL':['W05','W06','W07','RELEASE'],'G-GPU':['W06','W07','CUDA'],
        'G-RELEASE':['W08','RELEASE'],'G-APP':['HTTP','BROWSER','API_PERFORMANCE'],
        'G-RECOVERY':['RECOVERY','BACKUP_INTEGRITY','EXTERNAL_BACKUP','REPORT'],
    }
    gates={key:dict(status=outcome(names),evidence=names) for key,names in gate_dependencies.items()}
    states=[r['status'] for r in [*tests.values(),*requirements.values(),*gates.values()]]
    complete=len(requirements)==20 and len(tests)==20 and all(s=='PASS' for s in states)
    report=dict(schema_version='1.0',scope='W11_VERIFICATION',baseline='HL-DOC-1.0.0',
                status='PASS' if complete else 'INCOMPLETE', project_complete=complete,
                observed_at=datetime.now(timezone.utc).isoformat(),
                code_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                release_id=rid,experiment_id=eid,tests=tests,requirements=requirements,gates=gates,evidence=evidence,
                policy='Historical W01-W10 evidence is retained with hashes; W11 revalidates immutable release, current HTTP/UI, resources and isolated restore. NOT_RUN is never PASS.')
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(status=report['status'],project_complete=complete,open_evidence={n:e['status'] for n,e in evidence.items() if e['status']!='PASS'})))
    return 0 if complete else 3


if __name__=='__main__':
    raise SystemExit(main())
