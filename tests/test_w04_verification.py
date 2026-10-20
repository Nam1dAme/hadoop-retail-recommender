"""Reject broken provenance even when the files themselves have valid hashes."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import pytest
from orchestration.manifests import DATASETS,ManifestError

spec=importlib.util.spec_from_file_location('verify_w04',Path(__file__).resolve().parents[1]/'scripts/verify-w04.py')
verifier=importlib.util.module_from_spec(spec);spec.loader.exec_module(verifier)


def test_input_bindings_and_parameter_schema():
    files=[{'path':'/retail/v1/clean/batch_id=isolated/run_id=isolated/clean/part-r-00000','sha256':'a'*64,'rows':2}]
    manifest={'inputs':files,'execution':{'reducers':2}}
    params={'stage':'J02','inputs':deepcopy(files),'schemas':{'order_fact':DATASETS['order_fact']},'reducers':2}
    verifier.check_stage_inputs('J02',manifest,params,files)
    other=deepcopy(files);other[0]['path']='/retail/v1/clean/another-run/part-r-00000'
    with pytest.raises(ManifestError):verifier.check_stage_inputs('J02',manifest,params,other)
    broken=deepcopy(params);broken['inputs'][0]['rows']=3
    with pytest.raises(ManifestError):verifier.check_stage_inputs('J02',manifest,broken,files)
    broken=deepcopy(params);broken['schemas']['order_fact']='wrong'
    with pytest.raises(ManifestError):verifier.check_stage_inputs('J02',manifest,broken,files)
    broken=deepcopy(params);broken['reducers']=1
    with pytest.raises(ManifestError):verifier.check_stage_inputs('J02',manifest,broken,files)


def test_preflight_reconciliation_has_zero_tolerance():
    totals={'gross_pence':2100,'return_pence':300,'sale_quantity':11,'return_quantity':1}
    verifier.reconcile_preflight(totals,{'preflight_totals':totals})
    for key in totals:
        changed=totals|{key:totals[key]+1}
        with pytest.raises(ManifestError):verifier.reconcile_preflight(changed,{'preflight_totals':totals})
