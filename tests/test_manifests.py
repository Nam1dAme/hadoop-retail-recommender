"""Adversarial artifact/provenance checks without fabricated production metrics."""
import copy
import hashlib
from uuid import uuid4
import pytest
from orchestration.manifests import verify_manifest,LocalStore,HdfsStore,ManifestError
from test_database import run_manifest,H,C


def test_manifest_success_and_tampering(tmp_path):
    bid='B20261020000000abcdef12'; rid=str(uuid4())
    data,path=run_manifest(tmp_path,bid,rid)
    expected=dict(batch_id=bid,run_id=rid,config_sha256=H,code_commit=C)
    store=LocalStore(tmp_path)
    assert verify_manifest(data,store,expected).run_id==rid
    for change in (
        {'schema_version':'2.0'}, {'baseline_version':'other'}, {'synthetic':True},
        {'status':'FAILED'}, {'record_count':2}, {'schema':['wrong']},
        {'classification_counts':{'SALE':2,'RETURN':0,'REJECTED':0}},
        {'finished_at':'2026-10-19T00:00:00Z'}, {'created_at':'2026-10-20T00:00:00+08:00'},
        {'batch_id':'B20261020000000abcdef13'}, {'extra':True}):
        broken=copy.deepcopy(data); broken.update(change)
        with pytest.raises(ManifestError): verify_manifest(broken,store,expected)
    with pytest.raises(ManifestError): verify_manifest(data,store,{'batch_id':bid})
    output=tmp_path/'part-00000'; output.write_bytes(b'tampered\n')
    with pytest.raises(ManifestError): verify_manifest(data,store,expected)


def test_missing_success_bad_rows_and_symlink_escape(tmp_path):
    bid='B20261020000000abcdef12'; rid=str(uuid4())
    directory=tmp_path/'allowed'
    data,path=run_manifest(directory,bid,rid)
    expected=dict(batch_id=bid,run_id=rid,config_sha256=H,code_commit=C)
    store=LocalStore(directory)
    (directory/'_SUCCESS').unlink()
    with pytest.raises(ManifestError): verify_manifest(data,store,expected)
    (directory/'_SUCCESS').touch()
    data['inputs'][0]['rows']=2
    with pytest.raises(ManifestError): verify_manifest(data,store,expected)
    data['inputs'][0]['rows']=1
    outside=tmp_path/'outside'; outside.write_bytes(b'output\n')
    (directory/'part-00000').unlink(); (directory/'part-00000').symlink_to(outside)
    with pytest.raises(ManifestError): verify_manifest(data,store,expected)
    hdfs=HdfsStore()
    for path in ('/retail/v1/../private','/retail/v11/other','relative','hdfs://elsewhere/retail/v1/file'):
        with pytest.raises(ManifestError): hdfs.check(path)
