"""Versioned run/model manifests and content verification for local files or HDFS."""
from datetime import datetime
import hashlib
from pathlib import Path, PurePosixPath
import subprocess
from typing import Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SHA = r'^[0-9a-f]{64}$'
BATCH = r'^B[0-9]{14}[0-9a-f]{8}$'


class ManifestError(ValueError):
    pass


class Strict(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)


class Input(Strict):
    path: str = Field(min_length=1)
    sha256: str = Field(pattern=SHA)
    rows: int = Field(ge=0)


class Artifact(Strict):
    path: str = Field(min_length=1)
    sha256: str = Field(pattern=SHA)
    bytes: int = Field(ge=0)


class Common(Strict):
    schema_version: Literal['1.0']
    baseline_version: Literal['HL-DOC-1.0.0']
    batch_id: str = Field(pattern=BATCH)
    status: Literal['SUCCEEDED']
    code_commit: str = Field(pattern=r'^[0-9a-f]{40}$')
    config_sha256: str = Field(pattern=SHA)
    created_at: str
    finished_at: str
    synthetic: bool = False

    @model_validator(mode='after')
    def times(self):
        values=[]
        for raw in (self.created_at,self.finished_at):
            if not raw.endswith('Z'):
                raise ValueError('Audit timestamp must use UTC Z')
            values.append(datetime.fromisoformat(raw.replace('Z','+00:00')))
        if values[1] < values[0]:
            raise ValueError('Finish precedes start')
        return self


class RunManifest(Common):
    run_id: str
    model_run_id: str | None = None
    dataset: str
    # Exact TSV columns from docs/04, or an explicit specialized schema identifier.
    dataset_schema: list[str] = Field(alias='schema', min_length=1)
    inputs: list[Input] = Field(min_length=1)
    outputs: list[Input] = Field(min_length=1)
    record_count: int = Field(ge=0)
    classification_counts: dict[str,int]
    application_id: str | None
    success_marker: str

    @field_validator('run_id','model_run_id')
    @classmethod
    def uuid(cls,value):
        if value is not None and str(UUID(value)) != value:
            raise ValueError('Noncanonical UUID')
        return value

    @model_validator(mode='after')
    def counts(self):
        if any(type(v) is not int or v < 0 for v in self.classification_counts.values()):
            raise ValueError('Invalid classification counts')
        if self.classification_counts and (set(self.classification_counts) != {'SALE','RETURN','REJECTED'} or sum(self.classification_counts.values()) != self.record_count):
            raise ValueError('Classification conservation failed')
        if sum(o.rows for o in self.outputs) != self.record_count:
            raise ValueError('Output row count differs')
        return self


class ModelManifest(Common):
    model_run_id: str
    experiment_id: str
    algorithm: Literal['POP','ITEMCF','ALS','NCF']
    purpose: Literal['EVAL','FULLFIT']
    mapping_sha256: str = Field(pattern=SHA)
    environment_sha256: str = Field(pattern=SHA)
    inputs: list[Input] = Field(min_length=1)
    artifacts: list[Artifact] = Field(min_length=1)

    @field_validator('model_run_id','experiment_id')
    @classmethod
    def uuid(cls,value):
        if str(UUID(value)) != value:
            raise ValueError('Noncanonical UUID')
        return value


# TSV schema names and exact column order are part of the frozen contract.
DATASETS = {
 'normalized':'row_id,source_row_number,invoice_no,stock_code,description,quantity,invoice_at,unit_price_pence,customer_id,country,parse_error',
 'clean':'row_id,source_row_number,invoice_no,stock_code,description,quantity,quantity_abs,invoice_at,source_date,unit_price_pence,signed_amount_pence,customer_id,country,record_class,is_merchandise',
 'rejected':'row_id,source_row_number,primary_reason', 'warnings':'row_id,warning_code',
 'sales_cube':'source_date,country,stock_code,gross_pence,return_pence,sale_quantity,return_quantity',
 'order_fact':'invoice_no,source_date,country,customer_id,gross_pence',
 'product_order_fact':'invoice_no,stock_code,source_date,country,gross_pence,quantity',
 'rfm':'customer_id,last_purchase,recency_days,frequency,monetary_pence,r_score,f_score,m_score,segment',
 'baskets':'invoice_no,customer_id,invoice_at,stock_code',
 'interactions':'customer_id,stock_code,order_count,last_purchase',
 'item_pairs':'item_i,item_j,co_count', 'item_frequency':'stock_code,basket_count',
 'similarity':'stock_code,related_stock_code,similarity,co_count,confidence,lift,rank',
 'recommendations':'model_run_id,customer_id,stock_code,rank,score,reason_code,is_fallback',
}


class LocalStore:
    def __init__(self, root):
        self.root = Path(root).resolve(strict=True)

    def resolve(self,path):
        value = Path(path)
        if not value.is_absolute() or not value.resolve(strict=True).is_relative_to(self.root):
            raise ManifestError('Artifact path escapes configured root')
        return value.resolve(strict=True)

    def chunks(self,path):
        with self.resolve(path).open('rb') as stream:
            while chunk := stream.read(1024*1024):
                yield chunk

    def exists(self,path):
        return self.resolve(path).is_file()


class HdfsStore:
    def __init__(self, root='/retail/v1', executable='/usr/local/hadoop/bin/hdfs', uri=None, environment=None):
        self.root = PurePosixPath(root)
        self.executable = executable
        self.command = [executable, 'dfs'] + (['-fs',uri] if uri else [])
        self.environment = environment

    def check(self,path):
        value = PurePosixPath(path)
        if not value.is_absolute() or '..' in value.parts or not value.is_relative_to(self.root) or any(ord(c)<32 for c in path):
            raise ManifestError('HDFS path escapes configured root')
        return path

    def chunks(self,path):
        process = subprocess.Popen([*self.command,'-cat',self.check(path)],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,env=self.environment)
        try:
            while chunk := process.stdout.read(1024*1024):
                yield chunk
            if process.wait(timeout=30):
                raise ManifestError('Cannot read HDFS artifact')
        finally:
            process.stdout.close()
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)

    def exists(self,path):
        return subprocess.run([*self.command,'-test','-f',self.check(path)],
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,env=self.environment,timeout=30).returncode == 0


def fingerprint(store,path):
    digest=hashlib.sha256(); size=rows=0; last=None
    for chunk in store.chunks(path):
        digest.update(chunk); size+=len(chunk); rows+=chunk.count(b'\n'); last=chunk[-1:]
    if last and last != b'\n':
        rows+=1
    return digest.hexdigest(),size,rows


def verify_manifest(data, store, expected, formal=True):
    """Expected association/hash values come from the control DB, not directory names."""
    try:
        if 'algorithm' in data:
            manifest = ModelManifest.model_validate(data)
            files = manifest.inputs + manifest.artifacts
            artifacts = {PurePosixPath(a.path).name:a for a in manifest.artifacts}
            required = {'config.json','mapping.tsv','environment.lock','hardware.json','inputs.json'}
            required |= {'model.pt','training_history.tsv'} if manifest.algorithm == 'NCF' else ({'rules.json'} if manifest.algorithm in ('POP','ITEMCF') else set())
            if manifest.purpose == 'EVAL':
                required.add('metrics.json')
            if not required <= artifacts.keys():
                raise ManifestError('Incomplete model artifact bundle')
            if manifest.algorithm == 'ALS' and not any('spark_model' in PurePosixPath(a.path).parts for a in manifest.artifacts):
                raise ManifestError('Missing Spark model files')
            for name,expected_hash in (('mapping.tsv',manifest.mapping_sha256),('environment.lock',manifest.environment_sha256),('config.json',manifest.config_sha256)):
                if artifacts[name].sha256 != expected_hash:
                    raise ManifestError('Model configuration/mapping/environment hash differs')
        else:
            manifest = RunManifest.model_validate(data)
            if manifest.dataset not in DATASETS or manifest.dataset_schema != DATASETS[manifest.dataset].split(','):
                raise ManifestError('Unknown or mismatched dataset schema')
            if PurePosixPath(manifest.success_marker).name != '_SUCCESS' or not store.exists(manifest.success_marker):
                raise ManifestError('Missing success marker')
            files = manifest.inputs + manifest.outputs
        if formal and manifest.synthetic:
            raise ManifestError('Synthetic artifacts cannot be formal inputs')
        for key,value in expected.items():
            if not hasattr(manifest,key) or getattr(manifest,key) != value:
                raise ManifestError('Manifest association mismatch: '+key)
        if not {'batch_id','config_sha256','code_commit'} <= expected.keys():
            raise ManifestError('Expected provenance must be explicit')
        paths=[f.path for f in files]
        if len(set(paths)) != len(paths):
            raise ManifestError('Duplicate artifact path')
        for file in files:
            if formal and any(part in ('synthetic','fixtures') for part in PurePosixPath(file.path).parts):
                raise ManifestError('Test artifacts cannot be formal inputs')
            digest,size,rows = fingerprint(store,file.path)
            if digest != file.sha256 or (isinstance(file,Artifact) and size != file.bytes) or (isinstance(file,Input) and rows != file.rows):
                raise ManifestError('Artifact hash/size/rows mismatch')
        return manifest
    except (ValueError,OSError) as exc:
        if isinstance(exc,ManifestError):
            raise
        raise ManifestError('Invalid manifest or inaccessible artifact') from None
