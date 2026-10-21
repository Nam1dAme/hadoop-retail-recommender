"""Typed HTTP responses; money is never serialized through binary floats."""
from datetime import date, datetime, timezone
from typing import Annotated, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer
from service.parameters import Algorithm, Segment, TaskStatus

Money = Annotated[str, Field(pattern=r'^-?[0-9]+\.[0-9]{2}$')]
AuditTime = Annotated[datetime, PlainSerializer(
    lambda t: t.replace(tzinfo=timezone.utc).isoformat().replace('+00:00', 'Z'), return_type=str)]
T = TypeVar('T')


class Model(BaseModel):
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False)


class Meta(Model):
    request_id: str
    schema_version: Literal['1.0'] = '1.0'
    release_id: str | None = None
    batch_id: str | None = None


class Response(Model, Generic[T]):
    data: T
    meta: Meta


class ErrorBody(Model):
    code: str
    message: str
    details: dict[str, str]


class ErrorResponse(Model):
    error: ErrorBody
    meta: Meta


class Page(Model, Generic[T]):
    items: list[T]
    page: int
    page_size: int
    total: int
    has_next: bool


class Live(Model):
    status: Literal['UP']
    service_version: str


class Ready(Model):
    status: Literal['READY']


class NotReady(Model):
    status: Literal['NOT_READY']


class ReadinessError(ErrorResponse):
    data: NotReady


class Release(Model):
    release_id: str
    batch_id: str
    experiment_id: str
    champion_algorithm: Algorithm
    source_start: date | None
    source_end: date | None
    currency: Literal['GBP'] = 'GBP'
    published_at: AuditTime
    baseline_version: str


class ReleaseEntry(Release):
    status: Literal['READY']


class Summary(Model):
    currency: Literal['GBP'] = 'GBP'
    gross_amount: Money
    return_amount: Money
    net_amount: Money
    order_count: int
    customer_count: int
    average_order_value: Money | None
    sale_quantity: int
    return_quantity: int


class TrendPoint(Model):
    period_start: date
    gross_amount: Money
    return_amount: Money
    net_amount: Money
    order_count: int


class Trend(Model):
    currency: Literal['GBP'] = 'GBP'
    points: list[TrendPoint]


class Country(Model):
    country: str
    currency: Literal['GBP'] = 'GBP'
    gross_amount: Money
    return_amount: Money
    net_amount: Money
    order_count: int


class Product(Model):
    stock_code: str
    description: str | None
    currency: Literal['GBP'] = 'GBP'
    gross_amount: Money
    return_amount: Money
    net_amount: Money
    sale_quantity: int
    return_quantity: int
    order_count: int
    is_merchandise: bool


class Customer(Model):
    customer_id: str
    last_purchase: date
    as_of_date: date
    recency_days: int
    frequency: int
    monetary: Money
    currency: Literal['GBP'] = 'GBP'
    r_score: int
    f_score: int
    m_score: int
    segment: Segment


class SegmentCount(Model):
    segment: Segment
    customer_count: int


class Segments(Model):
    as_of_date: date | None
    items: list[SegmentCount]


class SimilarItem(Model):
    related_stock_code: str
    description: str | None
    rank: int
    similarity: float
    co_count: int
    confidence: float
    lift: float


class Similar(Model):
    stock_code: str
    description: str | None
    items: list[SimilarItem]


class RecommendationItem(Model):
    stock_code: str
    description: str | None
    rank: int
    score: float
    reason_code: Literal['POPULAR', 'CO_PURCHASE', 'LATENT_FACTOR', 'NEURAL_CF', 'FALLBACK_POPULAR']
    reason_text: str
    is_fallback: bool


class Recommendations(Model):
    customer_id: str
    known_customer: bool
    algorithm: Algorithm
    model_run_id: str
    eligible_candidates: int
    returned_count: int
    fallback_used: bool
    items: list[RecommendationItem]


class Batch(Model):
    batch_id: str
    status: Literal['REGISTERED', 'INGESTING', 'PROCESSING', 'SUCCEEDED', 'FAILED']
    source_name: str
    source_sha256: str
    policy_version: str
    raw_rows: int | None
    created_at: AuditTime
    updated_at: AuditTime


class QualityMetric(Model):
    metric_name: str
    metric_value: int
    description: str


class Quality(Model):
    batch_id: str
    status: str
    metrics: list[QualityMetric]
    report_available: bool


class Run(Model):
    run_id: str
    batch_id: str
    task_name: str
    attempt: int
    status: TaskStatus
    started_at: AuditTime | None
    finished_at: AuditTime | None
    application_id: str | None
    error_code: str | None
    error_message: str | None


class RunDetail(Run):
    parent_run_id: str | None
    heartbeat_at: AuditTime | None
    output_manifest_path: str | None
    log_available: bool


class Experiment(Model):
    experiment_id: str
    batch_id: str
    status: Literal['CREATED', 'RUNNING', 'SUCCEEDED', 'FAILED']
    split_policy_version: str
    champion_algorithm: Algorithm | None
    created_at: AuditTime
    finished_at: AuditTime | None


class Metric(Model):
    algorithm: Algorithm
    model_run_id: str
    user_count: int
    catalog_count: int
    window_user_count: int
    precision_at_k: float
    recall_at_k: float
    ndcg_at_k: float
    coverage: float
    cold_user_count: int
    cold_item_count: int
    cold_interaction_count: int
    excluded_user_count: int


class Metrics(Model):
    experiment_id: str
    split: Literal['VALIDATION', 'TEST']
    cohort: Literal['WARM', 'ALL_ELIGIBLE']
    k: Literal[10] = 10
    candidate_policy_version: Literal['full-catalog-v1'] = 'full-catalog-v1'
    items: list[Metric]


class Epoch(Model):
    epoch: int
    train_loss: float
    validation_ndcg: float | None
    elapsed_seconds: float
    peak_gpu_mib: float | None


class Epochs(Model):
    model_run_id: str
    purpose: Literal['EVAL', 'FULLFIT']
    algorithm: Algorithm
    items: list[Epoch]


class Benchmark(Model):
    benchmark_id: str
    model_run_id: str
    device: Literal['CPU', 'CUDA']
    repeat_index: int
    batch_size: int
    epochs: int
    warmup_steps: int
    train_seconds: float
    samples_per_second: float
    peak_memory_mib: float | None
    hardware: str
    torch_version: str
    cuda_runtime: str | None
    driver_version: str | None


class BenchmarkSummary(Model):
    cpu_median_seconds: float | None
    gpu_median_seconds: float | None
    speedup: float | None
    complete_repeats: bool
    comparison_valid: bool


class Benchmarks(Model):
    experiment_id: str
    items: list[Benchmark]
    summary: BenchmarkSummary
