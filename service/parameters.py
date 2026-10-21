"""The frozen v1 query vocabulary; extra or duplicate parameters are errors."""
from datetime import date
import re
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, AfterValidator, BeforeValidator, model_validator

Algorithm = Literal['POP', 'ITEMCF', 'ALS', 'NCF']
Segment = Literal['CORE', 'AT_RISK', 'NEW', 'POTENTIAL', 'OTHER']
TaskStatus = Literal['PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED']
BatchID = Annotated[str, Field(pattern=r'^B[0-9]{14}[0-9a-f]{8}$')]
ReleaseID = Annotated[str, Field(pattern=r'^R[0-9]{14}[0-9a-f]{8}$')]
UUID = Annotated[str, Field(pattern=r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')]


def business_id(value):
    if not 1 <= len(value) <= 32 or value in ('.', '..') or any(
            c in '/\\' or ord(c) < 32 or 127 <= ord(c) <= 159 for c in value):
        raise ValueError('Expected 1..32 characters without path separators or controls')
    return value


BusinessID = Annotated[str, AfterValidator(business_id)]


def source_date(value):
    if isinstance(value, str) and not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value):
        raise ValueError('Expected an ISO date YYYY-MM-DD')
    return value


SourceDate = Annotated[date, BeforeValidator(source_date)]


class NoQuery(BaseModel):
    model_config = ConfigDict(extra='forbid')


class Page(NoQuery):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class ReleaseQuery(NoQuery):
    release_id: ReleaseID | None = None


class ReleasePage(ReleaseQuery, Page):
    pass


class Dates(ReleaseQuery):
    start_date: SourceDate | None = None
    end_date: SourceDate | None = None
    country: str | None = Field(default=None, min_length=1, max_length=64)

    @model_validator(mode='after')
    def ordered_dates(self):
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError('start_date must not follow end_date')
        return self


class Trend(Dates):
    granularity: Literal['DAY', 'MONTH'] = 'DAY'


class Countries(Dates, Page):
    sort: Literal['NET_DESC', 'GROSS_DESC'] = 'NET_DESC'


class Products(Dates, Page):
    stock_code: BusinessID | None = None
    sort: Literal['GROSS_DESC', 'QUANTITY_DESC'] = 'GROSS_DESC'


class Customers(ReleasePage):
    segment: Segment | None = None
    customer_id: BusinessID | None = None


class Similar(ReleaseQuery):
    k: int = Field(default=10, ge=1, le=10)


class Recommendations(Similar):
    algorithm: Algorithm | None = None


class Runs(Page):
    batch_id: BatchID | None = None
    status: TaskStatus | None = None


class Experiments(Page):
    batch_id: BatchID | None = None


class Metrics(NoQuery):
    split: Literal['VALIDATION', 'TEST'] = 'TEST'
    cohort: Literal['WARM', 'ALL_ELIGIBLE'] = 'WARM'
