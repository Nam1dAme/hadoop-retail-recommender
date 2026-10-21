"""Read-only HTTP boundary. The UI never imports database/model code."""
from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import quote, urlsplit
from uuid import UUID

import httpx

MESSAGES = {
    'NO_ACTIVE_RELEASE': '尚无可用数据版本。发布完成后，请切换到当前版本。',
    'RESOURCE_NOT_FOUND': '没有找到对应记录，请检查输入的编号。',
    'RELEASE_NOT_READY': '这个数据版本尚未发布完成，请选择已就绪的版本。',
    'RELEASE_RETIRED': '固定的数据版本已不可用，请点击“切换到当前版本”。',
    'RESULT_NOT_READY': '这项结果尚未完成，可在“数据质量与任务”查看进度。',
    'INVALID_PARAMETER': '查询条件不符合要求，请检查日期、编号和取值范围。',
    'DEPENDENCY_UNAVAILABLE': '数据服务暂时不可用，请检查 API 与数据库后重试。',
    'INTERNAL_ERROR': '数据服务未能完成请求，请凭请求 ID 排查后重试。',
    'NETWORK_ERROR': '未连接到数据服务，请确认 W09 API 已在本机启动。',
    'REQUEST_TIMEOUT': '查询超过 5 秒，请保留当前条件并重试。',
    'INVALID_RESPONSE': '数据服务返回了无法识别的响应，请检查 API 版本。',
    'VERSION_MISMATCH': '返回结果与固定的数据版本不一致，已停止展示。',
}
BUSINESS_PATHS = re.compile(r'^/(summary|sales/(trend|countries)|products|customers|customers/segments|products/[^/]+/similar|customers/[^/]+/recommendations)$')


@dataclass
class ApiFault(Exception):
    code: str
    request_id: str | None = None
    status: int | None = None

    @property
    def message(self):
        return MESSAGES.get(self.code, MESSAGES['INTERNAL_ERROR'])

    def __str__(self):
        return self.code  # Exception/log strings must never include raw HTTP bodies.


def identifier(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 32 or value in ('.', '..') or any(
            c in '/\\' or ord(c) < 32 or 127 <= ord(c) <= 159 for c in value):
        raise ValueError('请输入 1—32 位编号，不包含路径分隔符或控制字符。')
    return quote(value, safe='')


class RetailAPI:
    def __init__(self, base_url, release=None, *, transport=None):
        url = urlsplit(base_url)
        if (url.scheme != 'http' or url.hostname != '127.0.0.1' or url.path != '/api/v1'
                or url.username or url.password or url.query or url.fragment or not url.port):
            raise ValueError('UI API must be the explicit loopback /api/v1 URL')
        self.release = release
        self.http = httpx.Client(base_url=base_url+'/', timeout=5.0, trust_env=False,
                                 follow_redirects=False, transport=transport)
        self.responses = {}  # This render only: never a cache of an earlier success.
        self.requests = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.http.close()

    def get(self, path, **params):
        if not path.startswith('/') or '?' in path or any(p in ('.','..') for p in path.split('/')):
            raise ValueError('Use a fixed endpoint and separately bound query parameters')
        business = bool(BUSINESS_PATHS.fullmatch(path))
        if 'release_id' in params:
            raise ValueError('The UI owns the fixed release context')
        if business:
            if self.release is None:
                raise ApiFault('NO_ACTIVE_RELEASE')
            params['release_id'] = self.release['release_id']
        params = {k: v for k, v in params.items() if v is not None}
        key = (path, tuple(sorted((k, str(v)) for k, v in params.items())))
        if key in self.responses:
            return self.responses[key]
        try:
            response = self.http.get(path.lstrip('/'), params=params)
        except httpx.TimeoutException:
            raise ApiFault('REQUEST_TIMEOUT') from None
        except httpx.HTTPError:
            raise ApiFault('NETWORK_ERROR') from None
        self.requests.append((path, params.copy(), response.status_code))
        request_id = response.headers.get('x-request-id')
        try:
            if str(UUID(request_id or '')) != request_id:request_id=None
        except ValueError:request_id=None
        try:
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError()
            if response.status_code != 200:
                error = payload.get('error', {})
                code = error.get('code') if isinstance(error, dict) else None
                raise ApiFault(code if code in MESSAGES else 'INTERNAL_ERROR', request_id, response.status_code)
            meta = payload['meta']
            data = payload['data']
            if not request_id or not isinstance(data, dict) or meta['schema_version'] != '1.0' or meta['request_id'] != request_id:
                raise ValueError()
            if business and (meta['release_id'] != self.release['release_id'] or meta['batch_id'] != self.release['batch_id']):
                raise ApiFault('VERSION_MISMATCH', request_id)
        except (ValueError, KeyError, TypeError):
            raise ApiFault('INVALID_RESPONSE', request_id) from None
        self.responses[key] = data
        return data

    def all_items(self, path, **params):
        items, page = [], 1
        while True:
            data = self.get(path, page=page, page_size=100, **params)
            items.extend(data['items'])
            if not data['has_next']:
                return items
            if not data['items']:
                raise ApiFault('INVALID_RESPONSE')
            page += 1
            if page > 1000:
                raise ApiFault('INVALID_RESPONSE')
