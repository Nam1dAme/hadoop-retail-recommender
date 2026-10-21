"""FastAPI application factory. Importing the module never opens a database."""
import asyncio
import logging
import time
from typing import Annotated
from uuid import uuid4

from fastapi import FastAPI, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from pymysql import MySQLError
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse

from orchestration.database import connect
from orchestration.migrations import MigrationError
from orchestration.settings import ConfigError
from service import parameters as p, schemas as s
from service.database import ReadSession
from service.errors import APIError, unavailable
from service.repository import Repository

LOG = logging.getLogger('retail.api')
VERSION = '0.1.0'


def error_response(scope, error):
    payload = s.ErrorResponse(error=s.ErrorBody(code=error.code, message=error.message, details=error.details),
                              meta=s.Meta(request_id=scope['state']['request_id'])).model_dump(mode='json')
    if scope['path'] == '/api/v1/health/ready' and error.status == 503:
        payload['data'] = {'status': 'NOT_READY'}
    return JSONResponse(payload, status_code=error.status)


class Boundary:
    """Uniform errors/IDs, a five-second budget, and no partial JSON replies."""
    def __init__(self, app, timeout=5.0):
        self.app, self.timeout = app, timeout

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        request_id = str(uuid4())
        scope.setdefault('state', {}).update(request_id=request_id, deadline=time.monotonic()+self.timeout)
        messages = []

        async def buffer(message):
            messages.append(message)

        try:
            if b'%2f' in scope.get('raw_path', b'').lower() or '\\' in scope['path'] or any(ord(c) < 32 for c in scope['path']):
                raise APIError(422, 'INVALID_PARAMETER', '路径标识包含非法分隔符或控制字符。')
            await asyncio.wait_for(self.app(scope, receive, buffer), timeout=self.timeout)
        except (TimeoutError, APIError) as exc:
            error = exc if isinstance(exc, APIError) else unavailable()
            messages.clear()
            await error_response(scope, error)(scope, receive, buffer)
            LOG.warning('request_id=%s code=%s', request_id, error.code)
        except Exception as exc:
            # Drivers can embed SQL/passwords in exception text and tracebacks.
            LOG.error('request_id=%s code=INTERNAL_ERROR exception_type=%s', request_id, type(exc).__name__)
            messages.clear()
            await error_response(scope, APIError(500, 'INTERNAL_ERROR', '服务内部错误，请凭 request_id 排查。'))(scope, receive, buffer)
        for message in messages:
            if message['type'] == 'http.response.start':
                message['headers'] = [(k, v) for k, v in message['headers'] if k.lower() != b'x-request-id']
                message['headers'].append((b'x-request-id', request_id.encode('ascii')))
            await send(message)


def create_app(config=None, defaults_file=None, *, connection_factory=None):
    if connection_factory is None:
        if config is None:
            raise ConfigError('An explicit project configuration is required')
        connection_factory = lambda: connect(config, defaults_file, connect_timeout=2, read_timeout=2, write_timeout=2)
    app = FastAPI(title='Retail Analytics Read-only API', version=VERSION,
                  description='HL-DOC-1.0.0 · GET only · GBP · published FULLFIT results',
                  responses={code: {'model': s.ErrorResponse} for code in (404, 409, 410, 422, 500, 503)})
    app.add_middleware(Boundary)

    @app.exception_handler(APIError)
    async def api_error(request, exc):
        return error_response(request.scope, exc)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        details = {}
        for error in exc.errors():
            key = '.'.join(str(x) for x in error['loc'])
            details[key] = '未知参数' if error['type'] == 'extra_forbidden' else '参数格式或取值不符合接口要求'
        return error_response(request.scope, APIError(422, 'INVALID_PARAMETER', '请求参数不合法。', details))

    @app.exception_handler(HTTPException)
    async def http_error(request, exc):
        code = 'RESOURCE_NOT_FOUND' if exc.status_code == 404 else 'METHOD_NOT_ALLOWED' if exc.status_code == 405 else 'HTTP_ERROR'
        return error_response(request.scope, APIError(exc.status_code, code, '请求路径或方法不可用。'))

    async def execute(request, result_model, operation, query, identifier=None, business=False):
        if len(request.query_params.multi_items()) != len(set(request.query_params.keys())):
            raise APIError(422, 'INVALID_PARAMETER', '不允许重复查询参数。')
        if request.method == 'GET' and (request.headers.get('content-length', '0') != '0' or 'transfer-encoding' in request.headers):
            raise APIError(422, 'INVALID_PARAMETER', 'GET 请求不接受请求体。')

        def work():
            try:
                with ReadSession(connection_factory, request.state.deadline) as session:
                    repo = Repository(session)
                    if business:
                        repo.resolve(query.release_id)
                    if operation == 'ready':
                        data = {'status': 'READY'}
                    elif operation == 'current':
                        data = repo.resolve()
                    else:
                        method = getattr(repo, operation)
                        if identifier is not None:
                            data = method(identifier, query) if operation in ('similar', 'recommendations', 'metrics') else method(identifier)
                        else:
                            data = method() if operation == 'segments' else method(query)
                    return s.Response[result_model](data=data, meta=s.Meta(request_id=request.state.request_id,
                        release_id=repo.release['release_id'] if repo.release else None, batch_id=repo.batch_id))
            except (MySQLError, MigrationError, ConfigError):
                LOG.warning('request_id=%s code=DEPENDENCY_UNAVAILABLE', request.state.request_id)
                raise unavailable() from None

        return await asyncio.to_thread(work)

    @app.get('/api/v1/health/live', response_model=s.Response[s.Live], tags=['health'])
    async def live(request: Request, query: Annotated[p.NoQuery, Query()]):
        if request.query_params or request.headers.get('content-length', '0') != '0' or 'transfer-encoding' in request.headers:
            raise APIError(422, 'INVALID_PARAMETER', '存活检查不接受查询参数或请求体。')
        return s.Response[s.Live](data=s.Live(status='UP', service_version=VERSION), meta=s.Meta(request_id=request.state.request_id))

    @app.get('/api/v1/health/ready', response_model=s.Response[s.Ready],
             responses={503: {'model': s.ReadinessError}}, tags=['health'])
    async def ready(request: Request, query: Annotated[p.NoQuery, Query()]):
        return await execute(request, s.Ready, 'ready', query)

    @app.get('/api/v1/releases/current', response_model=s.Response[s.Release], tags=['releases'])
    async def current(request: Request, query: Annotated[p.NoQuery, Query()]):
        return await execute(request, s.Release, 'current', query)

    @app.get('/api/v1/releases', response_model=s.Response[s.Page[s.ReleaseEntry]], tags=['releases'])
    async def releases(request: Request, query: Annotated[p.Page, Query()]):
        return await execute(request, s.Page[s.ReleaseEntry], 'releases', query)

    @app.get('/api/v1/summary', response_model=s.Response[s.Summary], tags=['sales'])
    async def summary(request: Request, query: Annotated[p.Dates, Query()]):
        return await execute(request, s.Summary, 'summary', query, business=True)

    @app.get('/api/v1/sales/trend', response_model=s.Response[s.Trend], tags=['sales'])
    async def trend(request: Request, query: Annotated[p.Trend, Query()]):
        return await execute(request, s.Trend, 'trend', query, business=True)

    @app.get('/api/v1/sales/countries', response_model=s.Response[s.Page[s.Country]], tags=['sales'])
    async def countries(request: Request, query: Annotated[p.Countries, Query()]):
        return await execute(request, s.Page[s.Country], 'countries', query, business=True)

    @app.get('/api/v1/products', response_model=s.Response[s.Page[s.Product]], tags=['sales'])
    async def products(request: Request, query: Annotated[p.Products, Query()]):
        return await execute(request, s.Page[s.Product], 'products', query, business=True)

    @app.get('/api/v1/customers', response_model=s.Response[s.Page[s.Customer]], tags=['customers'])
    async def customers(request: Request, query: Annotated[p.Customers, Query()]):
        return await execute(request, s.Page[s.Customer], 'customers', query, business=True)

    @app.get('/api/v1/customers/segments', response_model=s.Response[s.Segments], tags=['customers'])
    async def segments(request: Request, query: Annotated[p.ReleaseQuery, Query()]):
        return await execute(request, s.Segments, 'segments', query, business=True)

    @app.get('/api/v1/products/{stock_code}/similar', response_model=s.Response[s.Similar], tags=['recommendations'])
    async def similar(request: Request, stock_code: Annotated[p.BusinessID, Path()], query: Annotated[p.Similar, Query()]):
        return await execute(request, s.Similar, 'similar', query, stock_code, business=True)

    @app.get('/api/v1/customers/{customer_id}/recommendations', response_model=s.Response[s.Recommendations], tags=['recommendations'])
    async def recommendations(request: Request, customer_id: Annotated[p.BusinessID, Path()], query: Annotated[p.Recommendations, Query()]):
        return await execute(request, s.Recommendations, 'recommendations', query, customer_id, business=True)

    @app.get('/api/v1/batches', response_model=s.Response[s.Page[s.Batch]], tags=['control'])
    async def batches(request: Request, query: Annotated[p.Page, Query()]):
        return await execute(request, s.Page[s.Batch], 'batches', query)

    @app.get('/api/v1/batches/{batch_id}/quality', response_model=s.Response[s.Quality], tags=['control'])
    async def quality(request: Request, batch_id: Annotated[p.BatchID, Path()], query: Annotated[p.NoQuery, Query()]):
        return await execute(request, s.Quality, 'quality', query, batch_id)

    @app.get('/api/v1/runs', response_model=s.Response[s.Page[s.Run]], tags=['control'])
    async def runs(request: Request, query: Annotated[p.Runs, Query()]):
        return await execute(request, s.Page[s.Run], 'runs', query)

    @app.get('/api/v1/runs/{run_id}', response_model=s.Response[s.RunDetail], tags=['control'])
    async def run(request: Request, run_id: Annotated[p.UUID, Path()], query: Annotated[p.NoQuery, Query()]):
        return await execute(request, s.RunDetail, 'run', query, run_id)

    @app.get('/api/v1/experiments', response_model=s.Response[s.Page[s.Experiment]], tags=['experiments'])
    async def experiments(request: Request, query: Annotated[p.Experiments, Query()]):
        return await execute(request, s.Page[s.Experiment], 'experiments', query)

    @app.get('/api/v1/experiments/{experiment_id}/metrics', response_model=s.Response[s.Metrics], tags=['experiments'])
    async def metrics(request: Request, experiment_id: Annotated[p.UUID, Path()], query: Annotated[p.Metrics, Query()]):
        return await execute(request, s.Metrics, 'metrics', query, experiment_id)

    @app.get('/api/v1/models/{model_run_id}/epochs', response_model=s.Response[s.Epochs], tags=['experiments'])
    async def epochs(request: Request, model_run_id: Annotated[p.UUID, Path()], query: Annotated[p.NoQuery, Query()]):
        return await execute(request, s.Epochs, 'epochs', query, model_run_id)

    @app.get('/api/v1/experiments/{experiment_id}/benchmarks', response_model=s.Response[s.Benchmarks], tags=['experiments'])
    async def benchmarks(request: Request, experiment_id: Annotated[p.UUID, Path()], query: Annotated[p.NoQuery, Query()]):
        return await execute(request, s.Benchmarks, 'benchmarks', query, experiment_id)

    return app
