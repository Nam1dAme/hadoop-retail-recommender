"""Public errors contain no exception text, SQL, credentials or user input."""


class APIError(Exception):
    def __init__(self, status, code, message, details=None):
        super().__init__(code)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}


def missing():
    return APIError(404, 'RESOURCE_NOT_FOUND', '请求的资源不存在。')


def pending():
    return APIError(409, 'RESULT_NOT_READY', '请求的结果尚未完成，请查看任务状态。')


def unavailable():
    return APIError(503, 'DEPENDENCY_UNAVAILABLE', '数据库、迁移、只读权限或查询时限检查未通过。')
