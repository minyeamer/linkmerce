from __future__ import annotations


class AuthenticationError(RuntimeError):
    """인증 과정에서 실패했을 때 발생하는 예외."""
    ...


class ParseError(ValueError):
    """응답 데이터 파싱 중 오류가 발생했을 때 발생하는 예외."""
    ...


class RequestError(RuntimeError):
    """HTTP 요청 중 오류가 발생하거나 응답 데이터가 올바르지 않을 때 발생하는 예외."""
    ...


class UnauthorizedError(RuntimeError):
    """인증되지 않은 접근이 감지되었을 때 발생하는 예외."""
    ...
