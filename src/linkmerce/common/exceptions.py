from __future__ import annotations


class AuthenticationError(RuntimeError):
    ...


class ParseError(ValueError):
    ...


class RequestError(RuntimeError):
    ...


class UnauthorizedError(RuntimeError):
    ...
