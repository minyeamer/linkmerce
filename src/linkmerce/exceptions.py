from __future__ import annotations


class ParseError(ValueError):
    ...


class RequestError(RuntimeError):
    ...


class UnauthorizedError(RuntimeError):
    ...
