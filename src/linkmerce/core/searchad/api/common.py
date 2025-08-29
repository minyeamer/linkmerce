from __future__ import annotations

from linkmerce.common.extract import Extractor

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Variables


class NaverSearchAdAPI(Extractor):
    method: str | None = None
    origin: str = "https://api.searchad.naver.com"
    uri: str | None = None

    def set_variables(self, variables: Variables = dict()):
        try:
            self.set_api_key(**variables)
        except TypeError:
            raise TypeError("Naver Search Ad API requires variables for api_key and secret_key.")

    def set_api_key(self, api_key: str, secret_key: str, customer_id: int | str, **variables):
        super().set_variables(dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id, **variables))

    def set_request_headers(self, **kwargs):
        super().set_request_headers(headers=dict())

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.uri)

    def build_request_headers(self, **kwargs: str) -> dict[str,str]:
        import time

        method = self.method or kwargs.get("method")
        uri = self.uri or kwargs.get("uri")
        timestamp = str(round(time.time() * 1000))
        return {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Timestamp": timestamp,
            "X-API-KEY": self.get_variable("api_key"),
            "X-Customer": str(self.get_variable("customer_id")),
            "X-Signature": self.generate_signature(method, uri, timestamp)
        }

    def generate_signature(self, method: str, uri: str, timestamp: str) -> bytes:
        import base64
        import hashlib
        import hmac

        message = "{}.{}.{}".format(timestamp, method, uri)
        hash = hmac.new(bytes(self.get_variable("secret_key"), "utf-8"), bytes(message, "utf-8"), hashlib.sha256)
        hash.hexdigest()
        return base64.b64encode(hash.digest())
