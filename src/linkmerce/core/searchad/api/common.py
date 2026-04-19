from __future__ import annotations

from linkmerce.common.extract import Extractor

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Configs


class NaverSearchAdApi(Extractor):
    """네이버 검색광고 API 요청을 처리하는 공통 클래스.

    - **API Docs**: https://naver.github.io/searchad-apidoc/

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    api_key: str
        검색광고 API 키
    secret_key: str
        검색광고 API 시크릿 키
    customer_id: int | str
        검색광고 고객 ID
    """

    method: str | None = None
    origin: str = "https://api.searchad.naver.com"
    uri: str | None = None

    def set_configs(self, configs: Configs = dict()):
        try:
            self.set_api_key(**configs)
        except TypeError:
            raise TypeError("Naver Search Ad API requires configs for api_key and secret_key.")

    def set_api_key(self, api_key: str, secret_key: str, customer_id: int | str, **configs):
        super().set_configs(dict(api_key=api_key, secret_key=secret_key, customer_id=customer_id, **configs))

    def set_request_headers(self, **kwargs):
        super().set_request_headers(headers=dict())

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.uri)

    @property
    def customer_id(self) -> int:
        return self.get_config("customer_id")

    def build_request_headers(self, **kwargs: str) -> dict[str, str]:
        """`api_key`, `secret_key`, `customer_id`를 사용하여 HMAC 서명 기반 인증 헤더를 구성한다."""
        import time

        method = self.method or kwargs.get("method")
        uri = self.uri or kwargs.get("uri")
        timestamp = str(round(time.time() * 1000))
        return {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Timestamp": timestamp,
            "X-API-KEY": self.get_config("api_key"),
            "X-Customer": str(self.get_config("customer_id")),
            "X-Signature": self.generate_signature(method, uri, timestamp)
        }

    def generate_signature(self, method: str, uri: str, timestamp: str) -> bytes:
        """HMAC-SHA256 기반의 요청 서명을 생성한다."""
        import base64
        import hashlib
        import hmac

        message = "{}.{}.{}".format(timestamp, method, uri)
        hash = hmac.new(bytes(self.get_config("secret_key"), "utf-8"), bytes(message, "utf-8"), hashlib.sha256)
        hash.hexdigest()
        return base64.b64encode(hash.digest())
