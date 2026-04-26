from __future__ import annotations

from linkmerce.common.extract import Extractor


class NaverSearchAdApi(Extractor):
    """네이버 검색광고 API 요청을 처리하는 공통 클래스.

    - **Docs**: https://naver.github.io/searchad-apidoc/#/guides

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    api_key: str
        SA API 엑세스라이선스
    secret_key: str
        SA API 비밀키
    customer_id: int | str
        광고계정의 CUSTOMER_ID
    """

    method: str | None = None
    origin: str = "https://api.searchad.naver.com"
    uri: str | None = None
    config_fields = ["api_key", "secret_key", "customer_id"]

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.uri)

    @property
    def customer_id(self) -> int | str:
        return self.get_config("customer_id")

    def set_request_headers(self, **kwargs):
        super().set_request_headers(headers=dict())

    def build_request_headers(self, **kwargs: str) -> dict[str, str]:
        """HMAC 서명 기반 인증 헤더를 구성한다."""
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
