from __future__ import annotations

from linkmerce.common.extract import Extractor


class NaverOpenApi(Extractor):
    """네이버 오픈 API 요청을 처리하는 공통 클래스.

    - **API**: https://openapi.naver.com/v1/{path}
    - **Docs**: https://developers.naver.com/docs/common/openapiguide

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        네이버 오픈 API 클라이언트 아이디
    client_secret: str
        네이버 오픈 API 클라이언트 시크릿
    """

    method: str | None = None
    origin: str = "https://openapi.naver.com"
    version: str = "v1"
    path: str | None = None
    config_fields = ["client_id", "client_secret"]

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.version, self.path)

    def set_request_headers(self, **kwargs):
        super().set_request_headers(headers={
            "X-Naver-Client-Id": self.get_config("client_id"),
            "X-Naver-Client-Secret": self.get_config("client_secret"),
            "Content-Type": "application/json"
        })
