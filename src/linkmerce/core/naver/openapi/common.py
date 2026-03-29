from __future__ import annotations

from linkmerce.common.extract import Extractor

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Configs


class NaverOpenApi(Extractor):
    """네이버 오픈 API 요청을 처리하는 공통 클래스.

    API 요청을 위해 `client_id`와 `client_secret`이 필요하다."""

    method: str | None = None
    origin: str = "https://openapi.naver.com"
    version: str = "v1"
    path: str | None = None

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.version, self.path)

    def set_configs(self, configs: Configs = dict()):
        try:
            self.set_api_key(**configs)
        except TypeError:
            raise TypeError("Naver Open API requires variables for client_id and client_secret.")

    def set_api_key(self, client_id: str, client_secret: str, **configs):
        super().set_configs(dict(client_id=client_id, client_secret=client_secret, **configs))

    def set_request_headers(self, **kwargs):
        super().set_request_headers(headers={
            "X-Naver-Client-Id": self.get_config("client_id"),
            "X-Naver-Client-Secret": self.get_config("client_secret"),
            "Content-Type": "application/json"
        })
