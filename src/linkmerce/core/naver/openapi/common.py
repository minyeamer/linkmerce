from __future__ import annotations

from linkmerce.common.extract import Extractor

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.common.extract import Variables


class NaverOpenAPI(Extractor):
    method: str | None = None
    origin: str = "https://openapi.naver.com"
    version: str = "v1"
    path: str | None = None

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.version, self.path)

    def set_variables(self, variables: Variables = dict()):
        try:
            self.set_api_key(**variables)
        except TypeError:
            raise TypeError("Naver Open API requires variables for client_id and client_secret.")

    def set_api_key(self, client_id: str, client_secret: str, **variables):
        super().set_variables(dict(client_id=client_id, client_secret=client_secret, **variables))

    def set_request_headers(self, **kwargs):
        super().set_request_headers(headers={
            "X-Naver-Client-Id": self.get_variable("client_id"),
            "X-Naver-Client-Secret": self.get_variable("client_secret"),
            "Content-Type": "application/json"
        })
