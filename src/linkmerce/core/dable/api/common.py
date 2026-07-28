from __future__ import annotations

from linkmerce.common.extract import Extractor


class DableApi(Extractor):
    """데이블 API 요청을 처리하는 공통 클래스.

    - **URL**: https://marketing.dable.io/api/client/:client_name

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    api_key: str
        데이블 설정에서 발급 가능한 API KEY
    client_name: str
        데이블 URL 내 클라이언트 명칭
    """

    method: str = "GET"
    path: str | None = None
    config_fields = ["api_key", "client_name"]

    @property
    def origin(self) -> str:
        return "https://marketing.dable.io/api/client/" + self.get_config("client_name")

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)

    @property
    def api_key(self) -> str:
        return self.get_config("api_key")
