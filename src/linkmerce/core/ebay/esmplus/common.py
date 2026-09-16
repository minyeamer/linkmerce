from __future__ import annotations

from linkmerce.common.extract import Extractor


class EsmPlus(Extractor):
    """ESM PLUS 로그인 쿠키를 가지고 데이터를 조회하는 공통 클래스.

    - **URL**: https://www.esmplus.com/Home/v2

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.
    """

    method: str | None = None
    subdomain: str
    __origin = "https://{subdomain}.esmplus.com"
    path: str | None = None

    @property
    def origin(self) -> str:
        return self.__origin.format(subdomain=self.subdomain)

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, "api", self.path)

    def post_init(self, **kwargs):
        self.require_cookies()

    def set_request_headers(self, **kwargs):
        super().set_request_headers(host = self.origin, **kwargs)
