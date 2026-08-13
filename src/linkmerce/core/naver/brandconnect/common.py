from __future__ import annotations

from linkmerce.common.extract import Extractor


class BrandConnect(Extractor):
    """네이버 로그인 쿠키를 가지고 브랜드 커넥트 데이터를 조회하는 공통 클래스.

    - **URL**: https://brandconnect.naver.com

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.
    """

    method: str | None = None
    origin = "https://brandconnect.naver.com"
    api_url = "https://gw-brandconnect.naver.com"
    path: str | None = None

    @property
    def url(self) -> str:
        return self.concat_path(self.api_url, self.path)

    def post_init(self, **kwargs):
        self.require_cookies()

    def set_request_headers(self, **kwargs):
        super().set_request_headers(origin=self.origin, **kwargs)
