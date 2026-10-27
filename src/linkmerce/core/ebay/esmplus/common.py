from __future__ import annotations

from linkmerce.common.extract import Extractor

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from requests import Session


def has_cookies(session: Session) -> bool:
    """ESM PLUS 로그인 세션이 동시접속 정보에 접근할 수 있는지 검증한다."""
    from linkmerce.utils.headers import build_headers
    url = "https://www.esmplus.com/Home/MultiLoginUsers"
    headers = build_headers(referer="https://www.esmplus.com/Home/v2", ajax=True)
    with session.get(url, headers=headers, allow_redirects=False, timeout=10) as response:
        if response.status_code in (401, 403) or (300 <= response.status_code < 400):
            return False
        response.raise_for_status()
        try:
            data = response.json()
        except ValueError:
            return False
        return isinstance(data, dict) and isinstance(data.get("MultiLoginCount"), int)


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
