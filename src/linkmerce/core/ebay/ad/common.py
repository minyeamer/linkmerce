from __future__ import annotations

from linkmerce.common.extract import Extractor, LoginHandler

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal


class AuctionAdCenter(Extractor):
    """AUCTION 광고센터 로그인 쿠키를 가지고 데이터를 조회하는 공통 클래스.

    - **URL**: https://ad.esmplus.com

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.
    """

    method: str | None = None
    origin = "https://ad.esmplus.com"
    path: str | None = None

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)

    def post_init(self, **kwargs):
        self.require_cookies()

    def set_request_headers(self, **kwargs):
        super().set_request_headers(
            contents = {"type": "json", "charset": "UTF-8"},
            host = self.origin,
            origin = self.origin,
            ajax = True,
            **kwargs,
        )


class AuctionAdCenterLogin(LoginHandler):
    """AUCTION 광고센터 로그인을 수행하여 쿠키를 발급하는 클래스.

    - **URL**: https://ad.esmplus.com
    """

    origin = "https://ad.esmplus.com"

    @LoginHandler.with_session
    def login(
            self,
            userid: str,
            passwd: str,
            domain: Literal["esmplus", "auction", "gmarket"] = "esmplus",
            **kwargs,
        ) -> str:
        """Gmarket 광고센터에 로그인한다.

        Parameters
        ----------
        userid: str
            ESM PLUS, 옥션, G마켓 중 하나의 로그인 아이디
        passwd: str
            ESM PLUS, 옥션, G마켓 중 하나의 로그인 비밀번호
        domain: str
            로그인할 계정의 도메인
                - `"esmplus"`: ESM PLUS (기본값)
                - `"auction"`: 옥션
                - `"gmarket"`: G마켓

        Returns
        -------
        str
            Gmarket 광고센터 로그인 쿠키 문자열
        """
        site_type = {"esmplus": "ESM", "auction": "IAC", "gmarket": "GMKT"}
        if domain not in site_type:
            raise ValueError(f"Invalid domain: {domain}")

        login_url = self.origin + "/Member/SignIn/LogOn?ReturnUrl=%2Fcpc%2Fmain"
        self.init_login(login_url)
        see_data = self.fetch_see_data(login_url)
        face_data = self.fetch_face_data(login_url, see_data)

        self.ad_login(login_url, userid, passwd, site_type[domain], face_data)
        self.verify_login(userid)
        return self.get_cookies(to="str")

    def init_login(self, login_url: str):
        """광고센터 로그인 화면을 요청하여 초기 쿠키를 설정한다."""
        headers = self.build_headers(login_url, metadata="navigate", https=True)
        with self.request("GET", login_url, headers=headers) as response:
            response.raise_for_status()

    def fetch_see_data(self, login_url: str) -> dict:
        """로그인 검증용 see 데이터를 요청한다."""
        from uuid import uuid4

        url = "https://trust.esmplus.com/see"
        headers = self.build_headers(url, contents="form", origin=self.origin, referer=login_url)
        with self.request("POST", url, headers=headers, data={"auth": uuid4().hex}) as response:
            response.raise_for_status()
            return response.json()

    def fetch_face_data(self, login_url: str, see_data: dict) -> dict:
        """see 응답으로 face 검증 데이터를 요청한다."""
        from uuid import uuid4

        url = f"https://trust.esmplus.com/{uuid4().hex}/face"
        headers = self.build_headers(url, contents="json", origin=self.origin, referer=login_url)
        with self.request("POST", url, headers=headers, json=see_data) as response:
            response.raise_for_status()
            return response.json()

    def ad_login(
            self,
            login_url: str,
            userid: str,
            passwd: str,
            site_type: Literal["ESM", "GMKT", "IAC"],
            face_data: dict,
        ):
        """광고센터 인증 요청 및 로그인 후 리다이렉트를 처리한다."""
        import json

        url = self.origin + "/Member/SignIn/Authenticate"
        body = {
            "Id": userid,
            "Password": passwd,
            "SiteType": site_type,
            "AtoCollectResult": json.dumps(face_data),
        }
        headers = self.build_headers(url, contents="form", origin=self.origin, referer=login_url)

        with self.request("POST", url, data=body, headers=headers, allow_redirects=False) as response:
            response.raise_for_status()
            redirect_url = response.headers.get("Location")
        if not redirect_url:
            raise ValueError("Login redirect URL is missing.")

        headers = self.build_headers(redirect_url, referer=login_url, metadata="navigate", https=True)
        with self.request("GET", redirect_url, headers=headers) as response:
            response.raise_for_status()

    def verify_login(self, userid: str):
        """광고센터 메인 화면에 접속한다."""
        url = self.origin + "/cpc/main"
        headers = self.build_headers(url, metadata="navigate", https=True)
        with self.request("GET", url, headers=headers) as response:
            response.raise_for_status()
            if ("LOGOUT" not in response.text) or (userid not in response.text):
                raise ValueError("Gmarket Ad Center login verification failed.")
