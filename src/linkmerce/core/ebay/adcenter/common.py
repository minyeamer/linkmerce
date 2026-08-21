from __future__ import annotations

from linkmerce.common.extract import Extractor #, LoginHandler
from linkmerce.common.transform import JsonTransformer

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    import datetime as dt


class GmarketAdCenter(Extractor):
    """Gmarket 광고센터 로그인 쿠키를 가지고 데이터를 조회하는 공통 클래스.

    - **URL**: https://adcenter.esmplus.com

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 반드시 전달해야 한다.
    """

    method: str | None = None
    origin = "https://adcenter.esmplus.com"
    path: str | None = None

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)

    def post_init(self, **kwargs):
        self.require_cookies()

    def request_text(self, **kwargs) -> str:
        """HTTP 요청을 수행하고 응답 본문을 UTF-8 텍스트로 반환한다."""
        return self.request_content(**kwargs).decode("utf-8")

    def set_request_headers(self, **kwargs):
        super().set_request_headers(
            accept = "text/x-component",
            contents = {"type": "text", "charset": "UTF-8"},
            host = self.origin,
            origin = self.origin,
            **kwargs,
        )


class GmarketAdParser(JsonTransformer):
    """Gmarket 광고센터 응답 텍스트에서 JSON 데이터를 추출 및 파싱하는 공통 클래스."""

    scope: str | None = None
    fields: dict | list | None = None
    extends: dict | None = None
    on_missing: Literal["ignore", "raise"] = "raise"

    def transform(self, obj: str, **kwargs) -> list[dict]:
        """HTTP 응답 데이터 파싱 > scope 탐색 > 필드 선택 및 변환 순서로 파이프라인을 실행한다."""
        data = self.parse(obj, **kwargs)
        data = self.get_scope(data, **kwargs)
        return self.select_fields(data, **kwargs)

    def parse(self, obj: str, **kwargs) -> dict:
        """텍스트를 줄바꿈 문자로 구분하고, JSON 데이터가 담긴 줄을 찾아서 딕셔너리 객체로 변환한다."""
        import json

        for line in obj.split('\n'):
            if line.startswith("1:"):
                result = json.loads(line[2:])
                if isinstance(result, dict) and result.get("success"):
                    return result
                else:
                    from linkmerce.common.exceptions import RequestError
                    raise RequestError("Gmarket Ad Center request failed.")

        from linkmerce.common.exceptions import ParseError
        raise ParseError("Could not find the '1:' record in the response.")


def get_date_pair(
        start_date: dt.date | str | Literal[":today:"] = ":today:",
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
    ) -> dict[str, str]:
    """Gmarket 광고센터 조회용 날짜 쌍을 생성한다."""
    import datetime as dt

    if isinstance(start_date, str) and (start_date == ":today:"):
        start_date = dt.date.today()
    start_date = str(start_date).replace('-', '')

    if isinstance(end_date, str) and (end_date == ":start_date:"):
        return {"start_date": start_date, "end_date": start_date}
    return {"start_date": start_date, "end_date": str(end_date).replace('-', '')}


###################################################################
######################## Deprecated v1.0.13 #######################
###################################################################

# Gmarket 광고센터 시스템 리뉴얼로 사용 중단.

# class GmarketAdCenterLogin(LoginHandler):
#     """Gmarket 광고센터 로그인을 수행하여 쿠키를 발급하는 클래스.

#     - **URL**: https://ad.esmplus.com
#     """

#     origin = "https://ad.esmplus.com"

#     @LoginHandler.with_session
#     def login(
#             self,
#             userid: str,
#             passwd: str,
#             domain: Literal["esmplus", "auction", "gmarket"] = "esmplus",
#             **kwargs,
#         ) -> str:
#         """Gmarket 광고센터에 로그인한다.

#         Parameters
#         ----------
#         userid: str
#             ESM PLUS, 옥션, G마켓 중 하나의 로그인 아이디
#         passwd: str
#             ESM PLUS, 옥션, G마켓 중 하나의 로그인 비밀번호
#         domain: str
#             로그인할 계정의 도메인
#                 - `"esmplus"`: ESM PLUS (기본값)
#                 - `"auction"`: 옥션
#                 - `"gmarket"`: G마켓

#         Returns
#         -------
#         str
#             Gmarket 광고센터 로그인 쿠키 문자열
#         """
#         site_type = {"esmplus": "ESM", "auction": "IAC", "gmarket": "GMKT"}
#         if domain not in site_type:
#             raise ValueError(f"Invalid domain: {domain}")

#         login_url = self.origin + "/Member/SignIn/LogOn?ReturnUrl=%2Fcpc%2Fmain"
#         self.init_login(login_url)
#         see_data = self.fetch_see_data(login_url)
#         face_data = self.fetch_face_data(login_url, see_data)

#         self.ad_login(login_url, userid, passwd, site_type[domain], face_data)
#         self.verify_login(userid)
#         return self.get_cookies(to="str")

#     def init_login(self, login_url: str):
#         """광고센터 로그인 화면을 요청하여 초기 쿠키를 설정한다."""
#         headers = self.build_headers(login_url, metadata="navigate", https=True)
#         with self.request("GET", login_url, headers=headers) as response:
#             response.raise_for_status()

#     def fetch_see_data(self, login_url: str) -> dict:
#         """로그인 검증용 see 데이터를 요청한다."""
#         from uuid import uuid4

#         url = "https://trust.esmplus.com/see"
#         headers = self.build_headers(url, contents="form", origin=self.origin, referer=login_url)
#         with self.request("POST", url, headers=headers, data={"auth": uuid4().hex}) as response:
#             response.raise_for_status()
#             return response.json()

#     def fetch_face_data(self, login_url: str, see_data: dict) -> dict:
#         """see 응답으로 face 검증 데이터를 요청한다."""
#         from uuid import uuid4

#         url = f"https://trust.esmplus.com/{uuid4().hex}/face"
#         headers = self.build_headers(url, contents="json", origin=self.origin, referer=login_url)
#         with self.request("POST", url, headers=headers, json=see_data) as response:
#             response.raise_for_status()
#             return response.json()

#     def ad_login(
#             self,
#             login_url: str,
#             userid: str,
#             passwd: str,
#             site_type: Literal["ESM", "GMKT", "IAC"],
#             face_data: dict,
#         ):
#         """광고센터 인증 요청 및 로그인 후 리다이렉트를 처리한다."""
#         import json

#         url = self.origin + "/Member/SignIn/Authenticate"
#         body = {
#             "Id": userid,
#             "Password": passwd,
#             "SiteType": site_type,
#             "AtoCollectResult": json.dumps(face_data),
#         }
#         headers = self.build_headers(url, contents="form", origin=self.origin, referer=login_url)

#         with self.request("POST", url, data=body, headers=headers, allow_redirects=False) as response:
#             response.raise_for_status()
#             redirect_url = response.headers.get("Location")
#         if not redirect_url:
#             raise ValueError("Login redirect URL is missing.")

#         headers = self.build_headers(redirect_url, referer=login_url, metadata="navigate", https=True)
#         with self.request("GET", redirect_url, headers=headers) as response:
#             response.raise_for_status()

#     def verify_login(self, userid: str):
#         """광고센터 메인 화면에 접속한다."""
#         url = self.origin + "/cpc/main"
#         headers = self.build_headers(url, metadata="navigate", https=True)
#         with self.request("GET", url, headers=headers) as response:
#             response.raise_for_status()
#             if ("LOGOUT" not in response.text) or (userid not in response.text):
#                 raise ValueError("Gmarket Ad Center login verification failed.")
