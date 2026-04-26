from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools


class CjEflexs(Extractor):
    """CJ eFLEXs 로그인 및 2단계 인증을 처리하는 공통 클래스.

    - **URL**: https://eflexs-x.cjlogistics.com/index.do

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    userid: str
        CJ eFLEXs 로그인을 위한 User ID
    passwd: str
        CJ eFLEXs 로그인을 위한 Password
    mail_info: dict[str, str]
        2단계 인증을 위한 이메일 정보. 다음 키값을 포함해야 한다.
        - **origin**: 메일 서비스 도메인
        - **email**: 메일 주소
        - **passwd**: 메일 계정 비밀번호
    """

    method: str = "POST"
    origin = "https://eflexs-x.cjlogistics.com"
    menu: str
    path: str
    config_fields = ["userid", "passwd", {"mail_info": ["origin", "email", "passwd"]}]

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.menu, self.path)

    def with_auth_info(func):
        """데이터 수집 전에 로그인 및 2단계 인증을 처리하는 데코레이터."""
        @functools.wraps(func)
        def wrapper(self: CjEflexs, *args, **kwargs):
            self.login(**self.get_configs())
            return func(self, *args, **kwargs)
        return wrapper

    def login(self, userid: str, passwd: str, mail_info: dict, **context):
        """CJ eFLEXs 로그인 및 2단계 인증을 처리한다."""
        try:
            self._disable_warnings()
            self._init_session()
            key = self._login_action(userid, passwd)
            code = get_2fa_code(**mail_info)
            self._login_2fa(key, code)
            self._login_final(userid, key, code)
        except:
            from linkmerce.common.exceptions import AuthenticationError
            raise AuthenticationError("Failed to login in to CJ eFLEXs.")

    def _disable_warnings(self):
        from urllib3 import disable_warnings as disable
        from urllib3.exceptions import InsecureRequestWarning
        disable(InsecureRequestWarning)

    def _init_session(self):
        from linkmerce.utils.headers import build_headers

        url = self.origin + "/index.do"
        headers = build_headers(host=self.origin, metadata="navigate", https=True)
        self.request("GET", url, headers=headers, verify=False) # 'Set-Cookie': 'JSESSIONID='

    def _login_action(self, userid: str, passwd: str) -> str:
        url = self.origin + "/auth/loginProc.do"
        body = {
            "pgmId": "", "requestDataIds": "dmParam", "cjLoginId": userid, "cjLoginPw": passwd,
            "cjSecurityID": "", "langCd": "KO"
        }
        headers = dict(self.get_request_headers(), referer=(self.origin + "/index.do"))
        with self.request("POST", url, data=body, headers=headers, verify=False) as response:
            return response.json()["_METADATA_"]["key"]

    def _login_2fa(self, key: str, code: str) -> str:
        url = self.origin + "/CMLN0003M/checkAuthInfo.do"
        body = {
            "pgmId": None, "requestDataIds": "reqParam", "@d1#loginId": None, "@d1#freeYn": None,
            "@d1#checkKeyDe": code, "@d1#authKeyDe": key, "@d#": "@d1#", "@d1#": "reqParam", "@d1#tp": "dm"
        }
        headers = dict(self.get_request_headers(), referer=(self.origin+"/index.do"))
        with self.request("POST", url, data=body, headers=headers, verify=False) as response:
            results = response.json()["resParam"]
            if results["checkKeyYn"] != 'Y':
                raise ValueError()
            return results["checkKeyEnc"]

    def _login_final(self, userid: str, key: str, code: str):
        url = self.origin + "/CMLN0001P/certiLogin.do"
        body = {
            "pgmId": None, "requestDataIds": "reqParam", "@d1#loginId": userid, "@d1#freeYn": 'E',
            "@d1#checkKeyDe": code, "@d1#authKeyDe": key, "@d#": "@d1#", "@d1#": "reqParam", "@d1#tp": "dm"
        }
        headers = dict(self.get_request_headers(), referer=(self.origin+"/index.do"))
        with self.request("POST", url, data=body, headers=headers, verify=False) as response:
            if response.json()["usrStdInfo"]:
                return

    def set_request_headers(self, **kwargs):
        return super().set_request_headers(
            contents={"type": "form", "charset": "UTF-8"},
            host=self.origin, origin=self.origin, referer=self.origin, ajax=True)

    def build_request_message(self, **kwargs) -> dict:
        return dict(super().build_request_message(**kwargs), verify=False)


def get_2fa_code(
        origin: str,
        email: str,
        passwd: str,
        wait_seconds: int = (60*5-10),
        wait_interval: int = 1,
        **kwargs
    ) -> str:
    """메일 서비스에 로그인 후 2단계 인증 메일이 도착할 때까지 기다린다.   
    메일이 확인되면 내용을 읽고 인증 코드를 반환한다.

    Parameters
    ----------
    origin: str
        메일 서비스 도메인
    email: str
        메일 주소
    passwd: str
        메일 계정 비밀번호
    wait_seconds: int
        인증 메일 수신을 기다릴 시간(초). 기본값은 290(4분 50초)이고, 최대 유효시간은 5분이다.
    wait_interval: int
        인증 메일이 도착했는지 확인하기 위해 GET 요청을 보내는 간격(초). 기본값은 1(초)다.

    Returns
    -------
    str
        2단계 인증 코드
    """
    from linkmerce.utils.headers import build_headers
    import requests
    import time

    def _login_action(session: requests.Session, origin: str, email: str, passwd: str):
        url = f"https://auth-api.{origin}/office-web/login"
        body = {"id": email, "password": passwd, "ip_security_level": "1"}
        headers = build_headers(contents="json", host=f"auth-api.{origin}", origin=f"https://login.{origin}", referer=f"https://login.{origin}/")
        session.post(url, json=body, headers=headers)

    def _wait_2fa_mail(session: requests.Session, origin: str) -> int:
        url = f"https://mail-api.{origin}/v2/mails"
        params = {"page[limit]": 30, "page[offset]": 0, "sort[received_date]": "desc", "filter[mailbox_id][eq]": "b0"}
        headers = build_headers(host=f"mail-api.{origin}", origin=f"https://mails.{origin}", referer=f"https://mails.{origin}/")
        headers["x-skip-session-refresh"] = "true"
        for _ in range(wait_seconds):
            with session.get(url, params=params, headers=headers) as response:
                for mail in response.json()["data"][:5]:
                    if (mail["subject"] == "LoIS eFLEXs 인증번호") and mail["is_new"]:
                        return mail["no"]
            time.sleep(wait_interval)
        raise ValueError("인증코드가 전달되지 않았습니다")

    def _retrieve_2fa_code(session: requests.Session, origin: str, mail_no: int) -> str:
        import re
        url = f"https://mail-api.{origin}/v2/mails/{mail_no}"
        headers = build_headers(host=f"mail-api.{origin}", origin=f"https://mails.{origin}", referer=f"https://mails.{origin}/")
        with session.get(url, headers=headers) as response:
            try:
                content = response.json()["data"]["message"]["content"]
                return re.search(r"인증번호 : (\d{4})", content).group(1)
            finally:
                _make_mail_as_read(session, origin, mail_no)

    def _make_mail_as_read(session: requests.Session, origin: str, mail_no: int):
        url = f"https://mail-api.{origin}/v2/mails/{mail_no}"
        headers = build_headers(accept="application/json;charset=UTF-8", contents="application/json;charset=UTF-8",
            host=f"mail-api.{origin}", origin=f"https://mails.{origin}", referer=f"https://mails.{origin}/")
        session.patch(url, json={"is_read": True}, headers=headers)

    with requests.Session() as session:
        _login_action(session, origin, email, passwd)
        mail_no = _wait_2fa_mail(session, origin)
        return _retrieve_2fa_code(session, origin, mail_no)
