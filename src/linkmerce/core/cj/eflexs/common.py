from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Variables


class CJeFLEXs(Extractor):
    method: str = "POST"
    origin = "https://eflexs-x.cjlogistics.com"
    menu: str
    path: str

    def set_variables(self, variables: Variables = dict()):
        try:
            self.set_login_info(**variables)
        except TypeError:
            raise TypeError("CJ eFLEXs Login requires variables for userid, passwd, and mail_info.")

    def set_login_info(self, userid: str, passwd: str, mail_info: dict, **variables):
        if not (isinstance(mail_info, dict) and all([mail_info.get(key) for key in ["origin","email","passwd"]])):
            raise ValueError("The 2-step verification email information is incorrect.")
        super().set_variables(dict(userid=userid, passwd=passwd, mail_info=mail_info, **variables))

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.menu, self.path)

    @property
    def userid(self) -> str:
        return self.get_variable("userid")

    @property
    def passwd(self) -> str:
        return self.get_variable("passwd")

    @property
    def mail_info(self) -> dict:
        return self.get_variable("mail_info")

    def with_auth_info(func):
        @functools.wraps(func)
        def wrapper(self: CJeFLEXs, *args, **kwargs):
            self.login(self.userid, self.passwd, self.mail_info)
            return func(self, *args, **kwargs)
        return wrapper

    def login(self, userid: str, passwd: str, mail_info: dict, **context):
        try:
            self.disable_warnings()
            self.init_session()
            key = self.login_action(userid, passwd)
            code = get_2fa_code(**mail_info)
            self.login_2fa(key, code)
            self.login_final(userid, key, code)
        except:
            from linkmerce.common.exceptions import AuthenticationError
            raise AuthenticationError(f"Failed to login in to CJ eFLEXs.")

    def disable_warnings(self):
        from urllib3 import disable_warnings as disable
        from urllib3.exceptions import InsecureRequestWarning
        disable(InsecureRequestWarning)

    def init_session(self):
        from linkmerce.utils.headers import build_headers

        url = self.origin + "/index.do"
        headers = build_headers(host=self.origin, metadata="navigate", https=True)
        self.request("GET", url, headers=headers, verify=False) # 'Set-Cookie': 'JSESSIONID='

    def login_action(self, userid: str, passwd: str) -> str:
        url = self.origin + "/auth/loginProc.do"
        body = {
            "pgmId": "", "requestDataIds": "dmParam", "cjLoginId": userid, "cjLoginPw": passwd,
            "cjSecurityID": "", "langCd": "KO"
        }
        headers = dict(self.get_request_headers(), referer=(self.origin + "/index.do"))
        with self.request("POST", url, data=body, headers=headers, verify=False) as response:
            return response.json()["_METADATA_"]["key"]

    def login_2fa(self, key: str, code: str) -> str:
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

    def login_final(self, userid: str, key: str, code: str):
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
            contents=dict(type="form", charset="UTF-8"),
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
    from linkmerce.utils.headers import build_headers
    import requests
    import time

    def login_action(session: requests.Session, origin: str, email: str, passwd: str):
        url = f"https://auth-api.{origin}/office-web/login"
        body = {"id": email,"password": passwd, "ip_security_level": "1"}
        headers = build_headers(contents="json", host=f"auth-api.{origin}", origin=f"https://login.{origin}", referer=f"https://login.{origin}/")
        session.post(url, json=body, headers=headers)

    def wait_2fa_mail(session: requests.Session, origin: str) -> int:
        url = f"https://mail-api.{origin}/v2/mails"
        params = {"page[limit]": 30, "page[offset]": 0, "sort[received_date]": "desc", "filter[mailbox_id][eq]": "b0",}
        headers = build_headers(host=f"mail-api.{origin}", origin=f"https://mails.{origin}", referer=f"https://mails.{origin}/")
        headers["x-skip-session-refresh"] = "true"
        for _ in range(wait_seconds):
            with session.get(url, params=params, headers=headers) as response:
                for mail in response.json()["data"][:5]:
                    if (mail["subject"] == "LoIS eFLEXs 인증번호") and mail["is_new"]:
                        return mail["no"]
            time.sleep(wait_interval)
        raise ValueError("인증코드가 전달되지 않았습니다")

    def retrieve_2fa_code(session: requests.Session, origin: str, mail_no: int) -> str:
        import re
        url = f"https://mail-api.{origin}/v2/mails/{mail_no}"
        headers = build_headers(host=f"mail-api.{origin}", origin=f"https://mails.{origin}", referer=f"https://mails.{origin}/")
        with session.get(url, headers=headers) as response:
            try:
                content = response.json()["data"]["message"]["content"]
                return re.search(r"인증번호 : (\d{4})", content).group(1)
            finally:
                make_mail_as_read(session, origin, mail_no)

    def make_mail_as_read(session: requests.Session, origin: str, mail_no: int):
        url = f"https://mail-api.{origin}/v2/mails/{mail_no}"
        headers = build_headers(accept="application/json;charset=UTF-8", contents="application/json;charset=UTF-8",
            host=f"mail-api.{origin}", origin=f"https://mails.{origin}", referer=f"https://mails.{origin}/")
        session.patch(url, json={"is_read": True}, headers=headers)

    with requests.Session() as session:
        login_action(session, origin, email, passwd)
        mail_no = wait_2fa_mail(session, origin)
        return retrieve_2fa_code(session, origin, mail_no)
