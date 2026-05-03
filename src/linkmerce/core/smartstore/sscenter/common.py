from __future__ import annotations

from linkmerce.common.extract import LoginHandler

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from requests import Session


def has_accounts(session: Session) -> bool:
    """네이버 스마트스토어센터 로그인 세션으로 판매자 정보가 조회되는지 검증한다."""
    from linkmerce.utils.headers import build_headers
    origin = "https://accounts.commerce.naver.com"
    url = f"{origin}/graphql?query=userInfo"
    headers = build_headers(origin=origin, referer=login_url())
    with session.post(url, json=query_user_info(), headers=headers) as response:
        json = response.json()
        return isinstance(json, dict) and isinstance(json.get("data"), dict) and json["data"]["userInfo"]


def has_cookies(session: Session) -> bool:
    """네이버 스마트스토어센터 로그인 세션이 유효한지 검증한다."""
    from linkmerce.utils.headers import build_headers
    origin = "https://accounts.commerce.naver.com"
    url = f"{origin}/graphql?query=nidAuth"
    headers = build_headers(url, contents={"type": "json"}, origin=origin, referer=login_url())
    with session.post(url, json=query_nid_auth(), headers=headers) as response:
        return bool(response.json()["data"]["nidAuth"]["nid"])


###################################################################
######################### Smartstore Login ########################
###################################################################

class SmartstoreCenterLogin(LoginHandler):
    """네이버 스마트스토어센터 로그인을 수행하여 쿠키를 발급하는 클래스.

    - **URL**: https://accounts.commerce.naver.com/login?url=https%3A%2F%2Fsell.smartstore.naver.com%2F%23%2Flogin-callback
    """

    main_url = "https://sell.smartstore.naver.com"
    login_url = "https://accounts.commerce.naver.com"

    @LoginHandler.with_session
    def login(
            self,
            userid: str | None = None,
            passwd: str | None = None,
            channel_seq: int | str | None = None,
            cookies: str | None = None,
            **kwargs
        ) -> dict:
        """스마트스토어센터 로그인 수행 후 2단계 인증을 처리한다.

        Parameters
        ----------
        userid: str | None
            스마트스토어센터 판매자 아이디/이메일
        passwd: str | None
            스마트스토어센터 판매자 비밀번호
        channel_seq: int | str | None
            채널 번호. 로그인 후 접속된 채널이 다르면 채널 번호에 맞는 채널로 전환한다.
        cookies: str | None
            네이버 로그인 쿠키 문자열. 판매자 아이디 로그인을 대체하여 네이버 아이디로 로그인한다.

        Returns
        -------
        str
            접속된 스토어(채널) 정보
        """
        if userid and passwd:
            self.seller_login(userid, passwd)
        else:
            self.oauth_login(cookies)

        login_info = self.two_factor_login()

        if channel_seq and (int(channel_seq) != login_info["channelNo"]):
            return self.switch_channel(channel_seq)
        else:
            return login_info

    ########################### Seller Login ##########################

    def seller_login(self, userid: str, passwd: str):
        """판매자 아이디로 로그인한다."""

        def build_request_body(userid: str, passwd: str) -> dict:
            from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection

            variables = {
                "id": userid,
                "password": passwd,
                "saveId": False,
                "chkLong": False,
                "captchaKey": None,
                "captcha": "",
                "svcUrl": callback_url(return_url=False)
            }

            return GraphQLOperation(
                operation = "idLogin",
                variables = variables,
                types = {"id": "String!", "password": "String!", "saveId": "Boolean!", "chkLong": "Boolean!", "captchaKey": "String", "captcha": "String", "svcUrl": "String"},
                selection = GraphQLSelection(
                    name = "login",
                    variables = dict(loginRequest=list(variables.keys())),
                    fields = ["id", "statCd", "loginStatus", "showCaptcha", "captchaKey", "sessionKey", "idNo"]
                )
            ).generate_body(query_options = {"command": "mutation", "selection": {"variables": {"linebreak": True}}, "fields": {"linebreak": True}, "suffix": '\n'})

        url = self.login_url + "/graphql"
        headers = self.build_headers(url, referer=f"{self.main_url}/login?{callback_url(return_url=False)}")
        self.request("POST", url, json=build_request_body(userid, passwd), headers=headers)
        self.fetch_smartstore(referer=self.login_url)

    def fetch_smartstore(self, referer: str = str()):
        url = self.main_url
        headers = self.build_headers(self.main_url, contents={"type": "json"}, referer=referer)
        self.request("GET", url, headers=headers)

    ########################### OAuth Login ###########################

    def oauth_login(self, cookies: str) -> str:
        """네이버 쿠키로 OAUTH 인증한다."""
        valid_url = self.validate_cookies(cookies)
        # valid_url = "https://sell.smartstore.naver.com/#/login-callback?returnUrl=https%3A%2F%2Fsell.smartstore.naver.com%2F%23%2Fhome%2Fdashboard"
        auth_url = self.login_begin(valid_url)
        # auth_url = "https://nid.naver.com/oauth2.0/authorize?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}"
        redirect_url = self.login_redirect(auth_url, referer=login_url())
        # redirect_url = "https://nid.naver.com/login/noauth/allow_oauth?oauth_token={oauth_token}&with_pin&step=agree_term&inapp_view&oauth_os"
        callback_url = self.login_redirect(redirect_url, referer=auth_url)
        # callback_url = "https://accounts.commerce.naver.com/oauth/callback?code={code}&state={state}"
        self.login_redirect(callback_url, referer="https://nid.naver.com/", extract_location=False)
        next_url = self.login_callback(callback_url)
        # next_url = "https://sell.smartstore.naver.com/#/login-callback?returnUrl=https%3A%2F%2Fsell.smartstore.naver.com%2F%23%2Fhome%2Fdashboard"
        if next_url:
            self.fetch_smartstore(referer=callback_url)
        else:
            from linkmerce.common.exceptions import AuthenticationError
            raise AuthenticationError("Login error occurred.")

    def validate_cookies(self, cookies: str) -> str:
        self.set_cookies(cookies)
        url = self.login_url + "/graphql"
        headers = self.build_headers(url, contents={"type": "json"}, origin=self.login_url, referer=login_url())
        self.request("POST", url, json=query_valid_url(), params={"query": "ValidUrl"}, headers=headers)
        self.request("POST", url, json=query_foreign_captcha(), params={"query": "foreignCaptcha"}, headers=headers)
        self.request("POST", url, json=query_user_info(), params={"query": "userInfo"}, headers=headers)
        self.request("POST", url, json=query_nid_auth(), params={"query": "nidAuth"}, headers=headers)
        with self.request("POST", url, json=query_valid_url(), params={"query": "ValidUrl"}, headers=headers) as response:
            return response.json()["data"]["validUrl"]

    def login_begin(self, valid_url: str) -> str:

        def build_request_body() -> dict:
            from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection

            variables = {
                "mode": "login",
                "snsCd": "naver",
                "svcUrl": valid_url,
                "userInfos": [{"key": "IS_NOT_REAUTHENTICATE", "value": "true"}]
            }
            types = {"mode": "String!", "snsCd": "String!", "svcUrl": "String!", "oneTimeLoginSessionKey": "String", "userInfos": "[UserInfoEntry!]"}

            return GraphQLOperation(
                operation = "snsLoginBegin",
                variables = variables,
                types = types,
                selection = GraphQLSelection(
                    name = "snsBegin",
                    variables = dict(snsLoginBeginRequest=list(types.keys())),
                    fields = ["authUrl"],
                )
            ).generate_body(query_options = {"command": "mutation", "selection": {"variables": {"linebreak": True}}, "fields": {"linebreak": True}, "suffix": '\n'})

        url = self.login_url + "/graphql?query=snsLoginBegin"
        headers = self.build_headers(url, contents={"type": "json"}, origin=self.login_url, referer=login_url())
        with self.request("POST", url, json=build_request_body(), headers=headers) as response:
            return response.json()["data"]["snsBegin"]["authUrl"]

    def login_redirect(self, url: str, referer: str, extract_location: bool = True) -> str:
        from linkmerce.utils.regex import regexp_extract
        headers = self.build_headers(url, metadata="navigate", https=True, referer=referer)
        with self.request("GET", url, headers=headers) as response:
            if extract_location:
                return regexp_extract(r'location\.replace\("([^"]+)"\);', response.text)

    def login_callback(self, callback_url: str) -> str:
        from urllib.parse import urlsplit
        variables = dict([kv.split('=', 1) for kv in urlsplit(callback_url).query.split('&', 1)])

        def build_request_body(code: str, state: str, **kwargs) -> dict:
            from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection

            variables = {"code": code, "state": state}
            types = {"code": "String!", "state": "String!"}

            return GraphQLOperation(
                operation = "snsLoginCallback",
                variables = variables,
                types = types,
                selection = GraphQLSelection(
                    name = "snsCallback",
                    variables = dict(snsLoginCallbackRequest=list(variables.keys())),
                    fields = ["statCd", "loginStatus", "nextUrl", "sessionKey", "snsCd", "idNo", "realnm", "age", "email", "mode", "snsId"],
                )
            ).generate_body(query_options = {"command": "mutation", "selection": {"variables": {"linebreak": False}}, "fields": {"linebreak": True}, "suffix": '\n'})

        url = self.login_url + "/graphql?query=snsLoginCallback"
        headers = self.build_headers(url, contents={"type": "json"}, origin=self.login_url, referer=callback_url)
        with self.request("POST", url, json=build_request_body(**variables), headers=headers) as response:
            return response.json()["data"]["snsCallback"]["nextUrl"]

    ######################### Two Factor Login ########################

    def two_factor_login(self) -> dict:
        """2단계 인증 중 발생하는 쿠키를 받는다."""
        url = self.main_url + "/api/login"
        body = {"url": (self.main_url + "/#/home/dashboard")}
        with self.request("POST", url, data=body, params=body, headers=self.get_login_header()) as response:
            return self.get_login_info(response.headers)

    def get_login_header(self) -> dict[str, str]:
        headers = self.build_headers(self.main_url, contents={"type": "json", "charset": "UTF-8"}, referer=self.main_url)
        headers["x-current-state"] = self.main_url + "/#/login-callback"
        headers["x-current-statename"] = "login-callback"
        headers["x-to-statename"] = "login-callback"
        return headers

    def get_login_info(self, response_headers: dict) -> dict:
        from urllib.parse import unquote
        import json
        return json.loads(unquote(response_headers["X-NCP-LOGIN-INFO"]))

    ########################## Switch Channel #########################

    def switch_channel(self, channel_seq: int | str) -> dict:
        """현재 활성화된 채널을 조회하고, 주어진 채널 번호(`channel_seq`)와 다르다면 전환한다."""
        channel_info = self.select_channel(channel_seq)
        login_info = self.set_channel(**channel_info)
        url = login_info["redirectUrl"]
        headers = self.build_headers(url, https=True)
        self.request("GET", url, headers=headers)
        return login_info

    def select_channel(self, channel_seq: int | str) -> dict:
        """권한이 있는 채널 목록에서 채널 번호(`channel_seq`)에 해당하는 채널 정보를 반환한다."""
        for channel in self.fetch_channels():
            if channel["channelNo"] == int(channel_seq):
                return channel

    def fetch_channels(self) -> list[dict]:
        """권한이 있는 채널 목록을 조회한다."""
        url = self.main_url + "/api/login/channels"
        with self.request("GET", url, headers=self.get_channel_header()) as response:
            return response.json()

    def set_channel(self, channelNo: int, roleNo: int, **kwargs) -> dict:
        """주어진 채널 정보에 해당하는 채널로 전환한다."""
        from urllib.parse import quote_plus
        url = self.main_url + "/api/login/change-channel"
        body = {"channelNo": channelNo, "roleNo": roleNo, "url": url}
        params = dict(channelNo=channelNo, roleNo=roleNo, url=quote_plus(url))
        with self.request("POST", url, data=body, params=params, headers=self.get_channel_header(), allow_redirects=False) as response:
            return self.get_login_info(response.headers)

    def get_channel_header(self) -> dict[str, str]:
        headers = self.build_headers(self.main_url, referer=self.main_url)
        headers["x-current-state"] = self.main_url + "/#/home/dashboard"
        headers["x-current-statename"] = "work.channel-select"
        headers["x-to-statename"] = "work.channel-select"
        return headers


###################################################################
######################### Login Components ########################
###################################################################

def query_valid_url() -> dict:
    query = "query ValidUrl($reqType: String!, $url: String!) {\n  validUrl(reqType: $reqType, url: $url)\n}\n"
    return {"operationName": "ValidUrl", "variables": {"reqType": "login", "url": callback_url()}, "query": query}


def query_foreign_captcha() -> dict:
    query = "query foreignCaptcha {\n  foreignCaptcha {\n    ip\n    isForeignIp\n    captchaKey\n    __typename\n  }\n}\n"
    return {"operationName": "foreignCaptcha", "variables": dict(), "query": query}


def query_user_info() -> dict:
    from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection

    return GraphQLOperation(
        operation = "userInfo",
        variables = {"userSelectRequest": {"fields": ["NAME", "MEMBER_TYPE", "EMAIL"], "maskingFields": ["id", "EMAIL", "NAME"]}},
        types = {"userSelectRequest": "UserSelectRequest"},
        selection = GraphQLSelection(
            name = "userInfo",
            variables = ["userSelectRequest"],
            fields = [{"user": ["id", "idNo"]}, {"userInfos": ["key", "value"]}]
        )
    ).generate_body(query_options = {"selection": {"variables": {"linebreak": False}}, "fields": {"linebreak": True}, "suffix": '\n'})


def query_nid_auth() -> dict:
    query = "query nidAuth {\n  nidAuth {\n    nid\n    rawNid\n    nidNo\n    nidLoginStat\n    profileImageUrl\n    name\n    __typename\n  }\n}\n"
    return {"operationName": "nidAuth", "variables": dict(), "query": query}


def main_url(return_url: bool = True) -> str:
    from urllib.parse import urlencode
    return '?'.join(["https://sell.smartstore.naver.com/login", urlencode(dict(url=callback_url(return_url)))])


def login_url(return_url: bool = True) -> str:
    from urllib.parse import urlencode
    return '?'.join(["https://accounts.commerce.naver.com/login", urlencode(dict(url=callback_url(return_url)))])


def callback_url(return_url: bool = True) -> str:
    from urllib.parse import urlencode
    url = "https://sell.smartstore.naver.com/#/login-callback"
    if return_url:
        dashboard_url = "https://sell.smartstore.naver.com/#/home/dashboard"
        return '?'.join([url, urlencode({"returnUrl": dashboard_url})])
    else:
        return url
