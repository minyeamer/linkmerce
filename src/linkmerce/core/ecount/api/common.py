from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Variables, JsonObject


class EcountAPI(Extractor):
    method: str = "POST"
    origin: str = "https://oapi{ZONE}.ecount.com/OAPI/"
    version: str = "V2"
    path: str | None = None
    zone: str = str()
    session_id: str = str()
    locale: str = "ko-KR"

    def set_variables(self, variables: Variables = dict()):
        try:
            self.set_api_key(**variables)
        except TypeError:
            raise TypeError("Ecount Open API requires variables for com_code, userid and api_key.")

    def set_api_key(self,  com_code: int | str, userid: str, api_key: str, **variables):
        super().set_variables(dict(com_code=com_code, userid=userid, api_key=api_key, **variables))

    @property
    def url(self) -> str:
        return self.concat_path(self.origin.format(ZONE=self.zone), self.version, self.path)

    @property
    def com_code(self) -> int | str:
        return self.get_variable("com_code")

    @property
    def userid(self) -> str:
        return self.get_variable("userid")

    @property
    def api_key(self) -> str:
        return self.get_variable("api_key")

    def with_oapi(func):
        @functools.wraps(func)
        def wrapper(self: EcountAPI, *args, **kwargs):
            self.zone = self.oapi_zone(self.com_code)
            self.session_id = self.oapi_login(self.com_code, self.userid, self.api_key)
            return func(self, *args, **kwargs)
        return wrapper

    def oapi_zone(self, com_code: int | str) -> str:
        try:
            import requests
            url = self.concat_path(self.origin.format(ZONE=str()), self.version, "Zone")
            payload = dict(COM_CODE=com_code)
            with requests.request("POST", url, json=payload, headers=self.get_request_headers()) as response:
                return response.json()['Data']['ZONE']
        except:
            from linkmerce.common.exceptions import AuthenticationError
            raise AuthenticationError(f"Failed to retrieve Zone info.")

    def oapi_login(self, com_code: int | str, userid: str, api_key: str, locale: str = "ko-KR") -> str:
        try:
            import requests
            url = self.concat_path(self.origin.format(ZONE=self.zone), self.version, "OAPILogin")
            payload = dict(COM_CODE=com_code, USER_ID=userid, API_CERT_KEY=api_key, LAN_TYPE=locale, ZONE=self.zone)
            with requests.request("POST", url, json=payload, headers=self.get_request_headers()) as response:
                return response.json()['Data']["Datas"]["SESSION_ID"]
        except:
            from linkmerce.common.exceptions import AuthenticationError
            raise AuthenticationError(f"Failed to login with the Ecount API.")

    def build_request_params(self, **kwargs) -> dict[str,str]:
        return {"SESSION_ID": self.session_id}

    def set_request_headers(self, **kwargs):
        super().set_request_headers(headers={"content-type": "application/json"})


class EcountRequestAPI(EcountAPI):

    @EcountAPI.with_session
    @EcountAPI.with_oapi
    def extract(self, path: str, body: dict | None = None, **kwargs) -> JsonObject:
        self.path = path
        message = self.build_request_message(**kwargs)
        if isinstance(body, dict):
            if "SESSION_ID" in body:
                body["SESSION_ID"] = self.session_id
            message["json"] = body
        with self.request(**message) as response:
            return response.json()


class EcountTestAPI(EcountAPI):
    origin: str = "https://sboapi{ZONE}.ecount.com/OAPI/"

    @EcountAPI.with_session
    @EcountAPI.with_oapi
    def extract(self, path: str, body: dict | None = None, **kwargs) -> JsonObject:
        self.path = path
        message = self.build_request_message(**kwargs)
        if isinstance(body, dict):
            if "SESSION_ID" in body:
                body["SESSION_ID"] = self.session_id
            message["json"] = body
        with self.request(**message) as response:
            return response.json()
