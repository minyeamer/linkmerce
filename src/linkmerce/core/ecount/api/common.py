from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import JsonObject


class EcountApi(Extractor):
    """이카운트 오픈 API 요청을 처리하는 공통 클래스.

    - **API**: https://oapi{ZONE}.ecount.com/OAPI/
    - **Docs**: https://sboapi.ecount.com/ECERP/OAPI/OAPIView?lan_type=ko-KR

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    com_code: int | str
        이카운트 회사코드
    userid: str
        이카운트 아이디
    api_key: str
        오픈 API 인증키
    """

    method: str = "POST"
    origin: str = "https://oapi{ZONE}.ecount.com/OAPI/"
    version: str = "V2"
    path: str | None = None
    zone: str = str()
    session_id: str = str()
    locale: str = "ko-KR"
    config_fields = ["com_code", "userid", "api_key"]

    @property
    def url(self) -> str:
        return self.concat_path(self.origin.format(ZONE=self.zone), self.version, self.path)

    def with_oapi(func):
        """오픈 API 요청 전 세션 ID를 발급받는 데코레이터."""
        @functools.wraps(func)
        def wrapper(self: EcountApi, *args, **kwargs):
            self.zone = self.oapi_zone(self.get_config("com_code"))
            self.session_id = self.oapi_login(**self.get_configs())
            return func(self, *args, **kwargs)
        return wrapper

    def oapi_zone(self, com_code: int | str) -> str:
        """회사 코드로 오픈 API Zone 정보를 조회한다."""
        try:
            import requests
            url = self.concat_path(self.origin.format(ZONE=str()), self.version, "Zone")
            payload = {"COM_CODE": com_code}
            with requests.request("POST", url, json=payload, headers=self.get_request_headers()) as response:
                return response.json()['Data']['ZONE']
        except:
            from linkmerce.common.exceptions import AuthenticationError
            raise AuthenticationError("Failed to retrieve Zone info.")

    def oapi_login(self, com_code: int | str, userid: str, api_key: str, locale: str = "ko-KR") -> str:
        """오픈 API 로그인을 수행하여 세션 ID를 발급받는다."""
        try:
            import requests
            url = self.concat_path(self.origin.format(ZONE=self.zone), self.version, "OAPILogin")
            payload = {"COM_CODE": com_code, "USER_ID": userid, "API_CERT_KEY": api_key, "LAN_TYPE": locale, "ZONE": self.zone}
            with requests.request("POST", url, json=payload, headers=self.get_request_headers()) as response:
                return response.json()['Data']["Datas"]["SESSION_ID"]
        except:
            from linkmerce.common.exceptions import AuthenticationError
            raise AuthenticationError("Failed to login with the Ecount API.")

    def build_request_params(self, **kwargs) -> dict[str, str]:
        return {"SESSION_ID": self.session_id}

    def set_request_headers(self, **kwargs):
        super().set_request_headers(headers={"content-type": "application/json"})


class EcountRequestApi(EcountApi):
    """이카운트 오픈 API 경로를 직접 지정하여 요청을 처리하는 클래스.

    - **API**: https://oapi{ZONE}.ecount.com/OAPI/
    - **Docs**: https://sboapi.ecount.com/ECERP/OAPI/OAPIView?lan_type=ko-KR
    """

    @EcountApi.with_session
    @EcountApi.with_oapi
    def extract(self, path: str, body: dict | None = None, **kwargs) -> JsonObject:
        """오픈 API 경로와 본문을 전달하면 응답 결과를 JSON 형식으로 반환한다.

        Parameters
        ----------
        path: str
            오픈 API 경로
        body: dict | None
            오픈 API 요청 본문"""
        self.path = path
        message = self.build_request_message(**kwargs)
        if isinstance(body, dict):
            if "SESSION_ID" in body:
                body["SESSION_ID"] = self.session_id
            message["json"] = body
        with self.request(**message) as response:
            return response.json()


class EcountTestApi(EcountApi):
    """이카운트 테스트 API 경로를 직접 지정하여 요청을 처리하는 클래스.

    - **API**: https://sboapi{ZONE}.ecount.com/OAPI/
    - **Docs**: https://sboapi.ecount.com/ECERP/OAPI/OAPIView?lan_type=ko-KR
    """

    origin: str = "https://sboapi{ZONE}.ecount.com/OAPI/"

    @EcountApi.with_session
    @EcountApi.with_oapi
    def extract(self, path: str, body: dict | None = None, **kwargs) -> JsonObject:
        """테스트 API 경로와 본문을 전달하면 응답 결과를 JSON 형식으로 반환한다.

        Parameters
        ----------
        path: str
            테스트 API 경로
        body: dict | None
            테스트 API 요청 본문"""
        self.path = path
        message = self.build_request_message(**kwargs)
        if isinstance(body, dict):
            if "SESSION_ID" in body:
                body["SESSION_ID"] = self.session_id
            message["json"] = body
        with self.request(**message) as response:
            return response.json()
