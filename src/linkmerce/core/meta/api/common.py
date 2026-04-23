from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import JsonObject
    import datetime as dt


class OAuthException(Exception):
    """메타 OAuth 인증 실패 시 발생하는 예외."""
    ...


class MetaApi(Extractor):
    """메타 그래프 API 요청을 처리하는 공통 클래스.

    - **URL**: https://developers.facebook.com
    - **Docs**: https://developers.facebook.com/docs/graph-api

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_token: str
        메타 액세스 토큰
    app_id: str | None
        메타 앱 ID (토큰 자동 갱신 시 필요)
    app_secret: str | None
        메타 앱 시크릿 (토큰 자동 갱신 시 필요)
    """

    method: str = "GET"
    origin: str = "https://graph.facebook.com/"
    config_fields = ["access_token", {"app_id": None}, {"app_secret": None}]

    @property
    def access_token(self) -> int | str:
        return self.get_config("access_token")

    def auto_refresh_token(func):
        """`access_token`이 만료되어 `OAuthException`이 발생하면 토큰 자동 갱신을 시도하는 데코레이터."""
        @functools.wraps(func)
        def wrapper(self: MetaApi, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except OAuthException as exception:
                if self.get_config("app_id") and self.get_config("app_secret"):
                    self.refresh_long_lived_token()
                    return func(self, *args, **kwargs)
                raise exception
        return wrapper

    def refresh_long_lived_token(self):
        """장기 실행 토큰을 새로고침한다."""
        manager = MetaTokenManager(self.get_config("app_id"), self.get_config("app_secret"))
        refresh_token = manager.refresh_long_lived_token(self.access_token)
        self.set_configs(dict(self.get_configs(), access_token=refresh_token))

    def request_json_safe(self, **kwargs):
        """요청 중 `access_token`이 만료되면 `OAuthException`을 발생시킨다."""
        response = super().request_json_safe(**kwargs)
        if isinstance(response, dict) and ("error" in response) and isinstance(response["error"], dict):
            message = response["error"].get("message") or "Undefined"
            if response["error"].get("type") == "OAuthException":
                raise OAuthException(message)
            raise RuntimeError(message)
        return response


class MetaTokenManager:
    """메타 OAuth 토큰의 만료 여부 확인 및 장기 실행 토큰 갱신을 관리하는 클래스.

    - **Docs**: https://developers.facebook.com/documentation/facebook-login/guides/access-tokens
    """

    origin: str = "https://graph.facebook.com"

    def __init__(self, app_id: str, app_secret: str):
        self.app_id = app_id
        self.app_secret = app_secret

    def get_token_expiry(self, access_token: str) -> dt.datetime:
        """`access_token`의 유효 기간을 조회한다."""
        import datetime as dt
        import requests
        url = self.origin + "/debug_token"
        params = {"input_token": access_token, "access_token": access_token}
        with requests.request("GET", url, params=params) as response:
            data = response.json()
            if "data" in data:
                return dt.datetime.fromtimestamp(data["data"]["expires_at"])
            self.raise_oauth_error(data)

    def refresh_long_lived_token(self, access_token: str) -> str:
        """`access_token`의 유효 기간을 연장한다."""
        import requests
        url = self.origin + "/oauth/access_token"
        params = {
            "grant_type": "fb_exchange_token",
            "client_id": self.app_id,
            "client_secret": self.app_secret,
            "fb_exchange_token": access_token,
        }
        with requests.get(url, params=params) as response:
            data = response.json()
            if "access_token" in data:
                return data["access_token"]
            self.raise_oauth_error(data)

    def raise_oauth_error(self, data: JsonObject):
        """유효 기간 연장에 대한 요청 결과에서 `access_token`이 확인되지 않으면 `OAuthException`을 발생시킨다."""
        if isinstance(data, dict) and ("error" in data):
            raise OAuthException(data["error"]["message"])
        raise OAuthException("Invalid OAuth access token - Cannot parse access token")
