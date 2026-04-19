from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Configs, JsonObject
    import datetime as dt


class OAuthException(Exception):
    """메타 OAuth 인증 실패 시 발생하는 예외."""
    ...


class MetaApi(Extractor):
    """메타 Graph API를 처리하는 공통 클래스.

    - **API Docs**: https://developers.facebook.com/docs/graph-api

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_token: str
        메타 액세스 토큰
    app_id: str
        메타 앱 ID (토큰 자동 갱신 시 필수)
    app_secret: str
        메타 앱 시크릿 (토큰 자동 갱신 시 필수)
    """

    method: str = "GET"
    origin: str = "https://graph.facebook.com/"

    def set_configs(self, configs: Configs = dict()):
        try:
            self.set_access_token(**configs)
        except TypeError:
            raise TypeError("Meta API requires access_token.")

    def set_access_token(
            self,
            access_token: str,
            app_id: str = str(),
            app_secret: str = str(),
            **configs,
        ):
        super().set_configs(dict(access_token=access_token, app_id=app_id, app_secret=app_secret, **configs))

    @property
    def access_token(self) -> int | str:
        return self.get_config("access_token")

    @property
    def app_id(self) -> str:
        return self.get_config("app_id")

    @property
    def app_secret(self) -> str:
        return self.get_config("app_secret")

    def auto_refresh_token(func):
        """`access_token`이 만료되어 `OAuthException`이 발생하면 토큰 자동 갱신을 시도하는 데코레이터."""
        @functools.wraps(func)
        def wrapper(self: MetaApi, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except OAuthException as exception:
                if self.app_id and self.app_secret:
                    self.refresh_long_lived_token()
                    return func(self, *args, **kwargs)
                else:
                    raise exception
        return wrapper

    def refresh_long_lived_token(self):
        """장기 실행 토큰을 새로고침한다."""
        manager = MetaTokenManager(self.app_id, self.app_secret)
        refresh_token = manager.refresh_long_lived_token(self.access_token)
        self.set_configs(dict(self.get_configs(), access_token=refresh_token))

    def request_json_safe(self, **kwargs):
        """요청 중 `access_token`이 만료되면 `OAuthException`을 발생시킨다."""
        response = super().request_json_safe(**kwargs)
        if isinstance(response, dict) and ("error" in response) and isinstance(response["error"], dict):
            message = response["error"].get("message") or "Undefined"
            if response["error"].get("type") == "OAuthException":
                raise OAuthException(message)
            else:
                raise RuntimeError(message)
        else:
            return response


class MetaTokenManager:
    """메타 OAuth 토큰의 만료 여부 확인 및 장기 실행 토큰 갱신을 관리하는 클래스."""

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
            else:
                self.raise_oauth_error(data)

    def refresh_long_lived_token(self, access_token: str) -> str:
        """장기 실행 토큰을 새로고침한다."""
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
            else:
                self.raise_oauth_error(data)

    def raise_oauth_error(self, data: JsonObject):
        """토큰 새로고침 결과에서 `access_token`이 확인되지 않으면 `OAuthException`을 발생시킨다."""
        if isinstance(data, dict) and ("error" in data):
            raise OAuthException(data["error"]["message"])
        else:
            raise OAuthException("Invalid OAuth access token - Cannot parse access token")
