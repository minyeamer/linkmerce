from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Variables, JsonObject
    import datetime as dt


class OAuthException(Exception):
    ...


class MetaAPI(Extractor):
    method: str = "GET"
    origin: str = "https://graph.facebook.com/"
    version: str = "v24.0"
    path: str | None = None

    def extract(self, *args, **kwargs):
        return super().extract(*args, **kwargs)

    def set_variables(self, variables: Variables = dict()):
        try:
            self.set_access_token(**variables)
        except TypeError:
            raise TypeError("Meta API requires access_token.")

    def set_access_token(
            self,
            access_token: str,
            app_id: str = str(),
            app_secret: str = str(),
            version: str = str(),
            **variables,
        ):
        if version:
            self.version = version
        super().set_variables(dict(access_token=access_token, app_id=app_id, app_secret=app_secret, **variables))

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)

    @property
    def access_token(self) -> int | str:
        return self.get_variable("access_token")

    @property
    def app_id(self) -> str:
        return self.get_variable("app_id")

    @property
    def app_secret(self) -> str:
        return self.get_variable("app_secret")

    def auto_refresh_token(func):
        @functools.wraps(func)
        def wrapper(self: MetaAPI, *args, **kwargs):
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
        manager = MetaTokenManager(self.app_id, self.app_secret)
        refresh_token = manager.refresh_long_lived_token(self.access_token)
        self.set_variables(dict(self.get_variables(), access_token=refresh_token))

    def request_json_safe(self, **kwargs):
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
    origin: str = "https://graph.facebook.com"

    def __init__(self, app_id: str, app_secret: str):
        self.app_id = app_id
        self.app_secret = app_secret

    def get_token_expiry(self, access_token: str) -> dt.datetime:
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
        if isinstance(data, dict) and ("error" in data):
            raise OAuthException(data["error"]["message"])
        else:
            raise OAuthException("Invalid OAuth access token - Cannot parse access token")
