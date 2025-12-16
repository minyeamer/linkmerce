from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Variables, JsonObject


class SmartstoreAPI(Extractor):
    method: str | None = None
    origin: str = "https://api.commerce.naver.com/external"
    version: str = "v1"
    path: str | None = None

    def set_variables(self, variables: Variables = dict()):
        try:
            self.set_api_key(**variables)
        except TypeError:
            raise TypeError("Naver Commerce API requires variables for client_id and client_secret.")

    def set_api_key(self, client_id: str, client_secret: str, **variables):
        super().set_variables(dict(client_id=client_id, client_secret=client_secret, **variables))

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.version, self.path)

    @property
    def client_id(self) -> str:
        return self.get_variable("client_id")

    @property
    def client_secret(self) -> str:
        return self.get_variable("client_secret")

    def with_token(func):
        @functools.wraps(func)
        def wrapper(self: SmartstoreAPI, *args, **kwargs):
            authorization = self.authorize(self.client_id, self.client_secret)
            self.set_request_headers(headers={"Authorization": f"Bearer {authorization}"})
            return func(self, *args, **kwargs)
        return wrapper

    def authorize(self, client_id: str, client_secret: str, **context) -> str:
        try:
            import requests
            url = self.origin + "/v1/oauth2/token"
            params = self._build_auth_params(client_id, client_secret)
            response = requests.post(url, params=params, headers={"content-type":"application/x-www-form-urlencoded"})
            return response.json()["access_token"]
        except:
            from linkmerce.common.exceptions import AuthenticationError
            raise AuthenticationError(f"Failed to authenticate with the Naver Commerce API.")

    def _build_auth_params(self, client_id: str, client_secret: str) -> dict:
        import base64
        import bcrypt
        import time

        timestamp = int((time.time()-3) * 1000)
        hashed = bcrypt.hashpw(f'{client_id}_{timestamp}'.encode("utf-8"), client_secret.encode("utf-8"))
        secret = base64.b64encode(hashed).decode("utf-8")
        return dict(client_id=client_id, timestamp=timestamp, client_secret_sign=secret, grant_type="client_credentials", type="SELF")

    def request_json_until_success(self, max_retries: int = 5, **kwargs) -> JsonObject:
        session = self.get_session()
        message = self.build_request_message(**kwargs)
        for retry_count in range(1, max_retries+1):
            try:
                with session.request(**message) as response:
                    response = response.json()
            except Exception as error:
                response = dict(code="GW.RATE_LIMIT", message=f"{error.__class__.__name__}: {error}")
            if self.is_valid_response(response, (retry_count if retry_count != max_retries else None)):
                return response

    def is_valid_response(self, response: JsonObject, retry_count: int | None = None) -> bool:
        if isinstance(response, dict):
            rate_limit = (response.get("code") == "GW.RATE_LIMIT")
            internal_error = (response.get("message") == "Internal server error")
            if (rate_limit or internal_error) and isinstance(retry_count, int):
                import time
                time.sleep(retry_count)
                return False
            elif response.get("code"):
                raise ConnectionError(response.get("message") or str())
        return True


class SmartstoreTestAPI(SmartstoreAPI):

    @SmartstoreAPI.with_session
    @SmartstoreAPI.with_token
    def extract(
            self,
            method: str,
            path: str,
            version: str | None = None,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | None = None,
            json: JsonObject | None = None,
            headers: dict[str,str] = None,
            **kwargs
        ) -> JsonObject:
        url = self.concat_path(self.origin, version, path)
        message = self.build_request_message(method=method, url=url, **kwargs)
        if params is not None: message["params"] = params
        if data is not None: message["data"] = data
        if json is not None: message["json"] = json
        if isinstance(headers, dict): message["headers"].update(headers)
        with self.get_session().request(**message) as response:
            return response.json()
