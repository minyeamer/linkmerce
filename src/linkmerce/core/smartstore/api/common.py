from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Configs, JsonObject


class SmartstoreApi(Extractor):
    """네이버 커머스 API 요청을 처리하는 공통 클래스.

    - **API Docs**: https://apicenter.commerce.naver.com/ko/basic/commerce-api

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    client_id: str
        커머스 API 애플리케이션 ID
    client_secret: str
        커머스 API 애플리케이션 시크릿
    """

    method: str | None = None
    origin: str = "https://api.commerce.naver.com/external"
    version: str = "v1"
    path: str | None = None

    def set_configs(self, configs: Configs = dict()):
        try:
            self.set_api_key(**configs)
        except TypeError:
            raise TypeError("Naver Commerce API requires configs for client_id and client_secret.")

    def set_api_key(self, client_id: str, client_secret: str, **configs):
        super().set_configs(dict(client_id=client_id, client_secret=client_secret, **configs))

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.version, self.path)

    @property
    def client_id(self) -> str:
        return self.get_config("client_id")

    @property
    def client_secret(self) -> str:
        return self.get_config("client_secret")

    def with_token(func):
        """API 요청 전 `client_id`와 `client_secret`를 사용해 OAuth 토큰을 발급받는 데코레이터."""
        @functools.wraps(func)
        def wrapper(self: SmartstoreApi, *args, **kwargs):
            authorization = self.authorize(self.client_id, self.client_secret)
            self.set_request_headers(headers={"Authorization": f"Bearer {authorization}"})
            return func(self, *args, **kwargs)
        return wrapper

    def authorize(self, client_id: str, client_secret: str, **context) -> str:
        """`client_id`와 `client_secret`를 사용해 OAuth 토큰을 발급받는다."""
        try:
            import requests
            url = self.origin + "/v1/oauth2/token"
            params = self._build_auth_params(client_id, client_secret)
            response = requests.post(url, params=params, headers={"content-type":"application/x-www-form-urlencoded"})
            return response.json()["access_token"]
        except:
            from linkmerce.common.exceptions import AuthenticationError
            raise AuthenticationError("Failed to authenticate with the Naver Commerce API.")

    def _build_auth_params(self, client_id: str, client_secret: str) -> dict:
        import base64
        import bcrypt
        import time

        timestamp = int((time.time()-3) * 1000)
        hashed = bcrypt.hashpw(f'{client_id}_{timestamp}'.encode("utf-8"), client_secret.encode("utf-8"))
        secret = base64.b64encode(hashed).decode("utf-8")
        return {
            "client_id": client_id, "timestamp": timestamp,
            "client_secret_sign": secret, "grant_type": "client_credentials", "type": "SELF",
        }

    def request_json_until_success(self, max_retries: int = 5, **kwargs) -> JsonObject:
        """동시 요청 제한이 발생할 경우에 대비해 오류가 캐치하고 API 요청을 재시도 한다."""
        session = self.get_session()
        message = self.build_request_message(**kwargs)
        for retry_count in range(1, max_retries+1):
            try:
                with session.request(**message) as response:
                    response = response.json()
            except Exception as error:
                response = {"code": "GW.RATE_LIMIT", "message": f"{error.__class__.__name__}: {error}"}
            if self.is_valid_response(response, (retry_count if retry_count != max_retries else None)):
                return response

    def is_valid_response(self, response: JsonObject, retry_count: int | None = None) -> bool:
        """오류 메시지를 구분하여 재시도 요청 또는 `ConnectionError`를 발생시킨다."""
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


class SmartstoreTestAPI(SmartstoreApi):
    """지정된 커머스 API 경로에 대한 요청을 처리하는 테스트 클래스.

    - **API Docs**: https://apicenter.commerce.naver.com/ko/basic/commerce-api
    """

    @SmartstoreApi.with_session
    @SmartstoreApi.with_token
    def extract(
            self,
            method: str,
            path: str,
            version: str | None = None,
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | None = None,
            json: JsonObject | None = None,
            headers: dict[str, str] = None,
            **kwargs
        ) -> JsonObject:
        """커머스 API 경로와 메시지를 전달하면 응답 결과를 JSON 형식으로 반환한다."""
        url = self.concat_path(self.origin, version, path)
        message = self.build_request_message(method=method, url=url, **kwargs)
        if params is not None: message["params"] = params
        if data is not None: message["data"] = data
        if json is not None: message["json"] = json
        if isinstance(headers, dict): message["headers"].update(headers)
        with self.get_session().request(**message) as response:
            return response.json()
