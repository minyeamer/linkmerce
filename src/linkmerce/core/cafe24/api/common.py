from __future__ import annotations

from linkmerce.common.extract import Extractor

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable
    from requests import Response
    from pathlib import Path


class Cafe24Api(Extractor):
    """카페24 API 요청을 처리하는 공통 클래스.

    - **API**: https://{mall_id}.cafe24api.com/api/v2/
    - **Docs**: https://apidocs.cafe24.com/docs/guide/intro

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.

    **NOTE** `token_info`를 파일 경로로 전달하면 토큰 갱신 시 파일 내용도 업데이트한다.
    """

    method: str = "GET"
    origin: str = "https://{mall_id}.cafe24api.com/"
    version: str = "v2"
    path: str | None = None
    token_file: str | Path | None = None
    config_fields = ["mall_id", "client_id", "client_secret", "token_info"]

    @property
    def url(self) -> str:
        origin = self.origin.format(mall_id=self.get_config("mall_id"))
        return self.concat_path(origin, "api", self.version, self.path)

    def post_init(self, **kwargs):
        """액세스 토큰을 갱신하기 위한 `Cafe24ApiTokenManager` 객체를 생성한다."""
        configs = {"mall_id": self.get_config("mall_id")}
        self.token_manager = Cafe24ApiTokenManager(configs=configs)

    def set_configs(self, configs: dict):
        """`token_info`가 딕셔너리 형식이 아니라면 파일 경로로 인식하고 내용을 읽어서 설정에 적용한다."""
        if not isinstance(configs["token_info"], dict):
            from linkmerce.api.config import read_json
            token_info = read_json(configs["token_info"])
            self.token_file = configs["token_info"]
            configs = configs | {"token_info": token_info}
        super().set_configs(configs)

    def get_authorization(self) -> str:
        """액세스 토큰을 사용해 Authorization Header를 생성한다.

        액세스 토큰의 만료 여부를 판단하여 최소 30초 동안 사용이 보장되지 않으면
        리프레시 토큰을 사용해 갱신하고 `token_info` 설정값을 업데이트한다.

        **NOTE** `token_file` 속성값이 있다면 상위 경로를 자동 생성하면서 토큰 생성 응답 결과를 저장한다.
        """
        response = self.token_manager.validate_token(
            client_id = self.get_config("client_id"),
            client_secret = self.get_config("client_secret"),
            token_info = (token_info := self.get_config("token_info")),
            from_now = {"seconds": 30},
            save_to = self.token_file,
        )

        if token_info["access_token"] != response["access_token"]:
            self.set_configs(self.get_configs() | {"token_info": response})

        return (response.get("token_type") or "Bearer") + ' ' + response["access_token"]

    def build_request_headers(self, **kwargs: str) -> dict[str, str]:
        return self.get_request_headers() | {
            "Authorization": self.get_authorization(),
        }

    def set_request_headers(self, **kwargs):
        super().set_request_headers(headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        })


class Cafe24TestApi(Cafe24Api):
    """카페24 API 경로에 대한 요청을 처리하는 테스트 클래스.

    - **API**: https://{mall_id}.cafe24api.com/api/v2/
    - **Docs**: https://apidocs.cafe24.com/docs/guide/intro

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.

    **NOTE** `token_info`를 파일 경로로 전달하면 토큰 갱신 시 파일 내용도 업데이트한다.
    """

    @Cafe24Api.with_session
    def extract(
            self,
            method: str,
            path: str,
            version: str = "v2",
            params: dict | list[tuple] | bytes | None = None,
            data: dict | list[tuple] | bytes | None = None,
            json: dict | None = None,
            headers: dict[str, str] | None = None,
            **kwargs
        ) -> dict:
        """카페24 API 경로와 메시지를 전달하면 응답 결과를 JSON 형식으로 반환한다.

        Parameters
        ----------
        method: str
            HTTP 메서드
        path: str
            카페24 API 경로. 버전 이후의 경로 값을 전달한다.
        version: str | None
            카페24 API 버전. 생략하면 `v2`를 사용한다.
        params: dict | list[tuple] | bytes | None
            카페24 API 요청 파라미터
        data: dict | list[tuple] | bytes | None
            카페24 API 요청 본문
        json: dict | None
            카페24 API 요청 본문 (JSON)
        headers: dict[str, str] | None
            카페24 API 요청 헤더

        Returns
        -------
        dict
            카페24 API 응답 결과
        """
        origin = self.origin.format(mall_id=self.get_config("mall_id"))
        url = self.concat_path(origin, "api", version, path)
        message = self.build_request_message(method=method, url=url, **kwargs)

        if params is not None: message["params"] = params
        if data is not None: message["data"] = data
        if json is not None: message["json"] = json
        if headers is not None:
            message["headers"] = (message["headers"] or dict()) | headers

        with self.get_session().request(**message) as response:
            return response.json()


class Cafe24ApiTokenManager(Extractor):
    """카페24 인증 코드 발급 및 액세스 토큰 갱신을 처리하는 클래스.

    - **API**: https://{mall_id}.cafe24api.com/api/v2/oauth/token
    - **Docs**: https://apidocs.cafe24.com/docs/guide/intro

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값을 반드시 전달해야 한다.

    mall_id: str
        카페24 쇼핑몰 아이디
    """

    method: str = "POST"
    origin: str = "https://{mall_id}.cafe24api.com/"
    version: str = "v2"
    config_fields = ["mall_id"]

    @property
    def url(self) -> str:
        origin = self.origin.format(mall_id=self.get_config("mall_id"))
        return self.concat_path(origin, "api", self.version)

    def extract(self, *args, **kwargs):
        """액세스 토큰을 발급받기 위해 다음 메서드를 사용할 수 있다.
        1. `make_oauth_url`: 브라우저에서 권한 동의를 진행할 인증 요청 URL을 생성한다.
        2. `create_token`: 인증 코드로 액세스 토큰을 발급받는다.
        3. `refresh_token`: 리프레시 토큰으로 액세스 토큰을 갱신한다.
        4. `validate_token`: 액세스 토큰이 만료되었다면 갱신하고 아니면 그대로 반환한다.
        """
        raise NotImplementedError(
            "Use create_token, refresh_token, or validate_token instead."
        )

    def make_oauth_url(
            self,
            client_id: str,
            redirect_uri: str,
            scope: str | Iterable[str],
        ) -> str:
        """브라우저에서 권한 동의를 진행할 인증 요청 URL을 생성한다.

        Parameters
        ----------
        client_id: str
            카페24 앱 클라이언트 아이디
        redirect_uri: str
            인증 코드가 발급될 Redirect URI
        scope: str | Iterable[str]
            개발자센터에 등록한 API 권한 범위

        Returns
        -------
        str
            카페24 OAuth 인증 요청 URL
        """
        from urllib.parse import urlencode
        url = self.concat_path(self.url, "/oauth/authorize")
        params = {
            "response_type": "code",
            "client_id": client_id,
            "state": "app_install",
            "redirect_uri": redirect_uri,
            "scope": scope if isinstance(scope, str) else ','.join(scope),
        }
        return url + '?' + urlencode(params)

    def create_token(
            self,
            client_id: str,
            client_secret: str,
            code: str,
            redirect_uri: str,
            save_to: str | Path | None = None,
        ) -> dict:
        """인증 코드로 액세스 토큰을 발급받는다.

        **NOTE** 인증 코드는 발급 후 1분간 유효하며, 만료되면 `AuthenticationError`를 발생시킨다.

        Parameters
        ----------
        code: str
            브라우저에서 권한 동의를 완료한 후 발급받은 인증 코드
        client_id: str
            카페24 앱 클라이언트 아이디
        client_secret: str
            카페24 앱 클라이언트 시크릿
        redirect_uri: str
            인증 코드를 발급받은 Redirect URI
        save_to: str | Path | None
            OAuth 응답 결과를 저장할 JSON 파일 경로. 상위 경로가 없으면 자동 생성한다.

        Returns
        -------
        dict
            액세스 토큰, 리프레시 토큰 및 만료 시각을 포함한 카페24 OAuth 응답
        """
        body = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }
        return self._request_token(client_id, client_secret, body, save_to)

    def refresh_token(
            self,
            client_id: str,
            client_secret: str,
            token: str,
            save_to: str | Path | None = None,
        ) -> dict:
        """리프레시 토큰으로 액세스 토큰을 갱신한다.

        **NOTE** 리프레시 토큰은 발급 후 2주간 유효하며, 만료되면 `AuthenticationError`를 발생시킨다.

        Parameters
        ----------
        token: str
            리프레시 토큰 (발급 후 2주가 지나지 않은 토큰만 유효하다.)
        client_id: str
            카페24 앱 클라이언트 아이디
        client_secret: str
            카페24 앱 클라이언트 시크릿
        save_to: str | Path | None
            OAuth 응답 결과를 저장할 JSON 파일 경로. 상위 경로가 없으면 자동 생성한다.

        Returns
        -------
        dict
            액세스 토큰, 리프레시 토큰 및 만료 시각을 포함한 카페24 OAuth 응답
        """
        body = {"grant_type": "refresh_token", "refresh_token": token}
        return self._request_token(client_id, client_secret, body, save_to)

    def validate_token(
            self,
            client_id: str,
            client_secret: str,
            token_info: dict,
            from_now: dict | None = None,
            save_to: str | Path | None = None,
        ) -> dict:
        """액세스 토큰이 만료되었다면 갱신하고 아니면 그대로 반환한다.

        **NOTE** 액세스 토큰 만료 시각이 ISO 8601 형식이 아니면 `ValueError`를 발생시킨다.

        **NOTE** 리프레시 토큰은 발급 후 2주간 유효하며, 만료되면 `AuthenticationError`를 발생시킨다.

        Parameters
        ----------
        client_id: str
            카페24 앱 클라이언트 아이디
        client_secret: str
            카페24 앱 클라이언트 시크릿
        token_info: dict[str, str]
            카페24 액세스 토큰을 포함하는 OAuth 응답 결과.
            `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
            `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.
        from_now: dict
            만료 시각 판단 기준. 현재로부터 `timedelta` 이후의 시각을 기준으로 사용한다.
        save_to: str | Path | None
            OAuth 응답 결과를 저장할 JSON 파일 경로. 상위 경로가 없으면 자동 생성한다.

        Returns
        -------
        dict
            액세스 토큰, 리프레시 토큰 및 만료 시각을 포함한 카페24 OAuth 응답
        """
        if token_info["expires_at"] is None:
            return self.refresh_token(client_id, client_secret, token_info["refresh_token"], save_to)

        import datetime as dt
        try:
            # `expires_at`는 "YYYY-MM-DDTHH:mm:ss.SSS" 형식을 가지는 한국 시간이다.
            expires_dt = dt.datetime.fromisoformat(token_info["expires_at"])
        except (TypeError, ValueError):
            raise ValueError("Invalid OAuth token expiration timestamp.")

        from linkmerce.utils.date import now
        # `now()` 함수의 `timedelta`는 뺄셈으로 적용되므로 부호를 바꾼다.
        timedelta = {key: (value * -1) for key, value in (from_now or dict()).items()}
        target_dt = now(tzinfo="Asia/Seoul", droptz=True, **timedelta)
        if target_dt < expires_dt:
            return token_info
        else:
            return self.refresh_token(client_id, client_secret, token_info["refresh_token"], save_to)

    def build_request_headers(
            self,
            client_id: str,
            client_secret: str,
            **kwargs
        ) -> dict[str, str]:
        """클라이언트 정보를 Base64 인코딩한 Authorization이 포함된 헤더를 반환한다."""
        import base64
        auth_string = f"{client_id}:{client_secret}"
        auth_bytes = auth_string.encode("utf-8")
        auth_b64 = base64.b64encode(auth_bytes).decode("utf-8")
        return {
            "Authorization": f"Basic {auth_b64}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    @Extractor.with_session
    def _request_token(
            self,
            client_id: str,
            client_secret: str,
            body: dict,
            save_to: str | Path | None = None,
        ) -> dict:
        """클라이언트 정보와 요청 본문을 가지고 액세스 토큰을 발급받고, 응답 결과를 검증해 반환한다.

        **NOTE** 요청이 올바르지 않으면 `AuthenticationError`를 발생시킨다.

        **NOTE** 응답을 JSON 파싱할 수 없거나 유효한 필드가 없으면 `RequestError`를 발생시킨다.

        **NOTE** `save_to` 매개변수 값이 주어지면 상위 경로를 자동 생성하면서 JSON 응답 결과를 저장한다.
        """
        url = self.concat_path(self.url, "/oauth/token")
        headers = self.build_request_headers(client_id, client_secret)

        with self.request("POST", url, data=body, headers=headers) as response:
            token_info = self._get_valid_token_response(response)

            if save_to:
                from pathlib import Path
                import json
                file_path = save_to if isinstance(save_to, Path) else Path(save_to)
                file_path.parent.mkdir(parents=True, exist_ok=True)

                with open(file_path, 'w', encoding="utf-8") as file:
                    json.dump(token_info, file, indent=2, ensure_ascii=False, default=str)

            return token_info

    def _get_valid_token_response(self, response: Response) -> dict:
        """응답 결과를 검증해 올바르면 그대로 반환하고 아니면 오류를 발생시킨다."""
        from requests.exceptions import JSONDecodeError
        status = response.status_code
        prefix = f"Cafe24 OAuth token request failed (HTTP {status})"

        try:
            data = response.json()
        except JSONDecodeError:
            from linkmerce.common.exceptions import RequestError
            raise RequestError(f"{prefix}: response is not valid JSON.")

        if not isinstance(data, dict):
            from linkmerce.common.exceptions import RequestError
            raise RequestError(f"{prefix}: expected a JSON object.")

        if (not 200 <= status < 300) or (data.get("error")):
            from linkmerce.common.exceptions import AuthenticationError
            error = data.get("error") or "unknown_error"
            if (error_desc := data.get("error_description")):
                message = f"[{error}] {error_desc}"
            else:
                message = error
            raise AuthenticationError(message)

        invalid_fields = [
            key for key in ["access_token", "refresh_token", "expires_at"]
            if not data.get(key)]
        if invalid_fields:
            from linkmerce.common.exceptions import RequestError
            raise RequestError(
                f"Cafe24 OAuth token response has invalid fields: {', '.join(invalid_fields)}."
            )

        return data
