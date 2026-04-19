from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Configs
    from pathlib import Path


class GoogleApi(Extractor):
    """구글 API 요청을 처리하는 공통 클래스.

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    service_account: str | Path | dict[str, str]
        구글 서비스 계정 JSON 파일 경로 또는 딕셔너리
    scope: str
        OAuth 인증 스코프
    """

    service: str
    method: str = "POST"
    access_token: str | None = None

    @property
    def origin(self) -> str:
        return f"https://{self.service}.googleapis.com/"

    def set_configs(self, configs: Configs = dict()):
        try:
            self.set_service_account(**configs)
        except TypeError:
            raise TypeError("Google API requires configs for service_account and scope.")

    def set_service_account(
            self,
            service_account: str | Path | dict[str, str],
            scope: str,
            **configs,
        ):
        """구글 서비스 계정 인증 정보를 설정한다."""
        super().set_configs(dict(
            service_account = self._read_service_account(service_account),
            scope = scope,
            **configs
        ))

    def _read_service_account(self, service_account: str | Path | dict[str, str]) -> dict[str, str]:
        """구글 서비스 계정은 로컬 JSON 파일을 읽거나, 딕셔너리 객체를 전달받는다."""
        if not isinstance(service_account, dict):
            import json
            with open(str(service_account), 'r', encoding="utf-8") as file:
                service_account = json.loads(file.read())

        if isinstance(service_account, dict) and ("client_email" in service_account) and ("private_key" in service_account):
            return service_account
        else:
            raise ValueError("Service account is not valid.")

    @property
    def service_account(self) -> dict[str, str]:
        return self.get_config("service_account")

    @property
    def scope(self) -> str:
        return self.get_config("scope")

    def with_token(func):
        """API 요청 전 액세스 토큰을 발급받는 데코레이터."""
        @functools.wraps(func)
        def wrapper(self: GoogleApi, *args, **kwargs):
            self.set_access_token()
            return func(self, *args, **kwargs)
        return wrapper

    def set_access_token(self):
        """구글 서비스 계정 기반의 JWT 인증으로 발급받은 액세스 토큰을 설정한다."""
        auth = GoogleAuth(self.service_account, self.scope)
        self.access_token = auth.get_access_token()


class GoogleAuth:
    """구글 서비스 계정 기반의 JWT 인증을 수행하여 액세스 토큰을 발급하는 클래스."""

    def __init__(self, service_account: dict[str, str], scope: str, ttl: int = 3600):
        self.service_account = service_account
        self.scope = scope
        self.ttl = ttl

    def get_access_token(self) -> str:
        """구글 서비스 계정 기반의 JWT 인증으로 발급받은 액세스 토큰을 발급받는다."""
        from Crypto.PublicKey import RSA
        from Crypto.Signature import pkcs1_15
        from Crypto.Hash import SHA256
        import base64, json, requests, time

        now = int(time.time())

        # JWT Header
        header = json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(',', ':'))
        header_b64 = base64.urlsafe_b64encode(header.encode()).decode().rstrip('=')

        # JWT Payload
        payload_dict = {
            "iss": self.service_account["client_email"],
            "scope": self.scope,
            "aud": "https://oauth2.googleapis.com/token",
            "exp": now + self.ttl,
            "iat": now,
        }
        payload = json.dumps(payload_dict, separators=(',', ':'))
        payload_b64 = base64.urlsafe_b64encode(payload.encode()).decode().rstrip('=')

        # JWT Signature
        jwt_unsigned = f"{header_b64}.{payload_b64}"

        private_key = RSA.import_key(self.service_account["private_key"])
        signer = pkcs1_15.new(private_key)
        digest = SHA256.new()
        digest.update(jwt_unsigned.encode())
        signature = signer.sign(digest)
        signature_b64 = base64.urlsafe_b64encode(signature).decode().rstrip('=')

        # Token Request
        token_url = "https://oauth2.googleapis.com/token"
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": f"{jwt_unsigned}.{signature_b64}"
        }
        response = requests.post(token_url, data=data)
        response.raise_for_status()
        return response.json()["access_token"]
