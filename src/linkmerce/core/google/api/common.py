from __future__ import annotations

from linkmerce.common.extract import Extractor
import functools

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.extract import Variables
    from pathlib import Path


class GoogleAPI(Extractor):
    service: str
    method: str = "POST"
    access_token: str | None = None

    @property
    def origin(self) -> str:
        return f"https://{self.service}.googleapis.com/"

    def set_variables(self, variables: Variables = dict()):
        try:
            self.set_service_account(**variables)
        except TypeError:
            raise TypeError("Google API requires variables for service_account and scope.")

    def set_service_account(
            self,
            service_account: str | Path | dict[str,str],
            scope: str,
            **variables,
        ):
        super().set_variables(dict(
            service_account = self._read_service_account(service_account),
            scope = scope,
            **variables
        ))

    def _read_service_account(self, service_account: str | Path | dict[str,str]) -> dict[str,str]:
        if not isinstance(service_account, dict):
            import json
            with open(str(service_account), 'r', encoding="utf-8") as file:
                service_account = json.loads(file.read())

        if isinstance(service_account, dict) and ("client_email" in service_account) and ("private_key" in service_account):
            return service_account
        else:
            raise ValueError("Service account is not valid.")

    @property
    def service_account(self) -> dict[str,str]:
        return self.get_variable("service_account")

    @property
    def scope(self) -> str:
        return self.get_variable("scope")

    def with_token(func):
        @functools.wraps(func)
        def wrapper(self: GoogleAPI, *args, **kwargs):
            self.set_access_token()
            return func(self, *args, **kwargs)
        return wrapper

    def set_access_token(self):
        auth = GoogleAuth(self.service_account, self.scope)
        self.access_token = auth.get_access_token()


class GoogleAuth:

    def __init__(self, service_account: dict[str,str], scope: str, ttl: int = 3600):
        self.service_account = service_account
        self.scope = scope
        self.ttl = ttl

    def get_access_token(self) -> str:
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
