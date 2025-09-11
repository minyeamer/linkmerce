from __future__ import annotations
from linkmerce.common.extract import Extractor
from linkmerce.core.smartstore.center.common import SmartstoreLogin

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5
import base64


class PartnerCenter(Extractor):
    method: str | None = None
    origin: str = "https://hcenter.shopping.naver.com"
    path: str | None = None

    @property
    def url(self) -> str:
        return self.concat_path(self.origin, self.path)


###################################################################
####################### Partner Center Login ######################
###################################################################

class PartnerCenterLogin(SmartstoreLogin):
    center_url = "https://center.shopping.naver.com"
    hcenter_url = "https://hcenter.shopping.naver.com"

    @SmartstoreLogin.with_session
    def login(
            self,
            userid: str | None = None,
            passwd: str | None = None,
            channel_seq: int | str | None = None,
            cookies: str | None = None,
            **kwargs
        ) -> dict:
        from linkmerce.utils.regex import regexp_extract
        login_info = super().login(userid, passwd, channel_seq, cookies)
        self.login_init(login_info["redirectUrl"])
        self.celogin(pincode=regexp_extract(r"\?pincode=(.*)$", login_info["redirectUrl"]))
        self.fetch_embrace_token(subject="main", params=dict(targetUrl=(self.center_url + "/iframe/main.nhn")))
        self.fetch_embrace_token(subject="brand-analytics.dashboard", login=True)
        return login_info

    ############################ Login Init ###########################

    def login_init(self, redirect_url: str):
        self.redirect_begin(redirect_url)

        url = self.main_url + "/api/login/init"
        params = dict(needLoginInfoForAngular="true", stateName="home")
        headers = self.get_login_header()
        self.request("GET", url, params=params, headers=headers)

        self.click_to_center()

    def redirect_begin(self, redirect_url: str):
        headers = self.build_request_headers(redirect_url, referer=self.main_url)
        self.request("GET", redirect_url, headers=headers)

    def get_login_header(self) -> dict[str,str]:
        headers = self.build_request_headers(self.main_url, referer=self.main_url)
        headers["x-current-state"] = self.main_url + "/#/home/dashboard"
        headers["x-current-statename"] = "work.channel-select"
        headers["x-to-statename"] = "work.channel-select"
        return headers

    def click_to_center(self):
        url = self.main_url + "/api/sell/center/front-history/click"
        body = {
            "actionId": "gnb.link",
            "actionLocationId": "shoppingPartner",
            "groupStateCode": "layout",
            "stateCode": "main.dashboard-pay",
        }
        headers = self.build_request_headers(url, origin=self.main_url, referer=self.main_url)
        self.request("POST", url, json=body, headers=headers)

    ########################### Center Login ##########################

    def celogin(self, pincode: str):
        login_info = self.authenticate(pincode)
        link = self.fetch_link()
        pubkey = self.fetch_pubkey(link)

        url = self.center_url + "/oauth2/login"
        body = self.build_login_data(pincode, pubkey, login_info)
        headers = self.build_request_headers(url, contents="form", https=True, referer=(self.center_url + "/v1/slogin2/login"), origin=self.center_url)
        with self.request("POST", url, data=body, headers=headers, allow_redirects=False) as response:
            self.celogin_redirect(response.headers["location"])

    def authenticate(self, pincode: str) -> dict:
        url = self.center_url + "/oauth2/authenticate"
        body = {
            "clientId": "smartstore",
            "pincode": pincode,
            "redirectUri": (self.center_url + "/login/redirect"),
            "state": "xyz",
        }
        headers = self.build_request_headers(url, contents="json", origin=self.center_url, referer=(self.center_url + "/v1/slogin2/login"))
        with self.request("POST", url, json=body, headers=headers) as response:
            return response.json()

    def fetch_link(self) -> str:
        url = self.center_url + "/v1/slogin2/login"
        headers = self.build_request_headers(url, https=True, referer=self.center_url)
        with self.request("GET", url, headers=headers, allow_redirects=False) as response:
            return response.headers["Link"]

    def fetch_pubkey(self, link: str) -> str:
        from linkmerce.utils.regex import regexp_extract
        url = self.center_url + regexp_extract(r"(/_app/[^/]+/immutable/chunks/encrypt\.[^.]+\.js)", link)
        headers = self.build_request_headers(url, referer=self.center_url)
        with self.request("GET", url, headers=headers) as response:
            return regexp_extract(r"`(-----BEGIN PUBLIC KEY-----[^`]+-----END PUBLIC KEY-----)`", response.text)

    def build_login_data(self, pincode: str, pubkey: str, login_info: dict) -> dict:
        from urllib.parse import urlencode
        return_url = urlencode({"return-uri": f"{self.center_url}/v1/slogin2/redirect"})

        return {
            "pincode": pincode,
            "username": self.JSEncrypt(login_info["response"]["alternateName"], pubkey),
            "client_id": "smartstore",
            "redirect_uri": f"{self.center_url}/login/redirect?{return_url}",
            "target_uri": "",
            "response_type": "code",
            "state": "xyz",
            "authentication_id": login_info["identifier"],
        }

    def JSEncrypt(self, username: str, publicKey: str) -> bytes:
        rsa_key = RSA.importKey(publicKey.encode("utf-8"))
        cipher = PKCS1_v1_5.new(rsa_key)
        return base64.b64encode(cipher.encrypt(username.encode("utf-8")))

    def celogin_redirect(self, redirect_url: str):
        headers = self.build_request_headers(redirect_url, referer=(self.center_url + "/v1/slogin2/login"))
        self.request("GET", redirect_url, headers=headers)

    ###################################################################
    ########################## Embrace Token ##########################
    ###################################################################

    def fetch_embrace_token(self, subject: str, login: bool = False, params: dict = dict()) -> str:
        url = self.center_url + "/v2/members/me/embrace-token-at-url"
        params = dict(subject=subject, **params)
        referer = '/'.join(["https://center.shopping.naver.com", str(subject).replace('.','/')])
        headers = self.build_request_headers(url, https=True, referer=referer)
        headers["Sec-Fetch-Dest"] = "iframe"
        with self.request("GET", url, params=params, headers=headers, allow_redirects=False) as response:
            self.redirect_embrace_token(response.headers["location"], subject, login=login)

    def redirect_embrace_token(self, redirect_url: str, referer: str, login: bool = False):
        headers = self.build_request_headers(redirect_url, https=True, referer=referer)
        self.request("GET", redirect_url, headers=headers)
        if login:
            self.login_by_token(redirect_url)

    def login_by_token(self, redirect_url: str):
        from linkmerce.utils.regex import regexp_extract
        self.get_member()

        url = self.hcenter_url + "/v1/login/by-token"
        params = dict(token=regexp_extract(r"\?token=(.*)$", redirect_url))
        headers = self.build_request_headers(url, referer=redirect_url)
        self.request("GET", url, params=params, headers=headers)

    def get_member(self):
        url = self.hcenter_url + "/graphql"
        headers = self.build_request_headers(url, referer=self.center_url)
        self.request("POST", url, data=self.build_member_data(), headers=headers)

    def build_member_data(self) -> dict:
        from linkmerce.utils.graphql import GraphQLFields
        query = (GraphQLFields([
            "token", "loginForced",
            {
                "member": [
                    "identifier", "name", "alternateName",
                    {
                        "memberOf": [
                        "identifier", "sequence", "name", "alternateName", "taxID",
                        {
                            "brands": [
                            "identifier", "name",
                            {"owns": [{"category": ["identifier","name"]}]},
                            {"members": ["identifier", "name", "alternateName", "url"]},
                            {"mainEntityOfPage": ["identifier", "name", "alternateName", "url"]},
                            {"additionalProperties": ["propertyID", "value"]}
                            ]
                        },
                        {"termsOfSerivce": ["identifier"]},
                        "additionalType", "action", "potentialActions",
                        {"subOrganizations": ["sequence"]},
                        {"aggregateRatings": ["ratingValue", "additionalType", "potentialActions"]}
                        ]
                    },
                    {"roles": ["roleName"]},
                    {"authorities": ["authorityName"]}
                ]
            }
            ]).generate_fields(indent=2, prefix="query getMember ")[:(len("  __typename\n}")*-1)] + '}\n')
        return {"operationName": "getMember", "variables": {}, "query": query}
