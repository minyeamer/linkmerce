from __future__ import annotations
from linkmerce.collect.searchad.manage import SearchAdManager

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Dict, Literal
    from linkmerce.types import JsonObject


class ExposureDiagnosis(SearchAdManager):
    method = "GET"
    path = "/ncc/sam/exposure-status-shopping"

    @SearchAdManager.with_session
    def collect(
            self,
            keyword: str,
            domain: Literal["search","shopping"] = "search",
            mobile: bool = True,
            is_own: bool | None = None,
        ) -> JsonObject:
        kwargs = dict(keyword=keyword, domain=domain, mobile=mobile)
        message = self.build_request(params=kwargs)
        response = self.request_json(**message)
        return self.parse(response, is_own=is_own, **kwargs)

    def get_request_params(
            self,
            keyword: str,
            domain: Literal["search","shopping"] = "search",
            mobile: bool = True,
            ageTarget: int = 11,
            genderTarget: str = 'U',
            regionalCode: int = 99,
            **kwargs
        ) -> Dict:
        return {
            "keyword": str(keyword).upper(),
            "media": int(str(["search","shopping"].index(domain))+str(int(mobile)),2),
            "ageTarget": int(ageTarget),
            "genderTarget": genderTarget,
            "regionalCode": int(regionalCode),
        }

    def get_request_headers(self, **kwargs) -> Dict[str,str]:
        kwargs["authorization"] = self.get_authorization()
        return super().get_request_headers(**kwargs)

    @SearchAdManager.cookies_required
    def set_request_headers(self, **kwargs):
        referer = f"{self.main_url}/customers/{self.get_account('customer_id')}/tool/exposure-status"
        super().set_request_headers(referer=referer, **kwargs)
