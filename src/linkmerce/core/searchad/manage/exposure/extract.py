from __future__ import annotations
from linkmerce.core.searchad.manage import SearchAdManager

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.common.extract import JsonObject


class ExposureDiagnosis(SearchAdManager):
    method = "GET"
    path = "/ncc/sam/exposure-status-shopping"

    @property
    def default_options(self) -> dict:
        return dict(
            RequestLoop = dict(count=5, raise_errors=RuntimeError, ignored_errors=Exception),
            RequestEachLoop = dict(delay=1.01))

    @SearchAdManager.with_session
    @SearchAdManager.with_token
    def extract(
            self,
            keyword: str | Iterable[str],
            domain: Literal["search","shopping"] = "search",
            mobile: bool = True,
            is_own: bool | None = None,
            **kwargs
        ) -> JsonObject:
        return (self.request_each_loop(self.request_json_safe)
                .partial(domain=domain, mobile=mobile, is_own=is_own)
                .expand(keyword=keyword)
                .loop(self.is_valid_response)
                .run())

    def build_request_params(
            self,
            keyword: str,
            domain: Literal["search","shopping"] = "search",
            mobile: bool = True,
            ageTarget: int = 11,
            genderTarget: str = 'U',
            regionalCode: int = 99,
            **kwargs
        ) -> dict:
        return {
            "keyword": str(keyword).upper(),
            "media": int(str(["search","shopping"].index(domain))+str(int(mobile)),2),
            "ageTarget": int(ageTarget),
            "genderTarget": genderTarget,
            "regionalCode": int(regionalCode),
        }

    def build_request_headers(self, **kwargs: str) -> dict[str,str]:
        return dict(self.get_request_headers(), authorization=self.get_authorization())

    @SearchAdManager.cookies_required
    def set_request_headers(self, **kwargs: str):
        referer = f"{self.main_url}/customers/{self.customer_id}/tool/exposure-status"
        super().set_request_headers(referer=referer, **kwargs)


class ExposureRank(ExposureDiagnosis):
    ...
