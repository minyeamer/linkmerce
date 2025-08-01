from __future__ import annotations
from linkmerce.collect.searchad.manage import SearchAdManager

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.types import JsonObject, TaskOptions


class ExposureDiagnosis(SearchAdManager):
    method = "GET"
    path = "/ncc/sam/exposure-status-shopping"
    args = ["keyword", "domain", "mobile", "is_own"]

    def set_options(self, options: TaskOptions = dict()):
        super().set_options(options or dict(RequestLoop=dict(count=5), RequestEach=dict(delay=1.01)))

    @SearchAdManager.with_session
    @SearchAdManager.with_token
    def collect(
            self,
            keyword: str | Iterable[str],
            domain: Literal["search","shopping"] = "search",
            mobile: bool = True,
            is_own: bool | None = None,
            **kwargs
        ) -> JsonObject:
        return (self.request_each(self._build_and_request)
                .partial(domain=domain, mobile=mobile, is_own=is_own).expand(keyword=keyword)
                .parse(**self.update_parser(**kwargs))
                .loop(self._is_valid_response).concat("auto").run())

    def _build_and_request(self, **params) -> JsonObject:
        message = self.build_request(params=params)
        return self.request_json(**message)

    def _is_valid_response(self, response: JsonObject) -> bool:
        return not (isinstance(response, dict) and (response.get("code") == 90100))

    def get_request_params(
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

    def get_request_headers(self, **kwargs) -> dict[str,str]:
        kwargs["authorization"] = self.get_authorization()
        return super().get_request_headers(**kwargs)

    @SearchAdManager.cookies_required
    def set_request_headers(self, **kwargs):
        referer = f"{self.main_url}/customers/{self.get_customer_id()}/tool/exposure-status"
        super().set_request_headers(referer=referer, **kwargs)


class ExposureRank(ExposureDiagnosis):
    ...
