from __future__ import annotations
from linkmerce.core.smartstore.brand import PartnerCenter

from typing import Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal
    from linkmerce.common.extract import JsonObject


class _VPartnerCenter(PartnerCenter):
    method = "POST"
    origin: str = "https://vcenter.shopping.naver.com"
    max_page_size = 100
    page_start = 0

    @property
    def default_options(self) -> dict:
        return dict(
            PaginateAll = dict(request_delay=1, max_concurrent=3),
            RequestEachPages = dict(request_delay=1, max_concurrent=3))

    def count_total(self, response: JsonObject, **kwargs) -> int:
        from linkmerce.utils.map import hier_get
        return hier_get(response, ["totalCount"])

    def split_map_kwargs(
            self,
            brand_ids: str | Iterable[str],
            sort_type: Literal["popular","recent","price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int | list[int] | None = 0,
            page_size: int = 100,
        ) -> tuple[dict,dict]:
        partial = dict(sort_type=sort_type, is_brand_catalog=is_brand_catalog)
        expand = dict(brand_ids=brand_ids)
        if page is not None:
            partial["page_size"] = page_size
            expand["page"] = page
        return partial, expand

    @PartnerCenter.cookies_required
    def set_request_headers(self, **kwargs):
        referer = "https://center.shopping.naver.com/brand-management/catalog"
        super().set_request_headers(contents="json", origin=self.origin, referer=referer, **kwargs)

    def select_sort_type(self, sort_type: Literal["popular","recent","price"]) -> dict[str,str]:
        if sort_type == "product":
            return dict(sort="PopularDegree", direction="DESC")
        elif sort_type == "recent":
            return dict(sort="RegisterDate", direction="DESC")
        elif sort_type == "price":
            return dict(sort="MobilePrice", direction="ASC")
        else:
            return dict()


class BrandCatalog(_VPartnerCenter):
    path = "/api/catalogs"

    @PartnerCenter.with_session
    def extract(
            self,
            brand_ids: str | Iterable[str],
            sort_type: Literal["popular","recent","price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int | list[int] | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> JsonObject:
        partial, expand = self.split_map_kwargs(brand_ids, sort_type, is_brand_catalog, page, page_size)
        return (self.request_each_pages(self.request_json_safe)
                .partial(**partial)
                .expand(**expand)
                .all_pages(self.count_total, self.max_page_size, self.page_start, page)
                .run())

    @PartnerCenter.async_with_session
    async def extract_async(
            self,
            brand_ids: str | Iterable[str],
            sort_type: Literal["popular","recent","price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int | list[int] | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> JsonObject:
        partial, expand = self.split_map_kwargs(brand_ids, sort_type, is_brand_catalog, page, page_size)
        return await (self.request_each_pages(self.request_async_json_safe)
                .partial(**partial)
                .expand(**expand)
                .all_pages(self.count_total, self.max_page_size, self.page_start, page)
                .run_async())

    def build_request_json(
            self,
            brand_ids: str,
            sort_type: Literal["popular","recent","price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int = 0,
            page_size: int = 100,
            **kwargs
        ) -> dict:
        provider = {
            True: {"providerId": "9712639", "providerType": "BrandCompany"},
            False: {"providerType": "None"}
        }
        return {
                "connection": {
                    "page": int(page),
                    "size": int(page_size),
                    **self.select_sort_type(sort_type),
                },
                "includeNullBrand": "N",
                "serviceYn": "Y",
                "catalogStatusType": "Complete",
                "overseaProductType": "Nothing",
                "saleMethodType": "NothingOrRental",
                "brandSeqs": brand_ids.split(','),
                **provider.get(is_brand_catalog, dict()),
            }


class BrandProduct(_VPartnerCenter):
    path = "/api/offers"

    @PartnerCenter.with_session
    def extract(
            self,
            brand_ids: str | Iterable[str],
            mall_seq: int | str | Iterable[int | str] | None = None,
            sort_type: Literal["popular","recent","price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> JsonObject:
        context, partial, expand = self.split_map_kwargs(brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size)
        return (self.request_each_pages(self.request_json_safe, context)
                .partial(**partial)
                .expand(**expand)
                .all_pages(self.count_total, self.max_page_size, self.page_start, page)
                .run())

    @PartnerCenter.async_with_session
    async def extract_async(
            self,
            brand_ids: str | Iterable[str],
            mall_seq: int | str | Iterable | None = None,
            sort_type: Literal["popular","recent","price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> JsonObject:
        context, partial, expand = self.split_map_kwargs(brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size)
        return await (self.request_each_pages(self.request_async_json_safe, context)
                .partial(**partial)
                .expand(**expand)
                .all_pages(self.count_total, self.max_page_size, self.page_start, page)
                .run_async())

    def split_map_kwargs(
            self,
            brand_ids: str | Iterable[str],
            mall_seq: int | str | Iterable | None = None,
            sort_type: Literal["popular","recent","price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
        ) -> tuple[list,dict,dict]:
        context = list()
        partial, expand = super().split_map_kwargs(brand_ids, sort_type, is_brand_catalog, page, page_size)

        def is_iterable(obj: Any) -> bool:
            return (not isinstance(obj, str)) and isinstance(obj, Iterable)

        if is_iterable(brand_ids):
            if is_iterable(mall_seq) and (len(brand_ids) == len(mall_seq)):
                context = [dict(brand_ids=ids, mall_seq=seq) for ids, seq in zip(brand_ids, mall_seq)]
                expand = dict()
            else:
                partial.update(mall_seq=mall_seq)
        elif not is_iterable(mall_seq):
            partial.update(mall_seq=mall_seq)
        return context, partial, expand

    def build_request_json(
            self,
            brand_ids: str,
            mall_seq: int | str | None = None,
            sort_type: Literal["popular","recent","price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int = 0,
            page_size: int = 100,
            **kwargs
        ) -> dict:
        kv = lambda key, value: {key: value} if value is not None else {}
        return {
                "connection": {
                    "page": int(page),
                    "size": int(page_size),
                    **self.select_sort_type(sort_type),
                },
                **kv("isBrandOfficialMall", is_brand_catalog),
                "serviceYn": "Y",
                **kv("mallSeq", mall_seq),
                "brandSeqs": brand_ids.split(','),
            }
