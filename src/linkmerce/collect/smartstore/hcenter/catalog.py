from __future__ import annotations
from linkmerce.collect.smartstore.hcenter import PartnerCenter

from typing import Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, Coroutine, Literal, Sequence
    from linkmerce.collect.tasks import RequestEachPages
    from linkmerce.types import JsonObject, TaskOptions


class _CatalogCollector(PartnerCenter):
    method = "POST"
    path = "/graphql/product-catalog"
    max_page_size = 100
    page_start = 0
    object_type: Literal["catalogs","products"]
    param_types: dict[str,str]
    fields: list

    def set_options(self, options: TaskOptions = dict()):
        super().set_options(options or dict(
            PaginateAll=dict(delay=1, limit=3, tqdm_options=dict(disable=True)),
            RequestEachPages=dict(delay=1, limit=3)))

    def _init_task(
            self,
            func: Callable | Coroutine,
            context: Sequence = list(),
            expand: dict = dict(),
            partial: dict = dict(),
            page: int | list[int] | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> RequestEachPages:
        if page is not None:
            expand["page"] = page
            partial["page_size"] = page_size
        task = (self.request_each_pages(func, context).partial(**partial).expand(**expand)
                .parse(**self.update_parser(**kwargs)))
        if page is None:
            task = task.loop(self._count_total, self.max_page_size, self.page_start)
        return task.concat("auto")

    def _build_and_request(self, **json) -> JsonObject:
        message = self.build_request(json=json)
        return self.request_json(**message)

    async def _build_and_request_async(self, **json) -> JsonObject:
        message = self.build_request(json=json)
        return await self.request_async_json(**message)

    def _count_total(self, response: JsonObject) -> int:
        from linkmerce.utils.map import hier_get
        return hier_get(response, ["data",self.object_type,"totalCount"])

    def get_request_body(self, variables: dict, **kwargs) -> dict:
        return super().get_request_body(variables=variables)

    def set_request_body(self, *args, **kwargs):
        from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection
        param_types = self.param_types
        super().set_request_body(
            GraphQLOperation(
                operation = self.object_type,
                variables = dict(),
                types = param_types,
                selection = GraphQLSelection(
                    name = self.object_type,
                    variables = {"param": list(param_types.keys())},
                    fields = self.fields,
            )).generate_data(query_options = dict(
                selection = dict(variables=dict(linebreak=False, replace={"id: $id":"ids: $id"}), fields=dict(linebreak=True)),
                suffix = '\n')))

    @PartnerCenter.cookies_required
    def set_request_headers(self, **kwargs):
        contents = dict(type="text", charset="UTF-8")
        referer = "https://center.shopping.naver.com/brand-management/catalog"
        super().set_request_headers(contents=contents, referer=referer, **kwargs)

    def select_sort_type(self, sort_type: Literal["popular","recent","price"]) -> dict[str,str]:
        if sort_type == "product":
            return dict(sort="PopularDegree", direction="DESC")
        elif sort_type == "recent":
            return dict(sort="RegisterDate", direction="DESC")
        elif sort_type == "price":
            return dict(sort="MobilePrice", direction="ASC")
        else:
            return dict()

    @property
    def param_types(self) -> dict[str,str]:
        is_product = (self.object_type == "products")
        return {
            "id":"[ID]", "ids":"[ID!]", "name":"String", "mallSeq":"String", "mallProductIds":"[String!]",
            "catalogIds":"[String!]", "makerSeq":"String", "seriesSeq":"String", "category":"ItemCategoySearchParam",
            "catalogType":"CatalogType", "modelNo":"String", "registerDate":"DateTerm", "includeNullBrand":"YesOrNo",
            "releaseDate":"DateTerm", "brandSeqs":f"[String{'!' * is_product}]", "brandCertificationYn":"YesOrNo",
            "providerId":"String", "providerType":"ProviderType", "serviceYn":"YesOrNo",
            "catalogStatusType":"CatalogStatusType", "productAttributeValueTexts":"[String]",
            "saleMethodType":"SaleMethodType", "overseaProductType":"OverseaProductType", "modelYearSeason":"String",
            "excludeCategoryIds":"[String!]", "excludeCatalogTypes":"[CatalogType!]",
            "connection":("ProductPage!" if is_product else "CatalogPage")
        }


class BrandCatalog(_CatalogCollector):
    object_type = "catalogs"
    args = ["brand_ids", "sort_type", "is_brand_catalog", "page", "page_size"]

    @PartnerCenter.with_session
    def collect(
            self,
            brand_ids: str | Iterable[str],
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | list[int] | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> JsonObject:
        return self._init_task(self._build_and_request,
            expand=dict(brand_ids=brand_ids),
            partial=dict(sort_type=sort_type, is_brand_catalog=is_brand_catalog),
            page=page, page_size=page_size, **kwargs).run()

    @PartnerCenter.async_with_session
    async def collect_async(
            self,
            brand_ids: str | Iterable[str],
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | list[int] | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> JsonObject:
        return await self._init_task(self._build_and_request_async,
            expand=dict(brand_ids=brand_ids),
            partial=dict(sort_type=sort_type, is_brand_catalog=is_brand_catalog),
            page=page, page_size=page_size, **kwargs).run_async()

    def get_request_body(
            self,
            brand_ids: str,
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> dict:
        provider = {True: {"providerId": "268740", "providerType": "BrandCompany"}, False: {"providerType": "None"}}
        return super().get_request_body(
            variables={
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
            })

    @property
    def param_types(self) -> list:
        types = super().param_types
        return dict(map(lambda x: (x, types[x]), [
            "id", "name", "makerSeq", "seriesSeq", "category", "catalogType", "modelNo", "registerDate",
            "includeNullBrand", "releaseDate", "brandSeqs", "providerId", "providerType", "serviceYn",
            "catalogStatusType", "connection", "productAttributeValueTexts", "saleMethodType", "overseaProductType",
            "modelYearSeason", "excludeCategoryIds", "excludeCatalogTypes"
        ]))

    @property
    def fields(self) -> list:
        return [{
            "items": [
                "id", {"image": ["SRC", "F80", "F160"]}, "name", "makerName", "makerSeq", "brandName", "brandSeq",
                "seriesSeq", "seriesName", "lowestPrice", "productCount", "releaseDate", "registerDate", "fullCategoryName",
                "totalReviewCount", "categoryId", "fullCategoryId", "providerId", "providerType", "claimingOwnershipMemberIds",
                "modelNos", "productCountOfCertificated", "modelYearSeason", "serviceYn", "productStatusCode", "productStatusType",
                "categoryName", "reviewRating"
            ]
        }, "totalCount"]


class BrandProduct(_CatalogCollector):
    object_type = "products"

    @PartnerCenter.with_session
    def collect(
            self,
            brand_ids: str | Iterable[str],
            mall_seq: int | str | Iterable[int | str] | None = None,
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> JsonObject:
        return self._init_task(self._build_and_request,
            **self._zip_brand_mall(brand_ids, mall_seq, sort_type=sort_type, is_brand_catalog=is_brand_catalog),
            page=page, page_size=page_size, **kwargs).run()

    @PartnerCenter.async_with_session
    async def collect_async(
            self,
            brand_ids: str | Iterable[str],
            mall_seq: int | str | Iterable | None = None,
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> JsonObject:
        return self._init_task(self._build_and_request_async,
            **self._zip_brand_mall(brand_ids, mall_seq, sort_type=sort_type, is_brand_catalog=is_brand_catalog),
            page=page, page_size=page_size, **kwargs).run_async()

    def _zip_brand_mall(
            self,
            brand_ids: str | Iterable[str],
            mall_seq: int | str | Iterable[int | str] | None = None,
            **partial
        ) -> dict:
        is_iterable_brand = (not isinstance(brand_ids, str)) and isinstance(brand_ids, Iterable)
        is_iterable_mall = (not isinstance(mall_seq, str)) and isinstance(mall_seq, Iterable)
        if is_iterable_brand and (len(brand_ids) == len(mall_seq)):
            return dict(context=[dict(brand_ids=ids, mall_seq=seq) for ids, seq in zip(brand_ids, mall_seq)], partial=partial)
        elif not (is_iterable_brand or is_iterable_mall):
            return dict(expand=dict(brand_ids=brand_ids), partial=dict(mall_seq=mall_seq, **partial))
        else:
            return dict(expand=dict(brand_ids=brand_ids), partial=partial)

    def get_request_body(
            self,
            brand_ids: str,
            mall_seq: int | str | None = None,
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> dict:
        kv = lambda key, value: {key: value} if value is not None else {}
        return super().get_request_body(
            variables={
                "connection": {
                    "page": int(page),
                    "size": int(page_size),
                    **self.select_sort_type(sort_type),
                },
                **kv("isBrandOfficialMall", is_brand_catalog),
                "serviceYn": "Y",
                **kv("mallSeq", mall_seq),
                "brandSeqs": brand_ids.split(','),
            })

    @property
    def param_types(self) -> list:
        types = super().param_types
        return dict(map(lambda x: (x, types[x]), [
            "ids", "name", "mallSeq", "mallProductIds", "catalogIds", "makerSeq", "category", "registerDate", "serviceYn",
            "brandSeqs", "brandCertificationYn", "connection"
        ]))

    @property
    def fields(self) -> list:
        return [{
            "items": [
                "id", {"image": ["F60", "F80", "SRC"]}, "name", "makerName", "makerSeq", "brandName", "brandSeq",
                "serviceYn", "lowestPrice", "registerDate", "fullCategoryName", "categoryId", "fullCategoryId", "mallName",
                "mallProductId", "buyingOptionValue", "catalogId", "brandCertificationYn", "outLinkUrl", "categoryName",
                "categoryShapeType", "categoryLeafYn", "productStatusCode", "saleMethodTypeCode"
            ]
        }, "totalCount"]
