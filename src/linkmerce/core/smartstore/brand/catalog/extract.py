from __future__ import annotations
from linkmerce.core.smartstore.brand import PartnerCenter

from typing import Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal
    from linkmerce.common.extract import JsonObject


def _is_iterable(obj: Any) -> bool:
    return (not isinstance(obj, str)) and isinstance(obj, Iterable)


class _CatalogExtractor(PartnerCenter):
    method = "POST"
    path = "/graphql/product-catalog"
    max_page_size = 100
    page_start = 0
    object_type: Literal["catalogs","products"]
    param_types: dict[str,str]
    fields: list

    @property
    def default_options(self) -> dict:
        return dict(
            PaginateAll = dict(delay=1, limit=3),
            RequestEachPages = dict(delay=1, limit=3))

    def split_map_kwargs(
            self,
            brand_ids: str | Iterable[str],
            sort_type: Literal["popular","recent","price"] = "poular",
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

    def count_total(self, response: JsonObject, **kwargs) -> int:
        from linkmerce.utils.map import hier_get
        return hier_get(response, ["data",self.object_type,"totalCount"])

    def build_request_json(self, variables: dict, **kwargs) -> dict:
        return dict(self.get_request_body(), variables=variables)

    def set_request_body(self):
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
            )).generate_body(query_options = dict(
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


class BrandCatalog(_CatalogExtractor):
    object_type = "catalogs"

    @PartnerCenter.with_session
    def extract(
            self,
            brand_ids: str | Iterable[str],
            sort_type: Literal["popular","recent","price"] = "poular",
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
            sort_type: Literal["popular","recent","price"] = "poular",
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
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> dict:
        provider = {True: {"providerId": "268740", "providerType": "BrandCompany"}, False: {"providerType": "None"}}
        return super().build_request_json({
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


class BrandProduct(_CatalogExtractor):
    object_type = "products"

    @PartnerCenter.with_session
    def extract(
            self,
            brand_ids: str | Iterable[str],
            mall_seq: int | str | Iterable[int | str] | None = None,
            sort_type: Literal["popular","recent","price"] = "poular",
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
            sort_type: Literal["popular","recent","price"] = "poular",
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
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
        ) -> tuple[list,dict,dict]:
        context = list()
        partial, expand = super().split_map_kwargs(brand_ids, sort_type, is_brand_catalog, page, page_size)
        if _is_iterable(brand_ids):
            if _is_iterable(mall_seq) and (len(brand_ids) == len(mall_seq)):
                context = [dict(brand_ids=ids, mall_seq=seq) for ids, seq in zip(brand_ids, mall_seq)]
                expand = dict()
            else:
                partial.update(mall_seq=mall_seq)
        elif not _is_iterable(mall_seq):
            partial.update(mall_seq=mall_seq)
        return context, partial, expand

    def build_request_json(
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
        return super().build_request_json({
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


class _BrandStoreProduct(BrandProduct):
    object_type = "products"

    @PartnerCenter.with_session
    def extract(
            self,
            brand_ids: str | Iterable[str],
            mall_seq: int | str | Iterable[int | str],
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
            mall_seq: int | str | Iterable,
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


class BrandPrice(_BrandStoreProduct):
    object_type = "products"


class ProductCatalog(_BrandStoreProduct):
    object_type = "products"
