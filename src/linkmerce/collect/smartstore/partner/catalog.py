from __future__ import annotations
from linkmerce.collect.smartstore.partner import PartnerCenter

from typing import Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable, Dict, List, Literal, Tuple
    from linkmerce.types import JsonObject


MAX_SIZE = 100


class _CatalogCollector(PartnerCenter):
    method = "POST"
    path = "/graphql/product-catalog"
    date_format = "%Y-%m-%d"
    object_type: Literal["catalogs","products"]
    param_types: Dict[str,str]
    fields: List

    def _collect_all(self, parser: Callable | None = None, page=None, **kwargs) -> JsonObject:
        kwargs = dict(kwargs, page_size=MAX_SIZE)
        response = self.collect(parser=None, page=0, **kwargs)
        total = self._parse_total_count(response)
        data, is_sequence = self._parse_first(response, parser, **kwargs)
        if isinstance(total, int) and (total > MAX_SIZE):
            from math import ceil
            for page in range(1, ceil(total/MAX_SIZE)):
                if is_sequence:
                    data += self.collect(parser=parser, page=page, **kwargs)
                else:
                    data.append(self.collect(parser=parser, page=page, **kwargs))
        return data

    async def _collect_async_all(self, parser: Callable | None = None, page=None, **kwargs) -> JsonObject:
        kwargs = dict(kwargs, page_size=MAX_SIZE)
        response = await self.collect_async(parser=None, page=0, **kwargs)
        total = self._parse_total_count(response)
        data, is_sequence = self._parse_first(response, parser, **kwargs)
        if isinstance(total, int) and (total > MAX_SIZE):
            from math import ceil
            import asyncio
            results = await asyncio.gather(*[
                self.collect_async(parser=parser, page=page, **kwargs) for page in range(1, ceil(total/MAX_SIZE))])
            if is_sequence:
                from itertools import chain
                return data + list(chain.from_iterable(results))
            else:
                return data + results
        return data

    def _parse_first(self, response: JsonObject, parser: Callable | None = None, **kwargs) -> Tuple[JsonObject,bool]:
        data = self.parse(response, parser, **kwargs)
        return (data, True) if isinstance(data, Sequence) else ([data], False)

    def _parse_total_count(self, response: JsonObject) -> int:
        from linkmerce.utils.map import hier_get
        return hier_get(response, ["data",self.object_type,"totalCount"])

    def get_request_body(self, variables: Dict, **kwargs) -> Dict:
        return dict(self.__body, variables=variables)

    def set_request_body(self, **kwargs):
        from linkmerce.utils.graphql import GraphQLOperation, GraphQLSelection
        param_types = self.param_types
        self.__body = GraphQLOperation(
            operation = self.object_type,
            variables = dict(),
            types = param_types,
            selection = GraphQLSelection(
                name = self.object_type,
                variables = {"param": list(param_types.keys())},
                fields = self.fields,
        )).generate_data(query_options = dict(
            selection = dict(variables=dict(linebreak=False, replace={"id: $id":"ids: $id"}), fields=dict(linebreak=True)),
            suffix = '\n'))

    @PartnerCenter.cookies_required
    def set_request_headers(self, **kwargs):
        contents = dict(type="text", charset="UTF-8")
        referer = "https://center.shopping.naver.com/brand-management/catalog"
        super().set_request_headers(contents=contents, referer=referer, **kwargs)

    def select_sort_type(self, sort_type: Literal["popular","recent","price"]) -> Dict[str,str]:
        if sort_type == "product":
            return dict(sort="PopularDegree", direction="DESC")
        elif sort_type == "recent":
            return dict(sort="RegisterDate", direction="DESC")
        elif sort_type == "price":
            return dict(sort="MobilePrice", direction="ASC")
        else:
            return dict()

    def select_parser(self, parser: str | Callable | None = None) -> Callable:
        if isinstance(parser, str) and (parser == f"Brand{self.object_type[:-1].capitalize()}"):
            return self.import_parser(parser)
        else:
            return parser

    @property
    def param_types(self) -> Dict[str,str]:
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

    @PartnerCenter.with_session
    def collect(
            self,
            brand_ids: List[int | str],
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            parser: Literal["BrandCatalog"] | Callable | None = "BrandCatalog",
        ) -> JsonObject:
        kwargs = self.build_kwargs(brand_ids, sort_type, is_brand_catalog, page, page_size)
        if page is None:
            return self._collect_all(self.select_parser(parser), **kwargs)
        message = self.build_request(json=kwargs)
        response = self.request_json(**message)
        return self.parse(response, self.select_parser(parser), **kwargs)

    @PartnerCenter.with_client_session
    async def collect_async(
            self,
            brand_ids: List[int | str],
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            parser: Literal["BrandCatalog"] | Callable | None = "BrandCatalog",
        ) -> JsonObject:
        kwargs = self.build_kwargs(brand_ids, sort_type, is_brand_catalog, page, page_size)
        if page is None:
            return await self._collect_async_all(self.select_parser(parser), **kwargs)
        message = self.build_request(json=kwargs)
        response = await self.request_async_json(**message)
        return self.parse(response, self.select_parser(parser), **kwargs)

    def build_kwargs(self, brand_ids, sort_type, is_brand_catalog, page, page_size) -> Dict:
        return dict(
            brand_ids=brand_ids, sort_type=sort_type, is_brand_catalog=is_brand_catalog,
            page=page, page_size=page_size)

    def get_request_body(
            self,
            brand_ids: List[int | str],
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> Dict:
        provider = {True: {"providerId": "268740", "providerType": "BrandCompany"}, False: {"providerType": "None"}}
        return super().get_request_body({
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
            "brandSeqs": list(map(str, brand_ids)),
            **provider.get(is_brand_catalog, dict()),
        })

    @property
    def param_types(self) -> List:
        types = super().param_types
        return dict(map(lambda x: (x, types[x]), [
            "id", "name", "makerSeq", "seriesSeq", "category", "catalogType", "modelNo", "registerDate",
            "includeNullBrand", "releaseDate", "brandSeqs", "providerId", "providerType", "serviceYn",
            "catalogStatusType", "connection", "productAttributeValueTexts", "saleMethodType", "overseaProductType",
            "modelYearSeason", "excludeCategoryIds", "excludeCatalogTypes"
        ]))

    @property
    def fields(self) -> List:
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
            brand_ids: List[int | str],
            mall_seq: int | str | None = None,
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            parser: Literal["BrandProduct"] | Callable | None = "BrandProduct",
        ) -> JsonObject:
        kwargs = self.build_kwargs(brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size)
        if page is None:
            return self._collect_all(self.select_parser(parser), **kwargs)
        message = self.build_request(json=kwargs)
        response = self.request_json(**message)
        return self.parse(response, self.select_parser(parser), **kwargs)

    @PartnerCenter.with_client_session
    async def collect_async(
            self,
            brand_ids: List[int | str],
            mall_seq: int | str | None = None,
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            parser: Literal["BrandProduct"] | Callable | None = "BrandProduct",
        ) -> JsonObject:
        kwargs = self.build_kwargs(brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size)
        if page is None:
            return await self._collect_async_all(self.select_parser(parser), **kwargs)
        message = self.build_request(json=kwargs)
        response = await self.request_async_json(**message)
        return self.parse(response, self.select_parser(parser), **kwargs)

    def build_kwargs(self, brand_ids, mall_seq, sort_type, is_brand_catalog, page, page_size) -> Dict:
        return dict(
            brand_ids=brand_ids, mall_seq=mall_seq, sort_type=sort_type, is_brand_catalog=is_brand_catalog,
            page=page, page_size=page_size)

    def get_request_body(
            self,
            brand_ids: List[int | str],
            mall_seq: int | str | None = None,
            sort_type: Literal["popular","recent","price"] = "poular",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> Dict:
        kv = lambda key, value: {key: value} if value is not None else {}
        return super().get_request_body({
                "connection": {
                    "page": int(page),
                    "size": int(page_size),
                    **self.select_sort_type(sort_type),
                },
                **kv("isBrandOfficialMall", is_brand_catalog),
                "serviceYn": "Y",
                **kv("mallSeq", mall_seq),
                "brandSeqs": list(map(str, brand_ids)),
            })

    @property
    def param_types(self) -> List:
        types = super().param_types
        return dict(map(lambda x: (x, types[x]), [
            "ids", "name", "mallSeq", "mallProductIds", "catalogIds", "makerSeq", "category", "registerDate", "serviceYn",
            "brandSeqs", "brandCertificationYn", "connection"
        ]))

    @property
    def fields(self) -> List:
        return [{
            "items": [
                "id", {"image": ["F60", "F80", "SRC"]}, "name", "makerName", "makerSeq", "brandName", "brandSeq",
                "serviceYn", "lowestPrice", "registerDate", "fullCategoryName", "categoryId", "fullCategoryId", "mallName",
                "mallProductId", "buyingOptionValue", "catalogId", "brandCertificationYn", "outLinkUrl", "categoryName",
                "categoryShapeType", "categoryLeafYn", "productStatusCode", "saleMethodTypeCode"
            ]
        }, "totalCount"]
