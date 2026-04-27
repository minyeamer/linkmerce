from __future__ import annotations
from linkmerce.core.smartstore.hcenter import PartnerCenter

from typing import Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal
    from linkmerce.common.extract import JsonObject


class _CatalogProduct(PartnerCenter):
    """네이버 브랜드 카탈로그/상품 조회를 위한 공통 클래스.

    - **Menu**: 브랜드 관리 > 카탈로그 관리
    - **Referer**: https://center.shopping.naver.com/brand-management/catalog

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지별 요청 간 대기 시간. 기본값은 `1`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachPages` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        매개변수별 요청 간 대기 시간. 기본값은 `1`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "POST"
    origin: str = "https://vcenter.shopping.naver.com"
    max_page_size = 100
    page_start = 0
    default_options = {
        "PaginateAll": {"request_delay": 1, "max_concurrent": 3},
        "RequestEachPages": {"request_delay": 1, "max_concurrent": 3},
    }

    def count_total(self, response: JsonObject, **kwargs) -> int:
        """HTTP 응답에서 전체 항목 수를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "totalCount")

    def split_map_kwargs(
            self,
            brand_ids: str | Iterable[str],
            sort_type: Literal["popular", "recent", "count", "price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int | list[int] | None = 0,
            page_size: int = 100,
        ) -> tuple[dict, dict]:
        """키워드 인자를 목록(`expand`)과 상수(`partial`)로 나눈다."""
        partial = {"sort_type": sort_type, "is_brand_catalog": is_brand_catalog}
        expand = {"brand_ids": brand_ids}
        if page is not None:
            partial["page_size"] = page_size
            expand["page"] = page
        return partial, expand

    def set_request_headers(self, **kwargs):
        referer = "https://center.shopping.naver.com/brand-management/catalog"
        super().set_request_headers(contents="json", origin=self.origin, referer=referer, **kwargs)

    def select_sort_type(self, sort_type: Literal["popular", "recent", "count", "price"]) -> dict[str, str]:
        """정렬 기준을 API 파라미터로 변환한다."""
        if sort_type == "popular":
            return {"sort": "PopularDegree", "direction": "DESC"}
        elif sort_type == "recent":
            return {"sort": "RegisterDate", "direction": "DESC"}
        elif sort_type == "count":
            return {"sort": "ProductCount", "direction": "DESC"}
        elif sort_type == "price":
            return {"sort": "MobilePrice", "direction": "ASC"}
        else:
            return dict()


class BrandCatalog(_CatalogProduct):
    """네이버 브랜드 카탈로그 목록을 조회하는 클래스.

    - **Menu**: 브랜드 관리 > 카탈로그 관리 > 브랜드 카탈로그 관리
    - **API**: https://vcenter.shopping.naver.com/api/catalogs
    - **Referer**: https://center.shopping.naver.com/brand-management/catalog

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지별 요청 간 대기 시간. 기본값은 `1`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachPages` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        매개변수별 요청 간 대기 시간. 기본값은 `1`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    path = "/api/catalogs"

    @PartnerCenter.with_session
    def extract(
            self,
            brand_ids: str | Iterable[str],
            sort_type: Literal["popular", "recent", "count", "price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int | list[int] | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> JsonObject:
        """브랜드 ID(`brand_ids`)의 권한이 있는 카탈로그 목록을 동기 방식으로 순차 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        brand_ids: str | Iterable[str]
            브랜드 ID. 단일 또는 여러 개의 문자열 목록을 입력한다.
        sort_type: str
            정렬 기준
                - `"popular"`: 카탈로그 인기순
                - `"recent"`: 등록일 최신순
                - `"count"`: 판매처 많은순
                - `"price"`: 최저가 낮은순
        is_brand_catalog: bool | None
            브랜드공식 등록 여부
        page: int | list[int] | None
            조회할 페이지 번호 (0부터 시작)
        page_size: int
            한 번에 표시할 목록 수

        Returns
        -------
        list[dict] | dict
            카탈로그 목록
        """
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
            sort_type: Literal["popular", "recent", "count", "price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int | list[int] | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> JsonObject:
        """브랜드(`brand_ids`) 권한이 있는 카탈로그 목록을 비동기 방식으로 병렬 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        brand_ids: str | Iterable[str]
            브랜드 ID. 단일 또는 여러 개의 문자열 목록을 입력한다.   
            브랜드 ID를 쉼표(,)로 묶어서 OR 조건으로 동시에 조회할 수 있다.
        sort_type: str
            정렬 기준
                - `"popular"`: 카탈로그 인기순
                - `"recent"`: 등록일 최신순
                - `"count"`: 판매처 많은순
                - `"price"`: 최저가 낮은순
        is_brand_catalog: bool | None
            브랜드공식 등록 여부
        page: int | list[int] | None
            조회할 페이지 번호 (0부터 시작)
        page_size: int
            한 번에 표시할 목록 수

        Returns
        -------
        list[dict] | dict
            카탈로그 목록
        """
        partial, expand = self.split_map_kwargs(brand_ids, sort_type, is_brand_catalog, page, page_size)
        return await (self.request_each_pages(self.request_async_json_safe)
                .partial(**partial)
                .expand(**expand)
                .all_pages(self.count_total, self.max_page_size, self.page_start, page)
                .run_async())

    def build_request_json(
            self,
            brand_ids: str,
            sort_type: Literal["popular", "recent", "count", "price"] = "recent",
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


class BrandProduct(_CatalogProduct):
    """네이버 브랜드 상품 목록을 조회하는 클래스.

    - **Menu**: 브랜드 관리 > 카탈로그 관리 > 카탈로그 생성&상품 매칭
    - **API**: https://vcenter.shopping.naver.com/api/offers
    - **Referer**: https://center.shopping.naver.com/brand-management/catalog

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `cookies` 인자로 로그인 쿠키 문자열을 전달해야 한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `PaginateAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        페이지별 요청 간 대기 시간. 기본값은 `1`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEachPages` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        매개변수별 요청 간 대기 시간. 기본값은 `1`
    max_concurrent: int | None
        비동기 요청 시 최대 동시 실행 횟수. 기본값은 `3`
    tqdm_options: dict | None
        진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    path = "/api/offers"

    @PartnerCenter.with_session
    def extract(
            self,
            brand_ids: str | Iterable[str],
            mall_seq: int | str | Iterable[int | str] | None = None,
            sort_type: Literal["popular", "recent", "price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> JsonObject:
        """브랜드(`brand_ids`) 권한이 있고 특정 쇼핑몰(`mall_seq`)에 해당하는 브랜드 상품 목록을   
        동기 방식으로 순차 조회해 JSON 형식으로 반환한다.

        **NOTE** 권한 보유 브랜드가 정확히 등록되어 있는 상품만 조회된다.

        Parameters
        ----------
        brand_ids: str | Iterable[str]
            브랜드 ID. 단일 또는 여러 개의 문자열 목록을 입력한다.   
            브랜드 ID를 쉼표(,)로 묶어서 OR 조건으로 동시에 조회할 수 있다.
        mall_seq: int | str | Iterable[int | str] | None
            쇼핑몰 순번. 단일 또는 여러 개의 목록을 입력한다.   
            목록으로 입력할 시 브랜드 ID 목록에서 인덱스가 동일한 항목과 AND 조건으로 조회된다.
        is_brand_catalog: bool | None
            브랜드공식 등록 여부
        page: int | list[int] | None
            조회할 페이지 번호 (0부터 시작)
        page_size: int
            한 번에 표시할 목록 수

        Returns
        -------
        list[dict] | dict
            브랜드 상품 목록
        """
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
            sort_type: Literal["popular", "recent", "price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
            **kwargs
        ) -> JsonObject:
        """브랜드(`brand_ids`) 권한이 있고 특정 쇼핑몰(`mall_seq`)에 해당하는 브랜드 상품 목록을   
        비동기 방식으로 병렬 조회해 JSON 형식으로 반환한다.

        **NOTE** 권한 보유 브랜드가 정확히 등록되어 있는 상품만 조회된다.

        Parameters
        ----------
        brand_ids: str | Iterable[str]
            브랜드 ID. 단일 또는 여러 개의 문자열 목록을 입력한다.   
            브랜드 ID를 쉼표(,)로 묶어서 OR 조건으로 동시에 조회할 수 있다.
        mall_seq: int | str | Iterable[int | str] | None
            쇼핑몰 순번. 단일 또는 여러 개의 문자열 목록을 입력한다.   
            목록으로 입력할 시 브랜드 ID 목록에서 인덱스가 동일한 항목과 AND 조건으로 조회된다.
        is_brand_catalog: bool | None
            브랜드공식 등록 여부
        page: int | list[int] | None
            조회할 페이지 번호 (0부터 시작)
        page_size: int
            한 번에 표시할 목록 수

        Returns
        -------
        list[dict] | dict
            브랜드 상품 목록
        """
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
            sort_type: Literal["popular", "recent", "price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int | None = 0,
            page_size: int = 100,
        ) -> tuple[list, dict, dict]:
        """키워드 인자를 목록(`expand`)과 상수(`partial`)로 나눈다."""
        context = list()
        partial, expand = super().split_map_kwargs(brand_ids, sort_type, is_brand_catalog, page, page_size)

        def is_iterable(obj: Any) -> bool:
            return (not isinstance(obj, str)) and isinstance(obj, Iterable)

        if is_iterable(brand_ids):
            if is_iterable(mall_seq) and (len(brand_ids) == len(mall_seq)):
                context = [{"brand_ids": ids, "mall_seq": seq} for ids, seq in zip(brand_ids, mall_seq)]
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
            sort_type: Literal["popular", "recent", "price"] = "recent",
            is_brand_catalog: bool | None = None,
            page: int = 0,
            page_size: int = 100,
            **kwargs
        ) -> dict:
        kv = lambda key, value: {key: value} if value is not None else dict()
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
