from __future__ import annotations
from linkmerce.core.coupang.api import CoupangApi

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    import datetime as dt


class Product(CoupangApi):
    """쿠팡 마켓플레이스 또는 로켓그로스 상품 목록을 수집하는 클래스.

    ### 마켓플레이스 API
    - **Menu**: 상품 API > 상품 목록 페이징 조회
    - **API**: https://api-gateway.coupang.com/v2/providers/seller_api/apis/api/v1/marketplace/seller-products
    - **Docs**: https://developers.coupang.com/ko/api/products/product-list-paging-query

    ### 로켓그로스 API
    - **Menu**: Rocket Growth > 상품 목록 페이징 조회 (로켓그로스 및 로켓그로스/마켓플레이스 동시 운영 상품)
    - **API**: https://api-gateway.coupang.com/v2/providers/seller_api/apis/api/v1/marketplace/seller-products
    - **Docs**: https://developers.coupang.com/ko/api/rocket-growth/product-list-paging-query-rocket-growth-rocket-growthmarketplace-hybrid-products

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드

    **NOTE** 인스턴스 생성 시 `options` 인자로 `CursorAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        커서 요청 간 대기 시간(초). 기본값은 `0.3`
    """

    method = "GET"
    path = "/v2/providers/seller_api/apis/api/v1/marketplace/seller-products"
    default_options = {"CursorAll": {"request_delay": 0.3}}

    @CoupangApi.with_session
    def extract(
            self,
            business_type: Literal["rocketGrowth"] | None = None,
            seller_product_id: int | str | None = None,
            seller_product_name: str | None = None,
            status: Literal["IN_REVIEW", "SAVED", "APPROVING", "APPROVED", "PARTIAL_APPROVED", "DENIED", "DELETED"] | None = None,
            manufacture: str | None = None,
            created_at: dt.date | str | None = None,
            **kwargs
        ) -> list[dict]:
        """상품 목록을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        business_type: str | None
            - `"rocketGrowth"`: 로켓그로스 상품 또는 마켓플레이스 및 로켓그로스 아이템이 모두 존재하는 Hybrid 상품
            - `None`: 마켓플레이스 상품 또는 마켓플레이스 및 로켓그로스 아이템이 모두 존재하는 Hybrid 상품 (기본값)
        seller_product_id: int | str
            조회할 노출옵션ID를 선택할 수 있다.
        seller_product_name: str
            조회할 상품명을 입력할 수 있다.
        status: str
            업체상품상태. `status` 속성의 키를 전달할 수 있다.
        manufacture: str
            조회할 제조사를 입력할 수 있다.
        created_at: dt.date | str
            상품등록일시를 제한할 수 있다.   
            예) '2015-12-17'과 같이 입력하면, '2015-12-17T00:00:00' ~ '2015-12-17T23:59:59'와 같이 조회됨

        Returns
        -------
        list[dict]
            전체 또는 조건에 맞는 상품 목록
        """
        return (self.cursor_all(self.request_json, self.get_next_cursor)
            .run(
                business_type = business_type,
                seller_product_id = seller_product_id,
                seller_product_name = seller_product_name,
                status = status,
                manufacture = manufacture,
                created_at = created_at,
            ))

    def get_next_cursor(self, response: dict, **context) -> str | None:
        """다음 페이지를 가리키는 `nextToken` 커서를 추출한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(response, "nextToken") or None

    def build_request_params(
            self,
            business_type: Literal["rocketGrowth"] | None = None,
            seller_product_id: int | str | None = None,
            seller_product_name: str | None = None,
            status: Literal["IN_REVIEW", "SAVED", "APPROVING", "APPROVED", "PARTIAL_APPROVED", "DENIED", "DELETED"] | None = None,
            manufacture: str | None = None,
            created_at: dt.date | str | None = None,
            next_cursor: str | None = None,
            **kwargs
        ) -> dict:
        params = {
            "vendorId": self.vendor_id,
            "nextToken": next_cursor,
            "sellerProductId": seller_product_id,
            "sellerProductName": seller_product_name,
            "status": status,
            "manufacture": manufacture,
            "createdAt": created_at,
            "businessTypes": business_type,
        }
        return {key: value for key, value in params.items() if value is not None}

    @property
    def status(self) -> dict[str, str]:
        """업체상품상태 코드와 한글명 매핑을 반환한다."""
        return {
            "IN_REVIEW": "심사중", "SAVED": "임시저장", "APPROVING": "승인대기중",
            "APPROVED": "승인완료", "PARTIAL_APPROVED": "부분승인완료",
            "DENIED": "승인반려", "DELETED": "상품삭제"
        }


class ProductDetail(CoupangApi):
    """쿠팡 마켓플레이스 또는 로켓그로스 상품의 상세 정보를 수집하는 클래스.

    ### 마켓플레이스 API
    - **Menu**: 상품 API > 상품 조회
    - **API**: https://api-gateway.coupang.com/v2/providers/seller_api/apis/api/v1/marketplace/seller-products/{seller_product_id}
    - **Docs**: https://developers.coupang.com/ko/api/products/querying-product

    ### 로켓그로스 API
    - **Menu**: Rocket Growth > 상품 조회 (로켓그로스 또는 마켓플레이스/로켓그로스 동시 운영 상품)
    - **API**: https://api-gateway.coupang.com/v2/providers/seller_api/apis/api/v1/marketplace/seller-products/{seller_product_id}
    - **Docs**: https://developers.coupang.com/ko/api/rocket-growth/query-product-rocket-growth-or-rocket-growthmarketplace-hybrid-products

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        상품별 요청 간 대기 시간(초). 기본값은 `0.3`
    tqdm_options: dict | None
        반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/v2/providers/seller_api/apis/api/v1/marketplace/seller-products/{seller_product_id}"
    default_options = {"RequestEach": {"request_delay": 0.3}}

    @CoupangApi.with_session
    def extract(
            self,
            seller_product_id: int | str | Iterable[int | str],
            **kwargs
        ) -> dict | list[dict]:
        """상품별 상세 정보를 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        seller_product_id: int | str | Iterable[int | str]
            등록상품ID. 단일 값 또는 배열

        Returns
        -------
        dict | list[dict]
            상품별 상세 정보. `seller_product_id` 타입에 따라 반환 타입이 다르다.
                - `seller_product_id`가 `int | str` 타입일 때 -> `dict`
                - `seller_product_id`가 `Iterable[int | str]` 타입일 때 -> `list[dict]`
        """
        return (self.request_each(self.request_json)
                .expand(seller_product_id=seller_product_id)
                .run())

    def build_request_message(self, seller_product_id: int | str, **kwargs) -> dict:
        """등록상품ID를 상세 조회 경로에 붙인다."""
        path = self.path.format(seller_product_id=str(seller_product_id))
        kwargs["url"] =  self.concat_path(self.origin, path)
        return super().build_request_message(**kwargs)


class Inventory(CoupangApi):
    """쿠팡 옵션별 수량, 가격, 판매상태를 수집하는 클래스.

    - **Menu**: 상품 API > 상품 아이템별 수량/가격/상태 조회
    - **API**: https://api-gateway.coupang.com/v2/providers/seller_api/apis/api/v1/marketplace/vendor-items/{vendor_item_id}/inventories
    - **Docs**: https://developers.coupang.com/ko/api/products/query-quantitypricestatus-by-product-items

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    access_key: str
        쿠팡 Open API 액세스 키
    secret_key: str
        쿠팡 Open API 시크릿 키
    vendor_id: str
        업체 코드

    **NOTE** 인스턴스 생성 시 `options` 인자로 `CursorAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        옵션별 요청 간 대기 시간(초). 기본값은 `0.3`
    tqdm_options: dict | None
        반복 요청 작업 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    method = "GET"
    path = "/v2/providers/seller_api/apis/api/v1/marketplace/vendor-items/{vendor_item_id}/inventories"
    default_options = {"RequestEach": {"request_delay": 0.3}}

    @CoupangApi.with_session
    def extract(self, vendor_item_id: int | str | Iterable[int | str], **kwargs) -> list[dict]:
        """옵션별 수량, 가격, 판매상태를 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        vendor_item_id: int | str | Iterable[int | str]
            노출옵션ID. 단일 값 또는 배열

        Returns
        -------
        list[dict]
            옵션 수량 목록. `vendor_item_id` 타입에 따라 반환 타입이 다르다.
                - `vendor_item_id`가 `int | str` 타입일 때 -> `dict`
                - `vendor_item_id`가 `Iterable[int | str]` 타입일 때 -> `list[dict]`
        """
        return (self.request_each(self.request_json)
                .partial(vendor_id=self.vendor_id)
                .expand(vendor_item_id=vendor_item_id)
                .run())

    def build_request_message(self, vendor_item_id: int | str, **kwargs) -> dict:
        path = self.path.format(vendor_id=self.vendor_id, vendor_item_id=str(vendor_item_id))
        kwargs["url"] = self.concat_path(self.origin, path)
        return super().build_request_message(**kwargs)
