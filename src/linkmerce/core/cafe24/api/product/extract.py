from __future__ import annotations

from linkmerce.core.cafe24.api import Cafe24Api

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal


class Product(Cafe24Api):
    """카페24 Admin API로 상품 목록 조회 결과를 수집하는 클래스.

    - **API**: https://{mall_id}.cafe24api.com/api/v2/admin/products
    - **Docs**: https://apidocs.cafe24.com/docs/admin/get-products
    - **Referer**: https://{mall_id}.cafe24.com/disp/admin/shop1/product/productmanage

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.

    **NOTE** `token_info`를 파일 경로로 전달하면 토큰 갱신 시 파일 내용도 업데이트한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `CursorAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        커서 요청 간 대기 시간(초). 기본값은 `0.5`
    """

    path = "/admin/products"
    default_options = {"CursorAll": {"request_delay": 0.5}}

    @Cafe24Api.with_session
    def extract(
            self,
            product_name: str | None = None,
            display: Literal["T", "F"] | None = None,
            selling: Literal["T", "F"] | None = None,
            shop_no: int = 1,
            embed: str | None = None,
            page_size: int = 100,
            since_product_no: int = 0,
            **kwargs
        ) -> list[dict]:
        """상품 목록을 JSON 형식으로 반환한다.

        Parameters
        ----------
        product_name: str | None
            상품명 검색어. 생략하면 전체 상품을 조회한다.
        display: str | None
            진열상태. `"T"`: 진열함, `"F"`: 진열안함. 생략하면 전체 상태를 조회한다.
        selling: str | None
            판매상태. `"T"`: 판매함, `"F"`: 판매안함. 생략하면 전체 상태를 조회한다.
        shop_no: int
            멀티쇼핑몰 번호. 기본값은 `1`
        embed: str | None
            함께 조회할 하위 리소스. 쉼표(,)로 구분하여 여러 개를 지정할 수 있다.
            - 사용 가능한 값: channeldiscountprices, discountprice, decorationimages, benefits, options, variants, additionalimages, hits
        page_size: int
            조회결과 최대건수. 기본값은 `100`, 최대 `100`
        since_product_no: int
            해당 상품번호 이후 검색. 기본값은 `0`

        Returns
        -------
        list[dict]
            상품 목록
        """
        return (self.cursor_all(self.request_json, self.get_next_cursor, next_cursor=since_product_no)
                .run(
                    product_name = product_name,
                    display = display,
                    selling = selling,
                    shop_no = shop_no,
                    embed = embed,
                    page_size = page_size,
                    mall_id = self.get_config("mall_id"),
                ))

    def get_next_cursor(
            self,
            response: dict,
            next_cursor: int = 0,
            page_size: int = 100,
            **context
        ) -> int | None:
        """조회된 상품번호의 최댓값을 다음 검색의 시작점으로 반환한다."""
        products = response["products"]
        if (not products) or (len(products) < page_size):
            return None
        elif (cursor := products[-1]["product_no"]) > next_cursor:
            return cursor
        raise ValueError("The next product cursor must be greater than the current cursor.")

    def build_request_params(
            self,
            product_name: str | None = None,
            display: str | None = None,
            selling: str | None = None,
            shop_no: int = 1,
            embed: str | None = None,
            page_size: int = 100,
            next_cursor: int = 0,
            **kwargs
        ) -> dict:
        return {
            "shop_no": shop_no,
            **({"display": display} if display else dict()),
            **({"selling": selling} if selling else dict()),
            **({"product_name": product_name} if product_name else dict()),
            **({"embed": embed} if embed else dict()),
            "limit": page_size,
            "since_product_no": next_cursor,
        }


class Variant(Cafe24Api):
    """카페24 Admin API로 상품번호별 상품 품목 목록을 수집하는 클래스.

    - **API**: https://{mall_id}.cafe24api.com/api/v2/admin/products/{product_no}/variants
    - **Docs**: https://apidocs.cafe24.com/docs/admin/get-products-by-product-no-variants
    - **Referer**: https://{mall_id}.cafe24.com/disp/admin/shop1/product/ProductRegister?product_no={product_no}

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.

    **NOTE** `token_info`를 파일 경로로 전달하면 토큰 갱신 시 파일 내용도 업데이트한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `RequestEach` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        상품별 요청 간 대기 시간(초). 기본값은 `0.5`
    tqdm_options: dict | None
        반복 요청 작업의 진행도를 출력하는 `tqdm`에 전달할 매개변수
    """

    path = "/admin/products"
    default_options = {"RequestEach": {"request_delay": 0.5}}

    @Cafe24Api.with_session
    def extract(
            self,
            product_no: int | str | Iterable[int | str],
            shop_no: int = 1,
            embed: str | None = None,
            **kwargs
        ) -> dict | list[dict]:
        """상품번호별 상품 품목 목록을 조회해 JSON 형식으로 반환한다.

        Parameters
        ----------
        product_no: int | str | Iterable[int | str]
            상품번호. 단일 값 또는 배열
        shop_no: int
            멀티쇼핑몰 번호. 기본값은 `1`
        embed: str | None
            함께 조회할 하위 리소스. 사용 가능한 값: inventories

        Returns
        -------
        dict | list[dict]
            상품 품목 목록. `product_no` 타입에 따라 반환 타입이 다르다.
                - `product_no`가 `int | str` 타입일 때 -> `dict`
                - `product_no`가 `Iterable[int | str]` 타입일 때 -> `list[dict]`
        """
        return (self.request_each(self.request_json)
                .partial(shop_no=shop_no, embed=embed, mall_id=self.get_config("mall_id"))
                .expand(product_no=product_no)
                .run())

    def build_request_message(self, product_no: int | str, **kwargs) -> dict:
        """상품번호를 카페24 상품 품목 목록 조회 경로에 붙인다."""
        kwargs["url"] = self.concat_path(self.url, str(product_no), "variants")
        return super().build_request_message(**kwargs)

    def build_request_params(self, shop_no: int = 1, embed: str | None = None, **kwargs) -> dict:
        return {"shop_no": shop_no, **({"embed": embed} if embed else dict())}


class Category(Cafe24Api):
    """카페24 Admin API로 상품분류 목록 조회 결과를 수집하는 클래스.

    - **API**: https://{mall_id}.cafe24api.com/api/v2/admin/categories
    - **Docs**: https://apidocs.cafe24.com/docs/admin/get-categories
    - **Referer**: https://{mall_id}.cafe24.com/disp/admin/shop1/product/categorymanage

    Attributes
    ----------
    **NOTE** 인스턴스 생성 시 `configs` 인자로 아래 설정값들을 반드시 전달해야 한다.

    mall_id: str
        카페24 쇼핑몰 아이디
    client_id: str
        카페24 앱 클라이언트 아이디
    client_secret: str
        카페24 앱 클라이언트 시크릿
    token_info: dict[str, str] | str | Path
        카페24 액세스 토큰을 포함하는 OAuth 응답 결과가 기록된 파일 경로 또는 객체.
        `access_token`, `refresh_token` 키를 반드시 포함해야 하며,
        `expires_at` 값에 따라 액세스 토큰 만료 여부를 판단해 갱신한다.

    **NOTE** `token_info`를 파일 경로로 전달하면 토큰 갱신 시 파일 내용도 업데이트한다.

    **NOTE** 인스턴스 생성 시 `options` 인자로 `CursorAll` Task 옵션을 전달할 수 있다.

    request_delay: float | int | tuple[int, int]
        커서 요청 간 대기 시간(초). 기본값은 `0.5`
    """

    path = "/admin/categories"
    default_options = {"CursorAll": {"request_delay": 0.5}}

    @Cafe24Api.with_session
    def extract(
            self,
            category_depth: int | None = None,
            parent_category_no: int | None = None,
            shop_no: int = 1,
            page_size: int = 100,
            offset: int = 0,
            **kwargs
        ) -> list[dict]:
        """상품분류 목록을 페이지 단위로 수집해 JSON 형식으로 반환한다.

        Parameters
        ----------
        category_depth: int | None
            분류 깊이. `1`부터 `4`까지 입력할 수 있다. 생략하면 전체 깊이를 조회한다.
        parent_category_no: int | None
            상위 분류번호. 대분류만 검색하고자 할 경우 `1`을 입력한다.
        shop_no: int
            멀티쇼핑몰 번호. 기본값은 `1`
        page_size: int
            조회결과 최대건수. 기본값은 `100`, 최대 `100`
        offset: int
            조회결과 시작위치. 기본값은 `0`, 최대 `8000`

        Returns
        -------
        list[dict]
            상품분류 목록
        """
        return (self.cursor_all(self.request_json, self.get_next_cursor, next_cursor=offset)
                .run(
                    category_depth = category_depth,
                    parent_category_no = parent_category_no,
                    shop_no = shop_no,
                    page_size = page_size,
                    mall_id = self.get_config("mall_id"),
                ))

    def get_next_cursor(
            self,
            response: dict,
            next_cursor: int = 0,
            page_size: int = 100,
            **context
        ) -> int | None:
        """다음 페이지의 시작위치를 반환한다."""
        if len(response["categories"]) < page_size:
            return None
        elif (offset := next_cursor + page_size) <= 8000:
            return offset
        raise ValueError("The category offset must not exceed 8000. Narrow the search conditions.")

    def build_request_params(
            self,
            category_depth: int | None = None,
            parent_category_no: int | None = None,
            shop_no: int = 1,
            page_size: int = 100,
            next_cursor: int = 0,
            **kwargs
        ) -> dict:
        return {
            "shop_no": shop_no,
            **({"category_depth": category_depth} if category_depth else dict()),
            **({"parent_category_no": parent_category_no} if parent_category_no else dict()),
            "limit": page_size,
            "offset": next_cursor,
        }
