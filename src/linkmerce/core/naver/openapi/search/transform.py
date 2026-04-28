from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class SearchParser(JsonTransformer):
    """네이버 검색 API 응답 데이터를 파싱하는 클래스."""

    dtype = dict
    scope = "items"

    def assert_valid_response(self, obj: dict, **kwargs):
        """응답에 `errorMessage` 필드가 있으면 `RequestError`를 발생시킨다."""
        super().assert_valid_response(obj)
        if "errorMessage" in obj:
            self.raise_request_error(obj.get("errorMessage") or str())


class BlogSearch(DuckDBTransformer):
    """네이버 블로그 검색 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `BlogSearch`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `SearchParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_blog`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    query: str
        검색어
    start: int
        검색 시작 위치
    """

    extractor = "BlogSearch"
    tables = {"table": "naver_blog"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "description", "bloggername", "bloggerlink", "postdate"],
    )
    params = {"keyword": "$query", "start": "$start"}


class NewsSearch(DuckDBTransformer):
    """네이버 뉴스 검색 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `NewsSearch`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `SearchParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_news`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    query: str
        검색어
    start: int
        검색 시작 위치
    """

    extractor = "NewsSearch"
    tables = {"table": "naver_news"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "originallink", "description", "pubDate"],
    )
    params = {"keyword": "$query", "start": "$start"}


class BookSearch(DuckDBTransformer):
    """네이버 책 검색 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `BookSearch`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `SearchParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_book`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    query: str
        검색어
    start: int
        검색 시작 위치
    """

    extractor = "BookSearch"
    tables = {"table": "naver_book"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "description", "image", "author", "discount", "publisher", "isbn", "pubdate"],
    )
    params = {"keyword": "$query", "start": "$start"}


class CafeSearch(DuckDBTransformer):
    """네이버 카페글 검색 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `CafeSearch`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `SearchParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_cafe`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    query: str
        검색어
    start: int
        검색 시작 위치
    """

    extractor = "CafeSearch"
    tables = {"table": "naver_cafe"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "description", "cafename", "cafeurl"],
    )
    params = {"keyword": "$query", "start": "$start"}


class KiNSearch(DuckDBTransformer):
    """네이버 지식iN 검색 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `KiNSearch`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `SearchParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_kin`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    query: str
        검색어
    start: int
        검색 시작 위치
    """

    extractor = "KiNSearch"
    tables = {"table": "naver_kin"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "description"],
    )
    params = {"keyword": "$query", "start": "$start"}


class ImageSearch(DuckDBTransformer):
    """네이버 이미지 검색 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `ImageSearch`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `SearchParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_image`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    query: str
        검색어
    start: int
        검색 시작 위치
    """

    extractor = "ImageSearch"
    tables = {"table": "naver_image"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "thumbnail", "sizeheight", "sizewidth"],
    )
    params = {"keyword": "$query", "start": "$start"}


class ShopSearch(DuckDBTransformer):
    """네이버 쇼핑 검색 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `ShopSearch`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `SearchParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: naver_shop`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    query: str
        검색어
    start: int
        검색 시작 위치
    """

    extractor = "ShopSearch"
    tables = {"table": "naver_shop"}
    parser = SearchParser
    parser_config = dict(
        fields = [
            "productId", "link", "title", "productType", "mallName", "link", "brand",
            "maker", "category1", "category2", "category3", "category4", "image", "lprice"
        ],
    )
    params = {"keyword": "$query", "start": "$start"}


class ShopRank(ShopSearch):
    """네이버 쇼핑 검색 결과를 변환 및 적재하는 클래스.

    **NOTE** 쇼핑 검색 결과로부터 상품 순위와 상품 정보를 각각의 테이블로 분리한다.

    - **Extractor**: `ShopSearch`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `SearchParser`: `dict` -> `list[dict]`

    - **Table** ( *table_key: table_name (description)* ):
        1. `rank`: `naver_shop_rank` (상품 순위)
        2. `product`: `naver_shop_product` (상품 정보)

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    query: str
        검색어
    start: int
        검색 시작 위치
    """

    extractor = "ShopSearch"
    tables = {"rank": "naver_shop_rank", "product": "naver_shop_product"}
    parser = SearchParser
    parser_config = dict(
        fields = [
            "productId", "link", "title", "productType", "mallName", "link", "brand",
            "maker", "category1", "category2", "category3", "category4", "image", "lprice"
        ],
    )
    params = {"keyword": "$query", "start": "$start"}
