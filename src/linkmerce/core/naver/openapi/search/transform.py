from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class SearchParser(JsonTransformer):
    """네이버 검색 OpenAPI 응답 데이터를 변환하는 파서 클래스."""

    dtype = dict
    scope = "items"

    def assert_valid_response(self, obj: dict, **kwargs):
        """응답에 `errorMessage` 필드가 있으면 `RequestError`를 발생시킨다."""
        super().assert_valid_response(obj)
        if "errorMessage" in obj:
            self.raise_request_error(obj.get("errorMessage") or str())


class BlogSearch(DuckDBTransformer):
    """네이버 블로그 검색 결과를 `naver_blog` 테이블에 적재하는 클래스."""

    extractor = "BlogSearch"
    tables = {"table": "naver_blog"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "description", "bloggername", "bloggerlink", "postdate"],
    )
    params = {"keyword": "$query", "start": "$start"}


class NewsSearch(DuckDBTransformer):
    """네이버 뉴스 검색 결과를 `naver_news` 테이블에 적재하는 클래스."""

    extractor = "NewsSearch"
    tables = {"table": "naver_news"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "originallink", "description", "pubDate"],
    )
    params = {"keyword": "$query", "start": "$start"}


class BookSearch(DuckDBTransformer):
    """네이버 책 검색 결과를 `naver_book` 테이블에 적재하는 클래스."""

    extractor = "BookSearch"
    tables = {"table": "naver_book"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "description", "image", "author", "discount", "publisher", "isbn", "pubdate"],
    )
    params = {"keyword": "$query", "start": "$start"}


class CafeSearch(DuckDBTransformer):
    """네이버 카페 검색 결과를 `naver_cafe` 테이블에 적재하는 클래스."""

    extractor = "CafeSearch"
    tables = {"table": "naver_cafe"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "description", "cafename", "cafeurl"],
    )
    params = {"keyword": "$query", "start": "$start"}


class KiNSearch(DuckDBTransformer):
    """네이버 지식iN 검색 결과를 `naver_kin` 테이블에 적재하는 클래스."""

    extractor = "KiNSearch"
    tables = {"table": "naver_kin"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "description"],
    )
    params = {"keyword": "$query", "start": "$start"}


class ImageSearch(DuckDBTransformer):
    """네이버 이미지 검색 결과를 `naver_image` 테이블에 적재하는 클래스."""

    extractor = "ImageSearch"
    tables = {"table": "naver_image"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "thumbnail", "sizeheight", "sizewidth"],
    )
    params = {"keyword": "$query", "start": "$start"}


class ShoppingSearch(DuckDBTransformer):
    """네이버 쇼핑 검색 결과를 `naver_shop` 테이블에 적재하는 클래스."""

    extractor = "ShoppingSearch"
    tables = {"table": "naver_shop"}
    parser = SearchParser
    parser_config = dict(
        fields = [
            "productId", "link", "title", "productType", "mallName", "link", "brand",
            "maker", "category1", "category2", "category3", "category4", "image", "lprice"
        ],
    )
    params = {"keyword": "$query", "start": "$start"}


class ShoppingRank(ShoppingSearch):
    """네이버 쇼핑 검색 결과로부터 순위 및 상품 목록을 각각의 테이블에 변환 및 적재하는 클래스.

    테이블 키 | 테이블명 | 설명
    - `rank` | `naver_shop_rank` | 네이버 쇼핑 상품 순위
    - `product` | `naver_shop_product` | 네이버 쇼핑 상품 목록"""

    extractor = "ShoppingSearch"
    tables = {"rank": "naver_shop_rank", "product": "naver_shop_product"}
    parser = SearchParser
    parser_config = dict(
        fields = [
            "productId", "link", "title", "productType", "mallName", "link", "brand",
            "maker", "category1", "category2", "category3", "category4", "image", "lprice"
        ],
    )
    params = {"keyword": "$query", "start": "$start"}
