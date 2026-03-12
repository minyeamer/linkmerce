from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class SearchParser(JsonTransformer):
    dtype = dict
    scope = "items"
    defaults = {"keyword": "$query", "start": "$start"}

    def assert_valid_response(self, obj: dict, **kwargs):
        super().assert_valid_response(obj)
        if "errorMessage" in obj:
            self.raise_request_error(obj.get("errorMessage") or str())


class BlogSearch(DuckDBTransformer):
    tables = {"table": "naver_blog"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "description", "bloggername", "bloggerlink", "postdate"],
    )


class NewsSearch(DuckDBTransformer):
    tables = {"table": "naver_news"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "originallink", "description", "pubDate"],
    )


class BookSearch(DuckDBTransformer):
    tables = {"table": "naver_book"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "description", "image", "author", "discount", "publisher", "isbn", "pubdate"],
    )


class CafeSearch(DuckDBTransformer):
    tables = {"table": "naver_cafe"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "description", "cafename", "cafeurl"],
    )


class KiNSearch(DuckDBTransformer):
    tables = {"table": "naver_kin"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "description"],
    )


class ImageSearch(DuckDBTransformer):
    tables = {"table": "naver_image"}
    parser = SearchParser
    parser_config = dict(
        fields = ["title", "link", "thumbnail", "sizeheight", "sizewidth"],
    )


class ShoppingSearch(DuckDBTransformer):
    tables = {"table": "naver_shop"}
    parser = SearchParser
    parser_config = dict(
        fields = [
            "productId", "link", "title", "productType", "mallName", "link", "brand",
            "maker", "category1", "category2", "category3", "category4", "image", "lprice"
        ],
    )


class ShoppingRank(DuckDBTransformer):
    tables = {"rank": "naver_shop_rank", "product": "naver_shop_product"}
    parser = SearchParser
    parser_config = dict(
        fields = [
            "productId", "link", "title", "productType", "mallName", "link", "brand",
            "maker", "category1", "category2", "category3", "category4", "image", "lprice"
        ],
    )
