from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Product(DuckDBTransformer):
    """카페24 상품 목록 조회 결과를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Product`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: cafe24_product`

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    """

    extractor = "Product"
    tables = {"table": "cafe24_product"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "products",
        fields = [
            "product_no", "product_code", "custom_product_code", "product_name",
            "model_name", "price", "retail_price", "supply_price", "display", "selling",
            "product_tag", "margin_rate", "list_image", "created_date", "updated_date",
            "sold_out",
        ],
    )
    params = {"mall_id": "$mall_id"}


class Variant(DuckDBTransformer):
    """카페24 상품 품목 목록 조회 결과를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Variant`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: cafe24_product_variant`

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    product_no: int | str
        상품번호
    """

    extractor = "Variant"
    tables = {"table": "cafe24_product_variant"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "variants",
        fields = [
            "variant_code", "custom_variant_code", "display", "selling", "display_order",
            "additional_amount", "supply_price", "image",
            {"options": [{str(i): [{"name": None}, {"value": None}]} for i in range(0, 3)]},
        ],
    )
    params = {"mall_id": "$mall_id", "product_no": "$product_no"}


class Category(DuckDBTransformer):
    """카페24 상품분류 목록 조회 결과를 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Category`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: cafe24_category`

    Parameters
    ----------
    mall_id: str
        카페24 쇼핑몰 아이디
    """

    extractor = "Category"
    tables = {"table": "cafe24_category"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "categories",
        fields = [
            "category_no", "category_depth", "category_name",
            "full_category_name", "full_category_no", "use_display", "display_order",
        ],
    )
    params = {"mall_id": "$mall_id"}
