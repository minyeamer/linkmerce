from linkmerce.common.transform import DuckDBTransformer


class Item(DuckDBTransformer):
    """ESM PLUS 상품목록을 DuckDB 테이블로 변환 및 적재한다.

    - **Extractor**: `Item`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: esmplus_item`
    """

    extractor = "Item"
    tables = {"table": "esmplus_item"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "data.items",
        fields = [
            "goodsNo", {"siteGoodsNo": ["gmkt", "iac"]}, "goodsName",
            {"prmtGoodsName": ["gmkt", "iac"]}, {"siteSellerId": ["gmkt", "iac"]},
            "brand.name", "maker.name", {"category.esm": ["catCode", "catName"]},
            {"sellStatus": ["gmkt", "iac"]}, "imgUrl", {"price": ["gmkt", "iac"]},
            "createdDate", "updatedDate"
        ],
    )
