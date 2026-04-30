from __future__ import annotations

from linkmerce.common.transform import JsonTransformer, DuckDBTransformer


class ProductParser(JsonTransformer):
    """스마트스토어 상품 목록 조회 결과를 파싱하는 클래스."""

    scope = "contents"
    fields = [
        "channelProductNo", "originProductNo", {"modelId": None}, "channelServiceType", "name",
        {"sellerManagementCode": None}, {"modelName": None}, {"brandName": None}, {"manufacturerName": None},
        "categoryId", "wholeCategoryId", "wholeCategoryName", "statusType",
        "channelProductDisplayStatusType", "representativeImage.url", "sellerTags",
        "salePrice", {"discountedPrice": None}, "stockQuantity",
        "deliveryAttributeType", "deliveryFee", {"returnFee": None}, {"exchangeFee": None},
        "regDate", "modifiedDate", {"groupProductNo": None}
    ]

    def parse(self, contents: list[dict], **kwargs) -> list[dict]:
        """콘텐츠 목록에서 `channelProducts`를 평탄화한 상품 목록을 반환한다."""
        products = list()
        for content in contents:
            for product in content["channelProducts"]:
                products.append(dict(product,
                    sellerTags=','.join([tag["text"] for tag in product["sellerTags"]])))
        return products


class Product(DuckDBTransformer):
    """스마트스토어 상품 목록 조회 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `Product`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `ProductParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: smartstore_product`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    channel_seq: int | str
        채널 번호
    """

    extractor = "Product"
    tables = {"table": "smartstore_product"}
    parser = ProductParser
    params = {"channel_seq": "$channel_seq"}


class OptionSimpleParser(JsonTransformer):
    """스마트스토어 채널 상품 조회 결과의 단독형 옵션 목록을 파싱하는 클래스."""

    scope = "originProduct.detailAttribute.optionInfo.optionSimple"
    fields = ["id", "groupName", "name", "usable", {"price": None}, {"stockQuantity": None}]


class OptionCombParser(JsonTransformer):
    """스마트스토어 채널 상품 조회 결과의 조합형 옵션 목록을 파싱하는 클래스."""

    scope = "originProduct.detailAttribute.optionInfo"
    fields = [
        "id", "optionGroupName1", "optionName1", *[{key: None} for key in [
            "optionGroupName2", "optionName2", "optionGroupName3", "optionName3"]],
        {"sellerManagerCode": None}, "usable", "price", "stockQuantity"
    ]

    def get_scope(self, obj: dict, **kwargs) -> dict:
        """JSON 데이터로부터 옵션 정보를 탐색해 반환한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(obj, self.scope, on_missing="missing") or dict()

    def parse(self, option_info: dict, **kwargs) -> list[dict]:
        """옵션 정보로부터 옵션명을 추출해 조합형 옵션 목록에 추가한다."""
        options = list()
        option_groups = option_info.get("optionCombinationGroupNames") or dict()
        for option in (option_info.get("optionCombinations") or list()):
            if isinstance(option, dict):
                options.append(dict(option, **option_groups))
        return options


class SupplementParser(JsonTransformer):
    """스마트스토어 채널 상품 조회 결과의 추가 상품 목록을 파싱하는 클래스."""

    scope = "originProduct.detailAttribute.supplementProductInfo.supplementProducts"
    fields = [
        "id", "groupName", "name", {"sellerManagerCode": None}, "usable", "price", "stockQuantity"
    ]

    def get_scope(self, obj: dict, **kwargs) -> list[dict]:
        """JSON 데이터로부터 추가 상품 목록을 탐색해 반환한다."""
        from linkmerce.utils.nested import hier_get
        return hier_get(obj, self.scope, on_missing="missing") or list()


class Option(DuckDBTransformer):
    """스마트스토어 채널 상품 조회 결과를 변환 및 적재하는 클래스.

    - **Extractor**: `Option`

    - **Parsers** ( *parser_class: input_type -> output_type* ):
        1. `OptionSimpleParser: dict -> list[dict]`
        2. `OptionCombParser: dict -> list[dict]`
        3. `SupplementParser: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: smartstore_option`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    product_id: int | str
        상품코드 목록
    channel_seq: int | str
        채널 번호
    """

    extractor = "Option"
    tables = {"table": "smartstore_option"}
    parser = {
        "option_simple": OptionSimpleParser,
        "option_comb": OptionCombParser,
        "supplement": SupplementParser
    }
    params = {"product_id": "$product_id", "channel_seq": "$channel_seq"}
