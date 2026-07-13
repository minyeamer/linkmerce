from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class Settlement(DuckDBTransformer):
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

    extractor = "Settlement"
    tables = {"table": "smartstore_settlement"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "elements",
        fields = [
            "settleCompleteDate", "payDate", "orderId", "productOrderId", "productOrderType",
            "settleType", "productId", "paySettleAmount", "totalPayCommissionAmount",
            "freeInstallmentCommissionAmount", "sellingInterlockCommissionAmount",
            "benefitSettleAmount", "settleExpectAmount"
        ],
    )
    params = {"channel_seq": "$channel_seq"}
