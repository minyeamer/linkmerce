from __future__ import annotations

from linkmerce.common.transform import DuckDBTransformer


class MarketingChannel(DuckDBTransformer):
    """스마트스토어 사용자 정의 채널 상세 데이터를 변환 및 적재하는 클래스.

    - **Extractor**: `MarketingChannel`

    - **Parser** ( *parser_class: input_type -> output_type* ):
        `JsonTransformer: dict -> list[dict]`

    - **Table** ( *table_key: table_name* ):
        `table: marketing_channel`

    Parameters
    ----------
    **NOTE** DuckDB 쿼리 실행에 필요한 파라미터를 `transform` 메서드 호출 시 함께 전달해야 한다.

    channel_seq: int | str
        조회 채널 번호
    date: dt.date | str
        조회 일자
    """

    extractor = "MarketingChannel"
    tables = {"table": "marketing_channel"}
    parser = "json"
    parser_config = dict(
        dtype = dict,
        scope = "rows",
        fields = [
            "deviceCategory", "ntSource", "ntMedium", "ntDetail", "ntKeyword",
            "numUsers", "numInteractions", "pv", "numPurchases", "payAmount",
        ],
    )
    params = {"channel_seq": "$channel_seq", "ymd": "$date"}
