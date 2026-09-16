from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Sequence, Literal
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection


@with_duckdb_connection(table="esmplus_item")
def item(
        cookies: str,
        product_id: str | Sequence[str] = str(),
        keyword: str = str(),
        sell_status: Sequence[str] = list(),
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 1,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """ESM PLUS 상품목록을 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: esmplus_item`

    Parameters
    ----------
    cookies: str
        ESM PLUS 로그인 쿠키 문자열
    product_id: str | Iterable[str]
        검색할 상품번호/마스터상품번호/판매자관리코드/SKU번호/그룹 번호. 문자열 또는 배열을 입력할 수 있다.
    keyword: str
        검색할 상품명, 브랜드명, 제조사명을 입력할 수 있다.
    sell_status: list[str]
        판매상태 목록
            - `"11"`: 판매가능
            - `"21"`: 판매불가
            - `"22"`: 판매중지
            - `"31"`: SKU품절
            - `"01"`: 등록대기
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        페이지 요청 간 대기 시간(초). 기본값은 `1`
    progress: bool
        페이지 순회 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 페이지별 원본 응답 `list[dict]`를 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.ebay.esmplus.item.extract import Item
    from linkmerce.core.ebay.esmplus.item.transform import Item as T
    return Item(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        cookies = cookies,
        options = {
            "PaginateAll": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(product_id, keyword, sell_status)
