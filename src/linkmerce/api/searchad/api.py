from __future__ import annotations

from linkmerce.api.common import prepare_duckdb_extract, with_duckdb_connection

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Iterable, Literal
    from linkmerce.api.common import DuckDBResult
    from linkmerce.common.load import DuckDBConnection
    from linkmerce.core.searchad.api.adreport.extract import _ReportsDownload
    import datetime as dt


def _get_api_configs(api_key: str, secret_key: str, customer_id: int | str) -> dict:
    """네이버 검색광고 API 인증에 필요한 설정을 구성한다."""
    return {"api_key": api_key, "secret_key": secret_key, "customer_id": customer_id}


def download_report(
        report_cls: _ReportsDownload | str,
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        return_type: Literal["csv", "json", "raw"] = "json",
        **kwargs
    ) -> list[tuple] | list[dict] | str:
    """네이버 검색광고 대용량 다운로드 보고서를 다운로드해 지정한 형식으로 반환한다.

    Parameters
    ----------
    report_cls: _ReportsDownload | str
        실행할 보고서 `Extractor` 클래스 또는 클래스명 문자열
    api_key: str
        SA API 엑세스라이선스
    secret_key: str
        SA API 비밀키
    customer_id: int | str
        광고계정의 CUSTOMER_ID
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    **kwargs
        `Extractor`의 `extract()` 메서드에 전달할 추가 인자

    Returns
    -------
    list[tuple] | list[dict] | str
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: TSV 형식의 보고서를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: TSV 형식의 보고서를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"raw"`: TSV 형식의 보고서를 원본 텍스트로 반환한다.
    """
    if isinstance(report_cls, str):
        from importlib import import_module
        report_cls = getattr(import_module("linkmerce.core.searchad.api.adreport.extract"), report_cls)
    extractor: _ReportsDownload = report_cls(configs=_get_api_configs(api_key, secret_key, customer_id))
    tsv_data = extractor.extract(**kwargs)

    if return_type == "json":
        if tsv_data:
            return [dict(zip(extractor.columns, row.split('\t'))) for row in tsv_data.split('\n')]
        return list()
    elif return_type == "csv":
        if tsv_data:
            return [extractor.columns] + [row.split('\t') for row in tsv_data.split('\n')]
        return [extractor.columns]
    return tsv_data


@with_duckdb_connection(table="searchad_campaign")
def campaign(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | str | None:
    """네이버 검색광고 캠페인 마스터 데이터를 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: searchad_campaign`

    Parameters
    ----------
    api_key: str
        SA API 엑세스라이선스
    secret_key: str
        SA API 비밀키
    customer_id: int | str
        광고계정의 CUSTOMER_ID
    from_date: dt.date | str | None
        조회 기간. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        해당 날짜부터 현재까지 변경분을 포함한다. 생략하면 현재 시점을 적용한다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | str | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 다운로드 후 TSV 텍스트 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.searchad.api.adreport.extract import Campaign
    from linkmerce.core.searchad.api.adreport.transform import Campaign as T
    return Campaign(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
    )).extract(from_date)


@with_duckdb_connection(table="searchad_adgroup")
def adgroup(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | str | None:
    """네이버 검색광고 광고그룹 마스터 데이터를 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: searchad_adgroup`

    Parameters
    ----------
    api_key: str
        SA API 엑세스라이선스
    secret_key: str
        SA API 비밀키
    customer_id: int | str
        광고계정의 CUSTOMER_ID
    from_date: dt.date | str | None
        조회 기간. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        해당 날짜부터 현재까지 변경분을 포함한다. 생략하면 현재 시점을 적용한다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | str | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 다운로드 후 TSV 텍스트 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.searchad.api.adreport.extract import Adgroup
    from linkmerce.core.searchad.api.adreport.transform import Adgroup as T
    return Adgroup(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
    )).extract(from_date)


@with_duckdb_connection(table="searchad_ad")
def master_ad(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict[str, str] | None:
    """네이버 검색광고 소재 마스터 데이터를 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `table: searchad_ad` (전체 소재 목록)
        2. `link_ad: link_ad` (파워링크 소재 목록)
        3. `contents_ad: contents_ad` (파워컨텐츠 소재 목록)
        4. `shopping_product: shopping_product` (쇼핑상품 소재 목록)
        5. `brand_ad: brand_ad` (쇼핑브랜드 소재 목록)
        6. `product_group: product_group` (상품그룹 목록)
        7. `product_group_rel: product_group_rel` (상품그룹-광고그룹 관계)
        8. `brand_thumbnail_ad: brand_thumbnail_ad` (썸네일 이미지형 소재 목록)
        9. `brand_banner_ad: brand_banner_ad` (배너 이미지형 소재 목록)

    Parameters
    ----------
    api_key: str
        SA API 엑세스라이선스
    secret_key: str
        SA API 비밀키
    customer_id: int | str
        광고계정의 CUSTOMER_ID
    from_date: dt.date | str | None
        조회 기간. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        해당 날짜부터 현재까지 변경분을 포함한다. 생략하면 현재 시점을 적용한다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | dict[str, str] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 다운로드 후 `{보고서 유형: TSV 텍스트}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.searchad.api.adreport.extract import MasterAd
    from linkmerce.core.searchad.api.adreport.transform import MasterAd as T
    return MasterAd(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
    )).extract(from_date)


@with_duckdb_connection(table="searchad_media")
def media(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        from_date: dt.date | str | None = None,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | str | None:
    """네이버 검색광고 광고매체 마스터 데이터를 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: searchad_media`

    Parameters
    ----------
    api_key: str
        SA API 엑세스라이선스
    secret_key: str
        SA API 비밀키
    customer_id: int | str
        광고계정의 CUSTOMER_ID
    from_date: dt.date | str | None
        조회 기간. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
        해당 날짜부터 현재까지 변경분을 포함한다. 생략하면 현재 시점을 적용한다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | str | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 다운로드 후 TSV 텍스트 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.searchad.api.adreport.extract import Media
    from linkmerce.core.searchad.api.adreport.transform import Media as T
    return Media(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
    )).extract(from_date)


@with_duckdb_connection(table="searchad_report")
def advanced_report(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        start_date: dt.date | str,
        end_date: dt.date | str | Literal[":start_date:"] = ":start_date:",
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict[str, str] | None:
    """네이버 검색광고 성과 및 전환 보고서를 다운로드해 DuckDB 테이블에 변환 및 적재한다.

    **Tables** ( *table_key: table_name (description)* ):
        1. `table: searchad_report` (다차원 보고서)
        2. `ad_stat: ad_stat_report` (광고성과 보고서)
        3. `ad_conv: ad_conv_report` (전환 보고서)

    Parameters
    ----------
    api_key: str
        SA API 엑세스라이선스
    secret_key: str
        SA API 비밀키
    customer_id: int | str
        광고계정의 CUSTOMER_ID
    start_date: dt.date | str
        조회 시작일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
    end_date: dt.date | str
        조회 종료일. `dt.date` 객체 또는 `"YYYY-MM-DD"` 형식의 문자열을 입력한다.
            - `":start_date:"`: `start_date`와 동일한 날짜 (기본값)
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        조회 기간별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | dict[str, str] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 다운로드 후 `{보고서 유형: TSV 텍스트}` 구조의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.searchad.api.adreport.extract import AdvancedReport
    from linkmerce.core.searchad.api.adreport.transform import AdvancedReport as T
    return AdvancedReport(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(start_date, end_date)


@with_duckdb_connection(table="searchad_contract")
def time_contract(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """네이버 브랜드검색 광고 계약기간 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: searchad_contract`

    Parameters
    ----------
    api_key: str
        SA API 엑세스라이선스
    secret_key: str
        SA API 비밀키
    customer_id: int | str
        광고계정의 CUSTOMER_ID
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
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
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.searchad.api.contract.extract import TimeContract
    from linkmerce.core.searchad.api.contract.transform import TimeContract as T
    return TimeContract(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
    )).extract()


@with_duckdb_connection(table="searchad_contract_new")
def brand_new_contract(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        *,
        connection: DuckDBConnection | None = None,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | list[dict] | None:
    """네이버 신제품검색 광고 계약기간 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: searchad_contract_new`

    Parameters
    ----------
    api_key: str
        SA API 엑세스라이선스
    secret_key: str
        SA API 비밀키
    customer_id: int | str
        광고계정의 CUSTOMER_ID
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
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
            - `"raw"`: 데이터 수집 후 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.searchad.api.contract.extract import BrandNewContract
    from linkmerce.core.searchad.api.contract.transform import BrandNewContract as T
    return BrandNewContract(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
    )).extract()


@with_duckdb_connection(table="searchad_keyword")
def keyword(
        api_key: str,
        secret_key: str,
        customer_id: int | str,
        keywords: str | Iterable[str],
        max_rank: int | None = None,
        show_detail: bool = True,
        *,
        connection: DuckDBConnection | None = None,
        request_delay: float | int = 0.3,
        progress: bool = True,
        return_type: Literal["csv", "json", "parquet", "raw", "none"] = "json",
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> DuckDBResult | dict | list[dict] | None:
    """네이버 검색광고 연관키워드 데이터를 수집해 DuckDB 테이블에 변환 및 적재한다.

    **Table** ( *table_key: table_name* ):
        `table: searchad_keyword`

    Parameters
    ----------
    api_key: str
        SA API 엑세스라이선스
    secret_key: str
        SA API 비밀키
    customer_id: int | str
        광고계정의 CUSTOMER_ID
    keywords: str | Iterable[str]
        키워드. 문자열 또는 문자열의 배열을 입력한다. 키워드는 5개씩 묶어서 조회한다.
    max_rank: int | None
        최대 순위. 지정하면 상위 `max_rank`개의 키워드만 반환한다.
    show_detail: bool
        연관키워드의 상세 통계 정보 조회 여부
            - `True`: 상세 통계 정보를 조회한다. (기본값)
            - `False`: 월간검색수만 조회한다.
    connection: DuckDBConnection | None
        사용할 DuckDB 연결. 생략하면 실행 중 임시 연결을 생성하고 실행 종료 후 닫는다.
    request_delay: float | int | tuple[int, int]
        키워드별 요청 간 대기 시간(초). 기본값은 `0.3`
    progress: bool
        반복 요청 작업의 진행도 출력 여부. 기본값은 `True`
    return_type: str
        반환 형식. **Returns** 문단을 참고한다.
    extract_options: dict | None
        `Extractor` 초기화 옵션
    transform_options: dict | None
        `Transformer` 초기화 옵션

    Returns
    -------
    DuckDBResult | dict | list[dict] | None
        `return_type`에 따라 다음 형식 중 하나로 결과를 반환한다.
            - `"csv"`: 테이블 조회 결과를 CSV 형식의 `list[tuple]`로 반환한다.
            - `"json"`: 테이블 조회 결과를 JSON 형식의 `list[dict]`로 반환한다. (기본값)
            - `"parquet"`: 테이블 조회 결과를 Parquet 바이너리로 반환한다.
            - `"raw"`: 데이터 수집 후 `dict` 또는 `list[dict]` 형식의 원본 응답을 반환한다.
            - `"none"`: 모든 과정을 수행한 후 `None`을 반환한다.
    """
    from linkmerce.core.searchad.api.keyword.extract import Keyword
    from linkmerce.core.searchad.api.keyword.transform import Keyword as T
    return Keyword(**prepare_duckdb_extract(
        T, connection, extract_options, transform_options, return_type,
        configs = _get_api_configs(api_key, secret_key, customer_id),
        options = {
            "RequestEach": {
                "request_delay": request_delay,
                "tqdm_options": {"disable": (not progress)}
            }
        },
    )).extract(keywords, max_rank, (show_detail or (return_type != "raw")))
