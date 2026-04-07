from airflow.sdk import DAG, TaskGroup, task
from airflow.task.trigger_rule import TriggerRule
from airflow.models.taskinstance import TaskInstance
from airflow.providers.slack.hooks.slack import SlackHook
from datetime import timedelta
from textwrap import dedent
from typing import Literal
import pendulum


with DAG(
    dag_id = "ecount_stock_report",
    schedule = None, # `sabangnet_order` Dag 실행 후 트리거 (담당자가 수동으로 API 요청)
    start_date = pendulum.datetime(2025, 12, 18, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "eflexs:stock", "coupang:inventory", "ecount:inventory", "ecount:product",
            "login:cj-eflexs", "login:coupang", "api:ecount", "schedule:weekdays", "time:morning",
            "manual:api", "manual:dagrun", "provider:slack"],
    doc_md = dedent("""
        # 재고 현황 보고 파이프라인

        > 안내) 사방넷 주문 내역을 수집하는 `sabangnet_order` Dag 실행 후 트리거된다.

        ## 인증(Credentials)
        다음 3가지 플랫폼에 대한 인증 정보가 필요하다.
        1. CJ eFLEXs 로그인을 위한 아이디, 비밀번호 및 2단계 인증 메일 정보
        2. 쿠팡 윙 로그인 쿠키
        3. 이카운트 API 인증 키(회사코드, 사용자ID, API 키)

        ## 추출(Extract)
        1. CJ eFLEXs 재고 검색 결과를 수집한다.
        2. 쿠팡 로켓 재고 내역을 수집한다.
        3. 이카운트 창고별/품목별 재고 현황과 품목 리스트를 수집한다.

        ## 변환(Transform)
        각 플랫폼에서 추출한 JSON 형식의 응답 데이터를 파싱하여
        각각의 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        1. CJ eFLEXs 재고 테이블의 데이터를 기존 BigQuery 테이블을 지우고 덮어쓴다.
        2. 쿠팡 로켓 재고 데이터는 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
        3. 이카운트 재고와 상품 데이터도 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.

        ## 알림(Alert)
        생성된 테이블을 병합해 하나로 만드는 BigQuery 테이블 함수를 조회해
        통합 재고 데이터를 가져오고, 엑셀 파일로 변환해 메시지와 함께 Slack의 지정된 채널에 업로드한다.
    """).strip(),
) as dag:

    ###################################################################
    ############################# CJ Group ############################
    ###################################################################

    with TaskGroup(group_id="cj_group") as cj_group:

        CJ_PATH = "cjlogistics.eflexs.stock"

        @task(task_id="read_cj_configs", retries=3, retry_delay=timedelta(minutes=1))
        def read_cj_configs() -> dict:
            from airflow_utils import read_config
            return read_config(CJ_PATH, credentials="expand", tables=True, service_account=True)


        @task(task_id="etl_eflexs_stock")
        def etl_eflexs_stock(ti: TaskInstance, **kwargs) -> dict:
            """CJ eFLEXs 재고 검색 결과를 수집 및 BigQuery 테이블에 적재한다.
            (2단계 인증 메일 수신에 2분 정도 시간이 걸린다.)"""
            from airflow_utils import get_execution_date
            start_date, end_date = get_execution_date(kwargs, subdays=7), get_execution_date(kwargs)
            configs = ti.xcom_pull(task_ids="cj_group.read_cj_configs")
            return main_eflexs(start_date=start_date, end_date=end_date, **configs)

        def main_eflexs(
                userid: str,
                passwd: str,
                mail_info: dict,
                customer_id: list[int],
                start_date: str,
                end_date: str,
                service_account: dict,
                tables: dict[str, str],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.api.cj.eflexs import stock
            from linkmerce.extensions.bigquery import BigQueryClient
            source = "eflexs_stock"

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                stock(
                    userid = userid,
                    passwd = passwd,
                    mail_info = mail_info,
                    customer_id = customer_id,
                    start_date = start_date,
                    end_date = end_date,
                    connection = conn,
                    progress = False,
                    return_type = "none",
                )

                with BigQueryClient(service_account) as client:
                    return {
                        "params": {
                            "customer_id": customer_id,
                            "start_date": start_date,
                            "end_date": end_date,
                        },
                        "counts": {
                            "table": conn.count_table(source),
                        },
                        "status": {
                            "table": client.overwrite_table_from_duckdb(
                                connection = conn,
                                source_table = source,
                                target_table = tables["table"],
                                progress = False,
                                truncate_target_table = True,
                            ),
                        },
                    }


        read_cj_configs() >> etl_eflexs_stock()


    ###################################################################
    ########################## Coupang Group ##########################
    ###################################################################

    with TaskGroup(group_id="coupang_group") as coupang_group:

        COUPANG_PATH = "coupang.wing.inventory"

        @task(task_id="read_coupang_configs", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_coupang_configs() -> dict:
            from airflow_utils import read_config
            return read_config(COUPANG_PATH, tables=True, service_account=True)

        @task(task_id="read_coupang_credentials", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_coupang_credentials() -> list:
            from airflow_utils import read_config
            return read_config(COUPANG_PATH, credentials=True)["credentials"]


        @task(task_id="etl_coupang_inventory", map_index_template="{{ credentials['vendor_id'] }}", pool="coupang_pool")
        def etl_coupang_inventory(credentials: dict, configs: dict, **kwargs) -> dict:
            """업체별 쿠팡 로켓 배송 재고 내역을 수집하여 BigQuery 테이블에 적재한다."""
            return main_coupang(**credentials, **configs)

        def main_coupang(
                cookies: str,
                vendor_id: str,
                service_account: dict,
                tables: dict[str, str],
                merge: dict[str, dict],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.api.coupang.wing import rocket_inventory
            from linkmerce.extensions.bigquery import BigQueryClient
            source = "coupang_rocket_inventory"

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                rocket_inventory(
                    cookies = cookies,
                    hidden_status = None,
                    vendor_id = vendor_id,
                    connection = conn,
                    return_type = "none",
                )

                with BigQueryClient(service_account) as client:
                    return {
                        "params": {
                            "vendor_id": vendor_id,
                            "hidden_status": None,
                        },
                        "counts": {
                            "table": conn.count_table(source),
                        },
                        "status": {
                            "table": client.merge_into_table_from_duckdb(
                                connection = conn,
                                source_table = source,
                                staging_table = f'{tables["temp_table"]}_{vendor_id}',
                                target_table = tables["table"],
                                **merge["table"],
                                progress = False,
                            ),
                        },
                    }


        etl_coupang_inventory.partial(configs=read_coupang_configs()).expand(credentials=read_coupang_credentials())


    ###################################################################
    ########################### Ecount Group ##########################
    ###################################################################

    with TaskGroup(group_id="ecount_group") as ecount_group:

        ECOUNT_PATH = "ecount.api.inventory"

        @task(task_id="read_ecount_configs", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_ecount_configs() -> dict:
            from airflow_utils import read_config
            return read_config(ECOUNT_PATH, credentials="expand", tables=True, service_account=True)


        @task(task_id="etl_ecount_inventory")
        def etl_ecount_inventory(ti: TaskInstance, **kwargs) -> dict:
            """이카운트 API로 창고별/품목별 재고 현황을 수집하여 BigQuery에 적재한다."""
            from airflow_utils import get_execution_date
            configs = ti.xcom_pull(task_ids="ecount_group.read_ecount_configs")
            return main_ecount(api_type="inventory", base_date=get_execution_date(kwargs), **configs)

        @task(task_id="etl_ecount_product")
        def etl_ecount_product(ti: TaskInstance, **kwargs) -> dict:
            """이카운트 API로 품목 리스트를 수집하여 BigQuery에 적재한다."""
            from airflow_utils import get_execution_date
            configs = ti.xcom_pull(task_ids="ecount_group.read_ecount_configs")
            return main_ecount(api_type="product", base_date=get_execution_date(kwargs), **configs)

        def main_ecount(
                com_code: int | str,
                userid: str,
                api_key: str,
                base_date: str,
                api_type: Literal["product", "inventory"],
                service_account: dict,
                tables: dict[str, str],
                merge: dict[str, dict],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.extensions.bigquery import BigQueryClient
            from importlib import import_module
            extract = getattr(import_module("linkmerce.api.ecount.api"), api_type)
            params = {"base_date": base_date, "zero_yn": True} if api_type == "inventory" else {"comma_yn": True}
            source = f"ecount_{api_type}"

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                extract(
                    com_code = com_code,
                    userid = userid,
                    api_key = api_key,
                    **params,
                    connection = conn,
                    return_type = "none",
                )

                with BigQueryClient(service_account) as client:
                    return {
                        "params": {
                            "com_code": com_code,
                            **params,
                        },
                        "counts": {
                            api_type: conn.count_table(source),
                        },
                        "status": {
                            api_type: client.merge_into_table_from_duckdb(
                                connection = conn,
                                source_table = source,
                                staging_table = tables[f"temp_{api_type}"],
                                target_table = tables[api_type],
                                **merge[api_type],
                                progress = False,
                            )
                        },
                    }


        read_ecount_configs() >> etl_ecount_inventory() >> etl_ecount_product()


    ###################################################################
    ########################### Report Group ##########################
    ###################################################################

    with TaskGroup(group_id="report_group") as report_group:

        ALERT_PATH = "ecount.api.stock_report"

        @task(task_id="read_report_configs", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALL_SUCCESS)
        def read_report_configs() -> dict:
            from airflow_utils import read_config
            return read_config(ALERT_PATH, service_account=True)


        @task(task_id="send_stock_report")
        def send_stock_report(ti: TaskInstance, data_interval_end: pendulum.DateTime, **kwargs) -> dict:
            """수집된 재고 내역을 엑셀 파일로 정리하여 Slack의 지정된 채널에 업로드한다."""
            from airflow_utils import in_timezone
            configs = ti.xcom_pull(task_ids="report_group.read_report_configs")
            return main_report(datetime=in_timezone(data_interval_end), **configs)

        def main_report(
                table_function: str,
                slack_conn_id: str,
                channel_id: str,
                datetime: pendulum.DateTime,
                service_account: dict,
                max_fetch_retries: int = 10,
                delay_increment: float = 10.,
                **kwargs
            ) -> dict:
            from linkmerce.extensions.bigquery import BigQueryClient
            from linkmerce.utils.excel import csv2excel, save_excel_to_tempfile
            import time

            # [LOAD DATA] BigQuery 테이블에 업로드된 3가지 플랫폼의 재고 내역을 테이블 함수로 병합해 가져온다.
            headers = expected_headers(datetime)
            with BigQueryClient(service_account) as client:
                columns = ', '.join([header[0] for header in headers])
                for i in range(1, max_fetch_retries+1):
                    rows = client.fetch_all_to_csv(f"SELECT {columns} FROM {table_function}('{datetime.date()}');", header=True)
                    if sum_eflexs_quantity(rows) > 0:
                        break
                    time.sleep(delay_increment * i)

            # [PARSE DATA] 조회한 데이터에서 영어 헤더를 2단계 한글 헤더로 변경한다.
            headers0, headers1 = list(), list()
            for header, (expected, (header0, header1)) in zip(rows[0], headers):
                if header != expected:
                    raise ValueError(f"Expected header '{expected}' do not match actual column '{header}'")
                headers0.append(header0)
                headers1.append(header1)

            # [MAKE EXCEL] 데이터를 서식이 포함된 `openpyxl.Workbook` 객체로 변환한다.
            merge_headers, hedaer_styles = header_styles()
            column_styles, column_width = value_styles()

            wb = csv2excel(
                obj = ([headers0, headers1] + rows[1:]),
                sheet_name = "재고-소비기한",
                header_rows = [1, 2],
                header_styles = dict(),
                column_styles = column_styles,
                column_width = column_width,
                conditional_formatting = conditional_formatting(),
                merge_cells = merge_headers,
                range_styles = hedaer_styles,
                column_filters = {"range": "A2:X"},
                freeze_panes = "F3",
                zoom_scale = 85,
            )

            # [ALERT] Slack 채널에 메시지와 함께 엑셀 파일을 업로드한다.
            return send_excel_to_slack(
                slack_conn_id = slack_conn_id,
                channel_id = channel_id,
                file = save_excel_to_tempfile(wb),
                datetime = datetime,
                total = (len(rows) - 1),
                counts = count_by_brand(rows),
            )


        def expected_headers(datetime: pendulum.DateTime) -> list[tuple[str, tuple[str, str]]]:
            """테이블 함수 조회 결과의 전체 열 목록을 검증하고 2단계 한글 헤더로 변경하기 위한 자료구조를 생성한다."""
            from airflow_utils import format_date
            datetime_stock = format_date(datetime, "YYYY-MM-DD(dd) HH:mm")
            date_sales = tuple(format_date(datetime, "YYYY-MM-DD(dd)", subdays=delta) for delta in [30, 1])
            return [
                ("brand_name", (f"재고 기준: {datetime_stock}", "브랜드")),
                ("option_id", (None, "사방넷\n품번코드")),
                ("remarks_name", (None, "아이인리치코드")),
                ("product_code", (f"판매량 기준: {date_sales[0]} ~ {date_sales[1]}", "품목코드")),
                ("product_name", (None, "품목명")),
                ("expiration_date", (None, "유통기한")),
                ("ecount_quantity", ("본사창고", "본사재고")),
                ("sabangnet_sold_30d", (None, "총 판매량\n(최근 30일)")),
                ("sabangnet_avg_sold_30d", (None, "일 평균 판매량\n(최근 30일)")),
                ("ecount_remain_days", (None, "판매 가능일")),
                ("eflexs_quantity", ("N배송", "풀필재고")),
                ("eflexs_sold_30d", (None, "총 판매량\n(최근 30일)")),
                ("eflexs_avg_sold_30d", (None, "일 평균 판매량\n(최근 30일)")),
                ("eflexs_remain_days", (None, "판매 가능일")),
                ("rocket_quantity", ("로켓그로스", "그로스재고")),
                ("rocket_sold_30d", (None, "총 판매량\n(최근 30일)")),
                ("rocket_avg_sold_30d", (None, "일 평균 판매량\n(최근 30일)")),
                ("rocket_remain_days", (None, "판매 가능일")),
                ("total_quantity", ("소비기한별 재고소진 예상일", "총 재고")),
                ("total_sold_30d", (None, "총 판매량\n(최근 30일)")),
                ("total_avg_sold_30d", (None, "일 평균 판매량\n(최근 30일)")),
                ("total_remain_days", (None, "판매 가능일")),
                ("expected_date", (None, "예상 소진일")),
                ("performance", (None, "재고 알림")),
            ]

        def header_styles() -> tuple[list[dict], list[tuple]]:
            """2단계 헤더의 각 영역마다 배경색을 다르게 한 헤더 서식 설정을 생성한다."""
            PALETTE = ["#EDD777", "#FFFFFF", "#FFF2CC", "#FCE4D6", "#DDEBF7", "#E2EFDA", "#FF9999"]

            def _styles(color: str) -> dict:
                return {
                    "alignment": {"horizontal": "center", "vertical": "center", "wrap_text": True},
                    "fill": {"color": color, "fill_type": "solid"},
                    "font": {"color": "#000000", "bold": True},
                }

            merge_headers = [
                {"ranges": "A1:C1", "range_type": "range", "mode": "all", "styles": _styles(PALETTE[0])}, # date_ko
                {"ranges": "D1:E1", "range_type": "range", "mode": "all", "styles": _styles(PALETTE[1])}, # date_sales
                {"ranges": "G1:J1", "range_type": "range", "mode": "all", "styles": _styles(PALETTE[2])}, # 본사창고
                {"ranges": "K1:N1", "range_type": "range", "mode": "all", "styles": _styles(PALETTE[3])}, # N배송
                {"ranges": "O1:R1", "range_type": "range", "mode": "all", "styles": _styles(PALETTE[4])}, # 로켓그로스
                {"ranges": "S1:X1", "range_type": "range", "mode": "all", "styles": _styles(PALETTE[5])}, # 총재고
            ]

            hedaer_styles = [
                ("A2:C2", _styles(PALETTE[1])), # 코드
                ("D2:F2", _styles(PALETTE[1])), # 품목
                ("G2:J2", _styles(PALETTE[2])), # 본사창고
                ("K2:N2", _styles(PALETTE[3])), # N배송
                ("O2:R2", _styles(PALETTE[4])), # 로켓그로스
                ("S2:W2", _styles(PALETTE[5])), # 총재고
                ("X2", _styles(PALETTE[6])),
            ]

            return merge_headers, hedaer_styles

        def value_styles() -> tuple[dict, dict]:
            """열 명칭에 따라 가운데 정렬, 숫자 서식, 열 너비 등의 서식 설정을 생성한다."""
            headers = [column for _, (_, column) in expected_headers(pendulum.DateTime.now())]
            column_styles, column_width = dict(), dict()

            for col_idx, column in enumerate(headers, start=1):
                if column == "품목명":
                    column_width[col_idx] = 50
                elif column.endswith("재고"):
                    column_styles[col_idx] = {"number_format": "#,##0;-#,##0;-"}
                    column_width[col_idx] = 9.75
                elif column == "총 판매량\n(최근 30일)":
                    column_styles[col_idx] = {"number_format": "#,##0;-;-"}
                    column_width[col_idx] = 10.5
                elif column in ("일 평균 판매량\n(최근 30일)", "판매 가능일", "재고 알림"):
                    if column == "판매 가능일":
                        column_styles[col_idx] = {"alignment": {"horizontal": "center"}, "number_format": "#,##0일 이내;-;-"}
                    elif column == "재고 알림":
                        column_styles[col_idx] = {"alignment": {"horizontal": "center"}}
                    else:
                        column_styles[col_idx] = {"number_format": "#,##0;-#,##0;-"}
                    column_width[col_idx] = 13.5
                elif column in ("브랜드", "유통기한", "예상 소진일"):
                    column_styles[col_idx] = {"alignment": {"horizontal": "center"}}

                if col_idx not in column_width:
                    column_width[col_idx] = ":fit_values:"

            return column_styles, column_width

        def conditional_formatting() -> list[dict]:
            """`재고 알림` 열의 값을 기준으로 하는 전체 범위의 조건부 서식 설정을 생성한다."""
            danger = {
                "operator": "formula",
                "formula": ['$X1="소비기한 초과"'],
                "fill": {"color": "#FFC7CE", "fill_type": "solid"},
                "font": {"color": "#9C0006", "bold": True},
            }
            warning = {
                "operator": "formula",
                "formula": ['$X1="판매부진"'],
                "fill": {"color": "#FFEB9C", "fill_type": "solid"},
                "font": {"color": "#9C5700", "bold": True},
            }
            return [{"ranges": "A:X", "range_type": "range", "rule": danger}, {"ranges": "A:X", "range_type": "range", "rule": warning}]


        def sum_eflexs_quantity(rows: list[tuple]) -> int:
            """테이블 함수 조회 시 CJ eFLEXs 재고 내역이 가끔씩 누락된다. 재고 수량이 없다면 재시도를 요구하기 위한 함수다."""
            ELFEXS_QUANTITY = 10
            try:
                return sum([(row[ELFEXS_QUANTITY] or 0) for row in rows[1:]]) if rows else 0
            except:
                return 0

        def count_by_brand(rows: list[tuple], header_row: int = 0) -> list[dict]:
            """Slack 메시지를 구성하기 위한 브랜드별 재고 알림 대상을 카운팅한다."""
            from collections import defaultdict
            counter = lambda: {"total": 0, "expired": 0, "excess": 0}
            counts, keys = defaultdict(counter), list()

            brand_col = rows[header_row].index("brand_name")
            label_col = rows[header_row].index("performance")

            for row in rows[(header_row+1):]:
                brand_name = row[brand_col] or "브랜드없음"
                if brand_name not in keys:
                    keys.append(brand_name)

                counts[brand_name]["total"] += 1
                if row[label_col] == "소비기한 초과":
                    counts[brand_name]["expired"] += 1
                if row[label_col] == "판매부진":
                    counts[brand_name]["excess"] += 1

            return [{"name": brand_name, **counts[brand_name]} for brand_name in keys]


        def send_excel_to_slack(
                slack_conn_id: str,
                channel_id: str,
                file: str,
                datetime: pendulum.DateTime,
                total: int,
                counts: list[dict],
            ) -> dict:
            """Slack에 보낼 메시지를 구성하고, 엑셀 파일과 함께 지정된 채널에 업로드한다."""
            from airflow_utils import format_date
            import os
            slack_hook = SlackHook(slack_conn_id=slack_conn_id)

            message = (
                "안녕하세요\n"
                + "{} 브랜드별 재고-소비기한 현황 공유드립니다\n\n".format(format_date(datetime, "M/D(dd)"))
                + "전체 {:,}개 이카운트 상품에 대한 브랜드별 요약:".format(total))

            WARN, INFO = ":small_red_triangle:", ":small_blue_diamond:"
            for brand in counts:
                warning = ["{} {:,}개".format(label, brand[status])
                    for status, label in [("expired", "기한초과"), ("excess", "판매부진")] if brand[status]]
                if warning:
                    message += "\n{} {} : {:,}개 상품 ({})".format(WARN, brand["name"], brand["total"], " / ".join(warning))
                else:
                    message += "\n{} {} : {:,}개 상품".format(INFO, brand["name"], brand["total"])

            try:
                response = slack_hook.send_file_v2(
                    channel_id = channel_id,
                    file_uploads = [{
                        "file": file,
                        "filename": "재고_소비기한_{}.xlsx".format(format_date(datetime, "YYMMDD_HHmm")),
                        "title": "재고-소비기한 ({})".format(format_date(datetime, "YYMMDD")),
                    }],
                    initial_comment = message,
                )
                return {
                    "params": {
                        "channel_id": channel_id,
                        "timestamp": format_date(datetime, "YYYY-MM-DDTHH:mm:ss"),
                    },
                    "counts": {
                        "total": total,
                        "by_brand": {brand["name"]: brand["total"] for brand in counts},
                    },
                    "message": message,
                    "response": parse_slack_response(
                        files = response.get("files", list()),
                        ok = response.get("ok", False)
                    ),
                }
            finally:
                os.unlink(file)

        def parse_slack_response(files: list[dict], ok: bool) -> dict:
            """Slack 업로드 응답 결과를 Airflow 로그에 보여주기 위한 용도로 파싱한다."""
            keys = ["id", "name", "size", "created", "permalink"]
            return {
                "files": [{key: file.get(key) for key in keys} for file in files],
                "status": ("success" if ok else "failed"),
            }


        read_report_configs() >> send_stock_report()


    cj_group >> coupang_group >> ecount_group >> report_group
