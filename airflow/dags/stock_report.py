"""
# 재고-소비기한 알림 파이프라인

> 안내) 같은 날에 사방넷 주문 내역을 수집하는 'sabangnet_order' Dag이 실행된 경우만 알림을 보낸다.

## 의존성(Upstreams)
오전/오후 시간대에 트리거된 상위 3개의 Dag run 상태가 전부 성공할 때까지 실행 대기한다.
1. cj_eflexs_stock
2. ecount_inventory
3. coupang_inventory

## 추출(Extract)
BigQuery 테이블에 업로드된 3가지 플랫폼의 재고 내역을 테이블 함수로 병합해 가져온다.

## 변환(Transform)
조회한 데이터를 2단계 한글 헤더로 구성된 엑셀 파일로 변환한다.

## 알림(Alert)
서식이 포함된 엑셀 파일을 메시지와 함께 Slack의 지정된 채널에 업로드한다.
"""

from airflow.sdk import DAG, TaskGroup, task
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from datetime import timedelta
from typing import TypedDict
import pendulum


with DAG(
    dag_id = "stock_report",
    schedule = MultipleCronTriggerTimetable(
        "0 11 * * 1-5",
        "30 17 * * 1-5",
        timezone = "Asia/Seoul",
    ),
    start_date = pendulum.datetime(2026, 5, 27, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(hours=1),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:ecount", "platform:cj-eflexs", "platform:coupang-wing",
        "objective:alert", "objective:stock", "credentials:service-account",
        "schedule:weekdays", "time:morning", "time:afternoon", "provider:slack"
    ],
) as dag:

    PATH = "core.stock_report"
    AM_PM_CUTOFF_HOUR = 17
    COUPANG_CREDENTIALS = "coupang.vendor"

    SENSOR_POKE_INTERVAL = 30
    SENSOR_TIMEOUT = int(timedelta(minutes=59).total_seconds())

    class ReportTimes(TypedDict):
        last_updated_at: pendulum.DateTime
        order_start_date: pendulum.Date
        order_end_date: pendulum.Date


    def check_prior_run(ti: TaskInstance, data_interval_end: pendulum.DateTime, **kwargs) -> str:
        """`scheduled__` 실행 시 오늘 중 `sabangnet_order` Dag이 실행되었는지 확인하고 없다면 skip한다."""
        if not ti.run_id.startswith("scheduled__"):
            return "external_task_sensor"

        from airflow_api import authenticate, list_dagruns
        dag_runs = list_dagruns(
            dag_id = "sabangnet_order",
            access_token = authenticate(),
            logical_date_gte = data_interval_end.start_of("day"),
            logical_date_lte = data_interval_end.end_of("day"),
        )
        return "external_task_sensor" if dag_runs else None

    branch_prior_run = BranchPythonOperator(
        task_id = "branch_prior_run",
        python_callable = check_prior_run,
    )


    with TaskGroup(group_id="external_task_sensor") as external_task_sensor:

        @task(task_id="init_sensor_states")
        def init_sensor_states(**kwargs) -> dict:
            """KST 실행 시각을 기준으로 오전/오후를 구분하고, 재고 수집 이벤트 범위를 계산한다."""
            from airflow_utils import get_datetime, read_credentials

            # `AM_PM_CUTOFF_HOUR`을 기준으로 실행 시간이 오전/오후인지 구분한다.
            datetime = get_datetime(kwargs)
            start_of_day = datetime.start_of("day")
            afternoon_start = start_of_day.add(hours=AM_PM_CUTOFF_HOUR)

            # 쿠팡 업체 수를 집계하여 생성될 Dag run 개수를 미리 예측한다.
            credentials = read_credentials(COUPANG_CREDENTIALS)
            num_vendors = sum([1 for credential in credentials
                if isinstance(credential, dict) and credential.get("vendor_id")])

            if datetime < afternoon_start:
                return {
                    "upstream": {
                        "datetime": datetime.isoformat(),
                        "start_time": start_of_day.isoformat(),
                        "end_time": afternoon_start.subtract(microseconds=1).isoformat(),
                        "expected_coupang_dag_runs": num_vendors,
                    },
                    "downstream": {
                        "report_date": datetime.format("YYYY-MM-DD"),
                        "report_batch": 10,
                    },
                }
            else:
                return {
                    "upstream": {
                        "datetime": datetime.isoformat(),
                        "start_time": afternoon_start.isoformat(),
                        "end_time": datetime.end_of("day").isoformat(),
                        "expected_coupang_dag_runs": num_vendors,
                    },
                    "downstream": {
                        "report_date": datetime.format("YYYY-MM-DD"),
                        "report_batch": 20,
                    },
                }


        def wait_for_upstreams(ti: TaskInstance, **kwargs) -> bool:
            """재고 수집 이벤트의 완료 여부를 추적한다."""
            from airflow_api import authenticate, list_dagruns
            import logging

            logger = logging.getLogger(__name__)
            states = ti.xcom_pull(task_ids="external_task_sensor.init_sensor_states")["upstream"]
            params = {
                "access_token": authenticate(),
                "logical_date_gte": pendulum.parse(states["start_time"]),
                "logical_date_lte": pendulum.parse(states["end_time"]),
            }

            def _get_lastest_dagruns(dag_id: str) -> dict | None:
                """필터 조건에 해당하는 Dag run 중 실행 시간이 가장 빠른 것을 반환한다."""
                dag_runs = [dag_run for dag_run in list_dagruns(dag_id, **params)]
                return max(dag_runs, key=(lambda dag_run: dag_run.get("logical_date") or str()), default=None)

            def _wait_for_task(dag_id: str) -> bool:
                """이벤트 범위 내 `dag_id` 실행 내역을 조회하고 `state`에 따라 분기한다.
                - `"success"`: `True`를 반환한다.
                - `"failed"`: `AirflowException`을 발생시킨다.
                - 그 외 상태 또는 실행 내역이 없으면 INFO 로그와 함께 `False`를 반환한다.
                """
                dag_run = _get_lastest_dagruns(dag_id)
                if dag_run is None:
                    logger.info(f"[{dag_id}] Waiting for Dag run")
                    return False

                state = dag_run.get("state")
                if state == "failed":
                    raise AirflowException(f"'{dag_id}' failed before 'stock_report' could start")
                elif state == "success":
                    return True
                else:
                    logger.info(f"[{dag_id}] Current state: {state}")
                    return False

            def _wait_for_coupang_task(upstream_dag_id: str, downstream_dag_id: str) -> bool:
                """이벤트 범위 내 `ecount_inventory` Dag 실행 내역을 조회하고 다음 조건에 따라 순차적으로 분기한다.
                1. Dag(upstream) run이 없으면 INFO 로그와 함께 `False`를 반환한다.
                2. 성공한 Dag(downstream) runs 개수가 `credentials`의 `vendor_id` 개수 이상이면 `True`를 반환한다.
                3. 실패한 Dag(downstream) run이 있다면 `AirflowException`을 발생시킨다.
                4. Dag(upstream) run이 이미 종료되었다면 `AirflowException`을 발생시킨다.
                5. 그 외 경우는 `False`를 반환한다.
                """
                from linkmerce.utils.regex import regexp_extract
                upstream = _get_lastest_dagruns(upstream_dag_id)
                if upstream is None:
                    logger.info(f"[{upstream_dag_id}] Waiting for Dag run")
                    return False

                success_ids, failed_ids = set(), set()
                for dag_run in list_dagruns(downstream_dag_id, **params):
                    if not (i := regexp_extract(r"^expanded__(\d+)", dag_run.get("dag_run_id") or str())):
                        continue
                    dag_id_i, state = f"{downstream_dag_id}[{i}]", dag_run.get("state")

                    if state == "success":
                        success_ids.add(i)
                    elif state == "failed":
                        failed_ids.add(i)
                    else:
                        logger.info(f"[{dag_id_i}] Current state: {state}")

                if len(success_ids) >= states["expected_coupang_dag_runs"]:
                    return True
                elif len(failed_ids) > 0:
                    raise AirflowException(f"'{dag_id_i}' failed before 'stock_report' could start")

                count = "{}/{}".format(len(success_ids), states["expected_coupang_dag_runs"])
                if upstream.get("state") in ("success", "failed"):
                    message = f"[{upstream_dag_id}] Finished with only {count} '{downstream_dag_id}' runs completed"
                    raise AirflowException(message)

                logger.info(f"[{upstream_dag_id}] Waiting for '{downstream_dag_id}' runs: {count} completed")
                return False

            return (_wait_for_task("cj_eflexs_stock")
                and _wait_for_task("ecount_inventory")
                and _wait_for_coupang_task("coupang", "coupang_inventory"))

        execute_sensor = PythonSensor(
            task_id = "wait_for_upstreams",
            python_callable = wait_for_upstreams,
            mode = "reschedule",
            poke_interval = SENSOR_POKE_INTERVAL,
            timeout = SENSOR_TIMEOUT,
        )


        init_sensor_states() >> execute_sensor


    with TaskGroup(group_id="report_group") as report_group:

        @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
        def read_configs() -> dict:
            from airflow_utils import read_config
            return read_config(PATH, service_account=True)


        @task(task_id="send_stock_report")
        def send_stock_report(ti: TaskInstance, **kwargs) -> dict:
            from airflow_utils import get_datetime
            config = ti.xcom_pull(task_ids="report_group.read_configs")
            config["sensor_states"] = ti.xcom_pull(task_ids="external_task_sensor.init_sensor_states")
            return main(datetime=get_datetime(kwargs), **config)

        def main(
                stock_report_tvf: str,
                stock_time_table: str,
                slack_conn_id: str,
                channel_id: str,
                sensor_states: dict,
                service_account: dict,
                max_fetch_retries: int = 10,
                delay_increment: float = 10.,
                **kwargs
            ) -> dict:
            from linkmerce.extensions.bigquery import BigQueryClient
            from linkmerce.utils.excel import csv2excel, save_excel_to_tempfile
            import time

            states, rows = sensor_states["downstream"], list()
            ymd, batch = "'{}'".format(states["report_date"]), states["report_batch"]

            # [LOAD DATA] BigQuery 테이블에 업로드된 3가지 플랫폼의 재고 내역을 테이블 함수로 병합해 가져온다.
            with BigQueryClient(service_account) as client:
                tbl_query = f"SELECT max_updated_at FROM {stock_time_table} WHERE ymd = {ymd} AND batch = {batch};"
                if (tbl_result := client.fetch_one(tbl_query)) is None:
                    raise AirflowException(
                        f"Stock report cannot start: no stock update time found in "
                        f"{stock_time_table} for ymd={ymd} and batch={batch}"
                    )
                updated_at = pendulum.instance(tbl_result, tz="Asia/Seoul")

                tvf_columns = ", ".join([header[0] for header in expected_headers()])
                report_query = f"SELECT {tvf_columns} FROM {stock_report_tvf}({ymd}, {batch});"

                for i in range(1, max_fetch_retries+1): # CJ eFLEXs 재고 수량 반영이 늦어 최대 n회 재시도
                    rows = client.fetch_all_to_csv(report_query, header=True)
                    if sum_eflexs_quantity(rows) > 0:
                        break
                    time.sleep(delay_increment * i)

            if not rows:
                raise AirflowException(f"No rows returned after {max_fetch_retries} fetch attempts")

            # [PARSE DATA] 조회한 데이터에서 영어 헤더를 2단계 한글 헤더로 변경한다.
            headers0, headers1 = list(), list()
            for header, (expected, (header0, header1)) in zip(rows[0], expected_headers(updated_at)):
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
                freeze_panes = "G3",
                zoom_scale = 85,
            )

            # [ALERT] Slack 채널에 메시지와 함께 엑셀 파일을 업로드한다.
            return send_excel_to_slack(
                slack_conn_id = slack_conn_id,
                channel_id = channel_id,
                file = save_excel_to_tempfile(wb),
                datetime = updated_at,
                total = (len(rows) - 1),
                counts = count_by_brand(rows),
            )


        def make_header_dates(updated_at: pendulum.DateTime) -> dict[str, str]:
            """재고 내역의 최근 업데이트 날짜를 헤더에 삽입할 날짜 문자열로 변환한다."""
            return {
                "stock_updated_at": updated_at.format("YYYY-MM-DD(dd) HH:mm", locale="ko"),
                "order_date_range": "{} ~ {}".format(
                    updated_at.subtract(days=30).format("YYYY-MM-DD(dd)", locale="ko"),
                    updated_at.subtract(days=1).format("YYYY-MM-DD(dd)", locale="ko"),
                )
            }

        def expected_headers(updated_at: pendulum.DateTime | None = None) -> list[tuple[str, tuple[str, str]]]:
            """테이블 함수 조회 결과의 전체 열 목록을 검증하고 2단계 한글 헤더로 변경하기 위한 자료구조를 생성한다."""
            dates = make_header_dates(updated_at) if updated_at is not None else dict()
            return [
                ("brand_name", ("재고 기준: {}".format(dates.get("stock_updated_at", str())), "브랜드")),
                ("product_id", (None, "사방넷\n품번코드")),
                ("product_remarks", (None, "아이인리치코드")),
                ("product_code", ("판매량 기준: {}".format(dates.get("order_date_range", str())), "품목코드")),
                ("product_name", (None, "품목명")),
                ("expiration_date", (None, "소비기한")),
                ("ecount__stock_qty", ("본사창고", "본사재고")),
                ("sabangnet__sold_qty_30d", (None, "총 판매량\n(최근 30일)")),
                ("sabangnet__avg_sold_qty_30d", (None, "일 평균 판매량\n(최근 30일)")),
                ("ecount__remain_days", (None, "판매 가능일")),
                ("cj_eflexs__stock_qty", ("N배송", "풀필재고")),
                ("cj_eflexs__sold_qty_30d", (None, "총 판매량\n(최근 30일)")),
                ("cj_eflexs__avg_sold_qty_30d", (None, "일 평균 판매량\n(최근 30일)")),
                ("cj_eflexs__remain_days", (None, "판매 가능일")),
                ("coupang_rfm__stock_qty", ("로켓그로스", "그로스재고")),
                ("coupang_rfm__sold_qty_30d", (None, "총 판매량\n(최근 30일)")),
                ("coupang_rfm__avg_sold_qty_30d", (None, "일 평균 판매량\n(최근 30일)")),
                ("coupang_rfm__remain_days", (None, "판매 가능일")),
                ("stock_qty", ("소비기한별 재고소진 예상일", "총 재고")),
                ("sold_qty_30d", (None, "총 판매량\n(최근 30일)")),
                ("avg_sold_qty_30d", (None, "일 평균 판매량\n(최근 30일)")),
                ("remain_days", (None, "판매 가능일")),
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
            headers = [column for _, (_, column) in expected_headers()]
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
                elif column in ("브랜드", "소비기한", "예상 소진일"):
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
            """테이블 함수 조회 시 CJ대한통운 eFLEXs 재고 내역이 가끔씩 누락된다. 재고 수량이 없다면 재시도를 요구하기 위한 함수다."""
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
            import os
            slack_hook = SlackHook(slack_conn_id=slack_conn_id)

            datetime_ko = datetime.format("M/D(dd) A h시", locale="ko")
            message = (
                "안녕하세요\n"
                + f"{datetime_ko} · 브랜드별 재고-소비기한 현황 공유드립니다\n\n"
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
                        "filename": "재고_소비기한_{}.xlsx".format(datetime.format("YYMMDD_HHmm")),
                        "title": "재고-소비기한 ({})".format(datetime.format("YYMMDD_HHmm")),
                    }],
                    initial_comment = message,
                )
                return {
                    "params": {
                        "channel_id": channel_id,
                        "last_updated_at": datetime.format("YYYY-MM-DDTHH:mm:ss"),
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


        read_configs() >> send_stock_report()


    branch_prior_run >> external_task_sensor >> report_group
