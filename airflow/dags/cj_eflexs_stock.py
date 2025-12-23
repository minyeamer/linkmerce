from airflow.sdk import DAG, TaskGroup, task
from airflow.task.trigger_rule import TriggerRule
from airflow.models.taskinstance import TaskInstance
from airflow.providers.slack.hooks.slack import SlackHook
from datetime import timedelta
from typing import Literal
import pendulum


with DAG(
    dag_id = "cj_eflexs_stock",
    schedule = None, # triggered after sabangnet_order DAG run (managed by human)
    start_date = pendulum.datetime(2025, 12, 18, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "eflexs:stock", "coupang:inventory", "ecount:inventory", "ecount:product",
            "login:cj-eflexs", "login:coupang", "api:ecount", "schedule:weekdays", "time:morning", "manual:api", "manual:dagrun"],
) as dag:

    with TaskGroup(group_id="cj_group") as cj_group:

        CJ_PATH = ["cjlogistics", "eflexs", "cj_eflexs_stock"]

        @task(task_id="read_cj_variables", retries=3, retry_delay=timedelta(minutes=1))
        def read_cj_variables() -> dict:
            from variables import read
            return read(CJ_PATH, credentials="expand", tables=True, service_account=True)


        @task(task_id="etl_eflexs_stock")
        def etl_eflexs_stock(ti: TaskInstance, **kwargs) -> dict:
            from variables import get_execution_date
            start_date, end_date = get_execution_date(kwargs, subdays=7), get_execution_date(kwargs)
            variables = ti.xcom_pull(task_ids="cj_group.read_cj_variables")
            return main_eflexs(start_date=start_date, end_date=end_date, **variables)

        def main_eflexs(
                userid: str,
                passwd: str,
                mail_info: dict,
                customer_id: list[int],
                start_date: str,
                end_date: str,
                service_account: dict,
                tables: dict[str,str],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.api.cj.eflexs import stock as extract
            from linkmerce.extensions.bigquery import BigQueryClient

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                extract(
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
                    return dict(
                        params = dict(
                            customer_id = customer_id,
                            start_date = start_date,
                            end_date = end_date,
                        ),
                        counts = dict(
                            stock = conn.count_table("data"),
                        ),
                        status = dict(
                            stock = client.overwrite_table_from_duckdb(
                                connection = conn,
                                source_table = "data",
                                target_table = tables["stock"],
                                progress = False,
                                truncate_target_table = True,
                            ),
                        ),
                    )


        read_cj_variables() >> etl_eflexs_stock()


    with TaskGroup(group_id="coupang_group") as coupang_group:

        COUPANG_PATH = ["coupang", "wing", "coupang_inventory"]

        @task(task_id="read_coupang_variables", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_coupang_variables() -> dict:
            from variables import read
            return read(COUPANG_PATH, tables=True, service_account=True)

        @task(task_id="read_coupang_credentials", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_coupang_credentials() -> list:
            from variables import read
            return read(COUPANG_PATH, credentials=True)["credentials"]


        @task(task_id="etl_coupang_inventory", map_index_template="{{ credentials['vendor_id'] }}", pool="coupang_pool")
        def etl_coupang_inventory(credentials: dict, variables: dict, **kwargs) -> dict:
            return main_coupang(**credentials, **variables)

        def main_coupang(
                cookies: str,
                vendor_id: str,
                service_account: dict,
                tables: dict[str,str],
                merge: dict[str,dict],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.api.coupang.wing import rocket_inventory
            from linkmerce.extensions.bigquery import BigQueryClient

            with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
                rocket_inventory(
                    cookies = cookies,
                    hidden_status = None,
                    vendor_id = vendor_id,
                    connection = conn,
                    return_type = "none",
                )

                with BigQueryClient(service_account) as client:
                    return dict(
                        params = dict(
                            vendor_id = vendor_id,
                            hidden_status = None,
                        ),
                        counts = dict(
                            inventory = conn.count_table("data"),
                        ),
                        status = dict(
                            data = client.merge_into_table_from_duckdb(
                                connection = conn,
                                source_table = "data",
                                staging_table = f'{tables["temp_inventory"]}_{vendor_id}',
                                target_table = tables["inventory"],
                                **merge["inventory"],
                                progress = False,
                            ),
                        ),
                    )


        etl_coupang_inventory.partial(variables=read_coupang_variables()).expand(credentials=read_coupang_credentials())


    with TaskGroup(group_id="ecount_group") as ecount_group:

        ECOUNT_PATH = ["ecount", "api", "ecount_inventory"]

        @task(task_id="read_ecount_variables", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_ecount_variables() -> dict:
            from variables import read
            return read(ECOUNT_PATH, credentials="expand", tables=True, service_account=True)


        @task(task_id="etl_ecount_inventory")
        def etl_ecount_inventory(ti: TaskInstance, **kwargs) -> dict:
            from variables import get_execution_date
            variables = ti.xcom_pull(task_ids="ecount_group.read_ecount_variables")
            return main_ecount(api_type="inventory", base_date=get_execution_date(kwargs), **variables)

        @task(task_id="etl_ecount_product")
        def etl_ecount_product(ti: TaskInstance, **kwargs) -> dict:
            from variables import get_execution_date
            variables = ti.xcom_pull(task_ids="ecount_group.read_ecount_variables")
            return main_ecount(api_type="product", base_date=get_execution_date(kwargs), **variables)

        def main_ecount(
                com_code: int | str,
                userid: str,
                api_key: str,
                base_date: str,
                api_type: Literal["product","inventory"],
                service_account: dict,
                tables: dict[str,str],
                merge: dict[str,dict],
                **kwargs
            ) -> dict:
            from linkmerce.common.load import DuckDBConnection
            from linkmerce.extensions.bigquery import BigQueryClient
            from importlib import import_module
            extract = getattr(import_module("linkmerce.api.ecount.api"), api_type)
            params = dict(base_date=base_date, zero_yn=True) if api_type == "inventory" else dict(comma_yn=True)

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
                    return dict(
                        params = dict(
                            com_code = com_code,
                            **params,
                        ),
                        counts = {
                            api_type: conn.count_table("data"),
                        },
                        status = {
                            api_type: client.merge_into_table_from_duckdb(
                                connection = conn,
                                source_table = "data",
                                staging_table = tables[f"temp_{api_type}"],
                                target_table = tables[api_type],
                                **merge[api_type],
                                progress = False,
                            )
                        },
                    )


        read_ecount_variables() >> etl_ecount_inventory() >> etl_ecount_product()


    with TaskGroup(group_id="alert_group") as alert_group:

        ALERT_PATH = ["cjlogistics", "eflexs", "alert_agg_stock"]

        @task(task_id="read_alert_variables", retries=3, retry_delay=timedelta(minutes=1), trigger_rule=TriggerRule.ALWAYS)
        def read_alert_variables() -> dict:
            from variables import read
            return read(ALERT_PATH, service_account=True)


        @task(task_id="alert_agg_stock")
        def alert_agg_stock(ti: TaskInstance, data_interval_end: pendulum.DateTime, **kwargs) -> dict:
            from variables import in_timezone
            variables = ti.xcom_pull(task_ids="alert_group.read_alert_variables")
            return main_alert(date=in_timezone(data_interval_end), **variables)

        def main_alert(
                table_function: str,
                slack_conn_id: str,
                channel_id: str,
                date: pendulum.DateTime,
                service_account: dict,
            ) -> dict:
            from linkmerce.extensions.bigquery import BigQueryClient
            from linkmerce.utils.excel import csv2excel, save_excel_to_tempfile

            with BigQueryClient(service_account) as client:
                ymd = date.format("YYYY-MM-DD")
                rows = client.fetch_all_to_csv(f"SELECT * FROM {table_function}('{ymd}');", header=True)

            headers0, headers1 = list(), list()
            for header, (expected, (header0, header1)) in zip(rows[0], expected_headers(date)):
                if header != expected:
                    raise ValueError(f"Expected header '{expected}' do not match actual column '{header}'")
                headers0.append(header0)
                headers1.append(header1)

            merge_headers, hedaer_styles = header_styles()
            column_styles, column_width = value_styles()

            wb = csv2excel(
                obj = ([headers0, headers1] + rows[1:]),
                sheet_name = "재고-소비기한",
                header_rows = [1,2],
                header_styles = dict(),
                column_styles = column_styles,
                column_width = column_width,
                conditional_formatting = conditional_formatting(),
                merge_cells = merge_headers,
                range_styles = hedaer_styles,
                freeze_panes = "F3",
                zoom_scale = 85,
            )

            return send_excel_to_slack(
                slack_conn_id = slack_conn_id,
                channel_id = channel_id,
                file = save_excel_to_tempfile(wb),
                date_ymd_h = date.format("YYMMDD_HHmm"),
                date_ko = date.format("YY년 MM월 DD일 HH시 mm분 (dd)"),
                total = (len(rows) - 1),
            )


        def expected_headers(date: pendulum.DateTime) -> list[tuple[str, tuple[str,str]]]:
            date_stock = date.format("YYYY-MM-DD(dd) HH:mm")
            date_sales = tuple(date.subtract(days=delta).format("YYYY-MM-DD(dd)") for delta in [30, 1])
            return [
                ("brand_name", (f"재고 기준: {date_stock}", "브랜드")),
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
            PALETTE = ["#EDD777", "#FFFFFF", "#FFF2CC", "#FCE4D6", "#DDEBF7", "#E2EFDA", "#FF9999"]

            def _styles(color: str) -> dict:
                return dict(
                    alignment = {"horizontal": "center", "vertical": "center", "wrap_text": True},
                    fill = {"color": color, "fill_type": "solid"},
                    font = {"color": "#000000", "bold": True},
                )

            merge_headers = [
                dict(ranges="A1:C1", range_type="range", mode="all", styles=_styles(PALETTE[0])), # date_ko
                dict(ranges="D1:E1", range_type="range", mode="all", styles=_styles(PALETTE[1])), # date_sales
                dict(ranges="G1:J1", range_type="range", mode="all", styles=_styles(PALETTE[2])), # 본사창고
                dict(ranges="K1:N1", range_type="range", mode="all", styles=_styles(PALETTE[3])), # N배송
                dict(ranges="O1:R1", range_type="range", mode="all", styles=_styles(PALETTE[4])), # 로켓그로스
                dict(ranges="S1:X1", range_type="range", mode="all", styles=_styles(PALETTE[5])), # 총재고
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

        def value_styles() -> tuple[dict,dict]:
            headers = [column for _, (_, column) in expected_headers(str(), (None, None))]
            column_styles, column_width = dict(), dict()

            for col_idx, column in enumerate(headers, start=1):
                if column == "품목명":
                    column_width[col_idx] = 50
                elif column.endswith("재고"):
                    column_styles[col_idx] = dict(number_format="#,##0;-#,##0;-")
                    column_width[col_idx] = 9.75
                elif column == "총 판매량\n(최근 30일)":
                    column_styles[col_idx] = dict(number_format="#,##0;-;-")
                    column_width[col_idx] = 10.5
                elif column in ("일 평균 판매량\n(최근 30일)", "판매 가능일", "재고 알림"):
                    if column == "판매 가능일":
                        column_styles[col_idx] = dict(alignment = {"horizontal": "center"}, number_format="#,##0일 이내;-;-")
                    elif column == "재고 알림":
                        column_styles[col_idx] = dict(alignment = {"horizontal": "center"})
                    else:
                        column_styles[col_idx] = dict(number_format="#,##0;-#,##0;-")
                    column_width[col_idx] = 13.5
                elif column in ("브랜드", "유통기한", "예상 소진일"):
                    column_styles[col_idx] = dict(alignment = {"horizontal": "center"})

                if col_idx not in column_width:
                    column_width[col_idx] = ":fit_values:"

            return column_styles, column_width

        def conditional_formatting() -> list[dict]:
            danger = dict(
                operator = "formula",
                formula = ['$X1="소비기한 초과"'],
                fill = {"color": "#FFC7CE", "fill_type": "solid"},
                font = {"color": "#9C0006", "bold": True},
            )
            warning = dict(
                operator = "formula",
                formula = ['$X1="판매부진"'],
                fill = {"color": "#FFEB9C", "fill_type": "solid"},
                font = {"color": "#9C5700", "bold": True},
            )
            return [dict(ranges="A:X", range_type="range", rule=danger), dict(ranges="A:X", range_type="range", rule=warning)]


        def send_excel_to_slack(
                slack_conn_id: str,
                channel_id: str,
                file: str,
                date_ymd_h: str,
                date_ko: str,
                total: int,
            ) -> dict:
            import os
            slack_hook = SlackHook(slack_conn_id=slack_conn_id)

            message = None

            try:
                response = slack_hook.send_file_v2(
                    channel_id = channel_id,
                    file_uploads = [{
                        "file": file,
                        "filename": f"재고_소비기한_{date_ymd_h}.xlsx",
                        "title": f">{date_ko}",
                    }],
                    initial_comment = message,
                )
                return dict(
                    params = dict(
                        channel_id = channel_id,
                        date_ymd_h = date_ymd_h,
                        date_ko = date_ko,
                    ),
                    counts = dict(rows=total),
                    message = message,
                    response = parse_slack_response(
                        files = response.get("files", list()),
                        ok = response.get("ok", False)
                    ),
                )
            finally:
                os.unlink(file)

        def parse_slack_response(files: list[dict], ok: bool) -> dict:
            keys = ["id", "name", "size", "created", "permalink"]
            return dict(
                files = [{key: file.get(key) for key in keys} for file in files],
                status = ("success" if ok else "failed"),
            )


        read_alert_variables() >> alert_agg_stock()


    cj_group >> coupang_group >> ecount_group >> alert_group
