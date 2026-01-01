from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from airflow.providers.slack.hooks.slack import SlackHook
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "naver_cafe_search",
    schedule = "0,10,20,30,40,50 8,9 * * *",
    start_date = pendulum.datetime(2025, 12, 12, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(hours=1),
    catchup = False,
    tags = ["priority:high", "naver:cafe", "schedule:weekdays", "time:morning", "provider:slack"],
) as dag:

    PATH = ["naver", "main", "naver_cafe_search"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, sheets=True)


    @task(task_id="set_cookies", retries=3, retry_delay=timedelta(minutes=1), pool="nsearch_pool")
    def set_cookies(ti: TaskInstance, **kwargs) -> dict:
        variables = ti.xcom_pull(task_ids="read_variables")
        if not variables["records"]:
            return variables

        from playwright.sync_api import sync_playwright, Page
        import time

        def wait_cookies(page: Page, wait_seconds: int = 10, wait_interval: int = 1):
            for _ in range(wait_seconds // wait_interval):
                time.sleep(wait_interval)
                cookies = page.context.cookies()
                for cookie in cookies:
                    if cookie["name"] == "NNB":
                        return
            raise ValueError("Failed to set valid cookies.")

        def get_cookies(page: Page) -> str:
            return '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in page.context.cookies()])

        with sync_playwright() as playwright:
            ws_endpoint = "ws://playwright:3000/"
            browser = playwright.chromium.connect(ws_endpoint)
            try:
                page = browser.new_page()
                try:
                    page.goto("https://m.naver.com/")
                    wait_cookies(page)
                    return dict(variables, cookies=get_cookies(page))
                finally:
                    page.close()
            finally:
                browser.close()


    @task(task_id="etl_naver_cafe_search", pool="nsearch_pool")
    def etl_naver_cafe_search(ti: TaskInstance, data_interval_end: pendulum.DateTime, **kwargs) -> dict:
        from variables import in_timezone, format_date
        variables = ti.xcom_pull(task_ids="set_cookies")

        if variables["records"] and variables["cookies"]:
            return main(datetime=in_timezone(data_interval_end), **variables)
        else:
            return dict(
                params = dict(
                    channel_id = variables["channel_id"],
                    timestamp = format_date(in_timezone(data_interval_end), "YYYY-MM-DDTHH:mm:ss"),
                    max_rank = variables["max_rank"],
                    query_group = 'X',
                ),
                counts = dict(total=0),
                message = None,
                response = dict(
                    files = list(),
                    status = "skipped",
                ),
            )

    def main(
            cookies: str,
            records: list[dict],
            max_rank: int,
            slack_conn_id: str,
            channel_id: str,
            datetime: pendulum.DateTime,
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.naver.main import search_cafe_plus
        from linkmerce.utils.excel import csv2excel, json2excel, save_excel_to_tempfile
        sources = dict(search="naver_cafe_search", article="naver_cafe_article", merged="data")
        query, alias = "naver_cafe_query", "data_alias"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            conn.create_table_from_json(query, records, option="replace")
            query_map = dict(conn.fetch_all_to_csv(
                "SELECT product, '(''' || string_agg(query, ''',''') || ''')' AS keywords "
                + f"FROM {query} GROUP BY product ORDER BY product;"
                , header=False))

            search_cafe_plus(
                cookies = cookies,
                query = [row[0] for row in conn.execute(f"SELECT DISTINCT query FROM {query}").fetchall()],
                mobile = True,
                max_rank = max_rank,
                connection = conn,
                tables = sources,
                progress = False,
                return_type = "none",
            )

            # SUMMARIZE
            args = (sources["merged"], max_rank)
            headers = wb_summary_headers(max_rank)
            wb_summary = csv2excel({
                product: ([headers] + conn.fetch_all_to_csv(summarize(*args, where_clause=f"query IN {keywords}"), header=False))
                    for product, keywords in query_map.items()}, **wb_summary_styles(headers))

            # DETAIL
            conn.execute(detail(sources["merged"], alias, conn.table_exists(alias)))
            wb_details = json2excel({
                product: conn.fetch_all_to_json(f"SELECT * FROM {alias} WHERE \"검색어\" IN {keywords}")
                    for product, keywords in query_map.items()}, **wb_details_styles())

            # COUNT
            query_group = conn.execute(f"SELECT query_group FROM {query} LIMIT 1;").fetchall()[0][0]
            total = conn.execute(f"SELECT COUNT(DISTINCT query) FROM {query};").fetchall()[0][0]
            counts = dict(conn.fetch_all_to_csv(
                f"SELECT product, COUNT(DISTINCT query) FROM {query} GROUP BY product ORDER BY product;", header=False))

            return send_excel_to_slack(
                slack_conn_id = slack_conn_id,
                channel_id = channel_id,
                summary_file = save_excel_to_tempfile(wb_summary),
                details_file = save_excel_to_tempfile(wb_details),
                max_rank = max_rank,
                datetime = datetime,
                query_group = query_group,
                total = total,
                counts = counts,
            )


    def summarize(table: str, max_rank: int, where_clause: str = str()) -> str:
        return f"""
        SELECT * EXCLUDE (seq)
        FROM (
            SELECT
                query
                , MIN(seq) OVER (PARTITION BY query) AS seq
                , rank
                , (CASE
                    WHEN STARTS_WITH(cafe_name, '[') OR STARTS_WITH(cafe_name, '(')
                        THEN TRIM(cafe_name)
                    ELSE TRIM(REGEXP_REPLACE(cafe_name, '[[(].*', '')) END) AS cafe_name
                , (CASE
                    WHEN TRY_STRPTIME(write_date, '%Y-%m-%d %H:%M:%S') IS NOT NULL THEN
                        STRFTIME(TRY_STRPTIME(write_date, '%Y-%m-%d %H:%M:%S'), '%m.%d')
                    WHEN TRY_STRPTIME(write_date, '%Y-%m-%d') IS NOT NULL THEN
                        STRFTIME(TRY_STRPTIME(write_date, '%Y-%m-%d'), '%m.%d')
                    ELSE write_date END) AS write_date
                , IF(ad_id IS NULL, read_count, -1) AS read_count
            FROM (SELECT *, (ROW_NUMBER() OVER ()) AS seq FROM {table})
            {f"WHERE {where_clause}" if where_clause else str()}
        )
        PIVOT (
            FIRST(cafe_name) AS cafe_name
            , FIRST(write_date) AS write_date
            , FIRST(read_count) AS read_count
            FOR rank IN ({','.join([str(rank) for rank in range(1, max_rank+1)])})
        )
        ORDER BY seq;
        """

    def wb_summary_headers(max_rank: int) -> list[str]:
        headers = ["키워드"]
        for rank in range(1, max_rank+1):
            headers += [f"{rank}위", f"발행일({rank})", f"조회수({rank})"]
        return headers

    def wb_summary_styles(headers: list[str]) -> dict:
        column_styles, column_width, count_columns = dict(), dict(), list()
        ad_rule = dict(operator="equal", formula=[-1], fill={"color": "#F4CCCC", "fill_type": "solid"})

        for col_idx, column in enumerate(headers, start=1):
            if column.endswith('위'):
                column_styles[col_idx] = dict(fill={"color": "#FFF2CC", "fill_type": "solid"})
            elif column.startswith("조회수"):
                column_styles[col_idx] = dict(number_format="#,##0;광고")
                column_width[col_idx] = "auto"
                count_columns.append(col_idx)
            else:
                column_width[col_idx] = "auto"

        return dict(
            column_styles = column_styles,
            column_width = column_width,
            row_height = 16.5,
            conditional_formatting = [dict(ranges=count_columns, range_type="column", rule=ad_rule)],
            column_filters = dict(range=":all:"),
            truncate = True,
        )


    def detail(source_table: str, taraget_table: str, table_exists: bool) -> str:
        columns = ', '.join([f"{column_} AS \"{alias_}\"" for column_, alias_ in wb_details_alias()])
        return (
            (f"INSERT INTO {taraget_table} " if table_exists else f"CREATE TABLE {taraget_table} AS ")
            + f"SELECT {columns} "
            + f"FROM {source_table};")

    def wb_details_alias() -> list[tuple[str,str]]:
        return [
            ("query", "검색어"),
            ("rank", "순위"),
            ("cafe_id", "카페번호"),
            ("cafe_url", "카페ID"),
            ("article_id", "글번호"),
            ("ad_id", "소재ID"),
            ("cafe_name", "카페명"),
            ("title", "제목"),
            ("menu_name", "메뉴"),
            ("tags", "태그"),
            ("nick_name", "작성자"),
            ("url", "주소"),
            ("image_url", "썸네일주소"),
            ("title_length", "제목글자수"),
            ("content_length", "내용글자수"),
            ("image_count", "이미지수"),
            ("read_count", "조회수"),
            ("comment_count", "댓글수"),
            ("commenter_count", "댓글작성자수"),
            ("write_date", "작성일시"),
        ]

    def wb_details_styles() -> dict:
        number_columns = ["제목글자수", "내용글자수", "이미지수", "조회수", "댓글수", "댓글작성자수"]
        column_styles = {column: dict(number_format="#,##0") for column in number_columns}

        single_width = {"소재ID", "주소", "썸네일주소"}
        column_width = {column: "auto" for _, column in wb_details_alias() if column not in single_width}

        return dict(
            column_styles = column_styles,
            column_width = column_width,
            row_height = 16.5,
            column_filters = dict(range=":all:"),
            truncate = True,
        )


    def send_excel_to_slack(
            slack_conn_id: str,
            channel_id: str,
            summary_file: str,
            details_file: str,
            max_rank: int,
            datetime: pendulum.DateTime,
            query_group: str,
            total: int,
            counts: dict[str,int],
        ) -> dict:
        from variables import format_date
        import os
        slack_hook = SlackHook(slack_conn_id=slack_conn_id)

        message = (
            f">{format_date(datetime, 'YY년 MM월 DD일 HH시 mm분 (dd)')}\n"
            + f"*{query_group}* 그룹 - {total}개 키워드 조회 (상위 {max_rank}개 게시글)\n"
            + '\n'.join([f"• {product} : {count}개 키워드" for product, count in counts.items()]))

        ymd_hm = format_date(datetime, "YYMMDD_HHmm")
        products = '+'.join([product.replace(' ', '') for product in counts.keys()])

        try:
            response = slack_hook.send_file_v2(
                channel_id = channel_id,
                file_uploads = [{
                    "file": summary_file,
                    "filename": f"{ymd_hm}_요약_{query_group}그룹_{products}.xlsx",
                    "title": f"요약 - {', '.join(counts.keys())}",
                }, {
                    "file": details_file,
                    "filename": f"{ymd_hm}_세부_{query_group}그룹_{products}.xlsx",
                    "title": f"세부 - {', '.join(counts.keys())}",
                }],
                initial_comment = message,
            )
            return dict(
                params = dict(
                    channel_id = channel_id,
                    timestamp = format_date(datetime, 'YYYY-MM-DDTHH:mm:ss'),
                    mobile = True,
                    max_rank = max_rank,
                    query_group = query_group,
                ),
                counts = dict(
                    total = total,
                    **counts,
                ),
                message = message,
                response = parse_slack_response(
                    files = response.get("files", list()),
                    ok = response.get("ok", False)
                ),
            )
        finally:
            os.unlink(summary_file)
            os.unlink(details_file)

    def parse_slack_response(files: list[dict], ok: bool) -> dict:
        keys = ["id", "name", "size", "created", "permalink"]
        return dict(
            files = [{key: file.get(key) for key in keys} for file in files],
            status = ("success" if ok else "failed"),
        )


    read_variables() >> set_cookies() >> etl_naver_cafe_search()
