from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.providers.slack.hooks.slack import SlackHook
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "naver_cafe_search",
    schedule = "0,10,20,30,40,50 8,9 * * *",
    start_date = pendulum.datetime(2025, 12, 12, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:low", "naver:cafe", "schedule:weekdays", "time:morning", "provider:slack"],
    doc_md = dedent("""
        # 네이버 카페 검색 모니터링 파이프라인

        ## 인증(Credentials)
        브라우저로 네이버 모바일 메인 페이지에 접속해 'NNB' 값이 포함된 쿠키를 획득한다.
        (동일한 도커 네트워크의 Playwright 컨테이너가 실행 중이어야 한다.)

        ## 추출(Extract)
        키워드별 네이버 모바일 카페 탭 검색 결과를 수집하고,
        검색 결과로 보여지는 상위 게시글의 본문 내용을 추가로 수집한다.

        ## 변환(Transform)
        검색 결과(HTML), 게시글 내용(JSON) 응답 본문을 파싱하여 검색 결과와 게시글 테이블에 적재한다.
        추가로, 검색 결과 테이블과 게시글 테이블을 병합한 통합 테이블을 생성한다.

        ## 알림(Alert)
        생성된 테이블을 가지고 요약, 전체 엑셀 파일을 생성하고,
        메시지와 함께 Slack의 지정된 채널에 업로드한다.
    """).strip(),
) as dag:

    PATH = "naver.main.cafe_search"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read
        return read(PATH, sheets=True)


    def branch_condition(ti: TaskInstance, **kwargs) -> str | None:
        """검색어가 있으면 다음 Task로 넘어가고, 검색어가 없으면 Skip한다."""
        configs = ti.xcom_pull(task_ids="read_configs")
        if configs["records"]:
            return "set_cookies"
        else:
            return None

    branch_etl_trigger = BranchPythonOperator(
        task_id = "branch_etl_trigger",
        python_callable = branch_condition,
    )


    @task(task_id="set_cookies", retries=3, retry_delay=timedelta(minutes=1), pool="nsearch_pool")
    def set_cookies(ti: TaskInstance, **kwargs) -> dict:
        """Playwright 브라우저로 네이버 모바일 메인 페이지에 접속해 `NNB` 값이 포함된 쿠키를 획득한다."""
        from playwright.sync_api import sync_playwright, Page
        import time
        configs = ti.xcom_pull(task_ids="read_configs")

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
            ws_endpoint = "ws://playwright:3000/" # 실행 중인 Playwright 컨테이너에 연결
            browser = playwright.chromium.connect(ws_endpoint)
            try:
                page = browser.new_page()
                try:
                    page.goto("https://m.naver.com/")
                    wait_cookies(page)
                    return dict(configs, cookies=get_cookies(page))
                finally:
                    page.close()
            finally:
                browser.close()


    @task(task_id="etl_naver_cafe_search", pool="nsearch_pool")
    def etl_naver_cafe_search(ti: TaskInstance, data_interval_end: pendulum.DateTime, **kwargs) -> dict:
        from airflow_utils import in_timezone, format_date
        configs = ti.xcom_pull(task_ids="set_cookies")

        if configs["records"] and configs["cookies"]:
            return main(datetime=in_timezone(data_interval_end), **configs)
        else:
            return {
                "params": {
                    "channel_id": configs["channel_id"],
                    "timestamp": format_date(in_timezone(data_interval_end), "YYYY-MM-DDTHH:mm:ss"),
                    "mobile": True,
                    "max_rank": configs["max_rank"],
                    "query_group": 'X',
                },
                "counts": {
                    "total": 0,
                },
                "message": None,
                "response": {
                    "files": list(),
                    "status": "skipped",
                },
            }

    def main(
            cookies: str,
            records: list[dict],
            slack_conn_id: str,
            channel_id: str,
            datetime: pendulum.DateTime,
            max_rank: int | None = None,
            save_to: str = str(),
            **kwargs
        ) -> dict:
        """네이버 모바일 카페 탭 검색 결과를 수집하고, 서식이 포함된 엑셀 파일로 변환하여 Slack 채널에 업로드한다."""
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.naver.main import search_cafe_plus
        from linkmerce.utils.excel import csv2excel, save_excel_to_tempfile
        sources = {
            "query": "naver_cafe_query",
            "cafe": "naver_cafe_search",
            "article": "naver_cafe_article",
            "merged": "naver_cafe_result",
        }
        derived = {
            "details": "naver_cafe_details",
        }

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            # [CONFIG] Slack 메시지를 구성하기 위해 쿼리를 상품별 키워드 목록으로 집계한다.
            table = sources["query"]
            conn.create_table_from_json(table, records, option="replace", temp=True)
            query_group = conn.execute(f"SELECT query_group FROM {table} LIMIT 1")[0].fetchall()[0][0] or '0'
            query_map = dict(conn.fetch_all_to_csv(
                "SELECT product, '(''' || string_agg(query, ''',''') || ''')' AS keywords "
                + f"FROM {table} GROUP BY product ORDER BY product"
                , header=False))

            # [EXTRACT-TRANSFORM] 네이버 모바일 카페 탭 검색 결과를 수집하여 DuckDB 테이블에 적재한다.
            search_cafe_plus(
                cookies = cookies,
                query = [row[0] for row in conn.execute(f"SELECT DISTINCT query FROM {table}")[0].fetchall()],
                mobile = True,
                max_rank = max_rank,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            # [SAVE] 네이버 모바일 카페 탭 검색 결과를 로컬에 Parquet 파일로 저장한다.
            if save_to:
                from pathlib import Path
                path = Path(save_to, str(datetime.year), str(datetime.month), str(datetime.day))
                path.mkdir(parents=True, exist_ok=True)
                for table_key in ["search", "article"]:
                    table = sources[table_key]
                    save_path = path / (datetime.format("HHmm") + f"_{table_key}_" + query_group + ".parquet")
                    conn.fetch_all_to_parquet(f"SELECT * FROM {table}", save_to=str(save_path))

            # [SUMMARY] 검색 결과 요약 데이터를 서식이 포함된 `openpyxl.Workbook` 객체로 변환한다.
            headers = wb_summary_headers(max_rank)
            def _select_query(keywords: str) -> str:
                return wb_summary_query(sources["merged"], max_rank, keywords)

            wb_summary = csv2excel({
                product: wb_summary_concat(headers, conn.fetch_all_to_csv(_select_query(keywords), header=False))
                    for product, keywords in query_map.items()}, **wb_summary_styles(headers))

            # [DETAILS] 전체 검색 결과 데이터를 서식이 포함된 `openpyxl.Workbook` 객체로 변환한다.
            table = derived["details"]
            conn.execute(wb_details_query(sources["merged"], table))
            wb_details = csv2excel({
                product: conn.fetch_all_to_csv(f"SELECT * FROM {table} WHERE \"검색어\" IN {keywords}", header=True)
                    for product, keywords in query_map.items()}, **wb_details_styles())

            # [COUNT] Slack 메시지를 구성하기 위해 상품별 키워드 수를 카운팅한다.
            table = sources["query"]
            total = conn.execute(f"SELECT COUNT(DISTINCT query) FROM {table}")[0].fetchall()[0][0]
            counts = dict(conn.fetch_all_to_csv(
                f"SELECT product, COUNT(DISTINCT query) FROM {table} GROUP BY product ORDER BY product", header=False))

            # [ALERT] Slack 채널에 메시지와 함께 엑셀 파일을 업로드한다.
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


    def wb_summary_headers(max_rank: int) -> list[str]:
        """최대 검색 항목 수(`max_rank`)만큼 동적으로 요약 엑셀 헤더를 생성한다."""
        headers = ["검색어"]
        for rank in range(1, max_rank+1):
            headers += [f"{rank}위", f"발행일({rank})", f"조회수({rank})"]
        return headers

    def wb_summary_query(table: str, max_rank: int, keywords: str) -> str:
        """전체 검색 결과 데이터를 요약하는 DuckDB 쿼리 문자열을 반환한다."""
        return dedent(f"""
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
                WHERE query IN {keywords}
            )
            PIVOT (
                FIRST(cafe_name) AS cafe_name
                , FIRST(write_date) AS write_date
                , FIRST(read_count) AS read_count
                FOR rank IN ({','.join([str(rank) for rank in range(1, max_rank+1)])})
            )
            ORDER BY seq
            """).strip()

    def wb_summary_concat(header: list[str], summary: list[tuple]) -> list[tuple]:
        """소재 ID(`ad_id`)가 있는 항목은 정수 타입의 조회수 열에 "광고" 텍스트 값을 넣어야 한다.   
        타입에 민감한 DuckDB 쿼리로는 처리할 수 없으므로 파이썬 객체를 후처리한다."""
        def _decode(read_count: int) -> int | str:
            return "광고" if read_count == -1 else read_count

        return [header] + [
            tuple(_decode(value) if column.startswith("조회수") else value
                for column, value in zip(header, row)) for row in summary]

    def wb_summary_styles(headers: list[str]) -> dict:
        """`csv2excel` 함수에 전달할 검색 결과 요약 엑셀 파일 서식 설정을 생성한다."""
        column_styles, column_width, count_columns = dict(), dict(), list()
        ad_rule = {"operator": "equal", "formula": ['"광고"'], "fill": {"color": "#F4CCCC", "fill_type": "solid"}}

        for col_idx, column in enumerate(headers, start=1):
            if column.endswith('위'):
                column_styles[col_idx] = {"fill": {"color": "#FFF2CC", "fill_type": "solid"}}
            elif column.startswith("조회수"):
                column_styles[col_idx] = {"number_format": "#,##0;광고"}
                column_width[col_idx] = ":fit:"
                count_columns.append(col_idx)
            else:
                column_width[col_idx] = ":fit:"

        return {
            "header_styles": "yellow",
            "column_styles": column_styles,
            "column_width": column_width,
            "row_height": 16.5,
            "conditional_formatting": [{"ranges": count_columns, "range_type": "column", "rule": ad_rule}],
            "column_filters": {"range": ":all:"},
            "truncate": True,
        }


    def wb_details_query(source_table: str, taraget_table: str) -> str:
        """전체 검색 결과 테이블을 엑셀로 변환하기 전에 칼럼명을 한글로 변경한다."""
        columns = ', '.join([f"{column_} AS \"{alias_}\"" for column_, alias_ in wb_details_alias()])
        return dedent(f"""
            CREATE TABLE {taraget_table} AS
            SELECT {columns}
            FROM {source_table}
            """).strip()

    def wb_details_alias() -> list[tuple[str, str]]:
        """테이블을 엑셀로 변환할 때 원본 칼럼명과 한글 칼럼명을 매핑한다."""
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
        """`csv2excel` 함수에 전달할 전체 검색 결과 엑셀 파일 서식 설정을 생성한다."""
        number_columns = ["제목글자수", "내용글자수", "이미지수", "조회수", "댓글수", "댓글작성자수"]
        column_styles = {column: {"number_format": "#,##0"} for column in number_columns}

        single_width = {"소재ID", "주소", "썸네일주소"}
        column_width = {column: ":fit:" for _, column in wb_details_alias() if column not in single_width}

        return {
            "header_styles": "yellow",
            "column_styles": column_styles,
            "column_width": column_width,
            "row_height": 16.5,
            "column_filters": {"range": ":all:"},
            "truncate": True,
        }


    def send_excel_to_slack(
            slack_conn_id: str,
            channel_id: str,
            summary_file: str,
            details_file: str,
            max_rank: int,
            datetime: pendulum.DateTime,
            query_group: str,
            total: int,
            counts: dict[str, int],
        ) -> dict:
        """Slack에 보낼 메시지를 구성하고, 엑셀 파일과 함께 지정된 채널에 업로드한다."""
        from airflow_utils import format_date
        import os
        slack_hook = SlackHook(slack_conn_id=slack_conn_id)

        message = (
            f">{format_date(datetime, 'YY년 MM월 DD일 HH시 mm분 (dd)')}\n"
            + f"*{query_group}* 그룹 - {total}개 키워드 검색 (상위 {max_rank}개 게시글)\n"
            + '\n'.join([f"• {product} : {count}개 키워드" for product, count in counts.items()]))

        ymd_hm = format_date(datetime, "YYMMDD_HHmm")
        products = '+'.join([product.replace(' ', '') for product in counts.keys()])

        try:
            response = slack_hook.send_file_v2(
                channel_id = channel_id,
                file_uploads = [{
                    "file": summary_file,
                    "filename": f"{ymd_hm}_카페_요약_{query_group}그룹_{products}.xlsx",
                    "title": f"요약 - {', '.join(counts.keys())}",
                }, {
                    "file": details_file,
                    "filename": f"{ymd_hm}_카페_세부_{query_group}그룹_{products}.xlsx",
                    "title": f"세부 - {', '.join(counts.keys())}",
                }],
                initial_comment = message,
            )
            return {
                "params": {
                    "channel_id": channel_id,
                    "timestamp": format_date(datetime, 'YYYY-MM-DDTHH:mm:ss'),
                    "mobile": True,
                    "max_rank": max_rank,
                    "query_group": query_group,
                },
                "counts": {
                    "total": total,
                    **counts,
                },
                "message": message,
                "response": parse_slack_response(
                    files = response.get("files", list()),
                    ok = response.get("ok", False)
                ),
            }
        finally:
            os.unlink(summary_file)
            os.unlink(details_file)

    def parse_slack_response(files: list[dict], ok: bool) -> dict:
        """Slack 업로드 응답 결과를 Airflow 로그에 보여주기 위한 용도로 파싱한다."""
        keys = ["id", "name", "size", "created", "permalink"]
        return {
            "files": [{key: file.get(key) for key in keys} for file in files],
            "status": ("success" if ok else "failed"),
        }


    read_configs() >> branch_etl_trigger >> set_cookies() >> etl_naver_cafe_search()
