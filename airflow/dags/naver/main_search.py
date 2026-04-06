from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.providers.slack.hooks.slack import SlackHook
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "naver_main_search",
    schedule = "0,10,20,30,40,50 8,9 * * *",
    start_date = pendulum.datetime(2026, 1, 2, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:low", "naver:search", "schedule:weekdays", "time:morning", "provider:slack"],
    doc_md = dedent("""
        # 네이버 통합검색 모니터링 파이프라인

        ## 인증(Credentials)
        브라우저로 네이버 모바일 메인 페이지에 접속해 'NNB' 값이 포함된 쿠키를 획득한다.
        (동일한 도커 네트워크의 Playwright 컨테이너가 실행 중이어야 한다.)

        ## 추출(Extract)
        키워드별 네이버 모바일 통합검색 결과를 수집하고,
        검색 결과 내 카페 섹션 게시글의 본문 내용을 추가로 수집한다.

        ## 변환(Transform)
        HTML 형식의 검색 결과에서 스마트 블록, 웹문서 등 각 섹션을 파싱하여 검색 결과와 요약 테이블에 적재한다.
        추가로, JSON 형식의 카페 글 응답 본문에서 조회수를 파싱한다.

        ## 알림(Alert)
        생성된 테이블을 가지고 요약, 전체 엑셀 파일을 생성하고,
        메시지와 함께 Slack의 지정된 채널에 업로드한다.
    """).strip(),
) as dag:

    PATH = "naver.main.main_search"

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


    @task(task_id="etl_naver_main_search", pool="nsearch_pool")
    def etl_naver_main_search(ti: TaskInstance, data_interval_end: pendulum.DateTime, **kwargs) -> dict:
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
            search_cafe: dict = dict(),
            **kwargs
        ) -> dict:
        """네이버 모바일 통합검색 결과를 수집하고, 서식이 포함된 엑셀 파일로 변환하여 Slack 채널에 업로드한다."""
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.naver.main import search, cafe_article
        from linkmerce.utils.excel import csv2excel, save_excel_to_tempfile
        from pathlib import Path
        sources = {
            "query": "naver_search_query",
            "sections": "naver_search_sections",
            "summary": "naver_search_summary",
            "cafe": "naver_cafe_article",
        }
        derived = {
            "summary": "naver_contents_summary",
            "contents": "naver_contents_details",
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

            # [EXTRACT-TRANSFORM] 네이버 모바일 통합검색 결과를 수집하여 DuckDB 테이블에 적재한다.
            search(
                cookies = cookies,
                query = [row[0] for row in conn.execute(f"SELECT DISTINCT query FROM {table}")[0].fetchall()],
                mobile = True,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            # [TRANSFORM] JSON 값으로 검색 결과를 적재한 테이블을 조회해, 한글 칼럼명 적용과 함께 파이썬 객체로 변환한다.
            import json
            raw_data = [[query, json.loads(sections)]
                for query, sections in conn.sql("SELECT * FROM naver_search_sections")[0].fetchall()]
            contents = select_contents(raw_data)

            # [SAVE] 네이버 모바일 통합검색 결과를 로컬에 JSON 파일로 저장한다.
            if save_to:
                path = Path(save_to, str(datetime.year), str(datetime.month), str(datetime.day))
                path.mkdir(parents=True, exist_ok=True)
                with open(path / (datetime.format("HHmm_") + query_group + ".json"), 'w', encoding="utf-8") as file:
                    json.dump(raw_data, file, ensure_ascii=False, separators=(',', ':'), default=str)

            # [EXTRACT-TRANSFORM] 검색 결과 내 네이버 카페 섹션 게시글의 본문 내용을 수집하여 DuckDB 테이블에 적재한다.
            if query_group.startswith("카페") and isinstance(search_cafe.get("max_rank"), int):
                max_cafe_rank = max(search_cafe["max_rank"], 1)
                urls = list({row["주소"] for row in contents if (row["구분"] == "카페") and (row["순위"] <= max_cafe_rank)})
            else:
                urls = list()

            cafe_article(
                cookies = cookies,
                url = urls,
                domain = 'm',
                connection = conn,
                progress = False,
                return_type = "none",
            )
            conn.fetch_all_to_json(f"SELECT * FROM naver_cafe_article")

            # [SAVE] 네이버 카페 게시글의 본문 내용을 로컬에 JSON 파일로 저장한다.
            if search_cafe.get("save_to"):
                cafe_path = Path(search_cafe["save_to"], str(datetime.year), str(datetime.month), str(datetime.day))
                cafe_path.mkdir(parents=True, exist_ok=True)
                save_path = cafe_path / (datetime.format("HHmm") + "_article_" + query_group + ".parquet")
                conn.fetch_all_to_parquet(f"SELECT * FROM naver_cafe_article", save_to=str(save_path))
            cafe_articles = conn.fetch_all_to_json(f"SELECT * FROM naver_cafe_article")

            # [SUMMARY] 검색 결과 데이터를 요약하여 서식이 포함된 `openpyxl.Workbook` 객체로 변환한다.
            table = derived["summary"]
            summary = summarize_contents(raw_data)
            if cafe_articles:
                summary = enrich_summary(summary, cafe_articles)
            conn.create_table_from_json(table, summary, option="replace", temp=True)

            max_rank = min(max_rank, conn.sql(f"SELECT IFNULL(MAX(rank), 0) FROM {table}")[0].fetchall()[0][0])
            def _select_query(keywords: str) -> str:
                return wb_summary_query("naver_search_summary", table, max_rank, keywords)

            headers = wb_summary_headers(max_rank)
            wb_summary = csv2excel({
                product: wb_summary_concat(headers, conn.fetch_all_to_csv(_select_query(keywords), header=False))
                    for product, keywords in query_map.items()}, **wb_summary_styles(headers))

            # [CONTENTS] 검색 결과에 카페 게시글 본문 내용을 병합하여, 서식이 포함된 `openpyxl.Workbook` 객체로 변환한다.
            table = derived["contents"]
            if cafe_articles:
                contents = enrich_contents(contents, cafe_articles)
            conn.create_table_from_json(table, contents, option="replace", temp=True)

            wb_contents = csv2excel({
                product: conn.fetch_all_to_csv(f"SELECT * FROM {table} WHERE \"검색어\" IN {keywords}", header=True)
                    for product, keywords in query_map.items()}, **wb_contents_styles())

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
                contents_file = save_excel_to_tempfile(wb_contents),
                max_rank = max_rank,
                datetime = datetime,
                query_group = query_group,
                total = total,
                counts = counts,
            )

    def _get_site_type_from_url(url: str | None) -> str | None:
        """검색 결과인 웹사이트의 주소로부터 사이트 유형을 특정한다."""
        import re
        if not url:
            return None
        elif url.startswith("https://ader.naver.com/"):
            return "광고"
        elif re.match(r"^https://(m\.){,1}blog\.naver\.com/", url):
            return "블로그"
        elif re.match(r"^https://(m\.){,1}cafe\.naver\.com/", url):
            return "카페"
        elif re.match(r"^https://(m\.){,1}in\.naver\.com/", url):
            return "인플루언서"
        elif re.match(r"^https://(m\.){,1}youtube\.com/", url):
            return "유튜브"
        else:
            return "기타"

    def _get_site_labels(mode: str = "encode") -> dict:
        """사이트 유형을 인덱스로, 또는 반대로 인덱스를 사이트 유형으로 변환한다."""
        enum = enumerate(["카페", "광고", "블로그", "인플루언서", "유튜브", "기타"], start=1)
        if mode == "encode":
            return {label: (category * -1) for category, label in enum}
        else:
            return {(category * -1): label for category, label in enum}

    def _get_cafe_ids_from_url(url: str) -> tuple[str, str]:
        """카페 URL에서 카페 URL과 게시물 ID를 추출하여 튜플로 반환한다."""
        from linkmerce.utils.regex import regexp_groups
        return regexp_groups(r"/([^/]+)/(\d+)$", url.split('?')[0], indices=[0, 1]) if url else (None, None)


    def summarize_contents(raw_data: list[tuple[str, list[list[dict]]]]) -> list[dict]:
        """중첩된 구조의 검색 결과 데이터를 `list[dict]` 형식으로 요약한다."""
        from linkmerce.utils.nested import hier_get
        contents, labels = list(), _get_site_labels(mode="encode")
        for query, sections in raw_data:
            for seq, section in enumerate(sections, start=1):
                heading = hier_get(section, [0, "section"])
                if heading in ["스마트블록", "웹문서"]:
                    for rank, content in enumerate(section, start=1):
                        contents.append(
        {
            "query": query,
            "seq": seq,
            "url": content.get("url"),
            "subject": (content.get("subject") if heading == "스마트블록" else str()),
            "rank": rank,
            "profile": (content.get("profile_name") or '-'),
            "read_count": labels.get(_get_site_type_from_url(content.get("url"))),
            "created_date": (content.get("created_date") or '-'),
        })
        return contents

    def enrich_summary(data: list[dict], cafe_articles: list[dict]) -> list[dict]:
        """검색 결과 요약 데이터에 카페 글 조회수를 추가한다."""
        counts = {(article["cafe_url"], str(article["article_id"])): article["read_count"] for article in cafe_articles}

        def _update_row(row: dict) -> dict:
            cafe_url, article_id = _get_cafe_ids_from_url(row["url"])
            read_count = counts.get((cafe_url, article_id))
            if isinstance(read_count, (float, int)):
                row["read_count"] = int(read_count)
            return row

        CAFE = -1
        return [_update_row(row) if row["read_count"] == CAFE else row for row in data]


    def wb_summary_headers(max_rank: int) -> list[str]:
        """최대 검색 항목 수(`max_rank`)만큼 동적으로 요약 엑셀 헤더를 생성한다."""
        headers = ["키워드", "순번", "영역", "주제", "항목수"]
        for rank in range(1, max_rank+1):
            headers += [f"{rank}위", f"생성일({rank})", f"조회수({rank})"]
        return headers

    def wb_summary_query(section_table: str, contents_table: str, max_rank: int, keywords: str) -> str:
        """검색 결과 요약 테이블에 순위, 생성일, 조회수를 추가하는 DuckDB 쿼리 문자열을 반환한다."""
        return dedent(f"""
            SELECT
                S.* EXCLUDE (rn)
                , C.* EXCLUDE (query, seq, subject)
            FROM (
                SELECT
                    query
                    , seq
                    , section
                    , subject
                    , item_count
                    , (ROW_NUMBER() OVER ()) AS rn
                FROM {section_table}
            ) AS S
            LEFT JOIN (
            SELECT *
            FROM (
                SELECT
                    query
                    , seq
                    , subject
                    , rank
                    , (CASE
                        WHEN STARTS_WITH(profile, '[') OR STARTS_WITH(profile, '(')
                            THEN TRIM(profile)
                        ELSE TRIM(REGEXP_REPLACE(profile, '[[(].*', '')) END) AS profile
                    , (CASE
                        WHEN created_date IS NULL THEN NULL
                        WHEN TRY_STRPTIME(created_date, '%Y.%m.%d.') IS NOT NULL THEN
                            STRFTIME(TRY_STRPTIME(created_date, '%Y.%m.%d.'), '%m.%d')
                        ELSE created_date END) AS created_date
                    , read_count
                FROM {contents_table}
                WHERE query IN {keywords}
            )
            PIVOT (
                FIRST(profile) AS profile
                , FIRST(created_date) AS created_date
                , FIRST(read_count) AS read_count
                FOR rank IN ({','.join([str(rank) for rank in range(1, max_rank+1)])})
            )
            ORDER BY query, seq, subject
            ) AS C
            ON (S.query = C.query) AND (S.seq = C.seq) AND (S.subject = C.subject)
            WHERE S.query IN {keywords}
            ORDER BY S.rn, S.seq
            """).strip()

    def wb_summary_concat(header: list[str], summary: list[tuple]) -> list[tuple]:
        """카페를 제외한 검색 결과 항목은 조회수를 생략하고 사이트 유형 라벨로 표시한다.
        타입에 민감한 DuckDB 쿼리로는 처리할 수 없으므로 파이썬 객체를 후처리한다."""
        labels = _get_site_labels(mode="decode")

        def _decode(read_count: int) -> int | str:
            return labels.get(read_count) if isinstance(read_count, int) and (read_count < 0) else read_count

        return [header] + [
            tuple(_decode(value) if column.startswith("조회수") else value
                for column, value in zip(header, row)) for row in summary]

    def wb_summary_styles(headers: list[str]) -> dict:
        """`csv2excel` 함수에 전달할 검색 결과 요약 엑셀 파일 서식 설정을 생성한다."""
        from linkmerce.utils.excel import get_column_letter
        column_styles, column_width, count_columns = dict(), dict(), list()
        ad_rule = {"operator": "equal", "formula": ['"광고"'], "fill": {"color": "#F4CCCC", "fill_type": "solid"}}
        cafe_rule = {"operator": "equal", "formula": ['"카페"'], "fill": {"color": "#03C75A", "fill_type": "solid"}}
        count_rule = {"operator": "formula", "formula": list(), "fill": {"color": "#03C75A", "fill_type": "solid"}}
        na_rule = {"operator": "formula", "formula": ["ISBLANK(F2)"], "fill": {"color": "#BFBFBF", "fill_type": "solid"}}

        for col_idx, column in enumerate(headers, start=1):
            if column.endswith('위'):
                column_styles[col_idx] = {"fill": {"color": "#FFF2CC", "fill_type": "solid"}}
            elif column.startswith("생성일"):
                column_styles[col_idx] = {"alignment": {"horizontal": "center"}}
            elif column.startswith("조회수"):
                column_styles[col_idx] = {"alignment": {"horizontal": "center"}, "number_format": "#,##0;광고;블로그"}
                count_columns.append(col_idx)
            elif column == "영역":
                column_width[col_idx] = 12
            elif column == "항목수":
                column_styles[col_idx] = {"alignment": {"horizontal": "center"}}
            elif column != "순번":
                column_width[col_idx] = ":fit:"

        formatting_rules = list()
        if count_columns:
            count_rule["formula"] = ["=ISNUMBER({})".format(get_column_letter(min(count_columns))+'2')]
            formatting_rules = [
                {"ranges": count_columns, "range_type": "column", "rule": ad_rule},
                {"ranges": count_columns, "range_type": "column", "rule": cafe_rule},
                {"ranges": count_columns, "range_type": "column", "rule": count_rule},
                {"ranges": f"F2:{get_column_letter(len(headers))}", "range_type": "range", "rule": na_rule},
            ]

        return {
            "header_styles": "yellow",
            "column_styles": column_styles,
            "column_width": column_width,
            "row_height": 16.5,
            "conditional_formatting": formatting_rules,
            "column_filters": {
                "range": ":all:",
                "filters": [("영역", [{"filter_type": "value", "values": ["스마트블록", "웹문서"]}])]
            },
            "filter_mode": "xml",
            "truncate": True,
            "freeze_panes": "E2",
        }


    def select_contents(raw_data: list[tuple[str, list[list[dict]]]]) -> list[dict]:
        """중첩된 구조의 검색 결과 데이터에서 스마트블록, 웹문서 항목만 선택해 `list[dict]` 형식으로 반환한다."""
        from linkmerce.utils.nested import hier_get
        contents = list()
        for query, sections in raw_data:
            for seq, section in enumerate(sections, start=1):
                heading = hier_get(section, [0, "section"])
                if heading in ["스마트블록", "웹문서"]:
                    for rank, content in enumerate(section, start=1):
                        contents.append(
        {
            "검색어": query,
            "순번": seq,
            "영역": heading,
            "순위": rank,
            "주제": content.get("subject"),
            "구분": _get_site_type_from_url(content.get("url")),
            "제목": content.get("title"),
            "내용": content.get("description"),
            "프로필": content.get("profile_name"),
            "주소": content.get("url") or str(),
            "썸네일주소": content.get("image_url"),
            "소재ID": content.get("ad_id") or str(),
            "제목글자수": len(content.get("title") or str()),
            "내용글자수": None,
            "이미지수": content.get("image_count"),
            "조회수": None,
            "댓글수": None,
            "댓글작성자수": None,
            "생성일": content.get("created_date"),
        })
        return contents

    def zip_cafe_articles(cafe_articles: list[dict]) -> dict[tuple[str, str], dict]:
        """검색 결과 데이터에서 반복적으로 카페 글을 조회하기 위해 카페 글 목록을 딕셔너리로 변환한다."""
        return {
            (article["cafe_url"], str(article["article_id"])): {
                "프로필": article["cafe_name"],
                "제목": article["title"],
                "제목글자수": article["title_length"],
                "내용글자수": article["content_length"],
                "이미지수": article["image_count"],
                "조회수": article["read_count"],
                "댓글수": article["comment_count"],
                "댓글작성자수": article["commenter_count"],
                "생성일": article["write_dt"],
            }
            for article in cafe_articles
        }

    def enrich_contents(data: list[dict], cafe_articles: list[dict]) -> list[dict]:
        """검색 결과 데이터를 순회하면서 카페 글이고 카페 글 번호가 일치하는 항목이 있다면 카페 글 본문 내용을 추가한다."""
        __m = zip_cafe_articles(cafe_articles)

        def _update_row(row: dict) -> dict:
            import datetime as dt
            cafe_url, article_id = _get_cafe_ids_from_url(row["주소"])
            for key, value in (__m.get((cafe_url, article_id)) or dict()).items():
                if (key == "생성일") and isinstance(value, dt.datetime):
                    row[key] = value.strftime("%Y.%m.%d.")
                elif not row.get(key):
                    row[key] = value
            return row

        return [(_update_row(row) if (row["구분"] == "카페") and row["주소"] else row) for row in data]


    def wb_contents_headers() -> list[str]:
        """전체 검색 결과 헤더 목록을 반환한다."""
        return [
            "검색어", "순번", "영역", "순위", "주제", "구분", "제목", "내용", "프로필", "주소", "썸네일주소",
            "제목글자수", "내용글자수", "이미지수", "조회수", "댓글수", "댓글작성자수", "생성일"]

    def wb_contents_styles() -> dict:
        """`csv2excel` 함수에 전달할 전체 검색 결과 엑셀 파일 서식 설정을 생성한다."""
        number_columns = ["제목글자수", "내용글자수", "이미지수", "조회수", "댓글수", "댓글작성자수"]
        column_styles = {column: {"number_format": "#,##0"} for column in number_columns}

        single_width = {"순번", "순위", "주소", "썸네일주소", "소재ID"}
        column_width = {column: ":fit:" for column in wb_contents_headers() if column not in single_width}

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
            contents_file: str,
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
            + f"*{query_group}* 그룹 - {total}개 키워드 검색\n"
            + '\n'.join([f"• {product} : {count}개 키워드" for product, count in counts.items()]))

        ymd_hm = format_date(datetime, "YYMMDD_HHmm")
        products = '+'.join([product.replace(' ', '') for product in counts.keys()])

        try:
            response = slack_hook.send_file_v2(
                channel_id = channel_id,
                file_uploads = [{
                    "file": summary_file,
                    "filename": f"{ymd_hm}_통합_요약_{query_group}그룹_{products}.xlsx",
                    "title": f"요약 - {', '.join(counts.keys())}",
                }, {
                    "file": contents_file,
                    "filename": f"{ymd_hm}_통합_세부_{query_group}그룹_{products}.xlsx",
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
            os.unlink(contents_file)

    def parse_slack_response(files: list[dict], ok: bool) -> dict:
        """Slack 업로드 응답 결과를 Airflow 로그에 보여주기 위한 용도로 파싱한다."""
        keys = ["id", "name", "size", "created", "permalink"]
        return {
            "files": [{key: file.get(key) for key in keys} for file in files],
            "status": ("success" if ok else "failed"),
        }


    read_configs() >> branch_etl_trigger >> set_cookies() >> etl_naver_main_search()
