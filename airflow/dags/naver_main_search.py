from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.providers.slack.hooks.slack import SlackHook
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "naver_main_search",
    schedule = "0,10,20,30,40,50 8,9 * * *",
    start_date = pendulum.datetime(2026, 1, 2, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "naver:search", "schedule:weekdays", "time:morning", "provider:slack"],
) as dag:

    PATH = ["naver", "main", "naver_main_search"]

    @task(task_id="read_variables", retries=3, retry_delay=timedelta(minutes=1))
    def read_variables() -> dict:
        from variables import read
        return read(PATH, sheets=True)


    def branch_condition(ti: TaskInstance, **kwargs) -> str | None:
        variables = ti.xcom_pull(task_ids="read_variables")
        if variables["records"]:
            return "set_cookies"
        else:
            return None

    branch_etl_trigger = BranchPythonOperator(
        task_id = "branch_etl_trigger",
        python_callable = branch_condition,
    )


    @task(task_id="set_cookies", retries=3, retry_delay=timedelta(minutes=1), pool="nsearch_pool")
    def set_cookies(ti: TaskInstance, **kwargs) -> dict:
        from playwright.sync_api import sync_playwright, Page
        import time
        variables = ti.xcom_pull(task_ids="read_variables")

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


    @task(task_id="etl_naver_main_search", pool="nsearch_pool")
    def etl_naver_main_search(ti: TaskInstance, data_interval_end: pendulum.DateTime, **kwargs) -> dict:
        from variables import in_timezone, format_date
        variables = ti.xcom_pull(task_ids="set_cookies")

        if variables["records"] and variables["cookies"]:
            return main(datetime=in_timezone(data_interval_end), **variables)
        else:
            return dict(
                params = dict(
                    channel_id = variables["channel_id"],
                    timestamp = format_date(in_timezone(data_interval_end), "YYYY-MM-DDTHH:mm:ss"),
                    mobile = True,
                    max_rank = variables["max_rank"],
                    query_group = 'X',
                ),
                counts = dict(
                    total = 0,
                ),
                message = None,
                response = dict(
                    files = list(),
                    status = "skipped",
                ),
            )

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
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.naver.main import search, cafe_article
        from linkmerce.utils.excel import csv2excel, save_excel_to_tempfile
        from pathlib import Path
        sources = dict(sections="naver_search_sections", summary="naver_search_summary")
        query_table, cafe_table = "naver_search_query", "naver_cafe_article"

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            conn.create_table_from_json(query_table, records, option="replace", temp=True)
            query_group = conn.execute(f"SELECT query_group FROM {query_table} LIMIT 1;").fetchall()[0][0] or '0'
            query_map = dict(conn.fetch_all_to_csv(
                "SELECT product, '(''' || string_agg(query, ''',''') || ''')' AS keywords "
                + f"FROM {query_table} GROUP BY product ORDER BY product;"
                , header=False))

            search(
                cookies = cookies,
                query = [row[0] for row in conn.execute(f"SELECT DISTINCT query FROM {query_table}").fetchall()],
                mobile = True,
                connection = conn,
                tables = sources,
                progress = False,
                return_type = "none",
            )

            # RAW DATA
            import json
            raw_data = [[query, json.loads(sections)]
                for query, sections in conn.sql("SELECT * FROM {}".format(sources["sections"])).fetchall()]
            contents = select_contents(raw_data)

            # SAVE LOCAL
            if save_to:
                path = Path(save_to, str(datetime.year), str(datetime.month), str(datetime.day))
                path.mkdir(parents=True, exist_ok=True)
                with open(path / (datetime.format("HHmm_") + query_group + ".json"), 'w', encoding="utf-8") as file:
                    json.dump(raw_data, file, ensure_ascii=False, separators=(',', ':'), default=str)

            # SEARCH CAFE
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
                tables = {"default": cafe_table},
                progress = False,
                return_type = "none",
            )
            conn.fetch_all_to_json(f"SELECT * FROM {cafe_table};")

            if search_cafe.get("save_to"):
                cafe_path = Path(search_cafe["save_to"], str(datetime.year), str(datetime.month), str(datetime.day))
                cafe_path.mkdir(parents=True, exist_ok=True)
                save_path = cafe_path / (datetime.format("HHmm") + "_article_" + query_group + ".parquet")
                conn.fetch_all_to_parquet(f"SELECT * FROM {cafe_table};", save_to=str(save_path))
            cafe_articles = conn.fetch_all_to_json(f"SELECT * FROM {cafe_table};")

            # SUMMARY
            staging_table = "contents_summary"
            summary = summarize_contents(raw_data)
            if cafe_articles:
                summary = enrich_summary(summary, cafe_articles)
            conn.create_table_from_json(staging_table, summary, option="replace", temp=True)

            max_rank = min(max_rank, conn.sql(f"SELECT IFNULL(MAX(rank), 0) FROM {staging_table};").fetchall()[0][0])
            def _select_query(keywords: str) -> str:
                return wb_summary_query(sources["summary"], staging_table, max_rank, keywords)

            headers = wb_summary_headers(max_rank)
            wb_summary = csv2excel({
                product: wb_summary_concat(headers, conn.fetch_all_to_csv(_select_query(keywords), header=False))
                    for product, keywords in query_map.items()}, **wb_summary_styles(headers))

            # CONTENTS
            staging_table = "contents_data"
            if cafe_articles:
                contents = enrich_contents(contents, cafe_articles)
            conn.create_table_from_json(staging_table, contents, option="replace", temp=True)

            wb_contents = csv2excel({
                product: conn.fetch_all_to_csv(f"SELECT * FROM {staging_table} WHERE \"검색어\" IN {keywords};", header=True)
                    for product, keywords in query_map.items()}, **wb_contents_styles())

            # COUNT
            total = conn.execute(f"SELECT COUNT(DISTINCT query) FROM {query_table};").fetchall()[0][0]
            counts = dict(conn.fetch_all_to_csv(
                f"SELECT product, COUNT(DISTINCT query) FROM {query_table} GROUP BY product ORDER BY product;", header=False))

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
        enum = enumerate(["카페", "광고", "블로그", "인플루언서", "유튜브", "기타"])
        if mode == "encode":
            return {label: (category * -1) for category, label in enum}
        else:
            return {(category * -1): label for category, label in enum}

    def _get_cafe_ids_from_url(url: str) -> tuple[str,str]:
        from linkmerce.utils.regex import regexp_groups
        return regexp_groups(r"/([^/]+)/(\d+)$", url.split('?')[0], indices=[0,1]) if url else (None, None)


    def summarize_contents(raw_data: list[tuple[str, list[list[dict]]]]) -> list[dict]:
        from linkmerce.utils.map import hier_get
        contents, labels = list(), _get_site_labels(mode="encode")
        for query, sections in raw_data:
            for seq, section in enumerate(sections, start=1):
                heading = hier_get(section, [0,"section"])
                if heading in {"스마트블록","웹문서"}:
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
        counts = {(article["cafe_url"], str(article["article_id"])): article["read_count"] for article in cafe_articles}

        def _update_row(row: dict) -> dict:
            cafe_url, article_id = _get_cafe_ids_from_url(row["url"])
            read_count = counts.get((cafe_url, article_id)) or 0
            if isinstance(read_count, (float,int)):
                row["read_count"] = read_count
            return row

        return [_update_row(row) if row["read_count"] == 0 else row for row in data]


    def wb_summary_headers(max_rank: int) -> list[str]:
        headers = ["키워드", "순번", "영역", "주제", "항목수"]
        for rank in range(1, max_rank+1):
            headers += [f"{rank}위", f"생성일({rank})", f"조회수({rank})"]
        return headers

    def wb_summary_query(section_table: str, contents_table: str, max_rank: int, keywords: str) -> str:
        return f"""
        SELECT
            S.* EXCLUDE (rn)
            , C.* EXCLUDE (query, seq, subject)
        FROM (SELECT *, (ROW_NUMBER() OVER ()) AS rn FROM {section_table}) AS S
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
        ORDER BY S.rn, S.seq;
        """

    def wb_summary_concat(header: list[str], summary: list[tuple]) -> list[tuple]:
        labels = _get_site_labels(mode="decode")

        def _decode(read_count: int) -> int | str:
            return read_count if isinstance(read_count, int) and (read_count >= 0) else labels.get(read_count)

        return [header] + [
            tuple(_decode(value) if column.startswith("조회수") else value
                for column, value in zip(header, row)) for row in summary]

    def wb_summary_styles(headers: list[str]) -> dict:
        from linkmerce.utils.excel import get_column_letter
        column_styles, column_width, count_columns = dict(), dict(), list()
        ad_rule = dict(operator="equal", formula=['"광고"'], fill={"color": "#F4CCCC", "fill_type": "solid"})
        cafe_rule = dict(operator="formula", formula=[], fill={"color": "#03C75A", "fill_type": "solid"})
        na_rule = dict(operator="formula", formula=["ISBLANK(F2)"], fill={"color": "#BFBFBF", "fill_type": "solid"})

        for col_idx, column in enumerate(headers, start=1):
            if column.endswith('위'):
                column_styles[col_idx] = dict(fill={"color": "#FFF2CC", "fill_type": "solid"})
            elif column.startswith("생성일"):
                column_styles[col_idx] = dict(alignment={"horizontal": "center"})
            elif column.startswith("조회수"):
                column_styles[col_idx] = dict(alignment={"horizontal": "center"}, number_format="#,##0;광고;블로그")
                count_columns.append(col_idx)
            elif column == "영역":
                column_width[col_idx] = 12
            elif column == "항목수":
                column_styles[col_idx] = dict(alignment={"horizontal": "center"})
            elif column != "순번":
                column_width[col_idx] = ":fit:"
        cafe_rule["formula"] = [get_column_letter(min(count_columns))+'2'] if count_columns else []

        return dict(
            header_styles = "yellow",
            column_styles = column_styles,
            column_width = column_width,
            row_height = 16.5,
            conditional_formatting = [
                dict(ranges=count_columns, range_type="column", rule=ad_rule),
                dict(ranges=count_columns, range_type="column", rule=cafe_rule),
                *((dict(ranges=f"F2:{get_column_letter(len(headers))}", range_type="range", rule=na_rule),)
                if len(headers) > 6 else tuple()),
            ],
            column_filters = dict(
                range = ":all:",
                filters = [("영역", [dict(filter_type="value", values=["스마트블록","웹문서"])])]
            ),
            filter_mode = "xml",
            truncate = True,
            freeze_panes = "E2",
        )


    def select_contents(raw_data: list[tuple[str, list[list[dict]]]]) -> list[dict]:
        from linkmerce.utils.map import hier_get
        contents = list()
        for query, sections in raw_data:
            for seq, section in enumerate(sections, start=1):
                heading = hier_get(section, [0,"section"])
                if heading in {"스마트블록","웹문서"}:
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

    def zip_cafe_articles(cafe_articles: list[dict]) -> dict[tuple[str,str], dict]:
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
        return [
            "검색어", "순번", "영역", "순위", "주제", "구분", "제목", "내용", "프로필", "주소", "썸네일주소",
            "제목글자수", "내용글자수", "이미지수", "조회수", "댓글수", "댓글작성자수", "생성일"]

    def wb_contents_styles() -> dict:
        number_columns = ["제목글자수", "내용글자수", "이미지수", "조회수", "댓글수", "댓글작성자수"]
        column_styles = {column: dict(number_format="#,##0") for column in number_columns}

        single_width = {"순번", "순위", "주소", "썸네일주소", "소재ID"}
        column_width = {column: ":fit:" for column in wb_contents_headers() if column not in single_width}

        return dict(
            header_styles = "yellow",
            column_styles = column_styles,
            column_width = column_width,
            row_height = 16.5,
            column_filters = dict(range = ":all:"),
            truncate = True,
        )


    def send_excel_to_slack(
            slack_conn_id: str,
            channel_id: str,
            summary_file: str,
            contents_file: str,
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
            os.unlink(contents_file)

    def parse_slack_response(files: list[dict], ok: bool) -> dict:
        keys = ["id", "name", "size", "created", "permalink"]
        return dict(
            files = [{key: file.get(key) for key in keys} for file in files],
            status = ("success" if ok else "failed"),
        )


    read_variables() >> branch_etl_trigger >> set_cookies() >> etl_naver_main_search()
