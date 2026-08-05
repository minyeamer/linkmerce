"""
# 네이버 상품 재고 ETL 파이프라인

## 추출(Extract)
크롬 확장프로그램을 통해 수집한 상품별 재고수량이 Slack 채널에 업로드된다.
분할로 전송된 메시지 내 JSON 파일들을 읽어온다.

## 변환(Transform)
원본 JSON 파일에서 판매중 또는 품절 상품의 재고수량만 추출하여
분할된 파일을 하나의 객체로 합친다.

## 적재(Load)
재고수량을 포함한 데이터를 DuckDB 임시 테이블에 적재하고,
임시 테이블을 대응되는 BigQuery 테이블 끝에 추가한다.
"""

from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from airflow.providers.slack.hooks.slack import SlackHook
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "naver_product_stock",
    schedule = "0 3 * * *",
    start_date = pendulum.datetime(2026, 4, 9, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    doc_md = __doc__,
    tags = [
        "priority:high", "platform:naver-hcenter", "objective:benchmark", "objective:product",
        "schedule:daily", "time:night", "write:append", "provider:slack", "upstream:extension",
        "status:private"
    ],
) as dag:

    PATH = "naver.store.stock"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, tables=True)


    @task(task_id="etl_naver_product_stock", retries=3, retry_delay=timedelta(minutes=1))
    def etl_naver_product_stock(
            ti: TaskInstance,
            data_interval_end: pendulum.DateTime,
            **kwargs
        ) -> list:
        """Slack 채널에서 재고 데이터를 다운로드받고, DuckDB 테이블을 경유해 BigQuery/Postgres 테이블에 적재한다."""
        from linkmerce.common.load import DuckDBConnection
        from dual_load import load_table_from_duckdb, merge_table_from_duckdb
        from airflow_utils import in_timezone

        configs = ti.xcom_pull(task_ids="read_configs")
        tables, merge = configs["tables"], configs["merge"]

        sources = download_product_stock(datetime=in_timezone(data_interval_end), **configs)

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            for table, values in sources.items():
                conn.create_table_from_json(table, values)

            conn.execute(create_stock())
            conn.execute(create_stock_detail())
            conn.execute(create_stock_product())
            conn.execute(create_stock_option())

            conn.execute(bulk_insert_stock())
            conn.execute(bulk_insert_stock_detail())
            conn.execute(bulk_insert_stock_product())
            conn.execute(bulk_insert_stock_option())

            min_dt, max_dt = conn.fetch_values("SELECT MIN(created_at), MAX(created_at) FROM stock")

            return {
                "context": {
                    "partitions": conn.unique("stock", "DATE(created_at)"),
                },
                "params": {
                    "channel_id": configs["channel_id"],
                    "min_time": min_dt,
                    "max_time": max_dt,
                },
                "results": {
                    "stock": load_table_from_duckdb(
                        connection = conn,
                        source_table = "stock",
                        target_table = tables["stock"],
                    ),
                    "stock_detail": load_table_from_duckdb(
                        connection = conn,
                        source_table = "stock_detail",
                        target_table = tables["stock_detail"],
                    ),
                    "stock_product": merge_table_from_duckdb(
                        connection = conn,
                        source_table = "stock_product",
                        target_table = tables["stock_product"],
                        **merge["stock_product"],
                    ),
                    "stock_option": merge_table_from_duckdb(
                        connection = conn,
                        source_table = "stock_option",
                        target_table = tables["stock_option"],
                        **merge["stock_option"],
                    )
                }
            }


    def fetch_slack_history(
            slack_hook: SlackHook,
            channel_id: str,
            datetime: pendulum.DateTime,
        ) -> list:
        """Slack 채널에서 특정 날짜에 전송된 메시지 목록을 가져온다."""
        day_start = datetime.start_of("day")
        oldest = str(day_start.timestamp())
        latest = str(day_start.add(days=1).timestamp())

        params = {"channel": channel_id, "oldest": oldest, "latest": latest, "limit": 5}
        response = slack_hook.client.conversations_history(**params)
        return response.get("messages") or list()


    def download_product_stock(
            slack_conn_id: str,
            channel_id: str,
            datetime: pendulum.DateTime,
            save_to: str | None = None,
            **kwargs
        ) -> dict:
        """Slack 채널에서 특정 날짜로 업로드된 재고 데이터를 다운로드하여 항목별 하나의 리스트로 병합한다.

        재고 데이터를 읽어오면서 `save_to` 경로에 다음 파일을 저장한다.
        1. 다운로드받은 파일 원본을 날짜별 하위 경로(YYYY/M/D)로 구분해 저장한다.
        2. 삭제된 상품이 있다면 상품주소 목록을 `deleted_urls.txt` 파일에 덮어쓴다.
        """
        from pathlib import Path
        import json
        import requests

        # 1. Slack 채널에서 `data_interval_end` 날짜에 전송된 메시지 목록을 가져온다.
        slack_hook = SlackHook(slack_conn_id=slack_conn_id)
        messages: list[dict] = fetch_slack_history(slack_hook, channel_id, datetime)
        token = slack_hook.get_conn().token

        # 2. 통합된 재고 데이터 자료구조를 초기화하고 저장 경로를 확인 또는 생성한다.
        results: dict[str, list] = {"products": list(), "options": list(), "supplements": list()}
        deleted, div_cnt = list(), 0
        save_path, has_path = None, bool(save_to)
        if has_path:
            save_path = Path(save_to)
            save_path.mkdir(parents=True, exist_ok=True)

        for message in messages:
            for file in (message.get("files") or list()):
                # 3. Slack 메시지에 포함된 파일의 이름과 다운로드 주소를 가져온다.
                if not isinstance(file, dict):
                    continue
                url = file.get("url_private_download") or file.get("url_private")
                file_name = file.get("name") or file.get("title") or str()

                # 4. 파일명이 "네이버상품_"으로 시작하면 다운로드하고 삭제된 상품 목록에 구분자를 추가한다.
                if not (url and file_name.startswith("네이버상품_")):
                    continue

                response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
                response.raise_for_status()
                data = response.json()

                if not isinstance(data, dict):
                    continue

                deleted.append(f"# {file_name}")
                div_cnt += 1

                # 5. 저장 경로가 있다면 데이터를 날짜별 하위 경로(YYYY/M/D)를 생성해 파일로 저장한다.
                if has_path:
                    path = save_path / datetime.format("YYYY/M/D")
                    path.mkdir(parents=True, exist_ok=True)
                    with open(path / file_name, 'w', encoding="utf-8") as file:
                        json.dump(data, file, ensure_ascii=False, separators=(',', ':'), default=str)

                # 6. 데이터에서 항목별 리스트를 순회하면서 삭제된 상품을 확인하고 나머지를 추가한다.
                for product in (data.get("products") or list()):
                    if not isinstance(product, dict):
                        continue
                    elif product["status"] == "error":
                        continue
                    elif product["status"] == "deleted":
                        deleted.append(product["productUrl"])
                    else:
                        results["products"].append(product)

                results["options"] += (data.get("options") or list())
                results["supplements"] += (data.get("supplements") or list())

        # 7. 구분자를 제외하고 삭제된 상품이 있다면 `deleted_urls.txt` 파일에 덮어쓴다.
        if (len(deleted) > max(div_cnt, 1)) and has_path:
            with open(save_path / "product_deleted.txt", 'w', encoding="utf-8") as file:
                file.write('\n'.join(deleted))

        return results


    def create_stock(table: str = "stock") -> str:
        return dedent(f"""
            CREATE TABLE {table} (
                product_id BIGINT NOT NULL
                , product_status TINYINT
                , sales_price INTEGER
                , stock_quantity INTEGER
                , created_at TIMESTAMP NOT NULL
                , PRIMARY KEY (created_at, product_id)
            )""").strip()

    def bulk_insert_stock(table: str = "stock", rows: str = "products") -> str:
        return dedent(f"""
            INSERT INTO {table}
            SELECT *
            FROM (
                SELECT
                    CAST(productId AS BIGINT) AS product_id
                    , (CASE WHEN status = 'success' THEN 0 WHEN status = 'paused' THEN 1 WHEN status = 'soldout' THEN 2 ELSE NULL END) AS product_status
                    , COALESCE(salesPrice, price) AS sales_price
                    , stockQuantity AS stock_quantity
                    , DATE_TRUNC('second', CAST("timestamp" AS TIMESTAMPTZ) AT TIME ZONE 'Asia/Seoul') AS created_at
                FROM {rows}
            ) AS t_
            QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(created_at AS DATE), product_id ORDER BY created_at ASC) = 1
            ON CONFLICT DO NOTHING
            """).strip()


    def create_stock_detail(table: str = "stock_detail") -> str:
        return dedent(f"""
            CREATE TABLE {table} (
                product_id BIGINT NOT NULL
                , option_id BIGINT NOT NULL
                , option_price INTEGER
                , stock_quantity INTEGER
                , created_at TIMESTAMP NOT NULL
                , PRIMARY KEY (created_at, product_id, option_id)
            )""").strip()

    def bulk_insert_stock_detail(
            table: str = "stock_detail",
            option_rows: str = "options",
            supplement_rows: str = "supplements",
        ) -> str:
        return dedent(f"""
            INSERT INTO {table}
            SELECT *
            FROM (
                (SELECT
                    CAST(productId AS BIGINT) AS product_id
                    , CAST(optionId AS BIGINT) AS option_id
                    , optionPrice AS option_price
                    , stockQuantity AS stock_quantity
                    , DATE_TRUNC('second', CAST("timestamp" AS TIMESTAMPTZ) AT TIME ZONE 'Asia/Seoul') AS created_at
                FROM {option_rows})
                UNION ALL
                (SELECT
                    CAST(productId AS BIGINT) AS product_id
                    , CAST(optionId AS BIGINT) AS option_id
                    , optionPrice AS option_price
                    , stockQuantity AS stock_quantity
                    , DATE_TRUNC('second', CAST("timestamp" AS TIMESTAMPTZ) AT TIME ZONE 'Asia/Seoul') AS created_at
                FROM {supplement_rows})
            ) AS t_
            QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(created_at AS DATE), product_id, option_id ORDER BY created_at ASC) = 1
            ON CONFLICT DO NOTHING
            """).strip()


    def create_stock_product(table: str = "stock_product") -> str:
        return dedent(f"""
            CREATE TABLE {table} (
                product_id BIGINT NOT NULL
                , product_no BIGINT NOT NULL
                , mall_seq BIGINT NOT NULL
                , channel_seq BIGINT NOT NULL
                , category_id INTEGER
                , product_name VARCHAR
                , product_status TINYINT
                , delivery_type VARCHAR
                , price INTEGER
                , sales_price INTEGER
                , review_count INTEGER
                , review_score DECIMAL(3, 2)
                , first_timestamp TIMESTAMP
                , last_timestamp TIMESTAMP
                , PRIMARY KEY (product_id)
            )""").strip()

    def bulk_insert_stock_product(table: str = "stock_product", rows: str = "products") -> str:
        return dedent(f"""
            INSERT INTO {table}
            SELECT
                * EXCLUDE (created_at)
                , MIN(created_at) OVER (PARTITION BY product_id) AS first_timestamp
                , MAX(created_at) OVER (PARTITION BY product_id) AS last_timestamp
            FROM (
                SELECT
                    CAST(productId AS BIGINT) AS product_id
                    , CAST(productNo AS BIGINT) AS product_no
                    , CAST(mallNo AS BIGINT) AS mall_seq
                    , CAST(channelId AS BIGINT) AS channel_seq
                    , CAST(categoryId AS INTEGER) AS category_id
                    , CAST(productName AS VARCHAR) AS product_name
                    , (CASE WHEN status = 'success' THEN 0 WHEN status = 'paused' THEN 1 WHEN status = 'soldout' THEN 2 ELSE NULL END) AS product_status
                    , CAST(deliveryType AS VARCHAR) AS delivery_type
                    , price
                    , salesPrice AS sales_price
                    , reviewCount AS review_count
                    , reviewScore AS review_score
                    , DATE_TRUNC('second', CAST("timestamp" AS TIMESTAMPTZ) AT TIME ZONE 'Asia/Seoul') AS created_at
                FROM {rows}
            ) AS t_
            QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY created_at DESC) = 1
            ON CONFLICT DO NOTHING
            """).strip()


    def create_stock_option(table: str = "stock_option") -> str:
        return dedent(f"""
            CREATE TABLE {table} (
                product_id BIGINT NOT NULL
                , option_id BIGINT NOT NULL
                , product_type SMALLINT
                , option_group1 VARCHAR
                , option_name1 VARCHAR
                , option_group2 VARCHAR
                , option_name2 VARCHAR
                , option_group3 VARCHAR
                , option_name3 VARCHAR
                , option_price INTEGER
                , register_order INTEGER
                , register_dt TIMESTAMP
                , first_timestamp TIMESTAMP
                , last_timestamp TIMESTAMP
                , PRIMARY KEY (product_id, option_id)
            )""").strip()

    def bulk_insert_stock_option(
            table: str = "stock_option",
            option_rows: str = "options",
            supplement_rows: str = "supplements",
        ) -> str:
        return dedent(f"""
            INSERT INTO {table}
            SELECT
                * EXCLUDE (created_at)
                , MIN(created_at) OVER (PARTITION BY product_id, option_id) AS first_timestamp
                , MAX(created_at) OVER (PARTITION BY product_id, option_id) AS last_timestamp
            FROM (
                (SELECT
                    CAST(productId AS BIGINT) AS product_id
                    , CAST(optionId AS BIGINT) AS option_id
                    , 1 AS product_type
                    , CAST(optionGroup1 AS VARCHAR) AS option_group1
                    , CAST(optionName1 AS VARCHAR) AS option_name1
                    , CAST(optionGroup2 AS VARCHAR) AS option_group2
                    , CAST(optionName2 AS VARCHAR) AS option_name2
                    , CAST(optionGroup3 AS VARCHAR) AS option_group3
                    , CAST(optionName3 AS VARCHAR) AS option_name3
                    , optionPrice AS option_price
                    , optionSeq AS register_order
                    , DATE_TRUNC('second', CAST(registerDate AS TIMESTAMPTZ) AT TIME ZONE 'Asia/Seoul') AS register_dt
                    , DATE_TRUNC('second', CAST("timestamp" AS TIMESTAMPTZ) AT TIME ZONE 'Asia/Seoul') AS created_at
                FROM {option_rows})
                UNION ALL
                (SELECT
                    CAST(productId AS BIGINT) AS product_id
                    , CAST(optionId AS BIGINT) AS option_id
                    , 2 AS product_type
                    , CAST(optionGroup1 AS VARCHAR) AS option_group1
                    , CAST(optionName1 AS VARCHAR) AS option_name1
                    , CAST(optionGroup2 AS VARCHAR) AS option_group2
                    , CAST(optionName2 AS VARCHAR) AS option_name2
                    , CAST(optionGroup3 AS VARCHAR) AS option_group3
                    , CAST(optionName3 AS VARCHAR) AS option_name3
                    , optionPrice AS option_price
                    , optionSeq AS register_order
                    , DATE_TRUNC('second', CAST(registerDate AS TIMESTAMPTZ) AT TIME ZONE 'Asia/Seoul') AS register_dt
                    , DATE_TRUNC('second', CAST("timestamp" AS TIMESTAMPTZ) AT TIME ZONE 'Asia/Seoul') AS created_at
                FROM {supplement_rows})
            ) AS t_
            QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, option_id ORDER BY created_at DESC) = 1
            ON CONFLICT DO NOTHING
            """).strip()


    read_configs() >> etl_naver_product_stock()
