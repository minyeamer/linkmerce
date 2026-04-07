from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
from textwrap import dedent
from typing import Literal
import pendulum


with DAG(
    dag_id = "sabangnet_product",
    schedule = "20 23 * * 1-5",
    start_date = pendulum.datetime(2025, 10, 22, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=30),
    catchup = False,
    tags = ["priority:high", "sabangnet:product", "sabangnet:option", "sabangnet:mapping", "login:sabangnet", "schedule:weekdays", "time:night"],
    doc_md = dedent("""
        # 사방넷 상품/옵션/매핑 ETL 파이프라인

        ## 인증(Credentials)
        사방넷 아이디, 비밀번호와 시스템 도메인 번호가 필요하다.
        Task를 실행할 때마다 로그인하고, 쿠키와 `access_token`을 발급받아 활용한다.

        ## 추출(Extract)
        사방넷 시스템에서 상품 목록, 단품 옵션 목록, 추가상품 목록, 품번/단품코드 매핑 내역을
        순차적으로 수집한다.

        ## 변환(Transform)
        JSON 또는 엑셀 바이너리 형식의 각각의 데이터를 DuckDB 테이블에 적재한다.

        ## 적재(Load)
        Task마다 대응되는 BigQuery 테이블과 MERGE 문으로 병합해 최신 데이터를 덮어쓴다.
    """).strip(),
) as dag:

    PATH = "sabangnet.admin.product"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        return read_config(PATH, credentials="expand", tables=True, service_account=True)


    @task(task_id="etl_sabangnet_product", pool="sabangnet_pool")
    def etl_sabangnet_product(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import get_execution_date
        configs = ti.xcom_pull(task_ids="read_configs")
        return main_product(product_type="product", end_date=get_execution_date(kwargs), **configs)

    @task(task_id="etl_sabangnet_option", pool="sabangnet_pool")
    def etl_sabangnet_option(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import get_execution_date
        configs = ti.xcom_pull(task_ids="read_configs")
        return main_product(product_type="option", end_date=get_execution_date(kwargs), **configs)

    @task(task_id="etl_sabangnet_add_product", pool="sabangnet_pool")
    def etl_sabangnet_add_product(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import get_execution_date
        configs = ti.xcom_pull(task_ids="read_configs")
        return main_product(product_type="add_product", end_date=get_execution_date(kwargs), **configs)

    def main_product(
            userid: str,
            passwd: str,
            domain: int,
            end_date: str,
            product_type: Literal["product", "option", "add_product"],
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.extensions.bigquery import BigQueryClient
        from importlib import import_module
        modules = {"product": "product", "option": "option_download", "add_product": "add_product"}
        extract = getattr(import_module("linkmerce.api.sabangnet.admin"), modules[product_type])
        source = "sabangnet_{}".format(modules[product_type])
        include_deleted = (product_type in {"product", "option"})
        disable_progress = (product_type != "option")

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            for is_deleted in ([False, True] if include_deleted else [None]):
                extract(
                    userid = userid,
                    passwd = passwd,
                    domain = domain,
                    start_date = "2000-01-01",
                    end_date = end_date,
                    **({"is_deleted": is_deleted} if include_deleted else dict()),
                    connection = conn,
                    **(dict(progress = False) if disable_progress else dict()),
                    return_type = "none",
                )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "start_date": "2000-01-01",
                        "end_date": end_date,
                        **({"is_deleted": [False, True]} if include_deleted else dict()),
                    },
                    "count": {
                        product_type: conn.count_table(source),
                    },
                    "status": {
                        product_type: client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = source,
                            staging_table = tables[f"temp_{product_type}"],
                            target_table = tables[product_type],
                            **merge[product_type],
                            progress = False,
                        ),
                    },
                }


    @task(task_id="etl_sabangnet_mapping", pool="sabangnet_pool")
    def etl_sabangnet_mapping(ti: TaskInstance, **kwargs) -> dict:
        from airflow_utils import get_execution_date
        configs = ti.xcom_pull(task_ids="read_configs")
        return main_mapping(end_date=get_execution_date(kwargs), **configs)

    def main_mapping(
            userid: str,
            passwd: str,
            domain: int,
            end_date: str,
            service_account: dict,
            tables: dict[str, str],
            merge: dict[str, dict],
            **kwargs
        ) -> dict:
        from linkmerce.common.load import DuckDBConnection
        from linkmerce.api.sabangnet.admin import option_mapping
        from linkmerce.extensions.bigquery import BigQueryClient
        sources = {"product": "sabangnet_product_mapping", "sku": "sabangnet_sku_mapping"}

        with DuckDBConnection(tzinfo="Asia/Seoul") as conn:
            option_mapping(
                userid = userid,
                passwd = passwd,
                domain = domain,
                start_date = "2000-01-01",
                end_date = end_date,
                connection = conn,
                progress = False,
                return_type = "none",
            )

            with BigQueryClient(service_account) as client:
                return {
                    "params": {
                        "start_date": "2000-01-01",
                        "end_date": end_date,
                    },
                    "counts": {
                        "product": conn.count_table(sources["product"]),
                        "sku": conn.count_table(sources["sku"]),
                    },
                    "status": {
                        "product": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["product"],
                            staging_table = tables["temp_mapping_product"],
                            target_table = tables["mapping_product"],
                            **merge["mapping_product"],
                            progress = False,
                        ),
                        "sku": client.merge_into_table_from_duckdb(
                            connection = conn,
                            source_table = sources["sku"],
                            staging_table = tables["temp_mapping_sku"],
                            target_table = tables["mapping_sku"],
                            **merge["mapping_sku"],
                            progress = False,
                        ),
                    },
                }


    read_configs() >> [
        etl_sabangnet_product(),
        etl_sabangnet_option(),
        etl_sabangnet_add_product(),
        etl_sabangnet_mapping(),
    ]
