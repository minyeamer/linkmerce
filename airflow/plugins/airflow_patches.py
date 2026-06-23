from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.load import DuckDBConnection


def is_test_mode() -> bool:
    """Airflow 전역 변수 `test_mode`가 `true`로 설정되어 있는지 확인한다."""
    from airflow.sdk import Variable
    test_mode: str = Variable.get("test_mode", default="false")
    return test_mode.strip().lower() == "true"


def _simulate_table_upload(
        self,
        connection: DuckDBConnection,
        source_table: str,
        *args,
        **kwargs
    ) -> list[dict]:
    """원격 테이블 적재 동작을 DuckDB 테이블의 상위 5개 행을 반환하는 동작으로 치환한다."""
    return connection.fetch_all_to_json(f"SELECT * FROM {source_table} LIMIT 5")


def _patch_bigquery_client():
    """테스트 모드에서 BigQuery 클라이언트의 연결 생성을 막고 DuckDB 미리보기 반환 메서드로 교체한다."""
    from linkmerce.extensions.bigquery import BigQueryClient

    def _test_init(self: BigQueryClient, *args, **kwargs):
        self.__conn = None
        self.project_id = None

    BigQueryClient.__init__ = _test_init
    BigQueryClient.load_table_from_duckdb = _simulate_table_upload
    BigQueryClient.overwrite_table_from_duckdb = _simulate_table_upload
    BigQueryClient.merge_table_from_duckdb = _simulate_table_upload


def _patch_postgres_client():
    """테스트 모드에서 PostgreSQL 클라이언트의 연결 생성을 막고 DuckDB 미리보기 반환 메서드로 교체한다."""
    from linkmerce.extensions.postgres import PostgresClient

    def _test_init(self: PostgresClient, *args, **kwargs):
        self.__dsn = None
        self.__conn = None

    PostgresClient.__init__ = _test_init
    PostgresClient.load_table_from_duckdb = _simulate_table_upload
    PostgresClient.overwrite_table_from_duckdb = _simulate_table_upload
    PostgresClient.merge_table_from_duckdb = _simulate_table_upload


def _patch_dbt_execute():
    """테스트 모드에서 Cosmos operator의 execute 메서드를 빈 함수로 교체해 dbt task 실행을 건너뛴다."""
    from cosmos.operators.base import AbstractDbtBase

    def _test_execute(self: AbstractDbtBase, *args, **kwargs):
        return

    AbstractDbtBase.execute = _test_execute


def _simulate_slack_upload(self, *args, file_uploads: list[dict], **kwargs) -> dict:
    """테스트 모드에서 가상의 Slack 파일 업로드 응답 결과를 반환한다."""
    import os
    return {
        "files": [{
            "id": None,
            "name": file.get("filename"),
            "size": os.path.getsize(file_path) if isinstance(file_path := file.get("file"), str) else None,
            "created": None,
            "permalink": None,
        } for file in file_uploads],
        "ok": None,
    }


def _apply_test_mode_patches():
    """테스트 모드(`test_mode=true`)일 때 BigQuery/PostgreSQL/dbt 연동 및 Slack 알림을 차단하고 미리보기를 출력한다.
    이 함수는 각 태스크 프로세스 시작 시 자동 호출되며, 외부 라이브러리 메서드를 런타임에 패치한다.

    패치 대상:
    - `BigQueryClient`: 연결을 생성을 막고 소스 테이블 미리보기(LIMIT 5)를 반환하는 함수로 교체
    - `PostgresClient`: 연결을 생성을 막고 소스 테이블 미리보기(LIMIT 5)를 반환하는 함수로 교체
    - `DbtTaskGroup`: Task 실행 시 dbt 명령어를 호출하지 않도록 execute 메서드를 빈 함수로 교체
    - `SlackHook`: 파일 정보만 포함하는 가상의 파일 업로드 응답 결과를 반환하는 함수로 교체"""
    if not is_test_mode():
        return

    import logging
    logger = logging.getLogger(__name__)
    logger.warning("[TEST-MODE] BigQuery/PostgreSQL uploads, dbt execution, and Slack file uploads are disabled")

    try:
        _patch_bigquery_client()
        logger.warning("[TEST-MODE] BigQuery patched successfully")
    except ImportError:
        pass

    try:
        _patch_postgres_client()
        logger.warning("[TEST-MODE] PostgresClient patched successfully")
    except ImportError:
        pass

    try:
        _patch_dbt_execute()
        logger.warning("[TEST-MODE] Cosmos dbt patched successfully")
    except ImportError:
        pass

    try:
        from airflow.providers.slack.hooks.slack import SlackHook
        SlackHook.send_file_v2 = _simulate_slack_upload
        logger.warning("[TEST-MODE] SlackHook patched successfully")
    except ImportError:
        pass


_apply_test_mode_patches()
