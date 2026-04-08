from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from linkmerce.common.load import DuckDBConnection


def is_test_mode() -> bool:
    """Airflow 전역 변수 `test_mode`가 `true`로 설정되어 있는지 확인한다."""
    from airflow.sdk import Variable
    test_mode: str = Variable.get("test_mode", default="false")
    return test_mode.strip().lower() == "true"


def _apply_test_mode_patches() -> None:
    """테스트 모드(`test_mode=true`)일 때 BigQuery 적재 및 Slack 알림을 차단하고 미리보기를 출력한다.   
    이 함수는 각 태스크 프로세스 시작 시 자동 호출되며, 외부 라이브러리 메서드를 런타임에 패치한다.

    패치 대상:
    - `BigQueryClient`: 소스 테이블 미리보기(LIMIT 5)를 반환하는 함수로 교체
    - `SlackHook`: 파일 정보만 포함하는 가상의 파일 업로드 응답 결과를 반환하는 함수로 교체"""
    if not is_test_mode():
        return

    import logging
    logger = logging.getLogger(__name__)
    logger.warning("[TEST-MODE] BigQuery uploads and Slack notifications are disabled.")

    def _simulate_gbq_upload(self, connection: DuckDBConnection, source_table: str, *args, **kwargs) -> list[dict]:
        return connection.fetch_all_to_json(f"SELECT * FROM {source_table} LIMIT 5")

    try:
        from linkmerce.extensions.bigquery import BigQueryClient
        BigQueryClient.load_table_from_duckdb = _simulate_gbq_upload
        BigQueryClient.overwrite_table_from_duckdb = _simulate_gbq_upload
        BigQueryClient.merge_into_table_from_duckdb = _simulate_gbq_upload
        logger.warning("[TEST-MODE] BigQueryClient patched successfully.")
    except ImportError:
        pass

    def _simulate_slack_upload(self, *, file_uploads: list[dict], **kwargs) -> dict:
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

    try:
        from airflow.providers.slack.hooks.slack import SlackHook
        SlackHook.send_file_v2 = _simulate_slack_upload
        logger.warning("[TEST-MODE] SlackHook patched successfully.")
    except ImportError:
        pass


_apply_test_mode_patches()
