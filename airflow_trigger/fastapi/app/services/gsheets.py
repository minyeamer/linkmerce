from __future__ import annotations

from fastapi import HTTPException


GSHEETS_DAG_ID = "sync_gsheets"
GSHEETS_TARGET_DAG_IDS = {
    "ads_master": "sync_gsheets__ads_master",
    "expense": "sync_gsheets__expense",
    "extra_ads": "sync_gsheets__extra_ads",
    "extra_sales": "sync_gsheets__extra_sales",
    "opex": "sync_gsheets__opex",
    "order_status": "sync_gsheets__order_status",
}


def parse_task_ids(task_ids: str, sep: str = ',') -> list[str]:
    """쉼표로 전달된 `task_id` 문자열을 구분자로 나눠서 리스트로 변환한다."""
    parsed_task_ids = [task_id.strip() for task_id in task_ids.split(sep) if task_id.strip()]
    if not parsed_task_ids:
        raise HTTPException(status_code=400, detail="task_ids를 1개 이상 전달해야 합니다.")
    return parsed_task_ids


def get_target_dag_id(target: str) -> str:
    """축약된 구글시트 target 이름을 실제 Airflow Dag ID로 변환한다."""
    try:
        return GSHEETS_TARGET_DAG_IDS[target]
    except KeyError as exc:
        allowed_targets = ", ".join(sorted(GSHEETS_TARGET_DAG_IDS))
        raise HTTPException(status_code=404, detail=f"지원하지 않는 구글시트입니다: {allowed_targets}") from exc


def build_dbt_conf(
        ds_start_date: str | None,
        ds_end_date: str | None,
        run: str | None,
    ) -> dict:
    """개별 구글시트 Dag 실행에 사용할 dbt 설정을 요청 파라미터로 조립한다."""
    dbt_conf = dict()
    if run is not None:
        dbt_conf["run"] = _parse_bool(run, "run")

    if ds_start_date or ds_end_date:
        if not (ds_start_date and ds_end_date):
            raise HTTPException(status_code=400, detail="시작일과 종료일을 함께 전달해야 합니다.")
        dbt_conf["date_range"] = {
            "ds_start_date": ds_start_date,
            "ds_end_date": ds_end_date,
        }

    return {"dbt": dbt_conf} if dbt_conf else dict()


def _parse_bool(value: str, name: str) -> bool:
    """문자열로 들어온 boolean 값을 실제 bool 타입으로 정규화한다."""
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y", "on"}:
        return True
    if normalized in {"false", "0", "no", "n", "off"}:
        return False
    raise HTTPException(status_code=400, detail=f"{name} 값은 true 또는 false로 전달해야 합니다.")
