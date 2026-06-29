from __future__ import annotations

from fastapi import APIRouter, Query

from app.services.airflow_client import run_airflow_dag
from app.services.gsheets import GSHEETS_DAG_ID, build_dbt_conf, get_target_dag_id, parse_task_ids


router = APIRouter()


@router.get("/api/trigger")
def trigger_airflow_dag(
        dag_id: str = Query(..., description="실행할 Airflow Dag ID"),
        wait: bool = Query(True, description="Dag 종료까지 대기할지 여부"),
    ) -> dict:
    return run_airflow_dag(dag_id=dag_id, wait=wait)


@router.get("/api/trigger/gsheets")
def trigger_gsheets_sync_dag(
        task_ids: str = Query(..., description="쉼표로 구분한 구글시트 task_id 목록"),
        wait: bool = Query(True, description="Dag 종료까지 대기할지 여부"),
    ) -> dict:
    return run_airflow_dag(
        dag_id = GSHEETS_DAG_ID,
        wait = wait,
        conf = {"task_ids": parse_task_ids(task_ids)},
    )


@router.get("/api/trigger/gsheets/{target}")
def trigger_gsheets_target_dag(
        target: str,
        ds_start_date: str | None = Query(None, description="dbt 변수 - ds_start_date"),
        ds_end_date: str | None = Query(None, description="dbt 변수 - ds_end_date"),
        run: str | None = Query(None, description="dbt 실행 여부"),
        wait: bool = Query(True, description="Dag 종료까지 대기할지 여부"),
    ) -> dict:
    conf = build_dbt_conf(
        ds_start_date = ds_start_date,
        ds_end_date = ds_end_date,
        run = run,
    )
    return run_airflow_dag(dag_id=get_target_dag_id(target), wait=wait, conf=conf)
