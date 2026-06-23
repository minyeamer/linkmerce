from __future__ import annotations

import datetime as dt
import os
import time
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates


app = FastAPI(title="LinkMerce Airflow Trigger")
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")

TERMINAL_STATES = {"success", "failed"}
STATUS_LABELS = {
    "success": "완료",
    "running": "실행 중",
    "failed": "실패",
    "queued": "대기 중",
    "scheduled": "실행 예약",
    "timeout": "시간 초과",
}


@app.get("/health")
def health() -> dict:
    return {"ok": True, "service": "airflow-trigger"}


@app.get("/trigger", response_class=HTMLResponse)
def trigger_page(
        request: Request,
        dag_id: str = Query(..., description="실행할 Airflow Dag ID"),
        dag_name: str | None = Query(None, description="화면에 표시할 작업 이름"),
) -> HTMLResponse:
    display_name = dag_name.strip() if dag_name and dag_name.strip() else dag_id
    return templates.TemplateResponse(
        request = request,
        name = "trigger.html",
        context = {
            "dag_id": dag_id,
            "display_name": display_name,
        },
    )


@app.get("/api/trigger")
def trigger_airflow_dag(
        dag_id: str = Query(..., description="실행할 Airflow Dag ID"),
        wait: bool = Query(True, description="Dag 종료까지 대기할지 여부"),
    ) -> dict:
    _validate_allowed_dag(dag_id)

    access_token = _auth_airflow()
    logical_date = _utc_now()
    dag_run_id = "api__{}__{}".format(dag_id, logical_date.replace("-", "").replace(":", "").replace("+00:00", "Z"))

    result = _trigger_dag_run(
        dag_id = dag_id,
        dag_run_id = dag_run_id,
        logical_date = logical_date,
        access_token = access_token,
    )
    state = result.get("state", "queued")

    if wait:
        state = _wait_for_dag_run(dag_id, dag_run_id, access_token)

    return {
        "ok": ((state == "success") if wait else True),
        "dag_id": dag_id,
        "dag_run_id": result.get("dag_run_id", dag_run_id),
        "state": state,
        "state_label": STATUS_LABELS.get(state, state),
        "logical_date": result.get("logical_date", logical_date),
    }


def _validate_allowed_dag(dag_id: str):
    env_allowed = _env("AIRFLOW_TRIGGER_ALLOWED_DAGS", default=str())
    allowed = {value.strip() for value in env_allowed.split(",") if value.strip()}
    if allowed and (dag_id not in allowed):
        raise HTTPException(status_code=403, detail="허용되지 않은 Dag입니다.")


def _airflow_url(path: str) -> str:
    base_url = _env("AIRFLOW_SERVER_URL", "http://host.docker.internal:8080")
    return base_url.rstrip("/") + path


def _auth_headers(access_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }


def _auth_airflow() -> str:
    response = requests.post(
        _airflow_url("/auth/token"),
        json = {
            "username": _env("AIRFLOW_WWW_USER_USERNAME", "airflow"),
            "password": _env("AIRFLOW_WWW_USER_PASSWORD", "airflow"),
        },
        timeout = _timeout(),
    )
    _raise_for_response(response)
    try:
        return response.json()["access_token"]
    except KeyError as exc:
        raise HTTPException(status_code=502, detail="Airflow access token을 발급받지 못했습니다.") from exc


def _trigger_dag_run(
        dag_id: str,
        dag_run_id: str,
        logical_date: str,
        access_token: str,
    ) -> dict:
    response = requests.post(
        _airflow_url(f"/api/v2/dags/{dag_id}/dagRuns"),
        headers = _auth_headers(access_token),
        json = {
            "dag_run_id": dag_run_id,
            "logical_date": logical_date,
            "data_interval_start": logical_date,
            "data_interval_end": _utc_now(offset_seconds=1),
        },
        timeout = _timeout(),
    )
    _raise_for_response(response)
    return response.json()


def _wait_for_dag_run(dag_id: str, dag_run_id: str, access_token: str) -> str:
    deadline = time.monotonic() + int(_env("AIRFLOW_TRIGGER_WAIT_TIMEOUT", "600"))
    interval = int(_env("AIRFLOW_TRIGGER_WAIT_INTERVAL", "5"))

    while time.monotonic() < deadline:
        response = requests.get(
            _airflow_url(f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"),
            headers = _auth_headers(access_token),
            timeout = _timeout(),
        )
        _raise_for_response(response)
        state = response.json().get("state")
        if state in TERMINAL_STATES:
            return state
        time.sleep(interval)

    return "timeout"


def _raise_for_response(response: requests.Response):
    if response.ok:
        return
    try:
        detail = response.json()
    except ValueError:
        detail = response.text
    raise HTTPException(status_code=response.status_code, detail=detail)


def _timeout() -> int:
    return int(_env("AIRFLOW_TRIGGER_REQUEST_TIMEOUT", "30"))


def _utc_now(offset_seconds: int = 0) -> str:
    now = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=offset_seconds)
    return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _env(name: str, default: str | None = None) -> str:
    env_value = os.environ.get(name)
    if env_value:
        return env_value

    if default is None:
        raise HTTPException(status_code=503, detail=f"{name} 설정이 필요합니다.")
    return default
