from __future__ import annotations

import datetime as dt
import os
import time

import requests
from fastapi import HTTPException


TERMINAL_STATES = {"success", "failed"}
STATUS_LABELS = {
    "success": "완료",
    "running": "실행 중",
    "failed": "실패",
    "queued": "대기 중",
    "scheduled": "실행 예약",
    "timeout": "시간 초과",
}


def run_airflow_dag(
        dag_id: str,
        wait: bool,
        conf: dict | None = None,
    ) -> dict:
    """Airflow Dag을 실행하고, 필요하면 종료 상태까지 기다려 결과를 반환한다."""
    _validate_allowed_dag(dag_id)

    access_token = _auth_airflow()
    logical_date = _utc_now()
    dag_run_id = f"api__{logical_date}"

    result = _trigger_dag_run(
        dag_id = dag_id,
        dag_run_id = dag_run_id,
        logical_date = logical_date,
        access_token = access_token,
        conf = conf,
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
        "conf": conf or dict(),
    }


def _validate_allowed_dag(dag_id: str):
    """환경변수 허용 목록이 있으면 해당 Dag만 실행되도록 검증한다."""
    env_allowed = _env("AIRFLOW_TRIGGER_ALLOWED_DAGS", default=str())
    allowed = {value.strip() for value in env_allowed.split(",") if value.strip()}
    if allowed and (dag_id not in allowed):
        raise HTTPException(status_code=403, detail="허용되지 않은 Dag입니다.")


def _airflow_url(path: str) -> str:
    """Airflow 서버 기본 URL과 경로를 결합해 최종 요청 URL을 만든다."""
    base_url = _env("AIRFLOW_SERVER_URL", "http://host.docker.internal:8080")
    return base_url.rstrip("/") + path


def _auth_headers(access_token: str) -> dict[str, str]:
    """Airflow API 호출에 공통으로 사용하는 인증 헤더를 반환한다."""
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }


def _auth_airflow() -> str:
    """Airflow 인증 엔드포인트에 로그인해 access token을 발급받는다."""
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
        conf: dict | None = None,
    ) -> dict:
    """Dag run 식별자와 `conf`를 포함해 Airflow에 실제 실행 요청을 보낸다."""
    response = requests.post(
        _airflow_url(f"/api/v2/dags/{dag_id}/dagRuns"),
        headers = _auth_headers(access_token),
        json = {
            "dag_run_id": dag_run_id,
            "logical_date": logical_date,
            "data_interval_start": logical_date,
            "data_interval_end": _utc_now(offset_seconds=1),
            "conf": conf,
        },
        timeout = _timeout(),
    )
    _raise_for_response(response)
    return response.json()


def _wait_for_dag_run(dag_id: str, dag_run_id: str, access_token: str) -> str:
    """Dag run이 종료 상태가 될 때까지 주기적으로 상태를 조회한다."""
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
    """실패한 HTTP 응답을 FastAPI 예외로 변환하며 응답 본문을 보존한다."""
    if response.ok:
        return
    try:
        detail = response.json()
    except ValueError:
        detail = response.text
    raise HTTPException(status_code=response.status_code, detail=detail)


def _timeout() -> int:
    """Airflow HTTP 요청에 공통으로 사용할 타임아웃 초 값을 반환한다."""
    return int(_env("AIRFLOW_TRIGGER_REQUEST_TIMEOUT", "30"))


def _utc_now(offset_seconds: int = 0) -> str:
    """현재 UTC 시각을 Airflow API가 기대하는 ISO 문자열로 반환한다."""
    now = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=offset_seconds)
    return now.isoformat(timespec="seconds")


def _env(name: str, default: str | None = None) -> str:
    """환경변수를 읽고, 없으면 기본값 또는 서비스 설정 오류를 반환한다."""
    env_value = os.environ.get(name)
    if env_value:
        return env_value

    if default is None:
        raise HTTPException(status_code=503, detail=f"{name} 설정이 필요합니다.")
    return default
