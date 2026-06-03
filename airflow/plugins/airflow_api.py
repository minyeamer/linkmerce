from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import IO, Literal, Union
    import pendulum
    import requests
    JsonSerialize = Union[dict, list, bytes, IO]


def _base_url() -> str:
    """환경변수에서 Airflow API의 Base URL을 구성하여 반환한다."""
    import os
    url = os.environ.get("AIRFLOW_WWW_BASE_URL", "http://airflow-apiserver")
    port = os.environ.get("AIRFLOW_WWW_PORT", "8080")
    return f"{url}:{port}"


def authenticate(username: str | None = None, password: str | None = None, timeout: int = 30) -> str:
    """Airflow 계정 정보를 가지고 REST API 사용을 위한 JWT 액세스 토큰을 발급받는다."""
    import os
    import requests
    url = f"{_base_url()}/auth/token"
    body = {
        "username": (username or os.environ.get("AIRFLOW_WWW_USER_USERNAME", "airflow")),
        "password": (password or os.environ.get("AIRFLOW_WWW_USER_PASSWORD", "airflow")),
    }
    headers = {"Content-Type": "application/json"}
    with requests.post(url, json=body, headers=headers, timeout=timeout) as response:
        response.raise_for_status()
        return response.json()["access_token"]


def request(
        method: str,
        path: str,
        access_token: str,
        params: dict | list[tuple] | bytes | None = None,
        data: dict | list[tuple] | bytes | IO | None = None,
        json: JsonSerialize | None = None,
        timeout: int = 30,
        **message
    ) -> requests.Response:
    """액세스 토큰을 가지고 Airflow REST API에 대한 HTTP 요청을 수행한다."""
    import requests
    url = f"{_base_url()}/api/v2{path}"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    message.update(params=params, data=data, json=json, headers=headers)
    return requests.request(method, url, timeout=timeout, **message)


def list_dagruns(
        dag_id: str,
        access_token: str,
        logical_date_gte: pendulum.DateTime | None = None,
        logical_date_lte: pendulum.DateTime | None = None,
        limit: int = 100,
        timeout: int = 30,
    ) -> list[dict]:
    """특정 시간대의 DAG run 목록을 조회한다.

    Args:
        dag_id: 조회할 Dag ID.
        access_token: Airflow REST API JWT 액세스 토큰.
        logical_date_gte: 조회 시작 시각. (logical_date >=)
        logical_date_lte: 조회 종료 시각. (logical_date <=)
        limit: 최대 조회 건수.
        timeout: HTTP 요청 타임아웃(초).

    Returns: list[dict]
    ```python
    [{
        "dag_run_id": str,
        "dag_id": str,
        "logical_date": "YYYY-MM-DDTHH:mm:ss[Z]",
        "queued_at": "YYYY-MM-DDTHH:mm:ss.SSSSSS[Z]",
        "start_date": "YYYY-MM-DDTHH:mm:ss.SSSSSS[Z]",
        "end_date": "YYYY-MM-DDTHH:mm:ss.SSSSSS[Z]",
        "duration": float,
        "data_interval_start": "YYYY-MM-DDTHH:mm:ss[Z]",
        "data_interval_end": "YYYY-MM-DDTHH:mm:ss[Z]",
        "run_after": "YYYY-MM-DDTHH:mm:ss[Z]",
        "last_scheduling_decision": "YYYY-MM-DDTHH:mm:ss.SSSSSS[Z]",
        "run_type": ["scheduled" | "manual" | "backfill" | "dataset_triggered"],
        "state": ["scheduled" | "pending" | "queued" | "running" | "success" | "failed"],
        "triggered_by": str,
        "triggering_user_name": str,
        "conf": dict,
        "note": str,
        "dag_versions": list[dict],
        "bundle_version": str,
        "dag_display_name": str,
        "partition_key": str,
    }]
    ```
    """
    params = {"limit": limit}
    if logical_date_gte is not None:
        params["logical_date_gte"] = logical_date_gte.isoformat()
    if logical_date_lte is not None:
        params["logical_date_lte"] = logical_date_lte.isoformat()

    response = request("GET", f"/dags/{dag_id}/dagRuns", access_token, params=params, timeout=timeout)
    response.raise_for_status()
    try:
        return response.json()["dag_runs"]
    except:
        return list()


def trigger_dagrun(
        dag_id: str,
        run_id: str,
        access_token: str,
        logical_date: pendulum.DateTime,
        conf: dict | str | None = None,
        timeout: int = 30,
    ) -> dict:
    """Dag ID에 대한 DAG 실행을 트리거한다. (DAG Run ID는 고유해야 한다.)"""
    path = f"/dags/{dag_id}/dagRuns"
    body = {
        "dag_run_id": run_id,
        "logical_date": logical_date.isoformat(),
        "data_interval_start": logical_date.isoformat(),
        "data_interval_end": logical_date.add(seconds=1).isoformat(),
        "conf": conf,
    }
    response = request("POST", path, access_token, json=body, timeout=timeout)
    response.raise_for_status()
    return response.json()


def wait_for_completion(
        dag_id: str,
        run_id: str,
        access_token: str,
        poke_interval: int = 60,
        timeout: int = 60*10,
    ) -> str:
    """DAG Run ID에 대한 DAG 실행을 주기적으로 확인하면서 성공 또는 실패 시까지 대기한다."""
    import time
    path = f"/dags/{dag_id}/dagRuns/{run_id}"
    start_time = 0

    while start_time < timeout:
        response = request("GET", path, access_token)
        if response.ok:
            state = response.json().get("state", str())
            if state in ("success", "failed"):
                return state
        time.sleep(poke_interval)
        start_time += poke_interval
    return "timeout"
