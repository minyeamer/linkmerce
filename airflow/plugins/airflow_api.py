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
        access_token: str,
        dag_ids: list[str] | None = None,
        logical_date_gte: pendulum.DateTime | None = None,
        logical_date_lte: pendulum.DateTime | None = None,
        states: list[Literal["scheduled", "pending", "queued", "running", "success", "failed"]] | None = None,
        order_by: str | None = None,
        page_offset: int = 0,
        page_limit: int = 100,
        timeout: int = 30,
        **params
    ) -> list[dict]:
    """조건에 맞는 Dag Run 목록을 조회한다.

    Parameters: dict
    ```python
    {
        "order_by": "string",
        "page_offset": 0,
        "page_limit": 100,
        "dag_ids": [
            "string"
        ],
        "states": [
            "queued",
            null
        ],
        "run_after_gte": "2000-01-01T00:00:00.000Z",
        "run_after_gt": "2000-01-01T00:00:00.000Z",
        "run_after_lte": "2000-01-01T00:00:00.000Z",
        "run_after_lt": "2000-01-01T00:00:00.000Z",
        "logical_date_gte": "2000-01-01T00:00:00.000Z",
        "logical_date_gt": "2000-01-01T00:00:00.000Z",
        "logical_date_lte": "2000-01-01T00:00:00.000Z",
        "logical_date_lt": "2000-01-01T00:00:00.000Z",
        "start_date_gte": "2000-01-01T00:00:00.000Z",
        "start_date_gt": "2000-01-01T00:00:00.000Z",
        "start_date_lte": "2000-01-01T00:00:00.000Z",
        "start_date_lt": "2000-01-01T00:00:00.000Z",
        "end_date_gte": "2000-01-01T00:00:00.000Z",
        "end_date_gt": "2000-01-01T00:00:00.000Z",
        "end_date_lte": "2000-01-01T00:00:00.000Z",
        "end_date_lt": "2000-01-01T00:00:00.000Z",
        "duration_gte": 0,
        "duration_gt": 0,
        "duration_lte": 0,
        "duration_lt": 0,
        "conf_contains": "string"
    }
    ```

    Returns: list[dict]
    ```python
    [{
        "dag_run_id": "string",
        "dag_id": "string",
        "logical_date": "2000-01-01T00:00:00.000Z",
        "queued_at": "2000-01-01T00:00:00.000Z",
        "start_date": "2000-01-01T00:00:00.000Z",
        "end_date": "2000-01-01T00:00:00.000Z",
        "duration": 0,
        "data_interval_start": "2000-01-01T00:00:00.000Z",
        "data_interval_end": "2000-01-01T00:00:00.000Z",
        "run_after": "2000-01-01T00:00:00.000Z",
        "last_scheduling_decision": "2000-01-01T00:00:00.000Z",
        "run_type": "backfill",
        "state": "queued",
        "triggered_by": "cli",
        "triggering_user_name": "string",
        "conf": {
            "additionalProp1": {}
        },
        "note": "string",
        "dag_versions": [
            {
            "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
            "version_number": 0,
            "dag_id": "string",
            "bundle_name": "string",
            "bundle_version": "string",
            "created_at": "2000-01-01T00:00:00.000Z",
            "dag_display_name": "string",
            "bundle_url": "string"
            }
        ],
        "bundle_version": "string",
        "dag_display_name": "string",
        "partition_key": "string",
        "partition_date": "2000-01-01T00:00:00.000Z"
    }]
    ```
    """
    body = params | {"page_offset": max(0, page_offset), "page_limit": max(1, page_limit)}
    if dag_ids:
        body["dag_ids"] = dag_ids
    if logical_date_gte is not None:
        body["logical_date_gte"] = logical_date_gte.isoformat()
    if logical_date_lte is not None:
        body["logical_date_lte"] = logical_date_lte.isoformat()
    if states:
        body["states"] = states
    if order_by:
        body["order_by"] = order_by

    response = request("POST", "/dags/~/dagRuns/list", access_token, json=body, timeout=timeout)
    response.raise_for_status()
    try:
        return response.json()["dag_runs"]
    except Exception:
        return list()


def list_task_instances(
        access_token: str,
        dag_ids: list[str] | None = None,
        dag_run_ids: list[str] | None = None,
        task_ids: list[str] | None = None,
        logical_date_gte: pendulum.DateTime | None = None,
        logical_date_lte: pendulum.DateTime | None = None,
        states: list[Literal["scheduled", "pending", "queued", "running", "success", "failed"]] | None = None,
        order_by: str | None = None,
        page_offset: int = 0,
        page_limit: int = 100,
        timeout: int = 30,
        **params
    ) -> list[dict]:
    """조건에 맞는 Task Instance 목록을 조회한다.

    Parameters: dict
    ```python
    {
        "dag_ids": [
            "string"
        ],
        "dag_run_ids": [
            "string"
        ],
        "task_ids": [
            "string"
        ],
        "state": [
            "removed",
            null
        ],
        "run_after_gte": "2000-01-01T00:00:00.000Z",
        "run_after_gt": "2000-01-01T00:00:00.000Z",
        "run_after_lte": "2000-01-01T00:00:00.000Z",
        "run_after_lt": "2000-01-01T00:00:00.000Z",
        "logical_date_gte": "2000-01-01T00:00:00.000Z",
        "logical_date_gt": "2000-01-01T00:00:00.000Z",
        "logical_date_lte": "2000-01-01T00:00:00.000Z",
        "logical_date_lt": "2000-01-01T00:00:00.000Z",
        "start_date_gte": "2000-01-01T00:00:00.000Z",
        "start_date_gt": "2000-01-01T00:00:00.000Z",
        "start_date_lte": "2000-01-01T00:00:00.000Z",
        "start_date_lt": "2000-01-01T00:00:00.000Z",
        "end_date_gte": "2000-01-01T00:00:00.000Z",
        "end_date_gt": "2000-01-01T00:00:00.000Z",
        "end_date_lte": "2000-01-01T00:00:00.000Z",
        "end_date_lt": "2000-01-01T00:00:00.000Z",
        "duration_gte": 0,
        "duration_gt": 0,
        "duration_lte": 0,
        "duration_lt": 0,
        "pool": [
            "string"
        ],
        "queue": [
            "string"
        ],
        "executor": [
            "string"
        ],
        "page_offset": 0,
        "page_limit": 100,
        "order_by": "string"
    }
    ```

    Returns: list[dict]
    ```python
    [{
        "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "task_id": "string",
        "dag_id": "string",
        "dag_run_id": "string",
        "map_index": 0,
        "logical_date": "2000-01-01T00:00:00.000Z",
        "run_after": "2000-01-01T00:00:00.000Z",
        "start_date": "2000-01-01T00:00:00.000Z",
        "end_date": "2000-01-01T00:00:00.000Z",
        "duration": 0,
        "state": "removed",
        "try_number": 0,
        "max_tries": 0,
        "task_display_name": "string",
        "dag_display_name": "string",
        "hostname": "string",
        "unixname": "string",
        "pool": "string",
        "pool_slots": 0,
        "queue": "string",
        "priority_weight": 0,
        "operator": "string",
        "operator_name": "string",
        "queued_when": "2000-01-01T00:00:00.000Z",
        "scheduled_when": "2000-01-01T00:00:00.000Z",
        "pid": 0,
        "executor": "string",
        "executor_config": "string",
        "note": "string",
        "rendered_map_index": "string",
        "rendered_fields": {
            "additionalProp1": {}
        },
        "trigger": {
            "id": 0,
            "classpath": "string",
            "created_date": "2000-01-01T00:00:00.000Z",
            "queue": "string",
            "triggerer_id": 0
        },
        "triggerer_job": {
            "id": 0,
            "dag_id": "string",
            "state": "string",
            "job_type": "string",
            "start_date": "2000-01-01T00:00:00.000Z",
            "end_date": "2000-01-01T00:00:00.000Z",
            "latest_heartbeat": "2000-01-01T00:00:00.000Z",
            "executor_class": "string",
            "hostname": "string",
            "unixname": "string",
            "dag_display_name": "string"
        },
        "dag_version": {
            "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
            "version_number": 0,
            "dag_id": "string",
            "bundle_name": "string",
            "bundle_version": "string",
            "created_at": "2000-01-01T00:00:00.000Z",
            "dag_display_name": "string",
            "bundle_url": "string"
        }
    }]
    ```
    """
    body = params | {"page_offset": page_offset, "page_limit": page_limit}
    if dag_ids:
        body["dag_ids"] = dag_ids
    if dag_run_ids:
        body["dag_run_ids"] = dag_run_ids
    if task_ids:
        body["task_ids"] = task_ids
    if logical_date_gte is not None:
        body["logical_date_gte"] = logical_date_gte.isoformat()
    if logical_date_lte is not None:
        body["logical_date_lte"] = logical_date_lte.isoformat()
    if states:
        body["state"] = states
    if order_by:
        body["order_by"] = order_by

    response = request("POST", "/dags/~/dagRuns/~/taskInstances/list", access_token, json=body, timeout=timeout)
    response.raise_for_status()
    try:
        return response.json()["task_instances"]
    except Exception:
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
            state = (response.json() or dict()).get("state", str())
            if state in ("success", "failed"):
                return state
        time.sleep(poke_interval)
        start_time += poke_interval
    return "timeout"


def get_xcom_value(
        dag_id: str,
        run_id: str,
        task_id: str,
        access_token: str,
        key: str = "return_value",
        map_index: int = -1,
        timeout: int = 30,
    ):
    """DAG Run의 특정 Task에서 XCom 값을 조회한다."""
    path = f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}"
    response = request("GET", path, access_token, params={"map_index": map_index}, timeout=timeout)
    response.raise_for_status()
    return response.json()["value"]


def get_task_xcom_values(
        dag_id: str,
        run_id: str,
        task_id: str,
        access_token: str,
        key: str = "return_value",
        page_offset: int = 0,
        page_limit: int = 100,
        timeout: int = 30,
    ) -> list:
    """Dag Run의 Task에서 반환한 모든 XCom 값을 조회한다."""
    values = list()
    task_instances = list_task_instances(
        access_token = access_token,
        dag_ids = [dag_id],
        dag_run_ids = [run_id],
        task_ids = [task_id],
        page_offset = page_offset,
        page_limit = page_limit,
        timeout = timeout,
    )
    for task_instance in task_instances:
        try:
            values.append(get_xcom_value(
                dag_id = dag_id,
                run_id = run_id,
                task_id = task_id,
                access_token = access_token,
                key = key,
                map_index = task_instance["map_index"],
                timeout = timeout,
            ))
        except Exception:
            continue
    return values
