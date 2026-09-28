from __future__ import annotations

from typing import TYPE_CHECKING
import pendulum

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun


def get_datetime_range(
        channel_seq: str,
        dag_run: DagRun,
        etl_task_id: str,
        data_interval_end: pendulum.DateTime,
    ) -> dict[str, str]:
    """주문 조회 기간의 시작과 끝을 계산해 반환한다."""
    conf = (dag_run.conf or dict()).get("channels") or dict()
    channel_conf = conf.get(channel_seq) or conf.get('*') or dict()

    if channel_conf.get("skip") is True:
        from airflow.sdk.exceptions import AirflowSkipException
        raise AirflowSkipException(
            f"Task skipped for channel sequence '{channel_seq}' because the 'skip' flag is enabled"
        )

    if "start_datetime" in channel_conf:
        start_datetime = pendulum.parse(channel_conf["start_datetime"]).in_timezone("Asia/Seoul")
    else:
        start_datetime = get_start_datetime(channel_seq, [dag_run.dag_id], [etl_task_id], data_interval_end)

    if "end_datetime" in channel_conf:
        end_datetime = pendulum.parse(channel_conf["end_datetime"]).in_timezone("Asia/Seoul")
    else:
        end_datetime = data_interval_end.subtract(microseconds=1000)

    return {
        "start_datetime": start_datetime.format("YYYY-MM-DDTHH:mm:ss.SSSZ"),
        "end_datetime": end_datetime.format("YYYY-MM-DDTHH:mm:ss.SSSZ"),
    }


def get_start_datetime(
        channel_seq: str,
        dag_ids: list[str],
        task_ids: list[str],
        data_interval_end: pendulum.DateTime,
    ) -> pendulum.DateTime:
    """직전에 성공한 Dag에서 Map Index가 `channel_seq`와 일치하는 Task Instance의 실행 시간을 가져온다."""
    from airflow_api import authenticate, list_dagruns, list_task_instances, get_xcom_value
    common = {
        "access_token": authenticate(),
        "dag_ids": dag_ids,
        "logical_date_gte": data_interval_end.subtract(days=1),
        "logical_date_lte": data_interval_end,
        "states": ["success"],
        "page_limit": 100,
    }
    ti_params = {"task_ids": task_ids, "order_by": "-start_date"}

    for dag_run in list_dagruns(**common, order_by="-logical_date"):
        for ti in list_task_instances(**common, dag_run_ids=[dag_run["dag_run_id"]], **ti_params):
            if ti["rendered_map_index"] == channel_seq:
                try:
                    result = get_xcom_value(
                        dag_id = ti["dag_id"],
                        run_id = ti["dag_run_id"],
                        task_id = ti["task_id"],
                        access_token = common["access_token"],
                        map_index = ti["map_index"],
                    )
                    end_datetime = pendulum.parse(result["params"]["end_datetime"])
                    return end_datetime.in_timezone("Asia/Seoul").add(microseconds=1000)
                except (KeyError, TypeError, ValueError):
                    continue
    raise LookupError(f"No successful task result with end_datetime found for channel sequence '{channel_seq}'")
