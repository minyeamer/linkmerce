from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cosmos import DbtTaskGroup


def generate_date_array(results: dict | list[dict], key_path: str | list[str]) -> list[str]:
    """선행 Task들의 결과에서 `key_path`에 해당하는 날짜 배열 값이 있으면 추출해 하나의 날짜 배열로 변환한다."""
    from linkmerce.utils.nested import hier_get
    import re
    date_array = set()

    for result in (results if isinstance(results, list) else [results]):
        values = hier_get(result, key_path, on_missing="ignore")
        for date_value in (values or list()):
            if re.match(r"^\d{4}-\d{2}-\d{2}", str(date_value)):
                date_array.add(str(date_value)[:10])

    return sorted(date_array)


def generate_dbt_date_range(results: dict | list[dict], key_path: str | list[str]) -> dict[str, str]:
    """선행 Task들의 결과에서 `key_path`에 해당하는 날짜 배열 값이 있으면 추출해 dbt 기간 매개변수로 변환한다."""
    date_array = generate_date_array(results, key_path)
    if date_array:
        return {
            "ds_start_date": min(date_array),
            "ds_end_date": max(date_array),
        }
    return dict()


def dynamic_mapping_dbt_bigquery(
        group_id: str,
        selector: str,
        operator_args: dict | None = None,
        operator_vars: str | dict | None = None,
        ds_task_id: str | None = None,
        **kwargs
    ) -> DbtTaskGroup:
    """
    다음과 같은 dbt_bigquery 프로젝트에 대한 공통 설정 Variable과
    주어진 `group_id`, `selector`, `operator_args`를 조합해 `DbtTaskGroup`을 생성한다.
    ```
    Variable["dbt_bigquery"] = {
        "project_config": {
            "dbt_project_path": "...",
            "install_dbt_deps": true | false = false
        },
        "profile_config": {
            "profile_name": "...",
            "target_name": "dev" | "prod" = "dev",
            "profile_mapping": {
                "conn_id": "..."
                "profile_args": {
                    "dataset": "...",
                    "location": "...",
                    "threads": 1,
                    "job_execution_timeout_seconds": 600
                }
            }
        },
        "operator_args": {
            "install_deps": true | false = false,
            "pool": "dbt_bigquery_pool"
        }
    }
    ```
    """
    from airflow.sdk import Variable
    from cosmos import DbtTaskGroup, ExecutionConfig, ExecutionMode, ProfileConfig, ProjectConfig, RenderConfig
    from cosmos.constants import LoadMode, TestBehavior
    from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

    config: dict = Variable.get("dbt_bigquery", deserialize_json=True)
    project_config: dict = config["project_config"]

    profile_config: dict = config["profile_config"]
    profile_mapping: dict = profile_config["profile_mapping"]
    profile_args: dict = profile_mapping["profile_args"]

    operator_args: dict = (config.get("operator_args") or dict()) | (operator_args or dict())

    return DbtTaskGroup(
        group_id = group_id,
        project_config = ProjectConfig(
            dbt_project_path = project_config["dbt_project_path"],
            install_dbt_deps = project_config.get("install_dbt_deps") or False,
        ),
        profile_config = ProfileConfig(
            profile_name = profile_config["profile_name"],
            target_name = profile_config["target_name"],
            profile_mapping = GoogleCloudServiceAccountDictProfileMapping(
                conn_id = profile_mapping["conn_id"],
                profile_args = {
                    "dataset": profile_args["dataset"],
                    "location": profile_args["location"],
                    "threads": (profile_args.get("threads") or 1),
                    "job_execution_timeout_seconds": (profile_args.get("job_execution_timeout_seconds") or 600),
                },
            ),
        ),
        execution_config = ExecutionConfig(
            execution_mode = ExecutionMode.LOCAL,
        ),
        render_config = RenderConfig(
            load_method = LoadMode.DBT_LS,
            selector = selector,
            test_behavior = TestBehavior.NONE,
        ),
        operator_args = _parse_operator_args(operator_args, operator_vars, ds_task_id),
    )


def _parse_operator_args(
        operator_args: dict,
        operator_vars: str | dict | None = None,
        ds_task_id: str | None = None,
    ) -> dict:
    """`operator_args`에 변수를 추가한다. `ds_task_id`가 있다면 해당 Task의 XCom 값으로부터 변수를 추출한다."""
    if ds_task_id:
        operator_vars = operator_vars if isinstance(operator_vars, dict) else dict()
        for ds_key in ["ds_start_date", "ds_end_date"]:
            operator_vars[ds_key] = "{{ ti.xcom_pull(task_ids='"+ds_task_id+"')['"+ds_key+"'] }}"

    if operator_vars is not None:
        if isinstance(operator_vars, dict):
            import json
            operator_args["vars"] = json.dumps(operator_vars, default=str)
        else:
            operator_args["vars"] = str(operator_vars)

    return operator_args
