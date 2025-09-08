from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "all_login",
    schedule = "0 1 * * *",
    start_date = pendulum.datetime(2025, 9, 8, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(hours=1),
    catchup = False,
    tags = ["priority:high", "all:cookies", "login:partnercenter", "schedule:daily", "time:night"],
) as dag:

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> dict:
        from airflow.sdk import Variable
        from linkmerce.api.config import read_config
        return read_config(Variable.get("credentials"))

    def extract_path(cookies: str) -> str:
        from linkmerce.utils.regex import regexp_extract
        return regexp_extract(r"Path\(([^)]+)\)", cookies)


    @task(task_id="login_partner_center")
    def login_partner_center(ti: TaskInstance, **kwargs) -> str:
        credentials = ti.xcom_pull(task_ids="read_credentials")
        from linkmerce.api.smartstore.brand import login
        from typing import Sequence
        user = users[0] if isinstance((users := credentials["smartstore"]["users"]), Sequence) else users
        save_to = extract_path(credentials["smartstore"]["brand"]["cookies"])
        return dict(cookies = login(**user, save_to=save_to))


    read_credentials() >> [login_partner_center()]
