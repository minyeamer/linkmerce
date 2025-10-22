from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_login",
    schedule = "20 9 * * *",
    start_date = pendulum.datetime(2025, 10, 22, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = ["priority:high", "coupang:cookies", "login:coupang", "schedule:daily", "time:morning"],
) as dag:

    # Replaced with `crontab/coupang_login.sh` due to access denied error in Airflow worker
    do_nothing = EmptyOperator(task_id="login_coupang")
