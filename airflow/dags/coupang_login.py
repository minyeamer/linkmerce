from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "coupang_login",
    schedule = MultipleCronTriggerTimetable(
        "41 5 * * *",
        "1 9 * * *",
        timezone = "Asia/Seoul",
    ),
    start_date = pendulum.datetime(2025, 10, 22, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    tags = ["priority:high", "coupang:cookies", "login:coupang", "schedule:daily", "time:morning"],
) as dag:

    # Replaced by domain-specific shell scripts due to access denied error in Airflow worker:
    #   - advertising -> `crontab/coupang_login_advertising.sh` at "41 5 * * *"
    #   - wing        -> `crontab/coupang_login_wing.sh` at "20 9 * * *"
    do_nothing = EmptyOperator(task_id="login_coupang")
