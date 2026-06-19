"""
# PostgreSQL pg_partman 유지보수

PostgreSQL 'partman.run_maintenance_proc(...)' 함수를 호출해
일별 파티션을 미리 생성하고 pg_partman 메타데이터를 갱신한다.
"""

from airflow.sdk import DAG, task
from airflow.models.dagrun import DagRun
from datetime import timedelta
import pendulum


with DAG(
    dag_id = "postgres_partman_maintenance",
    schedule = "0 0 * * *",
    start_date = pendulum.datetime(2026, 6, 17, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=10),
    catchup = False,
    doc_md = __doc__,
    tags = ["priority:low", "platform:postgres", "objective:maintenance", "schedule:daily", "time:night"],
) as dag:

    def build_postgres_dsn(conn_id: str) -> str:
        """Airflow Connection으로부터 PostgreSQL DSN을 생성한다."""
        from airflow.sdk import Connection

        connection: Connection = Connection.get(conn_id)
        return "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(
            user = connection.login,
            password = connection.password,
            host = connection.host,
            port = connection.port,
            dbname = connection.schema,
        )


    @task(task_id="run_partman_maintenance")
    def run_partman_maintenance(dag_run: DagRun, **kwargs) -> dict:
        """PostgreSQL pg_partman 유지보수 쿼리를 실행한다."""
        import psycopg2

        pg_conn_id = (dag_run.conf or dict()).get("pg_conn_id") or "postgres"
        conn = psycopg2.connect(build_postgres_dsn(pg_conn_id))
        conn.autocommit = True
        try:
            with conn.cursor() as cursor:
                cursor.execute("CALL partman.run_maintenance_proc(p_wait := 0, p_analyze := false, p_jobmon := false);")
        finally:
            conn.close()

        return {"pg_conn_id": pg_conn_id, "status": "success"}


    run_partman_maintenance()
