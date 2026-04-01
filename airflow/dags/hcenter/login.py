from airflow.sdk import DAG, task
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "smartstore_login",
    schedule = "0 1 * * *",
    start_date = pendulum.datetime(2025, 9, 8, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(hours=1),
    catchup = False,
    tags = ["priority:high", "hcenter:cookies", "login:hcenter", "schedule:daily", "time:night", "playwright:true"],
    doc_md = dedent("""
        # 네이버 쇼핑파트너센터 로그인 파이프라인

        브랜드스토어 권한이 있는 스마트스토어 판매자 계정으로 스마트스토어센터에 로그인 후,
        채널 권한을 가지고 네이버 쇼핑파트너센터로 전환한다.
        브랜드 애널리틱스 메뉴까지 도달한 후 완성된 쿠키 문자열을 지정된 파일에 덮어쓴다.
    """).strip(),
) as dag:

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        """스마트스토어(`smartstore`) 인증 정보에서 판매자 계정(`users`) 중 첫 번째 항목을 조회한다.
        쇼핑파트너센터 인증 정보(`hcenter`)의 쿠키 파일 경로를 계정 정보에 더해 반환한다."""
        from linkmerce.api.config import read_config
        from linkmerce.utils.regex import regexp_extract
        from airflow.sdk import Variable
        from typing import Sequence

        credentials = read_config(Variable.get("credentials"))["smartstore"]
        user = users[0] if isinstance((users := credentials["users"]), Sequence) else users
        cookies_path = {"$cookies": Variable.get("cookies")}

        def extract_path(cookies: str) -> str:
            return regexp_extract(r"Path\(([^)]+)\)", cookies.format(cookies_path))

        return [{
            "userid": user["userid"],
            "passwd": user["passwd"],
            "channel_seq": user["channel_seq"],
            "save_to": extract_path(credentials["hcenter"]["cookies"]),
        }]

    @task(task_id="login_hcenter", map_index_template="{{ credentials['channel_seq'] }}", retries=3, retry_delay=timedelta(minutes=1), pool="login_pool")
    def login_hcenter(credentials: dict, **kwargs) -> str:
        """네이버 쇼핑파트너센터에 로그인하고 쿠키 문자열을 지정된 파일에 덮어쓴다."""
        from linkmerce.api.smartstore.hcenter import login

        def preview(cookies: str, size: int = 100) -> str:
            """쿠키를 최대 100자까지 로그에 출력한다."""
            return (cookies[:size] + "...") if len(cookies) > size else cookies

        return dict(cookies = preview(login(**credentials)))


    login_hcenter.expand(credentials=read_credentials())
