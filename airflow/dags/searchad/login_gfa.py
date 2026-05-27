from airflow.sdk import DAG, task
from airflow.providers.slack.hooks.slack import SlackHook
from datetime import timedelta
from textwrap import dedent
import pendulum


with DAG(
    dag_id = "searchad_login_gfa",
    schedule = "0 5 * * *",
    start_date = pendulum.datetime(2026, 4, 9, tz="Asia/Seoul"),
    dagrun_timeout = timedelta(minutes=20),
    catchup = False,
    tags = ["priority:high", "searchad:cookies", "login:gfa", "schedule:daily", "time:morning"],
    doc_md = dedent("""
        # 네이버 성과형 디스플레이 광고 로그인 파이프라인

        ## 동작 방식
        1. 크롬 확장프로그램을 통해 Slack 채널 '_네이버-쿠키'에 업로드되는 로그인 쿠키를 추출한다.
        2. 네이버 로그인 쿠키를 가지고 네이버 광고주센터의 디스플레이 광고 계정을 인증한다.
        3. 쿠키에 'XSRF-TOKEN'이 추가되었다면 인증된 것으로 인식하고, 완성된 쿠키 문자열을 지정된 파일에 덮어쓴다.

        ## 인증 정보
        - 네이버 계정 목록(searchad.users)과 디스플레이 광고 계정 목록(searchad.gfa)을 'customer_id'로 매칭한다.
        - 네이버와 디스플레이 광고 계정 정보에 각각 쿠키 문자열을 저장할 파일 경로(cookies)가 필요하다.

        ## 예외 상황
        - 계정 보호조치, 비밀번호 변경 등의 사례로 네이버 로그인이 해제될 수 있다.
        - 네이버 로그인이 해제되면 직접 수동 로그인 후 실패한 Task를 재실행한다.
    """).strip(),
) as dag:

    PATH = "searchad.gfa.login"

    @task(task_id="read_configs", retries=3, retry_delay=timedelta(minutes=1))
    def read_configs() -> dict:
        from airflow_utils import read_config
        config = read_config(PATH)
        return {
            "slack_conn_id": config["slack_conn_id"],
            "channel_id": config["channel_id"],
        }

    @task(task_id="read_credentials", retries=3, retry_delay=timedelta(minutes=1))
    def read_credentials() -> list:
        """네이버 광고 계정 목록(`searchad.gfa`)과 사용자 목록(`searchad.users`)을 읽고,   
        `customer_id`로 매칭하여 로그인에 필요한 인증 정보를 구성한다.
        """
        from airflow_utils import read_credentials as read
        from linkmerce.utils.regex import regexp_extract

        gfa_accounts = read("searchad.gfa", skip_subpath=True)
        naver_users = read("searchad.users", skip_subpath=True)
        user_map = {user["customer_id"]: user for user in naver_users}

        credentials = list()
        for account in gfa_accounts:
            user = user_map.get(account["customer_id"])
            if not user:
                continue
            credentials.append({
                "account_no": account["account_no"],
                "customer_id": account["customer_id"],
                "userid": user["userid"],
                "passwd": user["passwd"],
                "save_to": {
                    "naver": regexp_extract(r"Path\(([^)]+)\)", user["cookies"]),
                    "gfa": regexp_extract(r"Path\(([^)]+)\)", account["cookies"]),
                },
                "total": len(gfa_accounts),
            })
        return credentials


    @task(task_id="login_gfa", map_index_template="{{ credentials['account_no'] }}")
    def login_gfa(credentials: dict, configs: dict, **kwargs) -> dict:
        from linkmerce.api.searchad.center import login as center_login
        from airflow_utils import get_datetime

        # 1. 네이버 쿠키 획득 (Slack 채널에서 현재 계정의 쿠키 가져오기)
        cookies = get_naver_cookies_from_slack(
            slack_conn_id = configs["slack_conn_id"],
            channel_id = configs["channel_id"],
            userid = credentials["userid"],
            datetime = get_datetime(kwargs),
            limit = (credentials["total"] * 3),
        )
        save_naver_cookies(cookies, credentials["save_to"]["naver"])

        # 2. 네이버 광고주센터 인증 (`XSRF-TOKEN` 발급 및 쿠키 저장)
        cookies = center_login(credentials["account_no"], cookies, credentials["save_to"]["gfa"])
        return {
            "account_no": credentials["account_no"],
            "userid": credentials["userid"],
            "cookies": center_login(credentials["account_no"], cookies, credentials["save_to"]["gfa"]),
            "status": True,
        }


    def fetch_slack_history(
            slack_hook: SlackHook,
            channel_id: str,
            datetime: pendulum.DateTime,
            limit: int = 30,
        ) -> list:
        """Slack 채널에서 전달된 날짜로부터 최근 3일간 전송된 메시지 목록을 가져온다."""
        day_start = datetime.start_of("day")
        oldest = str(day_start.subtract(days=2).timestamp())
        latest = str(day_start.add(days=1).timestamp())

        params = {"channel": channel_id, "oldest": oldest, "latest": latest, "limit": limit}
        response = slack_hook.client.conversations_history(**params)
        return response.get("messages") or list()


    def get_naver_cookies_from_slack(
            slack_conn_id: str,
            channel_id: str,
            userid: str,
            datetime: pendulum.DateTime,
            limit: int = 30,
        ) -> str:
        """Slack 채널에 텍스트 파일 형태로 업로드된 최신 네이버 쿠키를 가져온다.

        `userid`를 파일명으로 가지는 메시지가 없을 경우 `AuthenticationError`를 발생시킨다.
        """
        from linkmerce.common.exceptions import AuthenticationError
        import requests

        slack_hook = SlackHook(slack_conn_id=slack_conn_id)
        messages: list[dict] = fetch_slack_history(slack_hook, channel_id, datetime, limit)
        token = slack_hook.get_conn().token

        for message in messages:
            for file in (message.get("files") or list()):
                if not isinstance(file, dict):
                    continue
                url = file.get("url_private_download") or file.get("url_private")
                file_name = file.get("name") or file.get("title") or str()

                if url and (file_name == f"{userid}.txt"):
                    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
                    response.raise_for_status()
                    return response.text.strip()
        raise AuthenticationError(f"No message found containing the filename {userid}.txt.")


    def save_naver_cookies(cookies: str, save_to: str | None = None, mkdir: bool = True):
        """네이버 쿠키를 지정된 경로에 저장한다."""
        from pathlib import Path
        file_path = save_to if isinstance(save_to, Path) else Path(save_to)
        if mkdir:
            file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, 'w', encoding="utf-8") as file:
            file.write(cookies)
        return cookies


    (login_gfa
    .partial(configs=read_configs())
    .expand(credentials=read_credentials()))
