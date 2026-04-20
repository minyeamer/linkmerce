from __future__ import annotations

from croniter import croniter
import datetime as dt
import os
import pendulum
import requests
import time


AIRFLOW_WWW_BASE_URL = os.environ.get("AIRFLOW_WWW_BASE_URL", "http://localhost")
AIRFLOW_WWW_PORT = os.environ.get("AIRFLOW_WWW_PORT", "8080")
AIRFLOW_URL = f"{AIRFLOW_WWW_BASE_URL}:{AIRFLOW_WWW_PORT}"

AIRFLOW_USERNAME = os.environ.get("AIRFLOW_WWW_USER_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_WWW_USER_PASSWORD", "airflow")

# ANSI 이스케이프 시퀀스 - 색상 코드
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"

KST = pendulum.timezone("Asia/Seoul")
NOW = pendulum.now(KST)
print(f"NOW(KST): {NOW.strftime('%Y-%m-%d %H:%M:%S %Z')}")


def last_cron_utc(cron_expr: str) -> str:
    """KST 기반 cron 표현식의 가장 최근 과거 실행 시점을 UTC ISO 문자열로 반환한다."""
    cron = croniter(cron_expr, dt.datetime(*NOW.timetuple()[:6]))
    prev = cron.get_prev(dt.datetime)
    prev_kst = pendulum.datetime(*prev.timetuple()[:6], tz=KST)
    return prev_kst.in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def last_multi_cron_utc(cron_exprs: list[str]) -> str:
    """MultipleCronTriggerTimetable: 여러 cron 표현식 중 가장 최근 과거 실행 시점을 반환한다."""
    return max(last_cron_utc(expr) for expr in cron_exprs)


# `schedule=None`인 DAG은 제외
SCHEDULED_DAGS = {
    # 매일 1회 ───────────────────────────────────────────────────────────────
    "cj_loisparcel_invoice": last_cron_utc("0 2 * * *"), # 02:00 KST
    "google_ads": last_cron_utc("50 7 * * *"), # 07:50 KST
    "meta_ads": last_cron_utc("40 7 * * *"), # 07:40 KST
    "coupang": last_cron_utc("20 8 * * *"), # 08:20 KST
    "naver_hcenter_login": last_cron_utc("0 1 * * *"), # 01:00 KST
    "naver_brand_price": last_cron_utc("1 0 * * *"), # 00:01 KST
    "sabangnet_order": last_cron_utc("30 23 * * *"), # 23:30 KST
    "smartstore_bizdata": last_cron_utc("10 8 * * *"), # 08:10 KST
    "smartstore_order": last_cron_utc("30 8 * * *"), # 08:30 KST
    "searchad_contract": last_cron_utc("30 5 * * *"), # 05:30 KST
    "searchad_report_gfa": last_cron_utc("20 5 * * *"), # 05:20 KST
    "searchad_report_sad": last_cron_utc("40 5 * * *"), # 05:40 KST
    # 평일(월-금)만 ───────────────────────────────────────────────────────────
    "sabangnet_product": last_cron_utc("20 23 * * 1-5"), # 23:20 KST
    "smartstore_product": last_cron_utc("30 23 * * 1-5"), # 23:30 KST
    "searchad_master_gfa": last_cron_utc("30 5 * * 1-5"), # 05:30 KST
    "searchad_master_sad": last_cron_utc("40 23 * * 1-5"), # 23:40 KST
    # 매일 n회 ───────────────────────────────────────────────────────────────
    "naver_shop_rank": last_cron_utc("0 6-18 * * *"), # 06~18시 정각
    "naver_cafe_search": last_cron_utc("0,10,20,30,40,50 8,9 * * *"), # 08~09시 10분 간격
    "naver_main_search": last_cron_utc("0,10,20,30,40,50 8,9 * * *"), # 08~09시 10분 간격
    # MultipleCronTriggerTimetable ──────────────────────────────────────────
    "sabangnet_invoice": last_multi_cron_utc(["30 10 * * 1-5", "30 14 * * 1-5", "50 23 * * 1-5"]),
    # ㄴ 평일 10:30 / 14:30 / 23:50
    "smartstore_invoice": last_multi_cron_utc(["0 3 * * *", "30 10 * * 1-5", "0 15 * * 1-5"]),
    # ㄴ 매일 03:00 / 평일 10:30 / 15:00
}


def build_auth_headers() -> dict[str, str]:
    """Airflow REST API 사용을 위한 액세스 토큰을 발급받고, 토큰을 HTTP 요청 헤더에 포함해 반환한다."""
    import os
    import requests
    url = f"{AIRFLOW_URL}/auth/token"
    body = {"username": AIRFLOW_USERNAME, "password": AIRFLOW_PASSWORD}
    headers = {"Content-Type": "application/json"}
    with requests.post(url, json=body, headers=headers, timeout=30) as response:
        response.raise_for_status()
        access_token = response.json()["access_token"]
        return {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}


def mark_dag_success(dag_id: str, logical_date: str, headers: dict[str, str]):
    """최근 과거 실행 시점을 기준으로 DAG을 실행 요청하고, 즉시 `success` 상태로 변경한다."""
    print(f"--- Processing {BLUE}{dag_id}{RESET} @ {BLUE}{logical_date}{RESET} ---")

    # 1. Trigger DagRun
    trigger_url = f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns"
    trigger_body = {
        "logical_date": logical_date,
        "note": "Initial setup: marked as success to prevent immediate execution"
    }
    trigger_response = requests.post(trigger_url, json=trigger_body, headers=headers)

    if trigger_response.status_code in [200, 201]:
        run_id = trigger_response.json()["dag_run_id"]
        print(f"{GREEN}Successfully triggered:{RESET} {run_id}")
    elif trigger_response.status_code == 409:
        print(f"{YELLOW}SKIP: DAG run already exists.{RESET}")
        return
    else:
        print(f"{RED}Failed to trigger: {trigger_response.status_code} - {trigger_response.text}{RESET}")
        return

    # 2. Mark as Success
    patch_url = f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns/{run_id}"
    patch_body = {"state": "success"}
    patch_response = requests.patch(patch_url, json=patch_body, headers=headers)

    if patch_response.status_code == 200:
        print(f"{GREEN}DONE: '{run_id}' marked as SUCCESS.{RESET}")
    else:
        print(f"{RED}Error marking success: {patch_response.status_code} - {patch_response.text}{RESET}")
    return


if __name__ == "__main__":
    headers = build_auth_headers()
    for dag_id, last_date in SCHEDULED_DAGS.items():
        mark_dag_success(dag_id, last_date, headers)
        time.sleep(0.5)
    print(f"\n{GREEN}--- {len(SCHEDULED_DAGS)} scheduled DAGs have been processed ---{RESET}")
