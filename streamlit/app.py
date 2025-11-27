import streamlit as st
import datetime as dt
import requests

from typing import Sequence


AIRFLOW_SERVER_URL = st.secrets["AIRFLOW_SERVER_URL"]
AIRFLOW_USERNAME = st.secrets["AIRFLOW_WWW_USER_USERNAME"]
AIRFLOW_PASSWORD = st.secrets["AIRFLOW_WWW_USER_PASSWORD"]

DATETIME_FORMAT = "%Y년 %-m월 %-d일<br>%-H시 %-M분 %-S초"

DAG_RUN_STATUS = {
    "success": "완료",
    "running": "실행 중",
    "failed": "실패(오류)",
    "skipped": "실패(스킵)",
    "upstream_failed": "실패(외부요인)",
    "queued": "대기 중",
    "none": "없음",
    "shutdown": "중단",
    "up_for_retry": "재시도 대기 중",
    "up_for_reschedule": "재스케줄 대기 중",
    "deferred": "이벤트 대기 중",
    "scheduled": "실행 예약",
    "removed": "작업 삭제",
}


class DagExecutor:
    access_token: str | None = None

    def __init__(self, session: requests.Session):
        self.session = session

    def auth_token(self):
        url = "{}/auth/token".format(AIRFLOW_SERVER_URL)
        body = dict(username=AIRFLOW_USERNAME, password=AIRFLOW_PASSWORD)
        with self.session.post(url, json=body) as response:
            if response.ok:
                self.access_token = response.json()["access_token"]

    def dag_run(
            self,
            dag_id: str,
            dag_run_id: str,
            logical_date: dt.datetime,
            data_interval_start: dt.datetime,
            data_interval_end: dt.datetime,
            **kwargs
        ) -> dict:
        url = "{}/api/v2/dags/{}/dagRuns".format(AIRFLOW_SERVER_URL, dag_id)
        headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}
        body = dict(
            dag_run_id = dag_run_id,
            logical_date = kst_to_utc(logical_date),
            data_interval_start = kst_to_utc(data_interval_start),
            data_interval_end = kst_to_utc(data_interval_end),
        )
        with self.session.post(url, json=body, headers=headers) as response:
            if response.ok:
                results = response.json()
                results["status_code"] = response.status_code
                return results
            else:
                return dict(state="failed", status_code=response.status_code, message=response.text)


def kst_to_utc(datetime: dt.datetime) -> str:
    return (datetime - dt.timedelta(hours=9)).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_to_kst(date_string: str) -> dt.datetime:
    utc_dt = dt.datetime.strptime(date_string[:19], "%Y-%m-%dT%H:%M:%S")
    return utc_dt + dt.timedelta(hours=9)


def round_dt(datetime: dt.datetime) -> dt.datetime:
    delta = 60 - datetime.second
    if delta < 30:
        return datetime + dt.timedelta(seconds=delta)
    else:
        return datetime - dt.timedelta(seconds=delta)


def build():

    # [Div0] Page Title
    st.title("DB 업로드")


    # [Div1] DAG Selection Controls
    col11, col12, col13 = st.columns([2,1,1])
    with col11:
        dag_id = {"사방넷 주문서확인처리": "sabangnet_order"}
        dag_name = st.selectbox("작업 선택", options=["사방넷 주문서확인처리"])
    with col12:
        date_type = {"수집일": "reg_dm"}
        date_name = st.selectbox("일자 유형", options=["수집일"])
    with col13:
        order = st.selectbox("차수", options=list(range(1, 10)), index=0)
        order_name = {1: "1st", 2: "2nd", 3: "3rd"}.get(order, f"{order}th")
        start_delta = dt.timedelta(days=(1 if order == 1 else 0))
        start_index = (10 if order == 2 else 14)
        end_index = {1: 9, 2: 13}.get(order, 23)


    # [Div2] Date Selection Controls (Start Date)
    col20, col21, col22, col23 = st.columns([4,2,2,2])
    with col20:
        start_ymd = st.date_input("시작일", value=(dt.date.today() - start_delta))
    with col21:
        start_hour = st.selectbox("시작시간(시)", options=list(range(24)), index=start_index)
    with col22:
        start_minute = st.selectbox("시작시간(분)", options=list(range(60)), index=0)
    with col23:
        start_second = st.selectbox("시작시간(초)", options=list(range(60)), index=0)

    start_date = dt.datetime(start_ymd.year, start_ymd.month, start_ymd.day, start_hour, start_minute, start_second)


    # [Div3] Date Selection Controls (End Date)
    col30, col31, col32, col33 = st.columns([4,2,2,2])
    with col30:
        end_ymd = st.date_input("종료일", value=dt.date.today())
    with col31:
        end_hour = st.selectbox("종료시간(시)", options=list(range(24)), index=end_index)
    with col32:
        end_minute = st.selectbox("종료시간(분)", options=list(range(60)), index=59)
    with col33:
        end_second = st.selectbox("종료시간(초)", options=list(range(60)), index=59)

    end_date = dt.datetime(end_ymd.year, end_ymd.month, end_ymd.day, end_hour, end_minute, end_second)


    # [Div4] Confirmation and Execution
    col40, col41 = st.columns(2)
    with col40:
        if st.button("실행", type="primary"):
            with requests.Session() as session:
                executor = DagExecutor(session)
                executor.auth_token()
                if executor.access_token:
                    response = executor.dag_run(
                        dag_id = dag_id[dag_name],
                        dag_run_id = "api__{}__{}".format(order_name, kst_to_utc(round_dt(end_date))[:-1]+"+00:00"),
                        logical_date = start_date,
                        data_interval_start = start_date,
                        data_interval_end = end_date,
                    )
                    if response.get("state") != "failed":
                        st.success("실행 요청 성공!")
                        build_markdown([
                            ("ID", str(response["dag_run_id"]).replace('T', '<br>T')),
                            ("요청결과", f"{'성공' if response["status_code"] == 200 else '실패'}({response['status_code']})"),
                            ("실행상태", DAG_RUN_STATUS.get(response["state"], "알 수 없음")),
                            ("실행일시", utc_to_kst(response["queued_at"]).strftime(DATETIME_FORMAT)),
                            ("시작일시", utc_to_kst(response["data_interval_start"]).strftime(DATETIME_FORMAT)),
                            ("종료일시", utc_to_kst(response["data_interval_end"]).strftime(DATETIME_FORMAT)),
                        ], columns=["항목", "값"])
                    else:
                        st.error("DAG 실행 실패: {}".format(response.get("status_code")))
                        st.text(response.get("message", str()))
                else:
                    st.error("로그인 실패")
    with col41:
        if st.button("확인"):
            build_markdown([
                ("작업명", dag_name),
                ("차수", f"{order}차"),
                ("일자유형", date_name),
                ("시작일시", start_date.strftime(DATETIME_FORMAT)),
                ("종료일시", end_date.strftime(DATETIME_FORMAT)),
            ], columns=["항목", "값"])


def build_markdown(
        contents: Sequence[Sequence],
        columns: Sequence[str],
        header_first: bool = True,
        unsafe_allow_html: bool = True
    ):
    rows = [
        '|'+'|'.join(columns)+'|',
        '|'+'|'.join([":---:" if header_first else "---"] + ["---" for _ in columns[1:]])+'|',
        *['|'+'|'.join([
            f"**{row[0]}**" if header_first else str(row[0])]
            + list(map(str, row[1:])))+'|' for row in contents],
    ]
    st.markdown('\n'.join(rows), unsafe_allow_html=unsafe_allow_html)


def style():
    st.html(
"""
<style>
.stMarkdown div table {
    width: 100%;

    thead tr th {
        text-align: center;
    }
}
</style>

""")


style()
build()
