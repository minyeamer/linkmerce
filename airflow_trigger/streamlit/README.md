# LinkMerce Streamlit Airflow Trigger

> 운영 담당자가 화면에서 날짜와 차수를 선택해 Airflow Dag을 실행하는 Streamlit UI를 설명한다.

## 목차

- [개요](#개요)
- [한눈에 보기](#한눈에-보기)
- [실행 흐름](#실행-흐름)
- [설정](#설정)
- [Docker 실행](#docker-실행)
- [로컬 실행](#로컬-실행)
- [화면 입력](#화면-입력)
- [Dag Run 생성 규칙](#dag-run-생성-규칙)
- [참고](#참고)

## 개요

`airflow_trigger/streamlit`은 Airflow Dag을 수동으로 실행하기 위한 운영용 Streamlit UI다.

현재 앱은 `sabangnet_order` Dag을 대상으로 한다.
사용자는 작업 일시 범위와 차수를 선택하고, 앱은 Airflow REST API로 Dag run을 생성한다.

## 한눈에 보기

- **기본 포트**: `8501`
- **UI 프레임워크**: Streamlit
- **대상 Dag**: `sabangnet_order`
- **Airflow 인증**: `/auth/token`
- **Airflow API**: `/api/v2/dags/{dag_id}/dagRuns`
- **설정 방식**: Streamlit secrets, `.env`
- **기본 URL**: `http://localhost:8501`

## 실행 흐름

1. Streamlit secrets에서 Airflow 접속 정보를 읽는다.
2. `/auth/token`으로 Airflow access token을 발급받는다.
3. 화면에서 선택한 작업, 날짜 유형, 차수, 시작/종료 일시로 Dag run 정보를 만든다.
4. 같은 `logical_date`의 기존 Dag run이 있으면 삭제한다.
5. `/api/v2/dags/{dag_id}/dagRuns`로 새 Dag run을 생성한다.
6. 실행 요청 결과, 상태, 실행 일시, 데이터 구간을 화면에 표시한다.

## 설정

Airflow 접속 정보는 `airflow_trigger/streamlit/.streamlit/secrets.toml`에 둔다.

| 변수 | 설명 |
| --- | --- |
| `AIRFLOW_SERVER_URL` | Streamlit 컨테이너 또는 로컬 프로세스에서 접근할 Airflow API 주소 |
| `AIRFLOW_WWW_USER_USERNAME` | Airflow access token 발급 계정 |
| `AIRFLOW_WWW_USER_PASSWORD` | Airflow access token 발급 비밀번호 |

Docker Compose 실행 옵션은 `.env`에서 지정한다.

| 변수 | 기본값 | 설명 |
| --- | --- | --- |
| `AIRFLOW_STREAMLIT_IMAGE_NAME` | `streamlit:1.56.0` | Compose에서 빌드하고 실행할 이미지 이름 |
| `AIRFLOW_STREAMLIT_PORT` | `8501` | 호스트에서 열 Streamlit 포트 |

## Docker 실행

Docker Compose로 이미지를 빌드하고 컨테이너를 실행한다.

```bash
docker compose up -d --build
```

실행 후 브라우저에서 다음 주소로 접속한다.

```text
http://localhost:8501
```

컨테이너에는 아래 파일과 디렉터리를 마운트한다.

- `app.py` -> `/app/app.py`
- `.streamlit` -> `/app/.streamlit`

앱 코드나 secrets를 수정한 뒤에는 컨테이너 재시작만으로 반영할 수 있다.

```bash
docker compose restart
```

## 로컬 실행

Docker 없이 실행할 때는 필요한 패키지를 설치한 뒤 Streamlit을 실행한다.

```bash
pip install -r requirements.txt
streamlit run app.py
```

`run.sh`를 사용할 수도 있다.

```bash
./run.sh
```

## 화면 입력

| 입력 | 설명 |
| --- | --- |
| 작업 선택 | 현재 `사방넷 주문서확인처리`만 제공하며 `sabangnet_order` Dag을 실행 |
| 일자 유형 | 현재 `수집일`만 제공 |
| 차수 | `1`부터 `9`까지 선택 |
| 시작 일자/시간 | `logical_date`와 `data_interval_start` 계산에 사용 |
| 종료 일자/시간 | `data_interval_end`와 `dag_run_id` 계산에 사용 |

차수가 `1`이면 기본 시간은 `09:00:00`부터 `09:59:59`까지다.
그 외 차수는 `13:00:00`부터 `13:59:59`까지를 기본값으로 사용한다.

시간 입력은 `HH:MM:SS` 형식이어야 한다.

## Dag Run 생성 규칙

날짜와 시간은 화면에서 KST 기준으로 입력한다.
Airflow API에 전달할 때는 UTC 문자열로 변환한다.

| 값 | 생성 규칙 |
| --- | --- |
| `logical_date` | 화면에서 선택한 시작 일시를 UTC로 변환 |
| `data_interval_start` | 화면에서 선택한 시작 일시를 UTC로 변환 |
| `data_interval_end` | 화면에서 선택한 종료 일시를 UTC로 변환 |
| `dag_run_id` | `api__{차수명}__{종료일시}` 형식 |

예시는 다음과 같다.

```text
api__1st__2026-06-23T00:59:59+00:00
```

앱은 같은 `logical_date`의 기존 Dag run을 먼저 조회한다.
기존 값이 있으면 삭제한 뒤 새 Dag run을 생성한다.

## 참고

- Airflow API는 `/api/v2` 엔드포인트를 사용한다.
- Airflow 로그인에 실패하면 Dag run 생성 요청을 보내지 않는다.
- 실행 실패 시 Airflow API 응답 메시지를 화면에 표시한다.
