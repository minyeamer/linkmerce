# LinkMerce FastAPI Airflow Trigger

> 로컬 네트워크에서 Google Sheets 기반 Airflow Dag을 실행하기 위한 FastAPI 서버를 설명한다.

## 목차

- [개요](#개요)
- [한눈에 보기](#한눈에-보기)
- [실행](#실행)
- [실행 URL](#실행-url)
- [환경변수](#환경변수)
- [동작 규칙](#동작-규칙)

## 개요

`airflow_trigger/fastapi`는 외부 링크나 Google Sheets 버튼에서
Airflow Dag을 실행하기 위한 작은 HTTP 서버다.

브라우저에서 `/trigger` 페이지에 접속하면 로딩 화면을 표시하고,
서버가 Airflow access token을 직접 발급받아 Dag run을 생성한다.
Dag run이 종료되면 사용자 안내 팝업을 띄운 뒤 페이지 종료를 시도한다.

프로그램 호출이 필요한 경우에는 `/api/trigger`를 사용한다.
이 엔드포인트는 같은 실행 로직을 JSON 응답으로 반환한다.

## 한눈에 보기

- **기본 포트**: `16160`
- **서버 프레임워크**: FastAPI
- **템플릿 엔진**: Jinja2
- **Airflow 인증**: `/auth/token`
- **Airflow API**: `/api/v2/dags/{dag_id}/dagRuns`
- **설정 방식**: 환경변수
- **기본 URL**: `http://localhost:16160`

## 실행

Docker Compose로 이미지를 빌드하고 컨테이너를 실행한다.

```bash
docker compose up -d --build
```

실행 중인 컨테이너를 다시 시작할 때는 다음 명령어를 사용한다.

```bash
docker compose restart
```

## 실행 URL

브라우저에서 접속할 때는 `/trigger`를 사용한다.

```text
http://localhost:16160/trigger?dag_id=gsheets_opex
```

화면에 표시할 작업 이름을 따로 지정하려면 `dag_name`을 함께 전달한다.
`dag_name`은 사용자 화면 문구에만 사용하며, 실제 실행 대상은 항상 `dag_id`로 결정한다.

```text
http://localhost:16160/trigger?dag_id=gsheets_opex&dag_name=운영비용%20업데이트
```

`dag_name`이 없거나 비어 있으면 화면에도 `dag_id`를 표시한다.

JSON 응답이 필요한 프로그램 호출에서는 `/api/trigger`를 사용한다.

```text
http://localhost:16160/api/trigger?dag_id=gsheets_opex
```

`wait=false`를 붙이면 Dag 실행 요청 직후 응답한다.

```text
http://localhost:16160/api/trigger?dag_id=gsheets_opex&wait=false
```

## 환경변수

| 변수 | 기본값 | 설명 |
| --- | --- | --- |
| `AIRFLOW_TRIGGER_IMAGE_NAME` | `linkmerce-airflow-trigger:latest` | Compose에서 빌드하고 실행할 이미지 이름 |
| `AIRFLOW_TRIGGER_PORT` | `16160` | 호스트에서 열 FastAPI 포트 |
| `AIRFLOW_SERVER_URL` | `http://host.docker.internal:8080` | FastAPI 컨테이너에서 접근할 Airflow API 주소 |
| `AIRFLOW_WWW_USER_USERNAME` | `airflow` | Airflow access token 발급 계정 |
| `AIRFLOW_WWW_USER_PASSWORD` | `airflow` | Airflow access token 발급 비밀번호 |
| `AIRFLOW_TRIGGER_ALLOWED_DAGS` | 빈 값 | 실행을 허용할 Dag ID 목록 |
| `AIRFLOW_TRIGGER_WAIT_TIMEOUT` | `600` | Dag 종료 대기 제한 시간(초) |
| `AIRFLOW_TRIGGER_WAIT_INTERVAL` | `5` | Dag 상태 확인 주기(초) |
| `AIRFLOW_TRIGGER_REQUEST_TIMEOUT` | `30` | Airflow API 요청 제한 시간(초) |

`AIRFLOW_TRIGGER_ALLOWED_DAGS`는 쉼표로 여러 값을 지정한다.
값이 없거나 비어 있으면 모든 Dag을 허용한다.

## 동작 규칙

서버는 요청마다 Airflow 계정으로 access token을 발급받는다.
클라이언트는 별도의 Airflow token을 전달하지 않는다.

Dag run 생성 시 다음 값만 전달한다.

- `dag_run_id`
- `logical_date`
- `data_interval_start`
- `data_interval_end`

`conf`나 다른 커스텀 파라미터는 전달하지 않는다.
`logical_date`는 서버의 현재 UTC 시각을 기준으로 생성한다.

`/trigger` 페이지는 Dag run이 종료될 때까지 기다린다.
성공하면 완료 안내만 표시하고, 실행 식별자는 사용자에게 노출하지 않는다.
