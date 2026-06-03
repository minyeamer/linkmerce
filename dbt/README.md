# dbt 프로젝트 안내

> LinkMerce 적재 테이블 위에 분석 모델을 작성하기 위한 dbt 작업 공간

## 개요

`dbt/` 경로는 PostgreSQL 또는 BigQuery에 적재된 LinkMerce 테이블을 분석 모델로 정리하기 위한 별도 프로젝트다.
현재는 dbt 기본 프로젝트 구조와 예제 모델만 포함되어 있으며, 운영 모델은 아직 추가하지 않았다.

## 디렉터리 구조

```bash
dbt/
├── analyses/
├── macros/
├── models/
│   └── example/
├── seeds/
├── snapshots/
├── tests/
├── dbt_project.yml
└── README.md
```

## 프로젝트 설정

- **프로젝트명**: `linkmerce_dbt`
- **프로필명**: `linkmerce_dbt`
- **기본 모델 경로**: `models/`
- **예제 모델 materialization**: `view`

접속 정보는 dbt `profiles.yml`에서 관리한다.
대상 웨어하우스가 PostgreSQL인지 BigQuery인지에 따라 adapter와 profile 설정을 맞춘다.

## 실행 예시

```bash
cd dbt
dbt run
dbt test
```

운영 모델을 추가할 때는 `models/example/` 아래의 예제 모델을 그대로 확장하기보다,
플랫폼 또는 분석 주제별 하위 경로를 새로 만들고 해당 경로에 맞는 `schema.yml`을 함께 둔다.
