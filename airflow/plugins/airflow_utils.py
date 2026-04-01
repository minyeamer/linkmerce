from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from linkmerce.utils.nested import KeyPath
    from pendulum.tz.timezone import Timezone, FixedTimezone
    import pendulum


def in_timezone(
        datetime: pendulum.DateTime,
        tz: str | Timezone | FixedTimezone | Literal["KST"] | None = "KST",
        add: dict | None = None,
        subtract: dict | None = None,
        subdays: int | None = None,
    ) -> pendulum.DateTime:
    """`pendulum.DateTime` 객체의 시간대를 지정하고, `delta`를 더하거나 빼는 연산을 한다.

    별도로 시간대를 지정하지 않았다면 기본으로 한국표준시(KST)를 설정한다."""
    from linkmerce.utils.date import in_timezone
    tz = ":default:" if tz == "KST" else tz
    return in_timezone(datetime, tz, add, subtract, subdays)


def format_date(
        datetime: pendulum.DateTime,
        fmt: str = "YYYY-MM-DD",
        locale: str = "ko",
        add: dict | None = None,
        subtract: dict | None = None,
        subdays: int | None = None,
        tz: str | Timezone | FixedTimezone | Literal["KST"] | None = "KST",
    ) -> str:
    """`pendulum.DateTime` 객체에 대해 `in_timezone` 연산 후 `fmt` 형식의 문자열로 변환한다."""
    return in_timezone(datetime, tz, add, subtract, subdays).format(fmt, locale)


def get_execution_date(
        kwargs: dict,
        fmt: str = "YYYY-MM-DD",
        locale: str = "ko",
        add: dict | None = None,
        subtract: dict | None = None,
        subdays: int | None = None,
        tz: str | Timezone | FixedTimezone | Literal["KST"] | None = "KST",
    ) -> str:
    """키워드 인자에서 `data_interval_end` 값을 추출하고, `fmt` 형식의 문자열로 변환한다."""
    return format_date(kwargs["data_interval_end"], fmt, locale, add, subtract, subdays, tz)


def read(
        key_path: KeyPath | None = None,
        format: Literal["auto", "json", "yaml"] = "auto",
        credentials: bool | Literal["expand"] = False,
        tables: bool = False,
        sheets: bool = False,
        service_account: bool = False,
    ) -> dict:
    """Airflow 전역 변수 경로가 가리키는 설정 파일을 읽는다. Airflow에 다음 변수들이 추가되어야 한다.
    - `config`, `credentials`, `cookies`, `schemas`, `service_account`

    Args:
        `key_path`: 설정 파일 내 하위 설정 경로. (딕셔너리 키)
        `format`: 설정 파일 형식. (`json`, `yaml`)
        `credentials`: 인증 정보 파일 포함 여부. (`expand` = 결과 딕셔너리에 인증 정보를 `update`로 추가)
        `tables`: 테이블 설정 포함 여부.
        `sheets`: 구글 시트 데이터 포함 여부.
        `service_account`: GCP 서비스 계정 포함 여부."""
    from airflow.sdk import Variable
    from linkmerce.api.config import read_config

    config = read_config(
        file_path = Variable.get("config"),
        key_path = key_path,
        format = format,
        credentials_path = (Variable.get("credentials") if credentials else None),
        schemas_path = (Variable.get("schemas") if tables else None),
        service_account = (Variable.get("service_account") if sheets or service_account else None),
        path_strings = ({"$cookies": Variable.get("cookies")} if credentials else None),
        skip_subpath = False,
        with_table_schema = False,
        read_google_sheets = sheets,
    )

    if (not credentials) and ("credentials" in config):
        config.pop("credentials")
    elif credentials == "expand":
        config.update(config.get("credentials", dict()))

    if (not tables) and ("tables" in config):
        config.pop("tables")
    if (not sheets) and ("sheets" in config):
        config.pop("sheets")

    if (not service_account) and ("service_account" in config):
        config.pop("service_account")
    elif service_account and ("service_account" not in config):
        config["service_account"] = Variable.get("service_account")

    return config


def split_by_credentials(credentials: list[dict], shuffle: bool = False, **kwargs: list) -> list[dict]:
    """각각의 인증 정보 목록에 키워드 인자의 값을 균등 분배한다."""
    from linkmerce.api.config import split_by_credentials
    return split_by_credentials(credentials, shuffle, **kwargs)
