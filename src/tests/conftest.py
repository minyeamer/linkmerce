from __future__ import annotations

import pytest

from typing import TypedDict, TYPE_CHECKING
from pathlib import Path
from ruamel.yaml import YAML
import datetime as dt
import json
import os
import unicodedata

if TYPE_CHECKING:
    from typing import Any, Callable, Generator, Literal, Sequence
    from pytest import FixtureRequest
    from linkmerce.common.extract import Extractor, JsonObject
    from linkmerce.common.load import DuckDBConnection
    from linkmerce.common.transform import Transformer, DuckDBTransformer
    from linkmerce.extensions.bigquery import BigQueryClient
    from linkmerce.extensions.postgres import PostgresClient
    from linkmerce.utils.nested import KeyPath


TEST_DIR = Path(__file__).parent
SRC_DIR = TEST_DIR.parent
ENV_DIR = SRC_DIR / "env"
DATA_DIR = TEST_DIR / "data"
RESULTS_DIR = TEST_DIR / "results"

DIRS = {
    "$test": str(TEST_DIR),
    "$src": str(SRC_DIR),
    "$env": str(ENV_DIR),
    "$data": str(DATA_DIR),
    "$results": str(RESULTS_DIR),
}

CONFIGS_PATH = TEST_DIR / "fixtures.yaml"
CREDENTIALS_PATH = ENV_DIR / "credentials.yaml"
SERVICE_ACCOUNT = ENV_DIR / "service_account.json"

# ANSI 이스케이프 시퀀스 - 색상 코드
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
CYAN = "\033[96m"
RESET = "\033[0m"


def has_marked_items(request: FixtureRequest, marker: str) -> bool:
    """선택된 테스트 중 `transform` 마커를 가진 항목이 있는지 확인한다."""
    return any(item.get_closest_marker(marker) for item in request.session.items)


###################################################################
############################# Configs #############################
###################################################################

@pytest.fixture(scope="session")
def configs_full() -> dict:
    """`fixtures.yaml`을 읽어 `dict`로 반환한다. 파일이 없으면 테스트를 스킵한다."""
    if not CONFIGS_PATH.exists():
        pytest.skip(f"`fixtures.yaml` not found: {CONFIGS_PATH}")

    with open(CONFIGS_PATH, 'r', encoding="utf-8") as file:
        return YAML(typ="safe").load(file.read())


@pytest.fixture(scope="session")
def configs(configs_full: dict) -> Callable[[KeyPath], dict]:
    """`fixtures.yaml`에서 특정 `key_path`에 해당하는 설정을 조회하는 함수를 반환한다."""
    from linkmerce.utils.nested import hier_get

    def _get_configs(key_path: KeyPath) -> dict:
        items = hier_get(configs_full, key_path, on_missing="raise")
        if not (isinstance(items, dict) and items):
            path_string = '.'.join(key_path) if isinstance(key_path, list) else key_path
            pytest.skip(f"Missing required config: '{path_string}' not found in `fixtures.yaml`")
        return {key: _read_path(value) for key, value in items.items()}

    return _get_configs


@pytest.fixture(scope="session")
def options(configs_full: dict) -> Callable[[KeyPath], dict]:
    """`fixtures.yaml`에서 특정 `key_path`에 해당하는 설정을 **선택적으로** 조회하는 함수를 반환한다."""
    from linkmerce.utils.nested import hier_get

    def _get_configs(key_path: KeyPath) -> dict:
        items = hier_get(configs_full, key_path, default=dict(), on_missing="ignore")
        if not isinstance(items, dict):
            path_string = '.'.join(key_path) if isinstance(key_path, list) else key_path
            pytest.skip(f"Missing required configs: '{path_string}' not found in `fixtures.yaml`")
        return {key: _read_path(value) for key, value in items.items()}

    return _get_configs


@pytest.fixture(scope="session")
def credentials_full() -> dict:
    """`credentials.yaml`을 읽어 `dict`로 반환한다. 파일이 없으면 테스트를 스킵한다."""
    if not CREDENTIALS_PATH.exists():
        pytest.skip(f"`credentials.yaml` not found: {CREDENTIALS_PATH}")

    from linkmerce.api.config import read_yaml
    return read_yaml(str(CREDENTIALS_PATH))


@pytest.fixture(scope="session")
def cookies_dir(credentials_full: dict) -> str:
    """`credentials.yaml`에서 `cookies.local` 경로를 읽어 반환한다."""
    from linkmerce.utils.nested import hier_get

    dir_path = hier_get(credentials_full, "cookies.local", on_missing="raise")
    if not (isinstance(dir_path, str) and dir_path):
        pytest.skip("Missing cookies path: 'cookies.local' not found in `credentials.yaml`")
    return dir_path


@pytest.fixture(scope="session")
def credentials(credentials_full: dict, cookies_dir: str) -> Callable[[KeyPath], dict]:
    """`credentials.yaml`에서 특정 `key_path`에 해당하는 설정을 조회하는 함수를 반환한다."""
    from linkmerce.utils.nested import hier_get

    def _get_credentials(key_path: KeyPath) -> dict:
        items = hier_get(credentials_full, key_path, on_missing="raise")
        if not (isinstance(items, dict) and items):
            path_string = '.'.join(key_path) if isinstance(key_path, list) else key_path
            pytest.skip(f"Missing required credentials: '{path_string}' not found in `credentials.yaml`")
        return {key: _read_path(value, {"$cookies": cookies_dir}) for key, value in items.items()}

    return _get_credentials


@pytest.fixture(scope="session")
def service_account() -> dict:
    """`service_account.json`을 읽어 `dict`로 반환한다. 파일이 없으면 테스트를 스킵한다."""
    import json
    try:
        with open(str(SERVICE_ACCOUNT), 'r', encoding="utf-8") as file:
            return json.loads(file.read())
    except Exception:
        pytest.skip(f"`service_account.json` not found: {SERVICE_ACCOUNT}")


def _read_path(value: Any, dirs: dict = dict()) -> tuple[str, Any]:
    if isinstance(value, str) and value.startswith("Path(") and value.endswith(")"):
        with open(value[5:-1].format(**(DIRS | dirs)), 'r', encoding="utf-8") as file:
            return file.read()
    return value


@pytest.fixture(scope="session")
def today() -> dt.date:
    """오늘 날짜를 반환한다."""
    return dt.date.today()


@pytest.fixture(scope="session")
def yesterday() -> dt.date:
    """어제 날짜를 반환한다."""
    return dt.date.today() - dt.timedelta(days=1)


@pytest.fixture(scope="session")
def days_ago(today: dt.date) -> Callable[[int], dt.date]:
    """오늘로부터 n일전 날짜를 생성하는 함수를 반환한다."""
    def _get_date(days: int) -> dt.date:
        return today - dt.timedelta(days=days)
    return _get_date


###################################################################
########################## Read and Save ##########################
###################################################################

def _build_core_dir(cls: type[Extractor | Transformer]) -> str:
    """클래스의 `linkmerce.core.` 이후 모듈 경로를 반환한다."""
    module = cls.__module__
    parts = module.split('.')
    if "core" in parts:
        idx = parts.index("core") + 1
        return '/'.join(parts[idx:-1])
    return '/'.join(parts[1:-1])


def _map_index(index: str | None = None, kwargs: dict | None = None, sep: str = '_') -> str:
    """`index`가 있다면 `sep` 구분자를 앞에 붙여 반환한다. `index`가 `$`로 시작한다면 `kwargs`에서 값을 조회한다."""
    if isinstance(index, str) and index.startswith('$'):
        if isinstance(kwargs, dict) and (index[1:] in kwargs):
            return f"{sep}{kwargs[index[1:]]}"
        return str()
    return f"{sep}{index}" if index is not None else str()


def _get_index(file_name: str, sep: str = '_') -> str | None:
    """파일명에서 `sep`로 구분되는 `index`에 해당하는 문자열을 추출한다."""
    name, _ = os.path.splitext(file_name)
    return name.split(sep, 1)[1] if sep in name else None


def _read_file(file_path: Path) -> Any:
    """`file_path`의 내용을 확장자에 맞게 역직렬화하여 불러온다."""
    try:
        ext = file_path.suffix[1:]

        if ext == "json":
            with open(file_path, 'r', encoding="utf-8") as file:
                return json.loads(file.read())

        elif ext == "html":
            with open(file_path, 'r', encoding="utf-8") as file:
                from bs4 import BeautifulSoup
                return BeautifulSoup(file.read(), "html.parser")

        elif ext in {"csv", "tsv", "txt"}:
            with open(file_path, 'r', encoding="utf-8") as file:
                return file.read()

        else: # "excel" | None
            with open(file_path, "rb") as file:
                return file.read()

    except Exception as e:
        print(f"  {YELLOW}[WARN]{RESET} Failed to read {CYAN}{file_path.relative_to(RESULTS_DIR)}{RESET}: {e}")
        return None


def _save_file(obj: Any, file_path: Path) -> Path | None:
    """`obj`를 `file_path`의 확장자에 맞게 직렬화하여 저장한다."""
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        ext = file_path.suffix[1:]

        if ext == "json":
            with open(file_path, 'w', encoding="utf-8") as file:
                json.dump(obj, file, indent=2, ensure_ascii=False, default=str)

        elif ext == "html":
            from bs4 import BeautifulSoup
            content = obj.prettify() if isinstance(obj, BeautifulSoup) else str(obj)
            with open(file_path, 'w', encoding="utf-8") as file:
                file.write(content)

        elif ext in {"csv", "tsv", "txt"}:
            with open(file_path, 'w', encoding="utf-8") as file:
                file.write(str(obj))

        else: # "excel" | None
            with open(file_path, "wb") as file:
                file.write(obj)

    except Exception as e:
        print(f"  {RED}[ERROR]{RESET} Failed to save to {CYAN}{file_path.relative_to(RESULTS_DIR)}{RESET}: {e}")
        return None


###################################################################
############################# Extract #############################
###################################################################

@pytest.fixture(scope="session")
def dump_extract() -> Callable:
    """`Extractor` 실행 결과를 파일로 저장하는 함수를 반환한다."""

    def _get_dump(
            cls: type[Extractor],
            format: Literal["json", "html", "xlsx", "csv", "tsv", "txt"] | None = None,
            map_index: str | None = None,
        ) -> Callable:

        def _dump(response: Any, *args, **kwargs):
            file_path = _build_extract_path(cls, format, map_index, kwargs)
            _save_file(response, file_path)
            print(f"  {BLUE}[INFO]{RESET} Saved to {CYAN}{file_path.relative_to(RESULTS_DIR)}{RESET}")
        return _dump

    return _get_dump


def _build_extract_path(
        extractor_cls: type[Extractor],
        format: Literal["json", "html", "xlsx", "csv", "tsv", "txt"] | None = None,
        map_index: str | None = None,
        kwargs: dict | None = None,
    ) -> Path:
    """`Extractor` 실행 결과를 저장할 파일 경로를 반환한다."""
    extractor_dir = RESULTS_DIR / _build_core_dir(extractor_cls) / extractor_cls.__name__
    format = ".{}".format(format) if format else str()
    return extractor_dir / f"extract{_map_index(map_index, kwargs)}{format}"


###################################################################
############################ Transform ############################
###################################################################

class TransformerHarness:
    """`DuckDBTransformer`의 테스트 실행을 보조하는 하네스 클래스.

    원본 `DuckDBTransformer` 객체를 래핑하여 다음과 같은 편의 기능을 제공한다.
    1. Extractor 결과 데이터(`extract.json`)를 자동으로 탐색 및 로드 (`load_extract`)
    2. `parse` 실행 결과를 파일로 자동 저장 (`dump_result`)
    3. `bulk_insert` 실행 후 DB 테이블 내용을 파일로 자동 저장 (`dump_tables`)
    """

    def __init__(self, transformer: DuckDBTransformer):
        """테스트 대상 `DuckDBTransformer` 객체를 주입받는다."""
        self._transformer = transformer

    @property
    def extractor_dir(self) -> Path:
        """`Extractor` 실행 결과가 저장된 폴더 경로를 반환한다."""
        return RESULTS_DIR / _build_core_dir(self._transformer.__class__) / self._transformer.extractor

    @property
    def transformer_dir(self) -> Path:
        """`DuckDBTransformer` 실행 결과를 저장할 폴더 경로를 반환한다."""
        return self.extractor_dir / self._transformer.__class__.__name__

    def load_extract(self, map_index: str | None = None, kwargs: dict | None = None) -> tuple[Any, str]:
        """`Extractor` 결과 파일을 읽어서, 역직렬화된 데이터와 함께 파일명에서 `index`를 추출해 반환한다."""
        pattern = f"extract{_map_index(map_index, kwargs)}.*"
        for form in ["NFC", "NFD"]: # OS별 파일 시스템의 한글 정규화 방식(NFD/NFC)에 맞춰 패턴 적용
            pattern_norm = unicodedata.normalize(form, pattern)
            for file_path in self.extractor_dir.glob(pattern_norm):
                return _read_file(file_path), _get_index(file_path.name)
        pytest.skip(f"Extraction results not found: {self.extractor_dir.relative_to(RESULTS_DIR) / pattern}")

    def transform(self, skip_dump: bool = False, map_index: str | None = None, **kwargs) -> Any:
        """`Extractor` 결과 파일을 읽어서 파싱한 뒤 DB에 삽입하는 파이프라인을 실행한다."""
        obj, map_index = self.load_extract(map_index, kwargs)
        kwargs.update(skip_dump=skip_dump, map_index=map_index)
        result = self.parse(obj, **kwargs)
        return self.bulk_insert(result, **kwargs)

    def parse(self, *args, skip_dump: bool = False, map_index: str | None = None, **kwargs) -> Any:
        """데이터를 파싱하고 결과를 파일로 저장한다."""
        result = self._transformer.parse(*args, **kwargs)
        if not skip_dump:
            self.dump_result(result, index=(_map_index(map_index, kwargs, sep='') or None))
        return result

    def bulk_insert(self, *args, skip_dump: bool = False, map_index: str | None = None, **kwargs) -> Any:
        """파싱 결과를 DB에 삽입하고 테이블 내용을 파일로 저장한다."""
        conn = self._transformer.bulk_insert(*args, **kwargs)
        if not skip_dump:
            self.dump_tables(index=(_map_index(map_index, kwargs, sep='') or None))
        return conn

    def dump_result(self, result: JsonObject, parser: str | type | dict | None = None, index: str | None = None):
        """`parse` 결과를 하위 경로에 JSON 파일로 저장한다."""
        parser = parser if parser is not None else self._transformer.parser
        if isinstance(parser, dict) and isinstance(result, dict):
            for table_key, data in result.items():
                self.dump_result(data, parser[table_key], index=index)
        else:
            parser = parser.__name__ if isinstance(parser, type) else str(parser)
            _save_file(result, self.transformer_dir / parser / f"transform{_map_index(index)}.json")

    def dump_tables(self, index: str | None = None):
        """`bulk_insert` 적재 후 각 테이블의 전체 행을 JSON 파일로 저장한다."""
        for table_key, table_name in self._transformer.tables.items():
            rows = self._transformer.conn.fetch_all_to_json(query=f"SELECT * FROM {table_name}")
            _save_file(rows, self.transformer_dir / f"{table_key}{_map_index(index)}.json")

    def __getattr__(self, name: str):
        """속성 조회를 내부 `_transformer`에 위임한다."""
        return getattr(self._transformer, name)


@pytest.fixture(scope="session")
def duckdb_conn(request: FixtureRequest) -> Generator[DuckDBConnection | None, None, None]:
    """변환 또는 적재 테스트 시작 시 DuckDB 연결을 생성하고 종료 시 닫는다."""
    if has_marked_items(request, "transform") or has_marked_items(request, "load"):
        from linkmerce.common.load import DuckDBConnection
        with DuckDBConnection() as conn:
            yield conn
    else:
        yield None


@pytest.fixture(scope="function")
def transformer_harness(
        duckdb_conn: DuckDBConnection | None,
    ) -> Callable[[type[DuckDBTransformer]], TransformerHarness]:
    """`DuckDBTransformer` 클래스를 하네스 객체로 초기화하여 반환하는 팩토리 함수."""
    def _init(transformer: type[DuckDBTransformer], *args, **kwargs) -> TransformerHarness:
        if duckdb_conn is None:
            pytest.skip(f"Missing DuckDB connection.")
        return TransformerHarness(transformer(db_info={"conn": duckdb_conn}, *args, **kwargs))
    return _init


###################################################################
############################## Load ###############################
###################################################################

class SourceConnOptions(TypedDict):
    """DuckDB 소스 테이블 연결 설정"""
    duckdb_table: str
    file_path: str | Path
    file_format: Literal["csv", "json", "parquet"]

class TargetConnOptions(TypedDict):
    """외부 저장소 연결 설정"""
    base_table: str
    test_table: str
    primary_key: str

class BigQueryConnOptions(TargetConnOptions):
    """BigQuery 타겟 테이블 연결 설정"""
    service_account: dict
    base_table: str
    test_table: str
    primary_key: str

class PostgresConnOptions(TargetConnOptions):
    """PostgreSQL 타겟 테이블 연결 설정"""
    dsn: str
    base_table: str
    test_table: str
    primary_key: str

class MergeRules(TypedDict):
    """MERGE 테스트 시 값 변경 규칙"""
    update: dict[str, str]
    insert: dict[str, str]

class ConflictAction(TypedDict):
    """MERGE 테스트 시 규칙"""
    on_conflict: str
    matched: str | dict[str, str]
    not_matched: str | dict[str, str]
    updated_columns: list[str]

class LoadTestSpec(TypedDict):
    """적재 테스트 설정"""
    select_columns: dict[str, list[str]]
    rename_columns: dict[str, str]
    replace_columns: dict[str, Any]
    overwrite_rules: dict[str, str]
    where_clauses: list[str | None]
    merge_rules: MergeRules
    conflict_actions: list[ConflictAction]

POSTGRES_DATABASE = "db"


class LoaderHarness:
    """DuckDB -> 외부 저장소 적재 테스트 실행을 보조하는 하네스 클래스."""

    def __init__(
            self,
            backend: Literal["bigquery", "postgres"],
            connections: dict,
            client: BigQueryClient | PostgresClient,
            duckdb_conn: DuckDBConnection,
        ):
        """외부 저장소 연결 설정을 읽고 소스/타겟 테이블을 생성한다."""
        self.backend = backend # 연결 설정을 읽는 과정에서 "bigquery" 또는 "postgres" 값으로 보장된다.
        self.source: SourceConnOptions = connections["source"]
        self.target: TargetConnOptions = connections["target"]
        self.spec: LoadTestSpec = connections["spec"]
        self.client = client
        self.duckdb = duckdb_conn
        self.pg_database = POSTGRES_DATABASE
        self.columns: list[str] = list()
        self.total: int = None
        self.source_table = self.create_source_table()
        self.target_table = self.create_target_table()

    @property
    def target_ref(self) -> str:
        return self.table_ref(self.target_table)

    def table_ref(self, table: str) -> str:
        """테이블명에 대해 DB 백엔드별 알맞은 테이블 참조를 반환한다."""
        if self.backend == "bigquery":
            return f"`{self.client.project_id}.{table}`"
        return table

    def create_source_table(self) -> str:
        """DuckDB 테이블을 생성하면서 소스 파일을 적재한다. 이미 있다면 삭제하고 다시 생성한다.

        소스 테이블에 대해 테스트 시점의 칼럼 목록을 수집하고 행 수를 집계하여 속성으로 저장한다.
        """
        source_table = self.source["duckdb_table"]
        self.duckdb.create_table(
            source_table, self.source["file_path"], self.source["file_format"], option="replace",
        )

        self.columns = self.duckdb.get_columns(source_table)
        self.total = self.duckdb.count_table(source_table)

        return source_table

    def create_target_table(self) -> str:
        """외부 저장소에서 `base_table`의 스키마를 복제해 `test_table`을 생성한다.   
        `base_table`이 없다면 테스트가 종료되고, 이미 `test_table`이 있다면 삭제하고 다시 생성한다.

        PostgreSQL 클라이언트를 사용한다면 타겟 테이블을 최초 생성할 때   
        DuckDB 연결에 PostgreSQL을 `self.pg_database`로 붙인다.
        """
        if not self.client.table_exists(base_table := self.target["base_table"]):
            pytest.skip(f"Missing {self.backend} table: '{base_table}' does not exist.")
        from textwrap import dedent

        target_table = self.target["test_table"]
        if self.backend == "bigquery":
            query = dedent(f"""
                CREATE OR REPLACE TABLE {self.table_ref(target_table)} AS
                SELECT * FROM {self.table_ref(base_table)} LIMIT 0;
                """).strip()
            self.client.execute_job(query)
        elif self.backend == "postgres":
            query = dedent(f"""
                DROP TABLE IF EXISTS {target_table};
                CREATE TABLE {target_table} (LIKE {base_table} INCLUDING ALL);
                """).strip()
            self.client.execute(query, commit=True, close=True)
        return target_table

    def execute(self, query: str):
        """타겟 테이블을 대상으로 쿼리를 실행한다. 타겟 테이블이 존재하지 않으면 참조 오류가 발생한다."""
        if self.backend == "bigquery":
            self.client.execute_job(query)
        elif self.backend == "postgres":
            self.client.execute(query, commit=True, close=True)
        return

    def load_table_from_duckdb(
            self,
            source_table: str | None = None,
            target_table: str | None = None,
            **kwargs
        ) -> bool:
        """소스 테이블을 타겟 테이블에 적재하고 성공 여부를 반환한다."""
        return self.client.load_table_from_duckdb(
            connection = self.duckdb,
            source_table = (source_table or self.source_table),
            target_table = (target_table or self.target_table),
            **kwargs
        )

    def overwrite_table_from_duckdb(
            self,
            source_table: str | None = None,
            target_table: str | None = None,
            where_clause: str | None = None,
            **kwargs
        ) -> bool:
        """소스 테이블을 타겟 테이블에 덮어쓰기하고 성공 여부를 반환한다."""
        return self.client.overwrite_table_from_duckdb(
            connection = self.duckdb,
            source_table = (source_table or self.source_table),
            target_table = (target_table or self.target_table),
            where_clause = where_clause,
            **kwargs
        )

    def merge_table_from_duckdb(
            self,
            backend: Literal["bigquery", "postgres"],
            source_table: str | None = None,
            target_table: str | None = None,
            where_clause: str | None = None,
            on_conflict: str | None = None,
            matched: str | dict[str, str] = ":replace_all:",
            not_matched: str | Sequence[str] = ":insert_all:",
            updated_columns: list[str] | None = None,
            **kwargs
        ) -> bool:
        """소스 테이블을 타겟 테이블에 MERGE하고 성공 여부를 반환한다."""
        if backend == "bigquery":
            return self.client.merge_table_from_duckdb(
                connection = self.duckdb,
                source_table = (source_table or self.source_table),
                target_table = (target_table or self.target_table),
                where_clause = where_clause,
                on_conflict = on_conflict,
                matched = matched,
                not_matched = not_matched,
                **kwargs
            )
        elif backend == "postgres":
            return self.client.merge_table_from_duckdb(
                connection = self.duckdb,
                source_table = (source_table or self.source_table),
                target_table = (target_table or self.target_table),
                where_clause = where_clause,
                on_conflict = on_conflict,
                matched = matched,
                not_matched = not_matched,
                **kwargs
            )
        return False


@pytest.fixture(scope="session")
def connections(configs_full: dict) -> Callable[[str], dict]:
    """`fixtures.yaml`에서 특정 DB 백엔드에 해당하는 연결 설정을 조회하는 함수를 반환한다."""
    from linkmerce.utils.nested import hier_get

    def _get_connections(backend: str) -> dict:
        items = hier_get(configs_full, backend, on_missing="raise")
        if not (isinstance(items, dict) and items):
            pytest.skip(f"Missing required config: '{backend}' not found in `fixtures.yaml`")

        file_path = str(items["source"]["file_path"]).format(**DIRS)
        items["source"]["file_path"] = Path(file_path)
        items["source"]["file_format"] = os.path.splitext(file_path)[1][1:]
        return items

    return _get_connections


@pytest.fixture(scope="session")
def bigquery_client(
        request: FixtureRequest,
        connections: Callable[[str], dict],
        service_account: dict,
    ) -> Generator[BigQueryClient | None, None, None]:
    """적재 테스트 시작 시 BigQuery 클라이언트를 생성하고 종료 시 닫는다."""
    if has_marked_items(request, "load"):
        from linkmerce.extensions.bigquery import BigQueryClient
        with BigQueryClient(service_account) as client:
            yield client

            target_table = connections("bigquery")["target"]["test_table"]
            client.execute_job(f"DROP TABLE IF EXISTS `{client.project_id}.{target_table}`;")
    else:
        yield None


@pytest.fixture(scope="session")
def postgres_client(
        request: FixtureRequest,
        connections: Callable[[str], dict],
        duckdb_conn: DuckDBConnection | None,
    ) -> Generator[PostgresClient | None, None, None]:
    """적재 테스트 시작 시 PostgreSQL 클라이언트를 생성하고 종료 시 닫는다."""
    if has_marked_items(request, "load"):
        if duckdb_conn is None:
            pytest.skip(f"Missing DuckDB connection.")

        from linkmerce.extensions.postgres import PostgresClient
        with PostgresClient(connections("postgres")["target"]["dsn"]) as client:
            client.attach_postgres_to_duckdb(duckdb_conn, POSTGRES_DATABASE, read_only=False)
            yield client

            target_table = connections("postgres")["target"]["test_table"]
            with client.conn.cursor() as cursor:
                client.execute(f"DROP TABLE IF EXISTS {target_table};", cursor=cursor, commit=True)
    else:
        yield None


@pytest.fixture(scope="function")
def loader_harness(
        connections: Callable[[str], dict],
        duckdb_conn: DuckDBConnection | None,
        bigquery_client: BigQueryClient | None,
        postgres_client: PostgresClient | None,
    ) -> Callable[[Literal["bigquery", "postgres"]], LoaderHarness]:
    """DB 백엔드별 `LoaderHarness` 객체를 생성하는 팩토리 함수."""
    def _select_client(backend: Literal["bigquery", "postgres"]) -> BigQueryClient | PostgresClient:
        if duckdb_conn is None:
            pytest.skip(f"Missing DuckDB connection.")
        elif (backend == "bigquery") and (bigquery_client is not None):
            return bigquery_client
        elif (backend == "postgres") and (postgres_client is not None):
            return postgres_client
        else:
            pytest.skip(f"Missing '{backend}' backend or connection.")

    def _init(backend: Literal["bigquery", "postgres"]) -> LoaderHarness:
        return LoaderHarness(backend, connections(backend), _select_client(backend), duckdb_conn)
    return _init
