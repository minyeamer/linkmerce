from __future__ import annotations

import pytest

from typing import TYPE_CHECKING
from pathlib import Path
from ruamel.yaml import YAML
import datetime as dt
import json
import os

if TYPE_CHECKING:
    from typing import Any, Callable, Literal
    from _pytest.config import Config as PytestConfig
    from linkmerce.common.extract import Extractor, JsonObject
    from linkmerce.common.load import DuckDBConnection
    from linkmerce.common.transform import Transformer, DuckDBTransformer
    from linkmerce.utils.nested import KeyPath


TEST_DIR = Path(__file__).parent
SRC_DIR = TEST_DIR.parent
ENV_DIR = SRC_DIR / "env"
RESULTS_DIR = TEST_DIR / "results"

DIRS = {
    "$test": str(TEST_DIR),
    "$src": str(SRC_DIR),
    "$env": str(ENV_DIR),
    "$results": str(RESULTS_DIR),
}

CONFIGS_PATH = TEST_DIR / "fixtures.yaml"
CREDENTIALS_PATH = ENV_DIR / "credentials.yaml"
SERVICE_ACCOUNT = ENV_DIR / "service_account.json"


def pytest_configure(config: PytestConfig):
    config.addinivalue_line("markers", "extract: 데이터 추출(Extract) 테스트")
    config.addinivalue_line("markers", "transform: 데이터 변환(Transform) 테스트")


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
def cookies_dir(credentials_full: dict) -> str:
    """`credentials.yaml`에서 `cookies.local` 경로를 읽어 반환한다."""
    from linkmerce.utils.nested import hier_get

    dir_path = hier_get(credentials_full, "cookies.local", on_missing="raise")
    if not (isinstance(dir_path, str) and dir_path):
        pytest.skip(f"Missing cookies path: 'cookies.local' not found in `credentials.yaml`")
    return dir_path


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
        print(f"  [WARN] Failed to read {file_path.relative_to(RESULTS_DIR)}: {e}")
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
        print(f"  [WARN] Failed to save to {file_path.relative_to(RESULTS_DIR)}: {e}")
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
            print(f"  [INFO] Saved to {file_path.relative_to(RESULTS_DIR)}")
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
        for file_path in self.extractor_dir.glob(pattern):
            return _read_file(file_path), _get_index(file_path.name)
        pytest.skip(f"Extraction results not found: {pattern}")

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


@pytest.fixture(scope="function")
def db_conn():
    """`DuckDBTransformer` 개별 테스트 시작 시 DuckDB 연결을 생성하고 종료 시 닫는다."""
    from linkmerce.common.load import DuckDBConnection
    with DuckDBConnection() as conn:
        yield conn


@pytest.fixture(scope="function")
def transformer_harness(db_conn: DuckDBConnection) -> Callable[[type[DuckDBTransformer]], TransformerHarness]:
    """`DuckDBTransformer` 클래스를 하네스 객체로 초기화하여 반환하는 팩토리 함수."""
    def _init(transformer: type[DuckDBTransformer], *args, **kwargs) -> TransformerHarness:
        return TransformerHarness(transformer(db_info={"conn": db_conn}, *args, **kwargs))
    return _init
