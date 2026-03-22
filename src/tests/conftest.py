from __future__ import annotations

import pytest

from typing import TYPE_CHECKING
from pathlib import Path
import datetime as dt
import json

if TYPE_CHECKING:
    from typing import Any, Callable, Literal
    from _pytest.config import Config as PytestConfig
    from linkmerce.common.extract import Extractor
    from linkmerce.common.transform import Transformer
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

    from linkmerce.api.config import read_yaml
    return read_yaml(str(CONFIGS_PATH))


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
        with open(str(SERVICE_ACCOUNT), "r", encoding="utf-8") as file:
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
############################# Save to #############################
###################################################################

def _core_dir(cls: type[Extractor | Transformer]) -> str:
    """클래스의 `linkmerce.core.` 이후 모듈 경로를 반환한다."""
    module = cls.__module__
    parts = module.split('.')
    if "core" in parts:
        idx = parts.index("core") + 1
        return '/'.join(parts[idx:-1])
    return '/'.join(parts[1:-1])


def _save_to(obj: Any, file_path: Path) -> Path | None:
    """`obj`를 `file_path`의 확장자에 맞게 직렬화하여 저장한다."""
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        ext = file_path.suffix[1:]

        if ext == "json":
            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(obj, file, indent=2, ensure_ascii=False, default=str)

        elif ext == "html":
            from bs4 import BeautifulSoup
            content = obj.prettify() if isinstance(obj, BeautifulSoup) else str(obj)
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(content)

        elif ext in ("csv", "tsv", "txt"):
            with open(file_path, "w", encoding="utf-8") as file:
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

@pytest.fixture
def dump_extract() -> Callable[[type[Extractor], str], Callable]:
    """`Extractor` 실행 결과를 파일로 저장하는 `parser` 함수를 반환한다."""

    def _get_dump(
            cls: type[Extractor],
            format: Literal["json", "html", "xlsx", "csv", "tsv", "txt"] | None = None,
            map_index: str | None = None,
        ) -> Callable:

        def _dump(response: Any, *args, **kwargs):
            file_path = _build_path_extract(cls, format, map_index, kwargs)
            _save_to(response, file_path)
            print(f"  [INFO] Saved to {file_path.relative_to(RESULTS_DIR)}")
        return _dump

    return _get_dump


def _build_path_extract(
        extractor_cls: type[Extractor],
        format: Literal["json", "html", "xlsx", "csv", "tsv", "txt"] | None = None,
        map_index: str | None = None,
        kwargs: dict = dict(),
    ) -> Path:
    """`Extractor` 실행 결과를 저장할 파일 경로를 반환한다."""
    extractor_dir = RESULTS_DIR / _core_dir(extractor_cls) / extractor_cls.__name__
    format = ".{}".format(format) if format else str()

    if isinstance(map_index, str) and map_index.startswith('$'):
        index = "_{}".format(kwargs.get(map_index[1:]))
    else:
        index = "_{}".format(map_index) if map_index else str()

    return extractor_dir / f"extract{index}{format}"
