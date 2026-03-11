from __future__ import annotations

from abc import ABCMeta, abstractmethod

from typing import TypedDict, Union, overload, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal, Sequence, TypeVar
    from types import TracebackType
    Expression = TypeVar("Expression", bound=str)
    TableAlias = TypeVar("TableAlias", bound=str)
    TableName = TypeVar("TableName", bound=str)
    QueryKey = TypeVar("QueryKey", bound=str)

    from bs4 import BeautifulSoup, Tag

    from linkmerce.common.load import Connection, DuckDBConnection
    from linkmerce.common.models import Models

    from duckdb import DuckDBPyConnection, DuckDBPyRelation
    from pathlib import Path

JsonObject = Union[dict, list]


class Transformer(metaclass=ABCMeta):
    """모든 Transformer의 최상위 추상 기반 클래스."""

    def __init__(self, **kwargs):
        ...

    @abstractmethod
    def transform(self, obj: Any, **kwargs) -> Any:
        """원본 데이터를 받아 변환된 결과를 반환한다. 서브클래스에서 반드시 구현해야 한다."""
        raise NotImplementedError("The 'transform' method must be implemented.")

    def raise_parse_error(self, msg: str):
        """파싱 실패 시 `ParseError`를 발생시킨다."""
        from linkmerce.common.exceptions import ParseError
        raise ParseError(msg)

    def raise_request_error(self, msg: str):
        """요청 실패 시 `RequestError`를 발생시킨다."""
        from linkmerce.common.exceptions import RequestError
        raise RequestError(msg)


###################################################################
########################## HTTP Response ##########################
###################################################################

class ResponseTransformer(Transformer, metaclass=ABCMeta):
    """HTTP 응답 데이터를 JSON 형식 객체로 변환하는 추상 클래스.

    아래 5단계 파이프라인을 통해 HTTP 응답 데이터를 처리한다:
    1. `assert_valid_response` - HTTP 응답 데이터의 타입 및 유효성 검증
    2. `get_scope` - 전체 데이터 중 파싱 대상이 되는 특정 지점(`scope`) 탐색
    3. `parse` - 탐색된 데이터를 필드 선택에 용이한 데이터 구조로 변환
    4. `select_fields` - 변환된 데이터에서 필요한 필드만 추출
    5. `set_defaults` - 필드 선택 결과에 기본값 추가
    """

    dtype: type | None = None
    scope: str | None = None
    fields: dict | list | None = None
    defaults: dict | None = None
    on_missing: Literal["ignore", "raise"] = "raise"

    def __init__(
            self,
            scope: str | None = None,
            fields: dict | None = None,
            defaults: dict | None = None,
            on_missing: Literal["ignore", "raise"] | None = None,
            **kwargs
        ):
        if scope is not None:
            self.scope = scope
        if fields is not None:
            self.fields = fields
        if defaults is not None:
            self.defaults = defaults
        if on_missing is not None:
            self.on_missing = on_missing

    def transform(self, obj: Any, **kwargs) -> JsonObject:
        """HTTP 응답 데이터 검증 > scope 탐색 > 파싱 > 필드 선택 > 기본값 추가 순서로 파이프라인을 실행한다."""
        self.assert_valid_response(obj, **kwargs)
        target = self.get_scope(obj, **kwargs)
        data = self.parse(target, **kwargs)
        result = self.select_fields(data, **kwargs)
        return self.set_defaults(result, **kwargs)

    def assert_valid_response(self, obj: Any, **kwargs):
        """`dtype`이 지정된 경우 HTTP 응답 데이터의 타입을 검증하고, 불일치 시 `RequestError`를 발생시킨다."""
        if not ((self.dtype is None) or isinstance(obj, self.dtype)):
            self.raise_request_error("HTTP response is not valid.")

    def get_scope(self, obj: Any, **kwargs) -> Any:
        """HTTP 응답 데이터에서 대상 데이터 범위를 탐색해 반환한다. 구현하지 않으면 입력을 그대로 반환한다."""
        return obj

    def parse(self, target: Any, **kwargs) -> Any:
        """`scope` 탐색 결과를 추가로 가공한다. 구현하지 않으면 입력을 그대로 반환한다."""
        return target

    @abstractmethod
    def select_fields(self, data: Any, **kwargs) -> JsonObject:
        """파싱된 데이터에서 필요한 필드만 추출한다. 서브클래스에서 반드시 구현해야 한다."""
        raise NotImplementedError("The 'select_fields' method must be implemented.")

    def set_defaults(self, result: JsonObject, **kwargs) -> JsonObject:
        """JSON 형식의 필드 선택 결과에 `defaults` 키-값을 추가한다."""
        if not isinstance(result, (dict, list)):
            self.raise_parse_error("The result must be in JSON format (dict or list).")
        elif self.defaults is None:
            return result
        defaults = self.init_defaults(**kwargs)

        from linkmerce.utils.nested import hier_update
        update = lambda __m: hier_update(__m, defaults, on_missing="create")
        return list(map(update, result)) if isinstance(result, list) else update(result)

    def init_defaults(self, **kwargs) -> dict:
        """`defaults` 딕셔너리를 평가한다. `$key` 형식의 값은 키워드 인자에서 참조한 값으로 치환한다."""
        defaults = dict()
        for path, value in (self.defaults or dict()).items():
            if isinstance(value, str) and value.startswith('$'):
                value = kwargs.get(value[1:])
            defaults[path] = value
        return defaults


class JsonTransformer(ResponseTransformer):
    """`dict` 또는 `list` 형태의 JSON 데이터를 변환하는 클래스.

    주요 설정 변수:
    - `scope` - `hier_get` 함수로 탐색할 데이터의 중첩 키 경로
    - `fields` - `select_values` 함수에 전달할 스키마 정의
    - `defaults` - 각 행마다 공통으로 추가할 기본 키-값 목록
    - `on_missing` - 필드 조회 실패 시 동작 (`raise` 또는 `ignore`)
    """

    dtype: type = dict
    scope: str | None = None
    fields: dict | list | None = None
    defaults: dict | None = None
    on_missing: Literal["ignore", "raise"] = "raise"

    def transform(self, obj: JsonObject, **kwargs) -> JsonObject:
        """JSON 객체 검증 > scope 탐색 > 파싱 > 필드 선택 > 기본값 추가 순서로 파이프라인을 실행한다."""
        return super().transform(obj, **kwargs)

    def get_scope(self, obj: JsonObject, **kwargs) -> JsonObject:
        """`scope`가 지정된 경우 `hier_get` 함수로 중첩 경로를 탐색해 대상 데이터를 반환한다."""
        if self.scope is not None:
            from linkmerce.utils.nested import hier_get
            return hier_get(obj, self.scope, on_missing="raise")
        return obj

    def select_fields(self, data: JsonObject, **kwargs) -> JsonObject:
        """`fields` 스키마에 따라 `select_values` 함수로 필드를 추출한다."""
        if not isinstance(data, (dict, list)):
            self.raise_parse_error("Could not select fields from the parsed data.")
        elif self.fields is None:
            return data

        from linkmerce.utils.nested import select_values
        common = dict(schema=self.fields, on_missing=self.on_missing)
        if isinstance(data, list):
            return [select_values(row, **common) for row in data if isinstance(row, dict)]
        return select_values(data, **common)


class HtmlTransformer(ResponseTransformer):
    """HTML(BeautifulSoup) 응답 데이터를 JSON 형식으로 변환하는 클래스.

    주요 설정 변수:
    - `scope` - 파싱 대상 요소를 탐색할 CSS 선택자 (없으면 입력 전체)
    - `fields` - `{필드명: CSS_선택자}` 형식의 스키마 정의
    - `defaults` - 각 행마다 공통으로 추가할 기본 키-값 목록
    - `on_missing` - CSS 선택자 탐색 실패 시 동작 (`raise` 또는 `ignore`)
    """

    scope: str | None = None
    fields: dict[str, str] | None = None
    defaults: dict | None = None
    on_missing: Literal["ignore", "raise"] = "raise"

    def transform(self, obj: BeautifulSoup | Tag, **kwargs) -> JsonObject:
        """BeautifulSoup 객체 검증 > scope 탐색 > 파싱 > 필드 선택 > 기본값 추가 순서로 파이프라인을 실행한다."""
        return super().transform(obj, **kwargs)

    def assert_valid_response(self, obj: BeautifulSoup | Tag, **kwargs):
        """HTTP 응답 데이터가 `Tag` 타입인지 검증하고, 불일치 시 `RequestError`를 발생시킨다."""
        from bs4 import Tag
        if not isinstance(obj, Tag):
            self.raise_request_error("HTTP response is not valid.")

    def get_scope(self, obj: BeautifulSoup | Tag, **kwargs) -> Tag | list[Tag]:
        """`scope`가 지정된 경우 `select` 함수로 CSS 선택자에 대한 요소를 탐색해 반환한다."""
        if self.scope is not None:
            from linkmerce.utils.parse import select
            return select(obj, self.scope, on_missing="raise")
        return obj

    def select_fields(self, data: Tag | list[Tag], **kwargs) -> JsonObject:
        """`{필드명: CSS_선택자}` 스키마에 따라 `select_attrs` 함수로 각 태그에서 값을 추출한다."""
        if (self.fields is None) or (not isinstance(data, (Tag, list))):
            self.raise_parse_error("Could not select fields from the parsed data.")

        from linkmerce.utils.parse import select_attrs
        common = dict(schema=self.fields, on_missing=self.on_missing)
        if isinstance(data, list):
            return [select_attrs(row, **common) for row in data if isinstance(row, Tag)]
        return select_attrs(data, **common)


class ExcelTransformer(JsonTransformer):
    """Excel(openpyxl) 응답 데이터를 JSON 형식으로 변환하는 클래스.

    주요 설정 변수:
    - `scope` - Excel 시트 이름 (없으면 활성 시트)
    - `fields` - `select_values` 함수에 전달할 스키마 정의
    - `defaults` - 각 행마다 공통으로 추가할 기본 키-값 목록
    - `on_missing` - 필드 조회 실패 시 동작 (`raise` 또는 `ignore`)
    """

    scope: str | None = None
    fields: dict[str, str] | None = None
    defaults: dict | None = None
    on_missing: Literal["ignore", "raise"] = "raise"

    def transform(self, obj: bytes | str | Path, **kwargs) -> JsonObject:
        """Excel 파일 검증 > Excel 시트 불러오기 > 파싱 > 필드 선택 > 기본값 추가 순서로 파이프라인을 실행한다."""
        return super().transform(obj, **kwargs)

    def assert_valid_response(self, obj: BeautifulSoup | Tag, **kwargs):
        """Excel 파일의 참조 객체에 대한 검증은 사용자가 정의한다."""
        pass

    def get_scope(self, obj: bytes | str | Path, **kwargs) -> list[dict]:
        """`scope`가 지정된 경우 해당 시트를, 지정되지 않은 경우 활성 시트를 JSON 형식으로 불러온다."""
        from linkmerce.utils.excel import excel2json
        return excel2json(obj, sheet_name=self.scope, warnings=False)


###################################################################
############################# Database ############################
###################################################################

class ParserConfig(TypedDict, total=False):
    """`ResponseTransformer` 객체 초기화 시 전달할 설정 변수."""
    dtype: type | None
    scope: str | None
    fields: dict | list | None
    defaults: dict | None
    on_missing: Literal["ignore", "raise"] | None


class DBTransformer(Transformer, metaclass=ABCMeta):
    """데이터베이스에 데이터를 변환하고 적재하는 추상 클래스.

    `parser`로 원본 데이터를 파싱한 뒤 `bulk_insert`로 DB에 삽입하는 2단계 파이프라인을 제공한다.

    주요 설정 변수:
    - `queries` - SQL 모델 파일에서 불러올 쿼리 키 목록
    - `tables` - `{테이블_별칭: 실제_테이블명}` 형식의 테이블 매핑
    - `parser` - 원본 데이터 파싱에 사용할 파서. 문자열 상수, 클래스 생성자, 또는 dict 중 하나
    - `parser_config` - 파서 객체 초기화 시 전달할 설정 변수 (`ParserConfig`)
    """

    queries: list[str] = ["create", "bulk_insert"]
    tables: dict[TableAlias, TableName] = {"table": "data"}
    parser: (
        Literal["json", "html", "excel"] |
        type[ResponseTransformer] |
        dict[TableAlias, type[ResponseTransformer]] |
        None
    ) = "json"
    parser_config: ParserConfig | None = None

    def __init__(
            self,
            db_info: dict = dict(),
            model_path: Literal["this"] | str | Path = "this",
            tables: dict[TableAlias, TableName] | None = None,
            create_options: dict[str, TableName] | None = None,
            parser: type[ResponseTransformer] | None = None,
            parser_config: ParserConfig | None = None,
        ):
        """DB 연결 및 SQL 쿼리를 불러오고 테이블을 생성한다.

        Args:
            `db_info`: DB 연결 정보 딕셔너리. `set_connection` 메서드 호출 시 전달된다.
            `model_path`: `models.sql` 파일 경로. `"this"` → 현재 모듈 경로 내에서 자동 탐색한다.
            `tables`: 초기화 시 `self.tables`에 병합할 추가 테이블 매핑.
            `create_options`: 초기화 시 테이블 생성에 사용할 옵션.
            `parser`: 원본 데이터 파싱에 사용할 파서. 문자열 상수, 클래스 생성자, 또는 dict 중 하나
            `parser_config`: 파서 객체 초기화 시 전달할 설정 변수.
        """
        self.set_connection(**db_info)
        self.set_models(model_path)
        self.set_queries(keys=self.queries)

        if isinstance(tables, dict):
            self.tables.update(tables)
        if isinstance(create_options, dict):
            self.create(**create_options)

        if parser is not None:
            self.parser = parser
        if parser_config is not None:
            self.parser_config = parser_config

    @property
    def conn(self) -> Connection:
        """현재 DB 연결 객체를 반환한다."""
        return self.get_connection()

    @property
    def table_count(self) -> int:
        """`tables`에 등록된 테이블 수를 반환한다."""
        return len(self.tables.keys())

    def transform(self, obj: Any, **kwargs) -> Any:
        """`parse`로 데이터를 파싱한 뒤 `bulk_insert`로 DB에 삽입하는 파이프라인을 실행한다."""
        result = self.parse(obj, **kwargs)
        return self.bulk_insert(result, **kwargs)

    def parse(self, obj: Any, **kwargs) -> list | dict[TableAlias, list]:
        """`parser` 설정에 따라 원본 데이터를 파싱해 삽입 가능한 형태로 변환한다.

        - `parser`가 문자열(`"json"`, `"html"`, `"excel"`) → 해당 `ResponseTransformer`를 사용한다.
        - `parser`가 클래스 → 해당 클래스를 인스턴스화해 `transform` 메서드로 원본 데이터를 변환한다.
        - `parser`가 `dict` → 테이블 별칭별로 각 파서를 실행해 `{테이블_별칭: list}` 형태로 반환한다.
        - `parser`가 `None` → 원본 데이터를 그대로 반환한다.
        """
        config = self.parser_config or dict()
        if isinstance(self.parser, dict):
            return {table: parser(**config).transform(obj, **kwargs) for table, parser in self.parser.items()}
        elif isinstance(self.parser, type):
            return self.parser(**config).transform(obj, **kwargs)
        elif isinstance(self.parser, str):
            if self.parser == "json":
                return JsonTransformer(**config).transform(obj, **kwargs)
            elif self.parser == "html":
                return HtmlTransformer(**config).transform(obj, **kwargs)
            elif self.parser == "excel":
                return ExcelTransformer(**config).transform(obj, **kwargs)
        elif self.parser is None:
            return obj
        self.raise_parse_error("Could not determine the parser for the HTTP response.")

    @abstractmethod
    def bulk_insert(self, result: list | dict[TableAlias, list], **kwargs) -> Any:
        """파싱된 데이터를 DB에 일괄 삽입한다. 서브클래스에서 반드시 구현해야 한다."""
        raise NotImplementedError("The 'bulk_insert' method must be implemented.")

    def execute(self, *args, **kwargs) -> Any:
        """DB 연결을 통해 쿼리를 실행한다."""
        return self.get_connection().execute(*args, **kwargs)

    def close(self):
        """DB 연결을 닫는다."""
        self.conn.close()

    ############################ Connection ###########################

    @abstractmethod
    def get_connection(self) -> Connection:
        """현재 DB 연결 객체를 반환한다. 서브클래스에서 반드시 구현해야 한다."""
        raise NotImplementedError("The 'get_connection' method must be implemented.")

    @abstractmethod
    def set_connection(self, **kwargs):
        """DB 연결을 초기화한다. 서브클래스에서 반드시 구현해야 한다."""
        raise NotImplementedError("The 'set_connection' method must be implemented.")

    def __enter__(self) -> DBTransformer:
        return self

    def __exit__(self, type: type[BaseException], value: BaseException, traceback: TracebackType):
        self.close()

    ############################## Models #############################

    def get_models(self) -> Models:
        return self.__models

    def set_models(self, models: Literal["this"] | str | Path = "this"):
        """`Models` 객체를 초기화한다. `models = "this"` → 현재 모듈 경로 내에서 자동 탐색한다."""
        from linkmerce.common.models import Models
        self.__models = Models(self.default_models if models == "this" else models)

    @property
    def default_models(self) -> Path:
        """현재 클래스 모듈 경로를 기준으로 기본 `models.sql` 파일 경로를 반환한다."""
        from pathlib import Path
        root = Path(__file__).parent.parent
        return root / '/'.join(self.__class__.__module__.split('.')[1:-1]) / "models.sql"

    ############################# Queries #############################

    def get_queries(self) -> dict[str,str]:
        return self.__queires

    def set_queries(self, name: Literal["self"] | str = "self", keys: Sequence[str] | None = None):
        """`models.sql` 파일에서 쿼리를 불러온다. `name = "self"` → 현재 클래스명을 키로 사용한다."""
        name = self.__class__.__name__ if name == "self" else name
        self.__queires = self.get_models().read_models(name, keys=(self.queries if keys is None else keys))

    def get_query(self, key: str, render: dict | None = None) -> str:
        """지정한 키의 SQL 쿼리를 반환한다. `render`가 주어지면 Jinja 렌더링을 적용한다."""
        if self.has_query(key):
            query = self.get_queries()[key]
            return self.render_query(query, **render) if isinstance(render, dict) else query
        else:
            raise KeyError(f"'{key}' query does not exist.")

    def has_query(self, key: str) -> bool:
        """지정한 키의 쿼리가 있는지 확인한다."""
        return key in self.get_queries()

    ############################## Fetch ##############################

    def fetch_all(self, format: Literal["csv","json","parquet"], query: str) -> list[tuple] | list[dict] | bytes:
        """쿼리를 실행하고 결과를 지정한 형식(`csv`, `json`, `parquet`)으로 반환한다."""
        return self.conn.fetch_all(format, query)

    def fetch_all_to_csv(self, query: str) -> list[tuple]:
        """쿼리를 실행하고 결과를 CSV 형식의 `list[tuple]`로 반환한다."""
        return self.conn.fetch_all_to_csv(query)

    def fetch_all_to_json(self, query: str) -> list[dict]:
        """쿼리를 실행하고 결과를 JSON 형식의 `list[dict]`로 반환한다."""
        return self.conn.fetch_all_to_json(query)

    def fetch_all_to_parquet(self, query: str) -> bytes:
        """쿼리를 실행하고 결과를 Parquet 바이너리로 반환한다."""
        return self.conn.fetch_all_to_parquet(query)

    ############################### CRUD ##############################

    def create(self, query: str = str(), key: str = "create", render: dict | None = None) -> Any:
        """테이블 생성 쿼리를 실행한다. 쿼리가 없으면 `key = "create"`로 검색한다."""
        query = self.prepare_query(query, key, render=render)
        return self.conn.execute(query)

    def select(self, query: str = str(), key: str = "select", render: dict | None = None) -> Any:
        """조회 쿼리를 실행한다. 쿼리가 없으면 `key = "select"`로 검색한다."""
        query = self.prepare_query(query, key, render=render)
        return self.conn.execute(query)

    def update(self, query: str = str(), key: str = "update", render: dict | None = None) -> Any:
        """수정 쿼리를 실행한다. 쿼리가 없으면 `key = "update"`로 검색한다."""
        query = self.prepare_query(query, key, render=render)
        return self.conn.execute(query)

    def delete(self, query: str = str(), key: str = "delete", render: dict | None = None) -> Any:
        """삭제 쿼리를 실행한다. 쿼리가 없으면 `key = "delete"`로 검색한다."""
        query = self.prepare_query(query, key, render=render)
        return self.conn.execute(query)

    def insert_into(self, query: str = str(), key: str = "insert", render: dict | None = None) -> Any:
        """삽입 쿼리를 실행한다. 쿼리가 없으면 `key = "insert"`로 검색한다."""
        query = self.prepare_query(query, key, render=render)
        return self.conn.execute(query)

    def merge_into(self, query: str = str(), key: str = "merge", render: dict | None = None) -> Any:
        """병합 쿼리를 실행한다. 쿼리가 없으면 `key = "merge"`로 검색한다."""
        query = self.prepare_query(query, key, render=render)
        return self.conn.execute(query)

    ############################## Render #############################

    def prepare_query(self, query: str = str(), key: str = str(), render: dict | None = None) -> str:
        """`query`가 있으면 그대로 사용하고, 없으면 `key`로 쿼리를 검색한다. `render`가 있으면 Jinja 렌더링을 적용한다."""
        if query:
            return self.render_query(query, **render) if isinstance(render, dict) else query
        else:
            return self.get_query(key, render)

    def render_query(self, query_: str, **kwargs) -> str:
        """Jinja 템플릿 문법으로 쿼리 문자열을 렌더링한다."""
        from linkmerce.utils.jinja import render_string
        return render_string(query_, **kwargs)


###################################################################
############################## DuckDB #############################
###################################################################

class DuckDBTransformer(DBTransformer):
    """DuckDB를 사용해 데이터를 변환하고 적재하는 Transformer 구현 클래스.

    `DBTransformer`를 상속하며, DuckDB 전용 구문을 활용한
    JSON 데이터 일괄 삽입(`bulk_insert`)을 제공한다.

    주요 설정 변수:
    - `queries` - SQL 모델 파일에서 불러올 쿼리 키 목록
    - `tables` - `{테이블_별칭: 실제_테이블명}` 형식의 테이블 매핑
    - `parser` - 원본 데이터 파싱에 사용할 파서. 문자열 상수, 클래스 생성자, 또는 dict 중 하나
    - `parser_config` - 파서 객체 초기화 시 전달할 설정 변수 (`ParserConfig`)
    """

    queries: list[str] = ["create", "bulk_insert"]
    tables: dict[TableAlias, TableName] = {"table": "data"}
    parser: (
        Literal["json", "html", "excel"] |
        type[ResponseTransformer] |
        dict[TableAlias, type[ResponseTransformer]] |
        None
    ) = "json"
    parser_config: ParserConfig | None = None

    @property
    def conn(self) -> DuckDBConnection:
        """현재 DuckDB 연결 객체를 반환한다."""
        return self.get_connection()

    def bulk_insert(self, result: list | dict[TableAlias, list], **kwargs) -> DuckDBPyConnection | None:
        """파싱된 데이터를 DuckDB에 일괄 삽입한다. 데이터가 없으면 삽입하지 않는다."""
        render, params, total = self.prepare_bulk_params(result, **kwargs)
        if total > 0:
            query = self.prepare_query(key="bulk_insert", render=render)
            return self.execute(query, **params)

    def prepare_bulk_params(
            self,
            result: list | dict[TableAlias, list],
            render: dict | Literal["tables"] | None = "tables",
            params: dict | None = None,
            **kwargs
        ) -> tuple[dict, dict, int]:
        """삽입 쿼리에 필요한 Jinja 렌더 변수와 SQL 파라미터를 준비한다.

        - `result: dict` → `{테이블_별칭}_rows` 파라미터를 사용한다.
        - `result: list` → `rows` 파라미터를 사용한다.
        - `render = "tables"` → `self.tables`로 초기화한다.

        Returns:
            `(render, params, total)` — 렌더 컨텍스트, SQL 파라미터, 전체 행 수
        """
        render = self.tables if render == "tables" else render
        params = params or dict()
        total = 0

        if isinstance(result, dict):
            for table, rows in result.items():
                render[f"{table}_rows"] = self.expr_rows(f"{table}_rows")
                params[f"{table}_rows"] = rows
                total += len(rows)
        else:
            render["rows"] = self.expr_rows("rows")
            params["rows"] = result
            total += len(result)

        return render, params, total

    def expr_rows(self, param_name: str) -> str:
        """DuckDB `UNNEST` 구문을 사용해 `list[dict]` 파라미터를 인라인 테이블 표현식으로 변환한다."""
        return f"(SELECT {param_name}.* FROM (SELECT UNNEST(${param_name}) AS {param_name}))"

    ############################ Connection ###########################

    def get_connection(self) -> DuckDBConnection:
        """현재 DuckDB 연결 객체를 반환한다."""
        return self.__conn

    def set_connection(self, conn: DuckDBConnection | None = None, **kwargs):
        """DuckDB 연결을 초기화한다. `conn`이 제공되면 재사용하고, 아니면 새로 생성한다."""
        from linkmerce.common.load import DuckDBConnection
        self.__conn = conn if isinstance(conn, DuckDBConnection) else DuckDBConnection(**kwargs)

    def __enter__(self) -> DuckDBTransformer:
        return self

    ############################# Execute #############################

    @overload
    def execute(self, query: str, **params) -> DuckDBPyConnection:
        ...

    @overload
    def execute(self, query: str, obj: list, **params) -> DuckDBPyConnection:
        ...

    def execute(self, query: str, obj: list | None = None, **params) -> DuckDBPyConnection:
        """DuckDB 쿼리를 실행한다. `obj`는 위치 인자, `params`는 키워드 인자로 전달한다."""
        if obj is None:
            return self.conn.execute(query, **params)
        else:
            return self.conn.execute(query, obj, **params)

    ############################### SQL ###############################

    @overload
    def sql(self, query: str, **params) -> DuckDBPyRelation:
        ...

    @overload
    def sql(self, query: str, obj: list, **params) -> DuckDBPyRelation:
        ...

    def sql(self, query: str, obj: list | None = None, **params) -> DuckDBPyRelation:
        """SQL 쿼리를 실행한다. `obj`는 위치 인자, `params`는 키워드 인자로 전달한다."""
        if obj is None:
            return self.conn.sql(query, **params)
        else:
            return self.conn.sql(query, obj, **params)

    ############################### CRUD ##############################

    def create(
            self,
            query: str = str(),
            key: str = "create",
            render: dict | Literal["tables"] | None = "tables",
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        """테이블 생성 쿼리를 실행한다. `render = "tables"` → `self.tables`를 렌더 컨텍스트로 사용한다."""
        render = self.tables if render == "tables" else render
        query = self.prepare_query(query, key, render=render)
        return self.conn.execute(query, **(params or dict()))

    def select(
            self,
            query: str = str(),
            key: str = "select",
            render: dict | None = None,
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        """조회 쿼리를 실행한다. 쿼리가 없으면 `key`로 검색하고, `params`를 SQL 파라미터로 전달한다."""
        query = self.prepare_query(query, key, render=render)
        return self.conn.execute(query, **(params or dict()))

    def update(
            self,
            query: str = str(),
            key: str = "update",
            render: dict | None = None,
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        """수정 쿼리를 실행한다. 쿼리가 없으면 `key`로 검색하고, `params`를 SQL 파라미터로 전달한다."""
        query = self.prepare_query(query, key, render=render)
        return self.conn.execute(query, **(params or dict()))

    def delete(
            self,
            query: str = str(),
            key: str = "delete",
            render: dict | None = None,
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        """삭제 쿼리를 실행한다. 쿼리가 없으면 `key`로 검색하고, `params`를 SQL 파라미터로 전달한다."""
        query = self.prepare_query(query, key, render=render)
        return self.conn.execute(query, **(params or dict()))

    def insert_into(
            self,
            query: str = str(),
            key: str = "insert",
            render: dict | None = None,
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        """삽입 쿼리를 실행한다. 쿼리가 없으면 `key`로 검색하고, `params`를 SQL 파라미터로 전달한다."""
        query = self.prepare_query(query, key, render=render)
        return self.conn.execute(query, **(params or dict()))

    def merge_into(
            self,
            query: str = str(),
            key: str = "merge",
            render: dict | None = None,
            params: dict | None = None,
        ) -> DuckDBPyConnection:
        """UPSERT 쿼리를 실행한다. 쿼리가 없으면 `key`로 검색하고, `params`를 SQL 파라미터로 전달한다."""
        query = self.prepare_query(query, key, render=render)
        return self.conn.execute(query, **(params or dict()))
