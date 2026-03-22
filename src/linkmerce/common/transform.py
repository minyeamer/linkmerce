from __future__ import annotations

from abc import ABCMeta, abstractmethod

from typing import TypedDict, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal, Sequence, TypeVar
    from types import TracebackType
    Expression = TypeVar("Expression", bound=str)
    TableKey = TypeVar("TableKey", bound=str)
    TableName = TypeVar("TableName", bound=str)

    from bs4 import BeautifulSoup, Tag

    from linkmerce.common.load import Connection, DuckDBConnection
    from linkmerce.common.models import Models

    from duckdb import DuckDBPyConnection
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

    def render_kwargs(
            self,
            template: dict,
            kwargs: dict,
            on_missing: Literal["ignore", "raise"] = "raise",
        ) -> dict:
        """딕셔너리를 변환한다. `$key` 형식의 키는 키워드 인자(kwargs) 값으로 치환한다."""
        from linkmerce.utils.nested import hier_get
        render = dict()
        for path, value in template.items():
            if isinstance(value, str) and value.startswith('$'):
                value = hier_get(kwargs, value[1:], on_missing=on_missing)
            render[path] = value
        return render


###################################################################
########################## HTTP Response ##########################
###################################################################

class ResponseTransformer(Transformer, metaclass=ABCMeta):
    """HTTP 응답 데이터를 `list[dict]` 형식 객체로 변환하는 추상 클래스.

    아래 5단계 파이프라인을 통해 HTTP 응답 데이터를 처리한다:
    1. `assert_valid_response` - HTTP 응답 데이터의 타입 및 유효성 검증
    2. `get_scope` - 전체 데이터 중 파싱 대상이 되는 특정 지점(`scope`) 탐색
    3. `parse` - 탐색된 데이터를 필드 선택에 용이한 데이터 구조로 변환
    4. `select_fields` - 변환된 데이터에서 필요한 필드만 추출
    5. `extend_fields` - 필드 선택 결과에 파생 필드 생성 또는 값 변환"""

    scope: str | None = None
    fields: dict | list | None = None
    extends: dict | None = None
    on_missing: Literal["ignore", "raise"] = "raise"

    def __init__(
            self,
            scope: str | None = None,
            fields: dict | list | None = None,
            extends: dict | None = None,
            on_missing: Literal["ignore", "raise"] | None = None,
            **kwargs
        ):
        self.pre_init(**kwargs)
        if scope is not None:
            self.scope = scope
        if fields is not None:
            self.fields = fields
        if extends is not None:
            self.extends = extends
        if on_missing is not None:
            self.on_missing = on_missing
        self.post_init(**kwargs)

    def pre_init(self, **kwargs):
        """초기화 전에 호출되는 후크 메서드."""
        ...

    def post_init(self, **kwargs):
        """초기화가 완료된 후에 호출되는 후크 메서드."""
        ...

    def transform(self, obj: Any, **kwargs) -> list[dict]:
        """HTTP 응답 데이터 검증 > scope 탐색 > 파싱 > 필드 선택 및 변환 순서로 파이프라인을 실행한다."""
        self.assert_valid_response(obj, **kwargs)
        obj = self.get_scope(obj, **kwargs)
        data = self.parse(obj, **kwargs)
        return self.select_fields(data, **kwargs)

    def assert_valid_response(self, obj: Any, **kwargs):
        """HTTP 응답 데이터의 타입을 검증하고, 불일치 시 `RequestError`를 발생시킨다."""
        ...

    def get_scope(self, obj: Any, **kwargs) -> Any:
        """HTTP 응답 데이터에서 대상 데이터 범위를 탐색해 반환한다. 구현하지 않으면 입력을 그대로 반환한다."""
        return obj

    def parse(self, obj: Any, **kwargs) -> Any:
        """`scope` 탐색 결과를 추가로 가공한다. 구현하지 않으면 입력을 그대로 반환한다."""
        return obj

    @abstractmethod
    def select_fields(self, data: Any, extends: dict | None = None, **kwargs) -> list[dict]:
        """파싱된 데이터에서 필요한 필드만 추출한다. 서브클래스에서 반드시 구현해야 한다."""
        raise NotImplementedError("The 'select_fields' method must be implemented.")

    def extend_fields(self, item: dict, extends: dict | None = None, **kwargs) -> dict:
        """필드를 추출한 후 파생 필드를 생성하거나 값을 변환한다. 구현하지 않으면 입력을 그대로 반환한다."""
        extends = self.render_extends(extends, kwargs)
        if extends:
            from linkmerce.utils.nested import hier_update
            hier_update(item, extends, on_missing="create")
        return item

    def render_extends(self, m: dict | None = None, kwargs: dict = dict()) -> dict:
        """클래스 기본 파생 필드와 실행 시 전달된 파생 필드를 병합하여 반환한다.   
        클래스 기본 파생 필드에서 `$key` 형식의 값은 `kwargs`에서 찾아 치환한다."""
        extends = self.render_kwargs(self.extends, kwargs, self.on_missing) if self.extends else dict()
        return (extends | m) if isinstance(m, dict) else extends


class JsonTransformer(ResponseTransformer):
    """`dict` 또는 `list` 형태의 JSON 데이터를 변환하는 클래스.

    아래 5단계 파이프라인을 통해 JSON 데이터를 처리한다:
    1. `assert_valid_response` - HTTP 응답 데이터의 타입 및 유효성 검증
    2. `get_scope` - 전체 데이터 중 파싱 대상이 되는 특정 지점(`scope`) 탐색
    3. `parse` - 탐색된 데이터를 필드 선택에 용이한 데이터 구조로 변환
    4. `select_fields` - 변환된 데이터에서 필요한 필드만 추출
    5. `extend_fields` - 필드 선택 결과에 파생 필드 생성 또는 값 변환

    주요 설정 변수:
    - `dtype` - HTTP 응답 데이터에 기대되는 JSON 타입
    - `scope` - `hier_get` 함수로 탐색할 데이터의 중첩 키 경로
    - `fields` - `select_values` 함수에 전달할 스키마 정의
    - `extends` - 각 행마다 공통으로 추가할 기본 키-값 목록
    - `on_missing` - 필드 조회 실패 시 동작 (`raise` 또는 `ignore`)"""

    dtype: type[dict | list] = dict
    scope: str | None = None
    fields: dict | list | None = None
    extends: dict | None = None
    on_missing: Literal["ignore", "raise"] = "raise"

    def pre_init(self, dtype: type[dict | list] | None = None, **kwargs):
        if dtype is not None:
            self.dtype = dtype

    def transform(self, obj: JsonObject, **kwargs) -> list[dict]:
        """JSON 데이터 검증 > scope 탐색 > 파싱 > 필드 선택 및 변환 순서로 파이프라인을 실행한다."""
        return super().transform(obj, **kwargs)

    def assert_valid_response(self, obj: Any, **kwargs):
        """HTTP 응답 데이터의 타입이 `dtype`과 일치하는지 검증하고, 불일치 시 `RequestError`를 발생시킨다."""
        if not ((self.dtype is None) or isinstance(obj, self.dtype)):
            self.raise_request_error("HTTP response is not valid.")

    def get_scope(self, obj: JsonObject, **kwargs) -> JsonObject:
        """`scope`가 지정된 경우 `hier_get` 함수로 중첩 경로를 탐색해 대상 데이터를 반환한다."""
        if self.scope is not None:
            from linkmerce.utils.nested import hier_get
            return hier_get(obj, self.scope, on_missing="raise")
        return obj

    def select_fields(
            self,
            data: JsonObject,
            extends: dict | None = None,
            metadata: dict | None = None,
            **kwargs
        ) -> list[dict]:
        """`fields` 스키마에 따라 `select_values` 함수로 필드를 추출한다."""
        if not isinstance(data, (dict, list)):
            self.raise_parse_error("Could not select fields from the parsed data.")

        from linkmerce.utils.nested import select_values
        has_fields = self.fields is not None

        def _select_and_extend(item: dict, seq: int = None, total: int = None) -> dict:
            if has_fields:
                item = select_values(item, schema=self.fields, on_missing=self.on_missing)
            extra = {"metadata": ((metadata or dict()) | {"seq": seq, "total": total})}
            return self.extend_fields(item, extends, **(kwargs | extra))

        if isinstance(data, list):
            total = len(data)
            return [_select_and_extend(item, seq, total) for seq, item in enumerate(data) if isinstance(item, dict)]
        return [_select_and_extend(data)]


class HtmlTransformer(ResponseTransformer):
    """HTML(BeautifulSoup) 응답 데이터를 JSON 형식으로 변환하는 클래스.

    아래 5단계 파이프라인을 통해 HTTP 응답 데이터를 처리한다:
    1. `assert_valid_response` - HTTP 응답 데이터의 타입 및 유효성 검증
    2. `get_scope` - 전체 데이터 중 파싱 대상이 되는 특정 지점(`scope`) 탐색
    3. `parse` - 탐색된 데이터를 필드 선택에 용이한 데이터 구조로 변환
    4. `select_fields` - 변환된 데이터에서 필요한 필드만 추출
    5. `extend_fields` - 필드 선택 결과에 파생 필드 생성 또는 값 변환

    주요 설정 변수:
    - `scope` - 파싱 대상 요소를 탐색할 CSS 선택자 (없으면 입력 전체)
    - `fields` - `{필드명: CSS_선택자}` 형식의 스키마 정의
    - `extends` - 각 행마다 공통으로 추가할 기본 키-값 목록
    - `on_missing` - CSS 선택자 탐색 실패 시 동작 (`raise` 또는 `ignore`)"""

    scope: str | None = None
    fields: dict[str, str] | None = None
    extends: dict | None = None
    on_missing: Literal["ignore", "raise"] = "raise"

    def transform(self, obj: BeautifulSoup | Tag, **kwargs) -> list[dict]:
        """BeautifulSoup 객체 검증 > scope 탐색 > 파싱 > 필드 선택 및 변환 순서로 파이프라인을 실행한다."""
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

    def select_fields(
            self,
            data: Tag | list[Tag],
            extends: dict | None = None,
            metadata: dict | None = None,
            **kwargs
        ) -> list[dict]:
        """`{필드명: CSS_선택자}` 스키마에 따라 `select_attrs` 함수로 각 태그에서 값을 추출한다."""
        if (self.fields is None) or (not isinstance(data, (Tag, list))):
            self.raise_parse_error("Could not select fields from the parsed data.")

        from linkmerce.utils.parse import select_attrs
        has_fields = self.fields is not None

        def _select_and_extend(item: dict, seq: int = None, total: int = None) -> dict:
            if has_fields:
                item = select_attrs(item, schema=self.fields, on_missing=self.on_missing)
            extra = {"metadata": ((metadata or dict()) | {"seq": seq, "total": total})}
            return self.extend_fields(item, extends, **(kwargs | extra))

        if isinstance(data, list):
            total = len(data)
            return [_select_and_extend(item, seq, total) for seq, item in enumerate(data) if isinstance(item, Tag)]
        return [_select_and_extend(data)]


class ExcelTransformer(JsonTransformer):
    """Excel(openpyxl) 응답 데이터를 JSON 형식으로 변환하는 클래스.

    아래 5단계 파이프라인을 통해 HTTP 응답 데이터를 처리한다:
    1. `assert_valid_response` - HTTP 응답 데이터의 타입 및 유효성 검증
    2. `get_scope` - 엑셀 불러오기 전 작업, 필요 시 구현
    3. `parse` - 엑셀 시트를 JSON 형식으로 불러오고 필드 선택에 용이한 데이터 구조로 변환
    4. `select_fields` - 변환된 데이터에서 필요한 필드만 추출
    5. `extend_fields` - 필드 선택 결과에 파생 필드 생성 또는 값 변환

    주요 설정 변수:
    - `sheet_name` - Excel 시트 이름 (없으면 활성 시트)
    - `header` - Excel 헤더 행 번호 (1부터 시작)
    - `fields` - `select_values` 함수에 전달할 스키마 정의
    - `extends` - 각 행마다 공통으로 추가할 기본 키-값 목록
    - `on_missing` - 필드 조회 실패 시 동작 (`raise` 또는 `ignore`)"""

    sheet_name: str | None = None
    header: int = 1
    fields: dict[str, str] | None = None
    extends: dict | None = None
    on_missing: Literal["ignore", "raise"] = "raise"

    def pre_init(self, sheet_name: str | None = None, header: int | None = None, **kwargs):
        if sheet_name is not None:
            self.sheet_name = sheet_name
        if header is not None:
            self.header = header

    def transform(self, obj: bytes | str | Path, **kwargs) -> list[dict]:
        """Excel 파일 검증 > Excel 시트 불러오기 > 필드 선택 및 변환 순서로 파이프라인을 실행한다."""
        return super().transform(obj, **kwargs)

    def parse(self, obj: bytes | str | Path, **kwargs) -> list[dict]:
        """`sheet_name`이 지정된 경우 해당 시트를, 지정되지 않은 경우 활성 시트를 JSON 형식으로 불러온다."""
        from linkmerce.utils.excel import excel2json
        return excel2json(obj, sheet_name=self.sheet_name, header=self.header, warnings=False)


###################################################################
############################# Database ############################
###################################################################

class ParserConfig(TypedDict, total=False):
    """`ResponseTransformer` 객체 초기화 시 전달할 설정 변수."""
    scope: str | None
    fields: dict | list | None
    extends: dict | None
    on_missing: Literal["ignore", "raise"] | None


class DBTransformer(Transformer, metaclass=ABCMeta):
    """데이터베이스에 데이터를 변환하고 적재하는 추상 클래스.

    아래 2단계 파이프라인을 통해 HTTP 응답 데이터를 DB에 적재한다:
    1. `parser` - HTTP 응답 데이터를 JSON 형식 객체로 변환
    2. `bulk_insert` - 변환된 데이터를 DB에 일괄 삽입

    주요 설정 변수:
    - `queries` - SQL 모델 파일에서 불러올 쿼리 키 목록
    - `tables` - `{테이블_별칭: 실제_테이블명}` 형식의 테이블 매핑
    - `parser` - 원본 데이터 파싱에 사용할 파서. 문자열 상수, 클래스 생성자, 또는 dict 중 하나
    - `parser_config` - 파서 객체 초기화 시 전달할 설정 변수 (`ParserConfig`)"""

    queries: list[str] = ["create", "bulk_insert"]
    tables: dict[TableKey, TableName] = {"table": "data"}
    parser: (
        Literal["json", "html", "excel"] |
        type[ResponseTransformer] |
        dict[TableKey, type[ResponseTransformer]] |
        None
    ) = None
    parser_config: ParserConfig | None = None
    render: dict | None = None

    def __init__(
            self,
            db_info: dict = dict(),
            model_path: Literal["this"] | str | Path = "this",
            tables: dict[TableKey, TableName] | None = None,
            create_options: dict[str, TableName] | None = None,
            parser: type[ResponseTransformer] | None = None,
            parser_config: ParserConfig | None = None,
            render: dict | None = None,
            **kwargs
        ):
        """DB 연결 및 SQL 쿼리를 불러오고 테이블을 생성한다.

        Args:
            `db_info`: DB 연결 정보 딕셔너리. `set_connection` 메서드 호출 시 전달된다.
            `model_path`: `models.sql` 파일 경로. `"this"` -> 현재 모듈 경로 내에서 자동 탐색한다.
            `tables`: 초기화 시 `self.tables`에 병합할 추가 테이블 매핑.
            `create_options`: 초기화 시 테이블 생성에 사용할 옵션.
            `parser`: 원본 데이터 파싱에 사용할 파서. 문자열 상수, 클래스 생성자, 또는 dict 중 하나
            `parser_config`: 파서 객체 초기화 시 전달할 설정 변수.
            `render`: SQL 쿼리 렌더링(Jinja)에 사용할 기본 컨텍스트 설정 변수.
            `**kwargs`: 하위 클래스에서 `pre_init` 또는 `post_init`을 통해 처리할 추가 인자."""
        self.pre_init(**kwargs)

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
        if render is not None:
            self.render = render

        self.post_init(**kwargs)

    def pre_init(self, **kwargs):
        """초기화 전에 호출되는 후크 메서드."""
        ...

    def post_init(self, **kwargs):
        """초기화가 완료된 후에 호출되는 후크 메서드."""
        ...

    @property
    def conn(self) -> Connection:
        """현재 DB 연결 객체를 반환한다."""
        return self.get_connection()

    @property
    def table(self) -> TableName:
        """기본 테이블명을 반환한다. `table` 키가 없으면 `KeyError`를 발생시킨다."""
        return self.tables["table"]

    @property
    def table_count(self) -> int:
        """`tables`에 등록된 테이블 수를 반환한다."""
        return len(self.tables.keys())

    def transform(self, obj: Any, **kwargs) -> Any:
        """`parse`로 데이터를 파싱한 뒤 `bulk_insert`로 DB에 삽입하는 파이프라인을 실행한다."""
        result = self.parse(obj, **kwargs)
        return self.bulk_insert(result, **kwargs)

    def parse(self, obj: Any, **kwargs) -> list | dict[TableKey, list]:
        """`parser` 설정에 따라 원본 데이터를 파싱해 삽입 가능한 형태로 변환한다.

        - `parser`가 문자열(`"json"`, `"html"`, `"excel"`) -> 해당 `ResponseTransformer`를 사용한다.
        - `parser`가 클래스 -> 해당 클래스를 인스턴스화해 `transform` 메서드로 원본 데이터를 변환한다.
        - `parser`가 `dict` -> 테이블 별칭별로 각 파서를 실행해 `{테이블_별칭: list}` 형태로 반환한다.
        - `parser`가 `None` -> 원본 데이터를 그대로 반환한다."""

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
    def bulk_insert(self, result: list | dict[TableKey, list], render: dict | None = None, **kwargs) -> Any:
        """파싱된 데이터를 DB에 일괄 삽입한다. 서브클래스에서 반드시 구현해야 한다."""
        raise NotImplementedError("The 'bulk_insert' method must be implemented.")

    def get_render(self, render: dict | None = None) -> dict:
        """클래스 기본 렌더 컨텍스트와 실행 시 전달된 렌더 컨텍스트를 병합하여 반환한다."""
        return (self.render or dict()) | (render or dict())

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

    def execute(self, *args, **kwargs) -> Any:
        """DB 연결을 통해 쿼리를 실행한다."""
        return self.get_connection().execute(*args, **kwargs)

    def __enter__(self) -> DBTransformer:
        return self

    def __exit__(self, type: type[BaseException], value: BaseException, traceback: TracebackType):
        self.close()

    ############################## Models #############################

    def get_models(self) -> Models:
        return self.__models

    def set_models(self, models: Literal["this"] | str | Path = "this"):
        """`Models` 객체를 초기화한다. `models = "this"` -> 현재 모듈 경로 내에서 자동 탐색한다."""
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
        """`models.sql` 파일에서 쿼리를 불러온다. `name = "self"` -> 현재 클래스명을 키로 사용한다."""
        name = self.__class__.__name__ if name == "self" else name
        self.__queires = self.get_models().read_models(name, keys=(self.queries if keys is None else keys))

    def get_query(self, query_key: str, render: dict | None = None) -> str:
        """지정한 키의 SQL 쿼리를 반환한다. `render`가 주어지면 Jinja 렌더링을 적용한다."""
        if self.has_query(query_key):
            query = self.get_queries()[query_key]
            return self.render_query(query, **render) if isinstance(render, dict) else query
        else:
            raise KeyError(f"'{query_key}' query does not exist.")

    def has_query(self, query_key: str) -> bool:
        """지정한 키의 쿼리가 있는지 확인한다."""
        return query_key in self.get_queries()

    ############################## Fetch ##############################

    def fetch_all(self, format: Literal["csv", "json", "parquet"], query: str) -> list[tuple] | list[dict] | bytes:
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

    def create(self, query_key: str = "create", query: str = str(), render: dict | None = None) -> Any:
        """테이블 생성 쿼리를 실행한다. 쿼리가 없으면 `key = "create"`로 검색한다."""
        query = self.prepare_query(query_key, query, render=render)
        return self.execute(query)

    def select(self, query_key: str = "select", query: str = str(), render: dict | None = None) -> Any:
        """조회 쿼리를 실행한다. 쿼리가 없으면 `key = "select"`로 검색한다."""
        query = self.prepare_query(query_key, query, render=render)
        return self.execute(query)

    def update(self, query_key: str = "update", query: str = str(), render: dict | None = None) -> Any:
        """수정 쿼리를 실행한다. 쿼리가 없으면 `key = "update"`로 검색한다."""
        query = self.prepare_query(query_key, query, render=render)
        return self.execute(query)

    def delete(self, query_key: str = "delete", query: str = str(), render: dict | None = None) -> Any:
        """삭제 쿼리를 실행한다. 쿼리가 없으면 `key = "delete"`로 검색한다."""
        query = self.prepare_query(query_key, query, render=render)
        return self.execute(query)

    def insert_into(self, query_key: str = "insert", query: str = str(), render: dict | None = None) -> Any:
        """삽입 쿼리를 실행한다. 쿼리가 없으면 `key = "insert"`로 검색한다."""
        query = self.prepare_query(query_key, query, render=render)
        return self.execute(query)

    def merge_into(self, query_key: str = "merge", query: str = str(), render: dict | None = None) -> Any:
        """병합 쿼리를 실행한다. 쿼리가 없으면 `key = "merge"`로 검색한다."""
        query = self.prepare_query(query_key, query, render=render)
        return self.execute(query)

    ############################## Render #############################

    def prepare_query(self, query_key: str = str(), query: str = str(), render: dict | None = None) -> str:
        """`query`가 있으면 그대로 사용하고, 없으면 `key`로 쿼리를 검색한다. `render`가 있으면 Jinja 렌더링을 적용한다."""
        if query:
            return self.render_query(query, **render) if render else query
        else:
            return self.get_query(query_key, render)

    def render_query(self, query_: str, **kwargs) -> str:
        """Jinja 템플릿 문법으로 쿼리 문자열을 렌더링한다."""
        from linkmerce.utils.jinja import render_string
        return render_string(query_, **kwargs)


###################################################################
############################## DuckDB #############################
###################################################################

class DuckDBTransformer(DBTransformer):
    """DuckDB를 사용해 데이터를 변환하고 적재하는 Transformer 구현 클래스.

    아래 2단계 파이프라인을 통해 HTTP 응답 데이터를 DB에 적재한다:
    1. `parser` - HTTP 응답 데이터를 JSON 형식 객체로 변환
    2. `prepare_bulk_params` - 쿼리에 적용할 Jinja 렌더 변수와 SQL 파라미터 설정
    3. `bulk_insert` - 변환된 데이터를 DB에 일괄 삽입

    DuckDB 전용 구문을 지원한다. (문서 참조: https://duckdb.org/docs/stable/)

    주요 설정 변수:
    - `extractor` - 변환할 HTTP 응답 데이터를 가져오는 `Extractor` 클래스명 매핑
    - `queries` - SQL 모델 파일에서 불러올 쿼리 키 목록
    - `tables` - `{테이블_별칭: 실제_테이블명}` 형식의 테이블 매핑
    - `parser` - 원본 데이터 파싱에 사용할 파서. 문자열 상수, 클래스 생성자, 또는 dict 중 하나
    - `parser_config` - 파서 객체 초기화 시 전달할 설정 변수 (`ParserConfig`)"""

    extractor: str | None = None
    queries: list[str] = ["create", "bulk_insert"]
    tables: dict[TableKey, TableName] = {"table": "data"}
    parser: (
        Literal["json", "html", "excel"] |
        type[ResponseTransformer] |
        dict[TableKey, type[ResponseTransformer]] |
        None
    ) = None
    parser_config: ParserConfig | None = None
    render: dict | None = None
    params: dict | None = None

    def __init__(
            self,
            db_info: dict = dict(),
            model_path: Literal["this"] | str | Path = "this",
            tables: dict[TableKey, TableName] | None = None,
            create_options: dict[str, TableName] | None = None,
            parser: type[ResponseTransformer] | None = None,
            parser_config: ParserConfig | None = None,
            render: dict | None = None,
            params: dict | None = None,
            **kwargs
        ):
        """DB 연결 및 SQL 쿼리를 불러오고 테이블을 생성한다.

        Args:
            `db_info`: DB 연결 정보 딕셔너리. `set_connection` 메서드 호출 시 전달된다.
            `model_path`: `models.sql` 파일 경로. `"this"` -> 현재 모듈 경로 내에서 자동 탐색한다.
            `tables`: 초기화 시 `self.tables`에 병합할 추가 테이블 매핑.
            `create_options`: 초기화 시 테이블 생성에 사용할 옵션.
            `parser`: 원본 데이터 파싱에 사용할 파서. 문자열 상수, 클래스 생성자, 또는 dict 중 하나
            `parser_config`: 파서 객체 초기화 시 전달할 설정 변수.
            `render`: SQL 쿼리 렌더링(Jinja)에 사용할 기본 컨텍스트 설정 변수.
            `params`: SQL 쿼리 실행 시 전달할 기본 파라미터($변수) 설정 변수.
            `**kwargs`: 하위 클래스에서 `pre_init` 또는 `post_init`을 통해 처리할 추가 인자.
        """
        self.pre_init(**kwargs)

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
        if render is not None:
            self.render = render
        if params is not None:
            self.params = params

        self.post_init(**kwargs)

    @property
    def conn(self) -> DuckDBConnection:
        """현재 DuckDB 연결 객체를 반환한다."""
        return self.get_connection()

    def bulk_insert(
            self,
            result: list | dict[TableKey, list],
            query_key: str = "bulk_insert",
            render: dict | Literal["tables"] | None = "tables",
            params: dict | None = None,
            **kwargs
        ) -> list[DuckDBPyConnection]:
        """파싱된 데이터를 DuckDB에 일괄 삽입한다. 데이터가 없으면 삽입하지 않는다."""
        render, params, total = self.prepare_bulk_params(result, render, params, **kwargs)
        if total > 0:
            query = self.prepare_query(query_key, render=render)
            print(query)
            return self.execute(query, params)
        else:
            return list()

    def prepare_bulk_params(
            self,
            result: list | dict[TableKey, list],
            render: dict | Literal["tables"] | None = "tables",
            params: dict | None = None,
            **kwargs
        ) -> tuple[dict, dict, int]:
        """삽입 쿼리에 필요한 Jinja 렌더 변수와 SQL 파라미터를 준비한다.

        - `result: dict` -> `{테이블_별칭}_rows` 파라미터를 사용한다.
        - `result: list` -> `rows` 파라미터를 사용한다.
        - `render = "tables"` -> `self.tables`로 초기화한다.

        Returns:
            `(render, params, total)` — 렌더 컨텍스트, SQL 파라미터, 전체 행 수
        """
        render = self.render_context(self.tables if render == "tables" else render, kwargs)
        params = self.render_params(params, kwargs)
        total = 0

        if isinstance(result, dict):
            for name, rows in result.items():
                render[f"{name}_rows"] = self.expr_rows(f"{name}_rows")
                params[f"{name}_rows"] = rows or list()
                if rows: total += len(rows)
        elif isinstance(result, list) or (not result):
            render["rows"] = self.expr_rows("rows")
            params["rows"] = result or list()
            if result: total += len(result)
        else:
            self.raise_parse_error("The result must be in JSON format (dict or list).")

        return render, params, total

    def render_context(self, m: dict | None = None, kwargs: dict = dict()) -> dict:
        """클래스 기본 렌더 컨텍스트와 실행 시 전달된 렌더 컨텍스트를 병합하여 반환한다.   
        클래스 기본 렌더 컨텍스트에서 `$key` 형식의 값은 `kwargs`에서 찾아 치환한다."""
        render = self.render_kwargs(self.render, kwargs, on_missing="ignore") if self.render else dict()
        return (render | m) if isinstance(m, dict) else render

    def render_params(self, m: dict | None = None, kwargs: dict = dict()) -> dict:
        """클래스 기본 파라미터와 실행 시 전달된 파라미터를 병합하여 반환한다.   
        클래스 기본 파라미터에 `$key` 형식의 값은 `kwargs`에서 찾아 치환한다."""
        params = self.render_kwargs(self.params, kwargs, on_missing="ignore") if self.params else dict()
        return (params | m) if isinstance(m, dict) else params

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

    def execute(self, query: str, params: dict | None = None) -> list[DuckDBPyConnection]:
        """DuckDB 연결을 통해 쿼리를 실행한다."""
        return self.get_connection().execute(query, params)

    def __enter__(self) -> DuckDBTransformer:
        return self

    ############################### CRUD ##############################

    def create(
            self,
            query_key: str = "create",
            query: str = str(),
            render: dict | Literal["tables"] | None = "tables",
            params: dict | None = None,
        ) -> list[DuckDBPyConnection]:
        """테이블 생성 쿼리를 실행한다. `render = "tables"` -> `self.tables`를 렌더 컨텍스트로 사용한다."""
        render = self.tables if render == "tables" else render
        query = self.prepare_query(query_key, query, render=render)
        return self.execute(query, params)

    def select(
            self,
            query_key: str = "select",
            query: str = str(),
            render: dict | None = None,
            params: dict | None = None,
        ) -> list[DuckDBPyConnection]:
        """조회 쿼리를 실행한다. 쿼리가 없으면 `key`로 검색하고, `params`를 SQL 파라미터로 전달한다."""
        query = self.prepare_query(query_key, query, render=render)
        return self.execute(query, params)

    def update(
            self,
            query_key: str = "update",
            query: str = str(),
            render: dict | None = None,
            params: dict | None = None,
        ) -> list[DuckDBPyConnection]:
        """수정 쿼리를 실행한다. 쿼리가 없으면 `key`로 검색하고, `params`를 SQL 파라미터로 전달한다."""
        query = self.prepare_query(query_key, query, render=render)
        return self.execute(query, params)

    def delete(
            self,
            query_key: str = "delete",
            query: str = str(),
            render: dict | None = None,
            params: dict | None = None,
        ) -> list[DuckDBPyConnection]:
        """삭제 쿼리를 실행한다. 쿼리가 없으면 `key`로 검색하고, `params`를 SQL 파라미터로 전달한다."""
        query = self.prepare_query(query_key, query, render=render)
        return self.execute(query, params)

    def insert_into(
            self,
            query_key: str = "insert",
            query: str = str(),
            render: dict | None = None,
            params: dict | None = None,
        ) -> list[DuckDBPyConnection]:
        """삽입 쿼리를 실행한다. 쿼리가 없으면 `key`로 검색하고, `params`를 SQL 파라미터로 전달한다."""
        query = self.prepare_query(query_key, query, render=render)
        return self.execute(query, params)

    def merge_into(
            self,
            query_key: str = "merge",
            query: str = str(),
            render: dict | None = None,
            params: dict | None = None,
        ) -> list[DuckDBPyConnection]:
        """UPSERT 쿼리를 실행한다. 쿼리가 없으면 `key`로 검색하고, `params`를 SQL 파라미터로 전달한다."""
        query = self.prepare_query(query_key, query, render=render)
        return self.execute(query, params)
