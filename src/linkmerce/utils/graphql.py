from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Hashable, Literal, TypeVar
    _KT = TypeVar("_KT", Hashable)
    _VT = TypeVar("_VT", Any)


class GraphQLObject:
    """GraphQL 객체를 문자열로 포맷팅하는 기반 클래스."""

    def format(self, object_: dict | list | str, indent: int=0, step: int=2, linebreak: bool=True, colons: bool=False) -> str:
        """딕셔너리, 리스트, 문자열을 들여쓰기와 줄바꾼이 적용된 문자열로 변환한다."""
        indent, seq = (indent if linebreak else 0), ('\n' if linebreak else ', ')
        if isinstance(object_, dict):
            return seq.join([self._format_kv(key, value, indent, step, linebreak, colons) for key, value in object_.items()])
        elif isinstance(object_, list):
            return seq.join([self.format(value, indent, step, linebreak, colons) for value in object_])
        elif isinstance(object_, str):
            return (' '*indent) + object_
        else:
            self.raise_type_error(object_)

    def _format_kv(self, key: _KT, value: _VT, indent: int=0, step: int=2, linebreak: bool=True, colons: bool=False) -> str:
        """키-값 쌍을 중첩된 중괄호 포맷으로 변환한다."""
        indent, seq = (indent if linebreak else 0), ('\n' if linebreak else '')
        formatted = self.format(value, indent+step, step, linebreak, colons)
        body = _add_brackets(f"{seq}{formatted}{seq}{(' '*indent)}", shape="curly")
        return f"{(' '*indent)}{key}{(':' if colons else '')} {body}"

    def raise_type_error(self, object_: Any):
        raise TypeError(f"'{type(object_)}' is not valid {self.__class__.__name__} type.")


class GraphQLVariables(GraphQLObject):
    """GraphQL 변수를 관리하고 쿼리 문자열로 포맷팅하는 클래스."""

    def __init__(self, variables: dict | list):
        self.set_variables(variables)

    def get_variables(self) -> dict | list:
        return self.variables

    def set_variables(self, variables: dict | list):
        if isinstance(variables, (dict, list)):
            self.variables = variables
        else:
            self.raise_type_error(variables)

    def generate_variables(
            self,
            indent: int = 4,
            step: int = 2,
            linebreak: bool = True,
            colons: bool = True,
            prefix: str = str(),
            suffix: str = str(),
            replace: dict = dict(),
            **kwargs
        ) -> str:
        """변수를 괄호와 포맷이 적용된 쿼리 문자열로 생성한다."""
        if isinstance(self.variables, dict):
            formatted = self.format_variables_dict(self.variables, indent, step, linebreak, colons)
        else:
            formatted = self.format_variables_list(self.variables, indent, step, linebreak, bracket=True)
        return prefix + _replace(formatted, replace) + suffix

    def format_variables_list(self, variables: list[str], indent: int=4, step: int=2, linebreak: bool=True, bracket: bool=True) -> str:
        """변수 리스트를 `"name: $name"` 포맷 문자열로 변환한다."""
        if linebreak:
            formatted = ('\n'+' '*indent).join([(name+': $'+name) for name in variables])
            lspace, rspace = ('\n'+' '*indent), ('\n'+' '*max(indent-step, 0))
            return _add_brackets(f"{lspace}{formatted}{rspace}", shape="round", disable=(not bracket))
        else:
            formatted = ', '.join([(name+': $'+name) for name in variables])
            return _add_brackets(formatted, shape="round", disable=(not bracket))

    def format_variables_dict(self, variables: dict, indent: int=4, step: int=2, linebreak: bool=True, colons: bool=True) -> str:
        """변수 딕셔너리를 중첩된 포맷 문자열로 변환한다."""
        formatted_map = {key: self.format_variables_list(value, linebreak=False, bracket=False) for key, value in variables.items()}
        formatted = self.format(formatted_map, indent, step, linebreak=False, colons=colons)
        lspace = ('\n'+' '*indent) if linebreak else str()
        rspace = ('\n'+' '*max(indent-step, 0)) if linebreak else str()
        return _add_brackets(f"{lspace}{formatted}{rspace}", shape="round")


class GraphQLFields(GraphQLObject):
    """GraphQL 필드 선택(Selection) 정의를 관리하는 클래스."""

    def __init__(self, fields: dict | list, typename: bool=True):
        self.set_fields(fields, typename)

    def get_fields(self) -> dict | list:
        return self.fields

    def set_fields(self, fields: dict | list | str, typename: bool=True):
        self.fields = self._set_nested_fields(fields, typename)

    def _set_nested_fields(self, fields: dict | list | str, typename: bool=True) -> dict | list:
        """중첩된 필드 구조를 재귀적으로 설정하고 `__typename`을 추가한다."""
        if isinstance(fields, GraphQLFragment):
            appendix = ["__typename"] if typename else list()
            return [fields, *appendix]
        elif isinstance(fields, dict):
            return {key: self._set_nested_fields(value, typename) for key, value in fields.items()}
        elif isinstance(fields, list):
            appendix = ["__typename"] if typename else list()
            return [self._set_nested_fields(field, typename) for field in fields] + appendix
        elif isinstance(fields, str):
            return fields
        else:
            self.raise_type_error(fields)

    def generate_fields(
            self,
            indent: int = 4,
            step: int = 2,
            linebreak: bool = True,
            colons: bool = False,
            prefix: str = str(),
            suffix: str = str(),
            replace: dict = dict(),
            **kwargs
        ) -> str:
        """필드를 중괄호와 포맷이 적용된 쿼리 문자열로 생성한다."""
        formatted = self.format(self.fields, indent, step, linebreak, colons)
        lspace, rspace = '\n', ('\n'+' '*max(indent-step, 0))
        return prefix + _replace(_add_brackets(f"{lspace}{formatted}{rspace}", shape="curly"), replace) + suffix

    def format(self, object_: dict | list | str, indent: int=0, step: int=2, linebreak: bool=True, colons: bool=False) -> str:
        object_ = f"...{object_.name}" if isinstance(object_, GraphQLFragment) else object_
        return super().format(object_, indent, step, linebreak, colons)


class GraphQLSelection(GraphQLObject):
    """GraphQL 셀렉션(필드 + 변수 조합)을 관리하는 클래스."""

    def __init__(self, name: str, variables: dict | list, fields: dict | list | None=None, alias: str=str(), typename: bool=True):
        self.name = name
        self.alias = alias
        self.set_variables(variables)
        self.set_fields(fields, typename)

    def get_variables(self) -> dict | list:
        return self.variables.get_variables()

    def set_variables(self, variables: dict | list | GraphQLVariables):
        self.variables = variables if isinstance(variables, GraphQLVariables) else GraphQLVariables(variables)

    def get_fields(self) -> dict | list:
        return self.fields.get_fields() if self.fields is not None else list()

    def set_fields(self, fields: dict | list | GraphQLFields | None=None, typename: bool=True):
        if fields is None:
            self.fields = None
        elif isinstance(fields, GraphQLFields):
            self.fields = fields
        else:
            self.fields = GraphQLFields(fields, typename)

    def generate_selection(
            self,
            indent: int = 2,
            step: int = 2,
            variables: dict = dict(),
            fields: dict = dict(),
            **kwargs
        ) -> str:
        """셀렉션을 변수와 필드가 결합된 쿼리 문자열로 생성한다."""
        name = f"{self.name}: {self.alias}" if self.alias else self.name
        variables = self.variables.generate_variables(indent+step, step, **variables)
        fields = (' '+self.fields.generate_fields(indent+step, step, **fields)) if self.fields is not None else str()
        lspace, rspace = ('\n'+' '*indent), ('\n'+' '*max(indent-step, 0))
        return _add_brackets(f"{lspace}{name}{variables}{fields}{rspace}", shape="curly")


class GraphQLFragment(GraphQLObject):
    """GraphQL Fragment 정의를 관리하는 클래스."""

    def __init__(self, name: str, type: str, fields: dict | list, typename: bool=True):
        self.name = name
        self.type = type
        self.set_fields(fields, typename)

    def get_fields(self) -> dict | list:
        return self.fields.get_fields()

    def set_fields(self, fields: dict | list, typename: bool=True):
        self.fields = fields if isinstance(fields, GraphQLFields) else GraphQLFields(fields, typename)

    def generate_fragment(
            self,
            indent: int = 0,
            step: int = 2,
            linebreak: bool = True,
            colons: bool = False,
            prefix: str = str(),
            suffix: str = str(),
            replace: dict = dict(),
            **kwargs
        ) -> str:
        """Fragment를 `"fragment Name on Type { ... }"` 포맷 문자열로 생성한다."""
        fields = self.fields.get_fields()
        formatted = self.format({f"fragment {self.name} on {self.type}": fields}, indent, step, linebreak, colons)
        return prefix + _replace(formatted, replace) + suffix


class GraphQLOperation(GraphQLObject):
    """GraphQL 전체 작업(query/mutation)을 조합하여 쿼리를 생성하는 클래스."""

    def __init__(self, operation: str, variables: dict, types: dict, selection: dict):
        self.operation = operation
        self.variables = variables
        self.types = types
        self.set_selection(selection)
        self.set_fragments(self.selection.get_fields())

    def set_selection(self, selection: dict) -> GraphQLSelection:
        """셀렉션 객체를 설정한다."""
        if isinstance(selection, GraphQLSelection):
            self.selection = selection
        elif isinstance(selection, dict):
            self.selection = GraphQLSelection(**selection)
        else:
            self.raise_type_error(selection)

    def set_fragments(self, fields: dict | list):
        """필드에서 `GraphQLFragment` 객체를 재귀적으로 추출한다."""
        def extract_fragments(data: dict | list) -> list[GraphQLFragment]:
            values = list()
            if isinstance(data, GraphQLFragment):
                values.append(data)
            elif isinstance(data, dict):
                for __value in data.values():
                    values.extend(extract_fragments(__value))
            elif isinstance(data, list):
                for __value in data:
                    values.extend(extract_fragments(__value))
            return values
        self.fragments = extract_fragments(fields)

    def generate_body(self, query_options: dict = dict()) -> dict:
        """쿼리 본문(`operationName`, `variables`, `query`)을 딕셔너리로 생성한다."""
        data = {"operationName": self.operation} if self.operation else dict()
        data["variables"] = self.variables
        data["query"] = self.generate_query(**query_options)
        return data

    def generate_query(
            self,
            command: str = "query",
            selection: dict = dict(),
            fragment: dict = dict(),
            prefix: str = str(),
            suffix: str = str(),
            **kwargs
        ) -> str:
        """완성된 GraphQL 쿼리 문자열을 생성한다."""
        signature = self.generate_signature()
        selection = self.selection.generate_selection(**selection)
        fragments = self.generate_fragments(**fragment)
        return f"{prefix}{command} {signature} {selection}{fragments}{suffix}"

    def generate_signature(self) -> str:
        """작업 시그니처(이름 + 변수 타입 선언)를 생성한다."""
        formatted = ', '.join([f"${__name}: {__type}" for __name, __type in self.types.items()])
        return self.operation + _add_brackets(formatted, shape="round")

    def generate_fragments(self, indent: int=0, step: int=2, **kwargs) -> str:
        """모든 Fragment를 쿼리 문자열로 생성한다."""
        fragments = '\n\n'.join([fragment.generate_fragment(indent, step) for fragment in self.fragments])
        return f"\n\n{fragments}" if fragments else str()


def _add_brackets(text: str, shape: Literal["round", "curly", "square"]="round", disable: bool=False) -> str:
    """텍스트에 지정된 형태의 괄호(`()`, `{}`, `[]`)를 추가한다."""
    if disable:
        return text
    if shape == "round":
        return '(' + text + ')'
    elif shape == "curly":
        return '{' + text + '}'
    elif shape == "square":
        return '[' + text + ']'
    else:
        return text


def _replace(text: str, replace: dict[str, str] = dict()) -> str:
    """텍스트에서 replace 딕셔너리의 키-값 쌍을 순차적으로 치환한다."""
    for old, new in replace.items():
        text = text.replace(old, new)
    return text
