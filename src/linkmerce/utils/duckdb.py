from __future__ import annotations

import duckdb

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Dict, List, Literal, Tuple
    from duckdb import DuckDBPyRelation


def execute_query(query: str, params: Dict | None = None, format: Literal["csv","json"] | None = None) -> List[Any]:
    relation = duckdb.query(query, params=params)
    return _parse_query(relation, format)


def _parse_query(relation: DuckDBPyRelation, format: Literal["csv","json"] | None = None) -> List[Dict] | List[Tuple]:
    if format == "csv":
        columns = [column[0] for column in relation.description]
        return [columns] + relation.fetchall()
    elif format == "json":
        columns = [column[0] for column in relation.description]
        return [dict(zip(columns, row)) for row in relation.fetchall()]
    else:
        return relation.fetchall()
