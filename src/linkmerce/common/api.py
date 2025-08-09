from __future__ import annotations

from typing import Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal, Type
    from linkmerce.common.extract import Extractor, JsonObject
    from linkmerce.common.transform import Transformer, DBTransformer, DuckDBTransformer
    from linkmerce.common.load import DuckDBConnection


###################################################################
############################## Import #############################
###################################################################

def import_extractor(module: str, attr: str) -> Type[Extractor]:
    from importlib import import_module
    obj = import_module(_join(module, "extract"))
    return getattr(obj, attr)


def import_transformer(module: str, attr: str) -> Type[Transformer]:
    from importlib import import_module
    obj = import_module(_join(module, "transform"))
    return getattr(obj, attr)


def import_dbt(module: str | None = None, attr: str | None = None) -> Type[DBTransformer]:
    if module and attr:
        return import_transformer(module, attr)
    else:
        from importlib import import_module
        module = import_module("linkmerce.common.transform")
        return getattr(module, "DummyDBTransformer")


def _join(path: str, name: str) -> str:
    if path.startswith('.'):
        path = "linkmerce.core" + path
    return path + '.' + name


###################################################################
############################### Run ###############################
###################################################################

def run(
        module: str,
        extractor: str,
        transformer: str | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        args: tuple = tuple(),
        kwargs: dict = dict(),
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> Any:
    extract_options = extract_options.copy() if isinstance(extract_options, dict) else dict()
    if transformer and ("parser" not in extract_options):
        transformer_ = import_transformer(module, transformer)(**(transform_options or dict()))
        extract_options["parser"] = transformer_.transform
    extractor_ = import_extractor(module, extractor)(**extract_options)
    return _extract(extractor_, how, args, kwargs)


def run_with_connection(
        module: str,
        extractor: str,
        transformer: str | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        args: tuple = tuple(),
        kwargs: dict = dict(),
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> Any:
    extract_options = extract_options.copy() if isinstance(extract_options, dict) else dict()
    has_parser = ("parser" not in extract_options)
    with import_dbt(module if has_parser else None, transformer)(**(transform_options or dict())) as transformer_:
        if transformer and has_parser:
            extract_options["parser"] = transformer_.transform
        extractor_ = import_extractor(module, extractor)(**extract_options)
        return _extract(extractor_, how, args, kwargs)


def _extract(
        extractor: Extractor,
        how: Literal["sync","async","async_loop"] = "sync",
        args: tuple = tuple(),
        kwargs: dict = dict(),
    ) -> Any:
    if how == "sync":
        return extractor.extract(*args, **kwargs)
    elif how == "async":
        import asyncio
        return asyncio.run(extractor.extract_async(*args, **kwargs))
    elif how == "async_loop":
        import asyncio, nest_asyncio
        nest_asyncio.apply()
        loop = asyncio.get_running_loop()
        task = asyncio.create_task(extractor.extract_async(*args, **kwargs))
        return loop.run_until_complete(task)
    else:
        raise ValueError("Invalid value for how to run. Supported values are: sync, async, async_loop.")


###################################################################
######################### Run with DuckDB #########################
###################################################################

def run_with_duckdb(
        module: str,
        extractor: str,
        transformer: str | None = None,
        connection: DuckDBConnection | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        table: str | Sequence[str] = ":default:",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        args: tuple = tuple(),
        kwargs: dict = dict(),
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> Any | dict[str,Any]:
    if connection is not None:
        return run_with_duckdb_connection(
            module, connection, extractor, transformer, how, table, return_type, args, kwargs, extract_options, transform_options)
    extract_options = extract_options.copy() if isinstance(extract_options, dict) else dict()
    has_parser = not ((return_type == "raw") or ("parser" in extract_options))
    with import_dbt(module if has_parser else None, transformer)(**(transform_options or dict())) as transformer_:
        if transformer and has_parser:
            extract_options["parser"] = transformer_.transform
        extractor_ = import_extractor(module, extractor)(**extract_options)
        results = _extract(extractor_, how, args, kwargs)
        return _fetch_all_from_table(transformer_, results, table, return_type)


def run_with_duckdb_connection(
        module: str,
        connection: DuckDBConnection,
        extractor: str,
        transformer: str | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        table: str | Sequence[str] = ":default:",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        args: tuple = tuple(),
        kwargs: dict = dict(),
        extract_options: dict | None = None,
        transform_options: dict | None = None,
    ) -> Any | dict[str,Any]:
    extract_options = extract_options.copy() if isinstance(extract_options, dict) else dict()
    transform_options = dict(transform_options or dict(), db_info=dict(conn=connection))
    has_parser = not ((return_type == "raw") or ("parser" in extract_options))
    transformer_ = import_dbt(module, transformer)(**transform_options)
    if transformer and has_parser:
        extract_options["parser"] = transformer_.transform
    extractor_ = import_extractor(module, extractor)(**extract_options)
    results = _extract(extractor_, how, args, kwargs)
    return _fetch_all_from_table(transformer_, results, table, return_type)


def get_table_from_options(transform_options: dict | None = None, key: str = "table", default: str = ":default:") -> str:
    if isinstance(transform_options, dict):
        create_options = transform_options.get("create_options")
        if isinstance(create_options, dict):
            return create_options.get(key, default)
    return default


def _get_table(transformer: DuckDBTransformer, table: str = ":default:") -> str:
    return transformer.default_table if table == ":default:" else table


def _fetch_all_from_table(
        transformer: DuckDBTransformer,
        results: JsonObject,
        table: str | Sequence[str] = ":default:",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
    ) -> Any | dict[str,Any]:
    if return_type == "none":
        return
    if (return_type == "raw") or (transformer.conn is None):
        return results
    elif isinstance(table, str):
        return transformer.fetch_all(return_type, _get_table(transformer, table))
    elif isinstance(table, Sequence):
        return {_get_table(transformer, table_): transformer.fetch_all(return_type, table_) for table_ in table}
    else:
        raise TypeError("Invalid type for table. A string or sequence type is allowed.")
