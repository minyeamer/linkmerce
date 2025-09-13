from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Literal, Type
    from linkmerce.common.extract import Extractor
    from linkmerce.common.transform import Transformer, DBTransformer, DuckDBTransformer
    from linkmerce.common.load import DuckDBConnection


def update_options(__m: dict, **options) -> dict:
    if options:
        from linkmerce.utils.map import hier_set
        return hier_set(__m, "ignore", **options)
    else:
        return __m


def update_tables(__m: dict, tables: dict | None = None) -> dict:
    if ("tables" not in __m) and isinstance(tables, dict):
        __m["tables"] = tables
    return __m


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


def import_dbt(module: str, attr: str) -> Type[DBTransformer]:
    return import_transformer(module, attr)


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
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> Any:
    if transformer and ("parser" not in extract_options):
        transformer_ = import_transformer(module, transformer)(**transform_options)
        extract_options = dict(extract_options, parser=transformer_.transform)
    extractor_ = import_extractor(module, extractor)(**extract_options)
    return extract(extractor_, how, args, kwargs)


def run_with_connection(
        module: str,
        extractor: str,
        transformer: str | None = None,
        tables: dict | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        args: tuple = tuple(),
        kwargs: dict = dict(),
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> Any:
    if (not transformer) or ("parser" in extract_options):
        return run(module, extractor, transformer, how, args, kwargs, extract_options, transform_options)
    with import_dbt(module, transformer)(**update_tables(transform_options, tables)) as transformer_:
        extractor_ = import_extractor(module, extractor)(parser=transformer_.transform, **extract_options)
        return extract(extractor_, how, args, kwargs)


def extract(
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
        tables: dict | None = None,
        how: Literal["sync","async","async_loop"] = "sync",
        return_type: Literal["csv","json","parquet","raw","none"] = "json",
        args: tuple = tuple(),
        kwargs: dict = dict(),
        extract_options: dict = dict(),
        transform_options: dict = dict(),
    ) -> Any | dict[str,Any]:
    if (not transformer) or (return_type == "raw") or ("parser" in extract_options):
        return run(module, extractor, None, how, args, kwargs, extract_options, transform_options)
    elif (connection is None) or ("db_info" in transform_options):
        with import_dbt(module, transformer)(**update_tables(transform_options, tables)) as transformer_:
            extractor_ = import_extractor(module, extractor)(parser=transformer_.transform, **extract_options)
            extract(extractor_, how, args, kwargs)
            return fetch_all_from_duckdb_table(transformer_, return_type)
    else:
        transformer_ = import_dbt(module, transformer)(db_info=dict(conn=connection), **update_tables(transform_options, tables))
        extractor_ = import_extractor(module, extractor)(parser=transformer_.transform, **extract_options)
        extract(extractor_, how, args, kwargs)
        return fetch_all_from_duckdb_table(transformer_, return_type)


def fetch_all_from_duckdb_table(
        transformer: DuckDBTransformer,
        return_type: Literal["csv","json","parquet","none"] = "json",
    ) -> Any | dict[str,Any]:
    tables = transformer.get_tables()
    if (return_type == "none") or (not tables):
        return
    elif len(tables) == 1:
        return transformer.fetch_all(return_type, list(tables.values())[0])
    else:
        return {key: transformer.fetch_all(return_type, table) for key, table in tables.items()}
