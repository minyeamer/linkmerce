from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Mapping, Iterable, Sequence, Iterator
    # import pandas as pd


class DeviceAllocationType:
    CPU: DeviceAllocationType
    CUDA: DeviceAllocationType


class Device:
    def __init__(self, device_type: DeviceAllocationType, device_id: int = 0):
        ...

    @property
    def device_type(self) -> DeviceAllocationType:
        ...

    @property
    def device_id(self) -> int:
        ...


class MemoryManager:
    """
    https://arrow.apache.org/docs/python/memory.html
    """

    def __init__(self, *args, **kwargs):
        ...

    @property
    def device(self) -> Device:
        ...

    @property
    def is_cpu(self) -> bool:
        ...

    def allocate_buffer(self, size: int) -> Buffer:
        ...

    def release_unused(self):
        ...


###################################################################
########################### Weakrefable ###########################
###################################################################

class _Weakrefable:
    ...


class Buffer(_Weakrefable):
    """
    https://arrow.apache.org/docs/python/generated/pyarrow.Buffer.html
    """

    def __init__(self, *args, **kwargs):
        ...

    def equals(self, other: Buffer) -> bool:
        ...

    def hex(self) -> str:
        ...

    def slice(self, offset: int = 0, length: int | None = None) -> Buffer:
        ...

    def to_pybytes(self) -> bytes:
        ...

    @property
    def address(self) -> int:
        ...

    @property
    def device(self) -> Device:
        ...

    @property
    def device_type(self) -> DeviceAllocationType:
        ...

    @property
    def is_cpu(self) -> bool:
        ...

    @property
    def is_mutable(self) -> bool:
        ...

    @property
    def memory_manager(self) -> MemoryManager:
        ...

    @property
    def parent(self) -> Buffer:
        ...

    @property
    def size(self) -> int:
        ...


class DataType(_Weakrefable):
    """
    https://arrow.apache.org/docs/python/generated/pyarrow.DataType.html
    """

    def __init__(self, *args, **kwargs):
        ...

    def equals(self, other: Any, *, check_metadata: bool = False) -> bool:
        ...

    def field(self, i: int) -> Field:
        ...

    @property
    def has_variadic_buffers(self) -> bool:
        ...

    @property
    def id(self) -> int:
        ...

    @property
    def num_buffers(self) -> int:
        ...

    @property
    def num_fields(self) -> int:
        ...

    def to_pandas_dtype(self) -> Any:
        ...

    @property
    def bit_width(self) -> int:
        ...

    @property
    def byte_width(self) -> int:
        ...


class Field(_Weakrefable):
    """
    https://arrow.apache.org/docs/python/generated/pyarrow.Field.html
    """

    def __init__(self, *args, **kwargs):
        ...

    def equals(self, other: Field, check_metadata: bool = False) -> bool:
        ...

    def flatten(self) -> list[Field]:
        ...

    def remove_metadata(self) -> Field:
        ...

    def with_metadata(self, metadata: dict[str, str]) -> Field:
        ...

    def with_name(self, name: str) -> Field:
        ...

    def with_nullable(self, nullable: bool) -> Field:
        ...

    def with_type(self, new_type: DataType) -> Field:
        ...

    @property
    def metadata(self) -> dict[bytes, bytes]:
        ...

    @property
    def name(self) -> str:
        ...

    @property
    def nullable(self) -> bool:
        ...


class Schema(_Weakrefable):
    """
    https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html
    """

    def __init__(self, *args, **kwargs):
        ...

    def append(self, field: Field) -> Schema:
        ...

    def empty_table(self) -> Table:
        ...

    def equals(self, other: Schema, check_metadata: bool = False) -> bool:
        ...

    def field(self, i: int | str) -> Field:
        ...

    @classmethod
    def from_pandas(cls, df: Any, preserve_index: bool | None = None) -> Schema:
        ...

    def get_all_field_indices(self, name: str) -> list[int]:
        ...

    def get_field_index(self, name: str) -> int:
        ...

    def insert(self, i: int, field: Field) -> Schema:
        ...

    @property
    def metadata(self) -> dict[bytes, bytes]:
        ...

    @property
    def names(self) -> list[str]:
        ...

    @property
    def pandas_metadata(self) -> dict[str, Any]:
        ...

    def remove(self, i: int) -> Schema:
        ...

    def remove_metadata(self) -> Schema:
        ...

    def serialize(self, memory_pool: Any | None = None) -> Buffer:
        ...

    def set(self, i: int, field: Field) -> Schema:
        ...

    def to_string(
        self,
        truncate_metadata: bool = True,
        show_field_metadata: bool = True,
        show_schema_metadata: bool = True,
        element_size_limit: int = 100
    ) -> str:
        ...

    @property
    def types(self) -> list[DataType]:
        ...

    def with_metadata(self, metadata: dict[str, str]) -> Schema:
        ...


class Scalar(_Weakrefable):
    """
    https://arrow.apache.org/docs/python/generated/pyarrow.Scalar.html
    """

    def __init__(self, *args, **kwargs):
        ...

    def as_py(self, maps_as_pydicts: str | None = None) -> Any:
        ...

    def cast(
        self,
        target_type: DataType | None = None,
        safe: bool | None = None,
        options: Any | None = None,
        memory_pool: Any | None = None
    ) -> Scalar:
        ...

    def equals(self, other: Scalar) -> bool:
        ...

    def validate(self, full: bool = False):
        ...

    @property
    def is_valid(self) -> bool:
        ...

    @property
    def type(self) -> DataType:
        ...


class RecordBatchReader(_Weakrefable):
    """
    https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatchReader.html
    """

    def __init__(self, *args, **kwargs):
        ...

    def cast(self, target_schema: Schema) -> RecordBatchReader:
        ...

    def close(self):
        ...

    @staticmethod
    def from_batches(schema: Schema, batches: Iterable[RecordBatch]) -> RecordBatchReader:
        ...

    @staticmethod
    def from_stream(data: Any, schema: Schema | None = None) -> RecordBatchReader:
        ...

    def iter_batches_with_custom_metadata(self) -> Iterator[Any]:
        ...

    def read_all(self) -> Table:
        ...

    def read_next_batch(self) -> RecordBatch:
        ...

    def read_next_batch_with_custom_metadata(self) -> Any:
        ...

    def read_pandas(self, **options: Any) -> Any: # pd.DataFrame
        ...

    @property
    def schema(self) -> Schema:
        ...


###################################################################
######################## PandasConvertible ########################
###################################################################

class _PandasConvertible:
    ...


class Array(_PandasConvertible):
    """
    https://arrow.apache.org/docs/python/generated/pyarrow.Array.html
    """

    def __init__(self, *args, **kwargs):
        ...

    def buffers(self) -> list[Buffer]:
        ...

    def cast(
        self,
        target_type: DataType | None = None,
        safe: bool | None = None,
        options: Any | None = None,
        memory_pool: Any | None = None
    ) -> Array:
        ...

    def copy_to(self, destination: Any) -> Array:
        ...

    def dictionary_encode(self, null_encoding: str = "mask") -> Array:
        ...

    def diff(self, other: Array) -> str:
        ...

    def drop_null(self) -> Array:
        ...

    def equals(self, other: Array) -> bool:
        ...

    def fill_null(self, fill_value: Any) -> Array:
        ...

    def filter(
        self,
        mask: list[bool] | Array,
        null_selection_behavior: str = "drop"
    ) -> Array:
        ...

    @staticmethod
    def from_buffers(
        type: DataType,
        length: int,
        buffers: list[Buffer],
        null_count: int = -1,
        offset: int = 0,
        children: list[Array] | None = None
    ) -> Array:
        ...

    @staticmethod
    def from_pandas(
        obj: Any, # pd.Series
        mask: Any | None = None,
        type: DataType | None = None,
        safe: bool = True,
        memory_pool: Any | None = None
    ) -> Array | ChunkedArray:
        ...

    def get_total_buffer_size(self) -> int:
        ...

    def index(
        self,
        value: Any,
        start: int | None = None,
        end: int | None = None,
        memory_pool: Any | None = None
    ) -> int:
        ...

    @property
    def is_cpu(self) -> bool:
        ...

    def is_nan(self) -> Array:
        ...

    def is_null(self, nan_is_null: bool = False) -> Array:
        ...

    def is_valid(self) -> Array:
        ...

    @property
    def nbytes(self) -> int:
        ...

    @property
    def null_count(self) -> int:
        ...

    @property
    def offset(self) -> int:
        ...

    def slice(self, offset: int = 0, length: int | None = None) -> Array:
        ...

    def sort(self, order: str = "ascending", **kwargs: Any) -> Array:
        ...

    @property
    def statistics(self) -> Any:
        ...

    def sum(self, **kwargs: Any) -> Scalar:
        ...

    def take(self, indices: list[int] | Array) -> Array:
        ...

    def to_numpy(self, zero_copy_only: bool = True, writable: bool = False) -> Any: # numpy.ndarray
        ...

    def to_pandas(
        self,
        memory_pool: Any | None = None,
        categories: list[str] | None = None,
        strings_to_categorical: bool = False,
        zero_copy_only: bool = False,
        integer_object_nulls: bool = False,
        date_as_object: bool = True,
        timestamp_as_object: bool = False,
        use_threads: bool = True,
        deduplicate_objects: bool = True,
        ignore_metadata: bool = False,
        safe: bool = True,
        split_blocks: bool = False,
        self_destruct: bool = False,
        maps_as_pydicts: str | None = None,
        types_mapper: Any | None = None,
        coerce_temporal_nanoseconds: bool = False
    ) -> Any: # pd.Series | pd.DataFrame
        ...

    def to_pylist(self, maps_as_pydicts: str | None = None) -> list[Any]:
        ...

    def to_string(
        self,
        indent: int = 2,
        top_level_indent: int = 0,
        window: int = 10,
        container_window: int = 2,
        skip_new_lines: bool = False,
        element_size_limit: int = 100
    ) -> str:
        ...

    def tolist(self) -> list[Any]:
        ...

    def unique(self) -> Array:
        ...

    def validate(self, full: bool = False):
        ...

    def value_counts(self) -> Array:
        ...

    def view(self, target_type: DataType) -> Array:
        ...


class ChunkedArray(_PandasConvertible):
    """
    https://arrow.apache.org/docs/python/generated/pyarrow.ChunkedArray.html
    """

    def __init__(self, *args, **kwargs):
        ...

    def cast(
        self,
        target_type: DataType | None = None,
        safe: bool | None = None,
        options: Any | None = None
    ) -> ChunkedArray:
        ...

    def chunk(self, i: int) -> Array:
        ...

    @property
    def chunks(self) -> list[Array]:
        ...

    def combine_chunks(self, memory_pool: Any | None = None) -> Array:
        ...

    def dictionary_encode(self, null_encoding: str = "mask") -> ChunkedArray:
        ...

    def drop_null(self) -> ChunkedArray:
        ...

    def equals(self, other: ChunkedArray) -> bool:
        ...

    def fill_null(self, fill_value: Any) -> ChunkedArray:
        ...

    def filter(
        self,
        mask: Array | list[bool],
        null_selection_behavior: str = "drop"
    ) -> ChunkedArray:
        ...

    def flatten(self, memory_pool: Any | None = None) -> list[ChunkedArray]:
        ...

    def format(self, **kwargs: Any) -> str:
        ...

    def get_total_buffer_size(self) -> int:
        ...

    def index(
        self,
        value: Any,
        start: int | None = None,
        end: int | None = None,
        memory_pool: Any | None = None
    ) -> int:
        ...

    @property
    def is_cpu(self) -> bool:
        ...

    def is_nan(self) -> ChunkedArray:
        ...

    def is_null(self, nan_is_null: bool = False) -> ChunkedArray:
        ...

    def is_valid(self) -> ChunkedArray:
        ...

    def iterchunks(self) -> Iterator[Array]:
        ...

    def length(self) -> int:
        ...

    @property
    def nbytes(self) -> int:
        ...

    @property
    def null_count(self) -> int:
        ...

    @property
    def num_chunks(self) -> int:
        ...

    def slice(self, offset: int = 0, length: int | None = None) -> ChunkedArray:
        ...

    def sort(self, order: str = "ascending", **kwargs: Any) -> ChunkedArray:
        ...

    def take(self, indices: list[int] | Array) -> ChunkedArray:
        ...

    def to_numpy(self, zero_copy_only: bool = False) -> Any: # numpy.ndarray
        ...

    def to_pandas(
        self,
        memory_pool: Any | None = None,
        categories: list[str] | None = None,
        strings_to_categorical: bool = False,
        zero_copy_only: bool = False,
        integer_object_nulls: bool = False,
        date_as_object: bool = True,
        timestamp_as_object: bool = False,
        use_threads: bool = True,
        deduplicate_objects: bool = True,
        ignore_metadata: bool = False,
        safe: bool = True,
        split_blocks: bool = False,
        self_destruct: bool = False,
        maps_as_pydicts: str | None = None,
        types_mapper: Any | None = None,
        coerce_temporal_nanoseconds: bool = False
    ) -> Any: # pd.Series | pd.DataFrame
        ...

    def to_pylist(self, maps_as_pydicts: str | None = None) -> list[Any]:
        ...

    def to_string(
        self,
        indent: int = 0,
        window: int = 5,
        container_window: int = 2,
        skip_new_lines: bool = False,
        element_size_limit: int = 100
    ) -> str:
        ...

    @property
    def type(self) -> DataType:
        ...

    def unify_dictionaries(self, memory_pool: Any | None = None) -> ChunkedArray:
        ...

    def unique(self) -> Array:
        ...

    def validate(self, full: bool = False):
        ...

    def value_counts(self) -> Array:
        ...


###################################################################
######################## PandasConvertible ########################
###################################################################

class _Tabular:
    ...


class Table(_Tabular):
    """
    https://arrow.apache.org/docs/python/generated/pyarrow.Table.html
    """

    def __init__(self, *args, **kwargs):
        ...

    @staticmethod
    def from_arrays(
        arrays: Sequence[Array],
        names: Sequence[str] | None = None,
        schema: Schema | None = None,
        metadata: Mapping[str, str] | None = None
    ) -> Table:
        ...

    @staticmethod
    def from_batches(
        batches: Sequence[RecordBatch],
        schema: Schema | None = None
    ) -> Table:
        ...

    @classmethod
    def from_pandas(
        cls,
        df: Any, # pd.DataFrame
        schema: Schema | None = None,
        preserve_index: bool | None = None,
        nthreads: int | None = None,
        columns: list[str] | None = None,
        safe: bool = True
    ) -> Table:
        ...

    @classmethod
    def from_pydict(
        cls,
        mapping: Mapping[str, Any],
        schema: Schema | None = None,
        metadata: Mapping[str, str] | None = None
    ) -> Table:
        ...

    @classmethod
    def from_pylist(
        cls,
        mapping: list[dict[str, Any]],
        schema: Schema | None = None,
        metadata: Mapping[str, str] | None = None
    ) -> Table:
        ...

    @staticmethod
    def from_struct_array(struct_array: Array) -> Table:
        ...

    def add_column(
        self,
        i: int,
        field_: str | Field,
        column: Array | list[Any]
    ) -> Table:
        ...

    def append_column(
        self,
        field_: str | Field,
        column: Array | list[Any]
    ) -> Table:
        ...

    def cast(
        self,
        target_schema: Schema,
        safe: bool | None = None,
        options: Any | None = None
    ) -> Table:
        ...

    def column(self, i: int | str) -> ChunkedArray:
        ...

    @property
    def column_names(self) -> list[str]:
        ...

    @property
    def columns(self) -> list[ChunkedArray]:
        ...

    def combine_chunks(self, memory_pool: Any | None = None) -> Table:
        ...

    def drop(self, columns: str | list[str]) -> Table:
        ...

    def drop_columns(self, columns: str | list[str]) -> Table:
        ...

    def drop_null(self) -> Table:
        ...

    def equals(self, other: Table, check_metadata: bool = False) -> bool:
        ...

    def field(self, i: int | str) -> Field:
        ...

    def filter(
        self,
        mask: Array | list[bool],
        null_selection_behavior: str = "drop"
    ) -> Table:
        ...

    def flatten(self, memory_pool: Any | None = None) -> Table:
        ...

    def get_total_buffer_size(self) -> int:
        ...

    def group_by(self, keys: str | list[str], use_threads: bool = True) -> TableGroupBy:
        ...

    @property
    def is_cpu(self) -> bool:
        ...

    def itercolumns(self) -> Iterator[ChunkedArray]:
        ...

    def join(
        self,
        right_table: Table,
        keys: str | list[str],
        right_keys: str | list[str] | None = None,
        join_type: str = "left outer",
        left_suffix: str | None = None,
        right_suffix: str | None = None,
        coalesce_keys: bool = True,
        use_threads: bool = True,
        filter_expression: Any | None = None
    ) -> Table:
        ...

    def join_asof(
        self,
        right_table: Table,
        on: str,
        by: str | list[str] | None = None,
        tolerance: Any | None = None,
        right_on: str | None = None,
        right_by: str | list[str] | None = None
    ) -> Table:
        ...

    @property
    def nbytes(self) -> int:
        ...

    @property
    def num_columns(self) -> int:
        ...

    @property
    def num_rows(self) -> int:
        ...

    def remove_column(self, i: int) -> Table:
        ...

    def rename_columns(self, names: list[str] | dict[str, str]) -> Table:
        ...

    def replace_schema_metadata(self, metadata: Mapping[str, str] | None = None) -> Table:
        ...

    @property
    def schema(self) -> Schema:
        ...

    def select(self, columns: list[str] | list[int]) -> Table:
        ...

    def set_column(
        self,
        i: int,
        field_: str | Field,
        column: Array | list[Any]
    ) -> Table:
        ...

    def slice(self, offset: int, length: int | None = None) -> Table:
        ...

    def sort_by(self, sorting, **kwargs) -> Table:
        ...

    def take(self, indices: list[int] | Array) -> Table:
        ...

    def to_batches(self, max_chunksize: int | None = None) -> list[RecordBatch]:
        ...

    def to_pandas(
        self,
        memory_pool: Any | None = None,
        categories: list[str] | None = None,
        **kwargs: Any
    ) -> Any: # pd.DataFrame
        ...

    def to_pylist(self, maps_as_pydicts: bool = False) -> list[dict[str, Any]]:
        ...

    def to_reader(self, max_chunksize: int | None = None) -> RecordBatchReader:
        ...

    def to_string(
        self,
        show_metadata: bool = False,
        preview_cols: int | None = None
    ) -> str:
        ...

    def to_struct_array(self, max_chunksize: int | None = None) -> Array:
        ...

    def unify_dictionaries(self, *args: Any, full: bool = False) -> Table:
        ...

    def validate(self, full: bool = False):
        ...

    def __dataframe__(
        self,
        nan_as_null: bool = False,
        allow_copy: bool = True
    ) -> Any: # pd.DataFrame
        ...


class TableGroupBy(object):
    """
    https://arrow.apache.org/docs/python/generated/pyarrow.TableGroupBy.html
    """

    def __init__(self, table: Table, keys: str | list[str], use_threads: bool = True):
        ...

    def aggregate(
        self,
        aggregations: list[tuple[str, list[str], str] | tuple[str, list[str], str, Any]]
    ) -> Table:
        ...


class RecordBatch(_Tabular):
    """
    https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html
    """

    def __init__(self, *args, **kwargs):
        ...

    @staticmethod
    def from_arrays(
        arrays: list[Array],
        names: list[str] | None = None,
        schema: Schema | None = None,
        metadata: Mapping[str, str] | None = None
    ) -> RecordBatch:
        ...

    @classmethod
    def from_pandas(
        cls,
        df: Any, # pd.DataFrame
        schema: Schema | None = None,
        preserve_index: bool | None = None,
        nthreads: int | None = None,
        columns: list[str] | None = None
    ) -> RecordBatch:
        ...

    @classmethod
    def from_pydict(
        cls,
        mapping: Mapping[str, Array | list[Any]],
        schema: Schema | None = None,
        metadata: Mapping[str, str] | None = None
    ) -> RecordBatch | Table:
        ...

    @classmethod
    def from_pylist(
        cls,
        mapping: list[Mapping[str, Any]],
        schema: Schema | None = None,
        metadata: Mapping[str, str] | None = None
    ) -> RecordBatch | Table:
        ...

    @staticmethod
    def from_struct_array(struct_array: Array) -> RecordBatch:
        ...

    def add_column(
        self,
        i: int,
        field_: str | Field,
        column: Array | list[Any]
    ) -> RecordBatch:
        ...

    def append_column(
        self,
        field_: str | Field,
        column: Array | list[Any]
    ) -> RecordBatch | Table:
        ...

    def cast(
        self,
        target_schema: Schema,
        safe: bool | None = None,
        options: Any | None = None
    ) -> RecordBatch:
        ...

    def column(self, i: int | str) -> ChunkedArray:
        ...

    @property
    def column_names(self) -> list[str]:
        ...

    @property
    def columns(self) -> list[ChunkedArray]:
        ...

    def copy_to(self, destination: MemoryManager | Device) -> RecordBatch:
        ...

    def drop_columns(self, columns: str | list[str]) -> RecordBatch | Table:
        ...

    def drop_null(self) -> RecordBatch | Table:
        ...

    def equals(self, other: RecordBatch, check_metadata: bool = False) -> bool:
        ...

    def field(self, i: int | str) -> Field:
        ...

    def filter(
        self,
        mask: Array | list[bool],
        null_selection_behavior: str = "drop"
    ) -> RecordBatch | Table:
        ...

    @property
    def is_cpu(self) -> bool:
        ...

    def itercolumns(self) -> Iterator[ChunkedArray]:
        ...

    @property
    def nbytes(self) -> int:
        ...

    @property
    def num_columns(self) -> int:
        ...

    @property
    def num_rows(self) -> int:
        ...

    def remove_column(self, i: int) -> RecordBatch:
        ...

    def rename_columns(self, names: list[str] | Mapping[str, str]) -> RecordBatch:
        ...

    def replace_schema_metadata(self, metadata: Mapping[str, str] | None = None) -> RecordBatch:
        ...

    @property
    def schema(self) -> Schema:
        ...

    def select(self, columns: list[str] | list[int]) -> RecordBatch:
        ...

    def serialize(self, memory_pool: Any | None = None) -> Buffer:
        ...

    def set_column(
        self,
        i: int,
        field_: str | Field,
        column: Array | list[Any]
    ) -> RecordBatch:
        ...

    @property
    def shape(self) -> tuple[int, int]:
        ...

    def slice(self, offset: int = 0, length: int | None = None) -> RecordBatch:
        ...

    def sort_by(self, sorting: Any, **kwargs: Any) -> RecordBatch | Table:
        ...

    def take(self, indices: list[int] | Array) -> RecordBatch | Table:
        ...

    def to_pandas(
        self,
        memory_pool: Any | None = None,
        categories: list[str] | None = None,
        strings_to_categorical: bool = False,
        zero_copy_only: bool = False,
        integer_object_nulls: bool = False,
        date_as_object: bool = True,
        timestamp_as_object: bool = False,
        use_threads: bool = True,
        deduplicate_objects: bool = True,
        ignore_metadata: bool = False,
        safe: bool = True,
        split_blocks: bool = False,
        self_destruct: bool = False,
        maps_as_pydicts: str | None = None,
        types_mapper: Any | None = None,
        coerce_temporal_nanoseconds: bool = False
    ) -> Any: # pd.Series | pd.DataFrame
        ...
