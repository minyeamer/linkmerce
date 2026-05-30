\echo Use "CREATE EXTENSION parquet_io" to load this file. \quit

CREATE OR REPLACE FUNCTION parquet_create(source_path TEXT, target_table TEXT, if_exists TEXT DEFAULT 'error')
RETURNS BIGINT
AS '$libdir/parquet_io', 'parquet_create_file'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION parquet_create(source_data BYTEA, target_table TEXT, if_exists TEXT DEFAULT 'error')
RETURNS BIGINT
AS '$libdir/parquet_io', 'parquet_create_byte'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION parquet_read(source_path TEXT, target_table TEXT)
RETURNS BIGINT
AS '$libdir/parquet_io', 'parquet_read_file'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION parquet_read(source_data BYTEA, target_table TEXT)
RETURNS BIGINT
AS '$libdir/parquet_io', 'parquet_read_byte'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION parquet_write(source_query TEXT, target_path TEXT)
RETURNS BIGINT
AS '$libdir/parquet_io', 'parquet_write_file'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION parquet_write(source_query TEXT)
RETURNS BYTEA
AS '$libdir/parquet_io', 'parquet_write_byte'
LANGUAGE C STRICT;
