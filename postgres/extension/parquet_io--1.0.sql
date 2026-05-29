\echo Use "CREATE EXTENSION parquet_io" to load this file. \quit

CREATE OR REPLACE FUNCTION parquet_create_table(source_path TEXT, target_table TEXT, if_exists TEXT DEFAULT 'error')
RETURNS BIGINT
AS '$libdir/parquet_io', 'parquet_create_table'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION parquet_read(source_path TEXT, target_table TEXT)
RETURNS BIGINT
AS '$libdir/parquet_io', 'parquet_read'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION parquet_write(source_query TEXT, target_path TEXT)
RETURNS BIGINT
AS '$libdir/parquet_io', 'parquet_write'
LANGUAGE C STRICT;
