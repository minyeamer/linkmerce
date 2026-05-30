/*
 * parquet_io.cpp
 *
 * PostgreSQL extension providing SQL-level read/write for local Parquet files.
 * Uses Apache Arrow / Parquet C++ library.
 *
 *   parquet_create(source_path TEXT, target_table TEXT, if_exists TEXT) -> BIGINT
 *   parquet_create(source_data BYTEA, target_table TEXT, if_exists TEXT) -> BIGINT
 *   parquet_read(source_path TEXT, target_table TEXT) -> BIGINT
 *   parquet_read(source_data BYTEA, target_table TEXT) -> BIGINT
 *   parquet_write(source_query TEXT, target_path TEXT) -> BIGINT
 *   parquet_write(source_query TEXT) -> BYTEA
 */

/* PostgreSQL headers must be wrapped in extern "C" */
extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "catalog/pg_type_d.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/numeric.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;
}  /* extern "C" */

#ifdef NIL
#undef NIL
#endif

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

/* ─────────────────────────────────────────────────────────────────────────────
 * Constants
 * ───────────────────────────────────────────────────────────────────────────── */

/* Days between Unix epoch (1970-01-01) and PostgreSQL epoch (2000-01-01) */
static constexpr int32_t PG_EPOCH_DAYS  = 10957;
static constexpr int64_t PARQUET_IO_USECS_PER_DAY = 86400LL * 1000000LL;
static constexpr int64_t PG_EPOCH_USECS =
    (int64_t)PG_EPOCH_DAYS * PARQUET_IO_USECS_PER_DAY;

/* Throw on non-OK Arrow status */
#define ARROW_THROW_NOT_OK(expr)                                            \
    do {                                                                     \
        const ::arrow::Status& _s = (expr);                                 \
        if (!_s.ok()) throw std::runtime_error(_s.ToString());              \
    } while (0)

static std::string
quote_ident_sql(const std::string& ident)
{
    std::string quoted = "\"";
    for (char ch : ident) {
        if (ch == '"') quoted += "\"\"";
        else quoted += ch;
    }
    quoted += "\"";
    return quoted;
}

static std::string
quote_literal_sql(const std::string& value)
{
    std::string quoted = "'";
    for (char ch : value) {
        if (ch == '\'') quoted += "''";
        else quoted += ch;
    }
    quoted += "'";
    return quoted;
}

static std::unique_ptr<parquet::arrow::FileReader>
open_parquet_reader(const std::shared_ptr<arrow::io::RandomAccessFile>& infile)
{
    auto reader_result =
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool());
    if (!reader_result.ok())
        throw std::runtime_error(reader_result.status().ToString());
    return std::move(reader_result).ValueOrDie();
}

static std::shared_ptr<arrow::io::BufferReader>
open_parquet_buffer(bytea* source_data)
{
    auto buffer = arrow::Buffer::Wrap(
        reinterpret_cast<const uint8_t*>(VARDATA_ANY(source_data)),
        VARSIZE_ANY_EXHDR(source_data));
    return std::make_shared<arrow::io::BufferReader>(buffer);
}

static std::string
arrow_type_to_pg_type(const std::shared_ptr<arrow::DataType>& type)
{
    switch (type->id()) {
        case arrow::Type::BOOL:
            return "BOOLEAN";
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
        case arrow::Type::UINT8:
        case arrow::Type::UINT16:
        case arrow::Type::UINT32:
        case arrow::Type::UINT64:
            return "BIGINT";
        case arrow::Type::HALF_FLOAT:
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE:
        case arrow::Type::DECIMAL128:
        case arrow::Type::DECIMAL256:
            return "DOUBLE PRECISION";
        case arrow::Type::DATE32:
        case arrow::Type::DATE64:
            return "DATE";
        case arrow::Type::TIMESTAMP:
            return "TIMESTAMPTZ";
        default:
            return "TEXT";
    }
}

/* ─────────────────────────────────────────────────────────────────────────────
 * Arrow → PostgreSQL Datum
 * ───────────────────────────────────────────────────────────────────────────── */

/*
 * Convert one element of an Arrow array to a PostgreSQL Datum.
 * Sets *is_null = true and returns 0 when the element is null.
 */
static Datum
arrow_to_datum(const arrow::Array& arr, int64_t idx, Oid pg_oid, bool* is_null)
{
    if (arr.IsNull(idx)) {
        *is_null = true;
        return (Datum)0;
    }
    *is_null = false;

    /* Helper: extract as int64 from common integer types */
    auto as_int64 = [&]() -> int64_t {
        switch (arr.type_id()) {
            case arrow::Type::INT8:
                return static_cast<const arrow::Int8Array&>(arr).Value(idx);
            case arrow::Type::INT16:
                return static_cast<const arrow::Int16Array&>(arr).Value(idx);
            case arrow::Type::INT32:
                return static_cast<const arrow::Int32Array&>(arr).Value(idx);
            case arrow::Type::UINT32:
                return (int64_t)static_cast<const arrow::UInt32Array&>(arr).Value(idx);
            case arrow::Type::UINT64:
                return (int64_t)static_cast<const arrow::UInt64Array&>(arr).Value(idx);
            default:  /* INT64, UINT64 */
                return static_cast<const arrow::Int64Array&>(arr).Value(idx);
        }
    };

    /* Helper: extract as double from common float/decimal types */
    auto as_double = [&]() -> double {
        switch (arr.type_id()) {
            case arrow::Type::FLOAT:
                return (double)static_cast<const arrow::FloatArray&>(arr).Value(idx);
            case arrow::Type::DECIMAL128: {
                auto& da = static_cast<const arrow::Decimal128Array&>(arr);
                arrow::Decimal128 dv(da.GetValue(idx));
                int32_t scale =
                    std::static_pointer_cast<arrow::Decimal128Type>(arr.type())->scale();
                return dv.ToDouble(scale);
            }
            default:  /* DOUBLE */
                return static_cast<const arrow::DoubleArray&>(arr).Value(idx);
        }
    };

    /* Helper: extract as std::string_view for utf8 types */
    auto as_sv = [&]() -> std::string {
        if (arr.type_id() == arrow::Type::STRING)
            return std::string(
                static_cast<const arrow::StringArray&>(arr).GetView(idx));
        if (arr.type_id() == arrow::Type::LARGE_STRING)
            return std::string(
                static_cast<const arrow::LargeStringArray&>(arr).GetView(idx));
        if (arr.type_id() == arrow::Type::DECIMAL128) {
            auto& da = static_cast<const arrow::Decimal128Array&>(arr);
            return da.FormatValue(idx);
        }
        /* Fallback: generic scalar string */
        auto sr = arr.GetScalar(idx);
        return sr.ok() ? sr.ValueOrDie()->ToString() : std::string();
    };

    switch (pg_oid) {
        case INT2OID:
            return Int16GetDatum((int16_t)as_int64());
        case INT4OID:
            return Int32GetDatum((int32_t)as_int64());
        case INT8OID:
            return Int64GetDatum(as_int64());

        case FLOAT4OID:
            return Float4GetDatum((float4)as_double());
        case FLOAT8OID:
            return Float8GetDatum((float8)as_double());

        case NUMERICOID: {
            /* Convert via float8 → numeric to produce a proper NUMERIC datum */
            Datum f8 = Float8GetDatum(as_double());
            return DirectFunctionCall1(float8_numeric, f8);
        }

        case BOOLOID:
            return BoolGetDatum(
                static_cast<const arrow::BooleanArray&>(arr).Value(idx));

        case DATEOID: {
            int32_t unix_days =
                (arr.type_id() == arrow::Type::DATE32)
                ? static_cast<const arrow::Date32Array&>(arr).Value(idx)
                : (int32_t)as_int64();
            return DateADTGetDatum((DateADT)(unix_days - PG_EPOCH_DAYS));
        }

        case TIMESTAMPOID:
        case TIMESTAMPTZOID: {
            int64_t usecs = 0;
            if (arr.type_id() == arrow::Type::TIMESTAMP) {
                usecs = static_cast<const arrow::TimestampArray&>(arr).Value(idx);
                auto ts_type =
                    std::static_pointer_cast<arrow::TimestampType>(arr.type());
                switch (ts_type->unit()) {
                    case arrow::TimeUnit::SECOND: usecs *= 1000000LL; break;
                    case arrow::TimeUnit::MILLI:  usecs *= 1000LL;    break;
                    case arrow::TimeUnit::NANO:   usecs /= 1000LL;    break;
                    case arrow::TimeUnit::MICRO:                       break;
                }
            } else {
                usecs = as_int64();
            }
            return TimestampGetDatum((Timestamp)(usecs - PG_EPOCH_USECS));
        }

        /* TEXT and everything else */
        default: {
            std::string s = as_sv();
            return CStringGetTextDatum(s.c_str());
        }
    }
}

/* ─────────────────────────────────────────────────────────────────────────────
 * PostgreSQL Datum → Arrow builder
 * ───────────────────────────────────────────────────────────────────────────── */

static std::shared_ptr<arrow::DataType>
pg_oid_to_arrow_type(Oid oid)
{
    switch (oid) {
        case INT2OID:        return arrow::int64();
        case INT4OID:        return arrow::int64();
        case INT8OID:        return arrow::int64();
        case FLOAT4OID:      return arrow::float64();
        case FLOAT8OID:      return arrow::float64();
        case NUMERICOID:     return arrow::float64();
        case BOOLOID:        return arrow::boolean();
        case DATEOID:        return arrow::date32();
        case TIMESTAMPOID:   return arrow::timestamp(arrow::TimeUnit::MICRO);
        case TIMESTAMPTZOID: return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
        default:             return arrow::large_utf8();
    }
}

static void
datum_to_arrow(arrow::ArrayBuilder* builder,
                Datum datum, bool is_null, Oid pg_oid)
{
    if (is_null) {
        ARROW_THROW_NOT_OK(builder->AppendNull());
        return;
    }

    switch (pg_oid) {
        case INT2OID:
            ARROW_THROW_NOT_OK(
                static_cast<arrow::Int64Builder*>(builder)->Append(
                    DatumGetInt16(datum)));
            break;
        case INT4OID:
            ARROW_THROW_NOT_OK(
                static_cast<arrow::Int64Builder*>(builder)->Append(
                    DatumGetInt32(datum)));
            break;
        case INT8OID:
            ARROW_THROW_NOT_OK(
                static_cast<arrow::Int64Builder*>(builder)->Append(
                    DatumGetInt64(datum)));
            break;

        case FLOAT4OID:
            ARROW_THROW_NOT_OK(
                static_cast<arrow::DoubleBuilder*>(builder)->Append(
                    (double)DatumGetFloat4(datum)));
            break;
        case FLOAT8OID:
            ARROW_THROW_NOT_OK(
                static_cast<arrow::DoubleBuilder*>(builder)->Append(
                    DatumGetFloat8(datum)));
            break;

        case NUMERICOID: {
            Datum f8 = DirectFunctionCall1(numeric_float8, datum);
            ARROW_THROW_NOT_OK(
                static_cast<arrow::DoubleBuilder*>(builder)->Append(
                    DatumGetFloat8(f8)));
            break;
        }

        case BOOLOID:
            ARROW_THROW_NOT_OK(
                static_cast<arrow::BooleanBuilder*>(builder)->Append(
                    DatumGetBool(datum)));
            break;

        case DATEOID: {
            DateADT pg_date = DatumGetDateADT(datum);
            int32_t unix_days = (int32_t)pg_date + PG_EPOCH_DAYS;
            ARROW_THROW_NOT_OK(
                static_cast<arrow::Date32Builder*>(builder)->Append(unix_days));
            break;
        }

        case TIMESTAMPOID:
        case TIMESTAMPTZOID: {
            Timestamp pg_ts = DatumGetTimestamp(datum);
            int64_t usecs = (int64_t)pg_ts + PG_EPOCH_USECS;
            ARROW_THROW_NOT_OK(
                static_cast<arrow::TimestampBuilder*>(builder)->Append(usecs));
            break;
        }

        default: {
            Oid out_func;
            bool is_varlena;
            getTypeOutputInfo(pg_oid, &out_func, &is_varlena);
            char* str = OidOutputFunctionCall(out_func, datum);
            ARROW_THROW_NOT_OK(
                static_cast<arrow::LargeStringBuilder*>(builder)->Append(str));
            pfree(str);
            break;
        }
    }
}

/* ─────────────────────────────────────────────────────────────────────────────
 * parquet_create: Parquet schema -> PostgreSQL table
 * ───────────────────────────────────────────────────────────────────────────── */

static int64_t
do_parquet_create(
    const std::shared_ptr<arrow::io::RandomAccessFile>& infile,
    const char* target_table_str,
    const char* if_exists)
{
    std::unique_ptr<parquet::arrow::FileReader> reader = open_parquet_reader(infile);

    std::shared_ptr<arrow::Schema> parquet_schema;
    PARQUET_THROW_NOT_OK(reader->GetSchema(&parquet_schema));

    std::string target(target_table_str);
    std::string schema_name, table_name;
    auto dot = target.find('.');
    if (dot != std::string::npos) {
        schema_name = target.substr(0, dot);
        table_name = target.substr(dot + 1);
    } else {
        schema_name = "public";
        table_name = target;
        target = schema_name + "." + table_name;
    }

    std::string mode = if_exists ? std::string(if_exists) : "error";
    if ((mode != "error") && (mode != "ignore") && (mode != "replace"))
        throw std::runtime_error("Invalid if_exists value: " + mode);

    std::string exists_sql =
        "SELECT 1 FROM information_schema.tables WHERE table_schema = "
        + quote_literal_sql(schema_name)
        + " AND table_name = "
        + quote_literal_sql(table_name)
        + " LIMIT 1";
    if (SPI_execute(exists_sql.c_str(), true, 1) != SPI_OK_SELECT)
        throw std::runtime_error("Failed to check table existence: " + target);
    bool exists = SPI_processed > 0;
    if (SPI_tuptable) SPI_freetuptable(SPI_tuptable);

    if (exists) {
        if (mode == "ignore")
            return 0;
        if (mode == "replace") {
            std::string drop_sql =
                "DROP TABLE IF EXISTS "
                + quote_ident_sql(schema_name)
                + "."
                + quote_ident_sql(table_name);
            if (SPI_execute(drop_sql.c_str(), false, 0) != SPI_OK_UTILITY)
                throw std::runtime_error("Failed to drop table: " + target);
        } else {
            throw std::runtime_error("Table already exists: " + target);
        }
    }

    std::string create_sql =
        "CREATE TABLE "
        + quote_ident_sql(schema_name)
        + "."
        + quote_ident_sql(table_name)
        + " (";
    for (int i = 0; i < parquet_schema->num_fields(); i++) {
        if (i) create_sql += ", ";
        auto field = parquet_schema->field(i);
        create_sql += quote_ident_sql(field->name());
        create_sql += " ";
        create_sql += arrow_type_to_pg_type(field->type());
    }
    create_sql += ")";

    if (SPI_execute(create_sql.c_str(), false, 0) != SPI_OK_UTILITY)
        throw std::runtime_error("Failed to create table: " + target);
    return parquet_schema->num_fields();
}

extern "C" {
PG_FUNCTION_INFO_V1(parquet_create_file);
Datum
parquet_create_file(PG_FUNCTION_ARGS)
{
    char* source_path = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* target_table = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* if_exists = text_to_cstring(PG_GETARG_TEXT_PP(2));
    int64_t column_count = 0;

    SPI_connect();
    PG_TRY(); {
        try {
            std::shared_ptr<arrow::io::ReadableFile> infile;
            PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(source_path));
            column_count = do_parquet_create(infile, target_table, if_exists);
        } catch (const std::exception& e) {
            SPI_finish();
            ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                errmsg("parquet_create: %s", e.what())));
        }
        SPI_finish();
    } PG_CATCH(); {
        SPI_finish();
        PG_RE_THROW();
    } PG_END_TRY();

    PG_RETURN_INT64(column_count);
}
}  /* extern "C" */

extern "C" {
PG_FUNCTION_INFO_V1(parquet_create_byte);
Datum
parquet_create_byte(PG_FUNCTION_ARGS)
{
    bytea* source_data = PG_GETARG_BYTEA_PP(0);
    char* target_table = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char* if_exists = text_to_cstring(PG_GETARG_TEXT_PP(2));
    int64_t column_count = 0;

    SPI_connect();
    PG_TRY(); {
        try {
            column_count = do_parquet_create(
                open_parquet_buffer(source_data), target_table, if_exists);
        } catch (const std::exception& e) {
            SPI_finish();
            ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                errmsg("parquet_create: %s", e.what())));
        }
        SPI_finish();
    } PG_CATCH(); {
        SPI_finish();
        PG_RE_THROW();
    } PG_END_TRY();

    PG_RETURN_INT64(column_count);
}
}  /* extern "C" */

/* ─────────────────────────────────────────────────────────────────────────────
 * parquet_read: Parquet file -> PostgreSQL table
 * ───────────────────────────────────────────────────────────────────────────── */

static int64_t
do_parquet_read(
    const std::shared_ptr<arrow::io::RandomAccessFile>& infile,
    const char* target_table_str)
{
    std::unique_ptr<parquet::arrow::FileReader> reader = open_parquet_reader(infile);

    std::shared_ptr<arrow::Schema> parquet_schema;
    PARQUET_THROW_NOT_OK(reader->GetSchema(&parquet_schema));

    /* Parse "schema.table" ------------------------------------------------- */
    std::string target(target_table_str);
    std::string schema_name, table_name;
    auto dot = target.find('.');
    if (dot != std::string::npos) {
        schema_name = target.substr(0, dot);
        table_name  = target.substr(dot + 1);
    } else {
        schema_name = "public";
        table_name  = target;
    }

    /* Query target table columns via catalog ------------------------------- */
    std::string col_sql =
        "SELECT a.attname, a.atttypid "
        "FROM pg_attribute a "
        "JOIN pg_class c ON a.attrelid = c.oid "
        "JOIN pg_namespace n ON c.relnamespace = n.oid "
        "WHERE c.relname = '" + table_name + "' "
        "  AND n.nspname = '" + schema_name + "' "
        "  AND a.attnum > 0 AND NOT a.attisdropped "
        "ORDER BY a.attnum";

    if (SPI_execute(col_sql.c_str(), true, 0) != SPI_OK_SELECT)
        throw std::runtime_error("Failed to query columns for: " + target);

    std::vector<std::string> pg_names;
    std::vector<Oid>         pg_oids;
    uint64_t ntups = SPI_processed;
    for (uint64_t r = 0; r < ntups; r++) {
        HeapTuple tup   = SPI_tuptable->vals[r];
        TupleDesc tdesc = SPI_tuptable->tupdesc;
        bool isnull;
        char* attname = SPI_getvalue(tup, tdesc, 1);
        if (attname == nullptr)
            throw std::runtime_error("Failed to read target column name");
        pg_names.push_back(std::string(attname));
        pfree(attname);
        pg_oids.push_back(
            (Oid)DatumGetObjectId(
                SPI_getbinval(tup, tdesc, 2, &isnull)));
    }
    SPI_freetuptable(SPI_tuptable);

    if (pg_names.empty())
        throw std::runtime_error("Table not found or has no columns: " + target);

    /* Match columns by name (intersection, PostgreSQL column order) --------- */
    std::vector<std::string> ins_names;
    std::vector<int>         pq_indices;
    std::vector<Oid>         ins_oids;

    for (size_t k = 0; k < pg_names.size(); k++) {
        int idx = parquet_schema->GetFieldIndex(pg_names[k]);
        if (idx >= 0) {
            ins_names.push_back(pg_names[k]);
            pq_indices.push_back(idx);
            ins_oids.push_back(pg_oids[k]);
        }
    }
    if (ins_names.empty())
        throw std::runtime_error(
            "No matching columns between parquet and table: " + target);

    int ncols = (int)ins_names.size();

    /* Build parameterized INSERT plan -------------------------------------- */
    std::string col_list, plc_list;
    for (int i = 0; i < ncols; i++) {
        if (i) { col_list += ','; plc_list += ','; }
        col_list += '"' + ins_names[i] + '"';
        plc_list += '$' + std::to_string(i + 1);
    }
    std::string ins_sql =
        "INSERT INTO " + target + " (" + col_list + ") VALUES (" + plc_list + ")";

    SPIPlanPtr plan = SPI_prepare(ins_sql.c_str(), ncols, ins_oids.data());
    if (!plan)
        throw std::runtime_error("Failed to prepare INSERT for: " + target);

    /* Read row groups and insert -------------------------------------------- */
    int64_t total = 0;
    int num_rg    = reader->num_row_groups();
    std::vector<Datum> vals(ncols);
    std::vector<char>  nulls(ncols);

    for (int rg = 0; rg < num_rg; rg++) {
        std::shared_ptr<arrow::Table> batch;
        PARQUET_THROW_NOT_OK(reader->ReadRowGroup(rg, pq_indices, &batch));

        /* CombineChunks ensures single chunk per column for indexed access */
        auto combined = batch->CombineChunks(arrow::default_memory_pool());
        if (!combined.ok())
            throw std::runtime_error(combined.status().ToString());
        auto tbl = combined.ValueOrDie();

        int64_t nrows = tbl->num_rows();
        for (int64_t row = 0; row < nrows; row++) {
            for (int c = 0; c < ncols; c++) {
                bool is_null;
                auto& arr = *tbl->column(c)->chunk(0);
                vals[c]   = arrow_to_datum(arr, row, ins_oids[c], &is_null);
                nulls[c]  = is_null ? 'n' : ' ';
            }
            if (SPI_execute_plan(plan, vals.data(), nulls.data(), false, 0)
                    != SPI_OK_INSERT)
                throw std::runtime_error(
                    "INSERT failed at row " + std::to_string(total + row));
        }
        total += nrows;
    }

    SPI_freeplan(plan);
    return total;
}

extern "C" {
PG_FUNCTION_INFO_V1(parquet_read_file);
Datum
parquet_read_file(PG_FUNCTION_ARGS)
{
    char*   source_path  = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char*   target_table = text_to_cstring(PG_GETARG_TEXT_PP(1));
    int64_t row_count    = 0;

    SPI_connect();
    PG_TRY(); {
        try {
            std::shared_ptr<arrow::io::ReadableFile> infile;
            PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(source_path));
            row_count = do_parquet_read(infile, target_table);
        } catch (const std::exception& e) {
            SPI_finish();
            ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                errmsg("parquet_read: %s", e.what())));
        }
        SPI_finish();
    } PG_CATCH(); {
        SPI_finish();
        PG_RE_THROW();
    } PG_END_TRY();

    PG_RETURN_INT64(row_count);
}
}  /* extern "C" */

extern "C" {
PG_FUNCTION_INFO_V1(parquet_read_byte);
Datum
parquet_read_byte(PG_FUNCTION_ARGS)
{
    bytea* source_data = PG_GETARG_BYTEA_PP(0);
    char* target_table = text_to_cstring(PG_GETARG_TEXT_PP(1));
    int64_t row_count = 0;

    SPI_connect();
    PG_TRY(); {
        try {
            row_count = do_parquet_read(open_parquet_buffer(source_data), target_table);
        } catch (const std::exception& e) {
            SPI_finish();
            ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                errmsg("parquet_read: %s", e.what())));
        }
        SPI_finish();
    } PG_CATCH(); {
        SPI_finish();
        PG_RE_THROW();
    } PG_END_TRY();

    PG_RETURN_INT64(row_count);
}
}  /* extern "C" */

/* ─────────────────────────────────────────────────────────────────────────────
 * parquet_write: PostgreSQL query → Parquet file
 * ───────────────────────────────────────────────────────────────────────────── */

static int64_t
do_parquet_write(
    const char* source_query,
    const std::shared_ptr<arrow::io::OutputStream>& outfile)
{
    const int BATCH_SIZE = 65536;

    /* Open cursor over the query ------------------------------------------- */
    Portal portal = SPI_cursor_open_with_args(
        NULL, source_query, 0, NULL, NULL, NULL, true, 0);
    if (!portal)
        throw std::runtime_error("Failed to open cursor for query");

    /* Fetch first batch to derive output schema ----------------------------- */
    SPI_cursor_fetch(portal, true, BATCH_SIZE);
    if (SPI_processed == 0) {
        SPI_cursor_close(portal);
        return 0;
    }

    /* Extract column metadata from first TupleDesc */
    TupleDesc       first_tdesc = SPI_tuptable->tupdesc;
    int             raw_ncols   = first_tdesc->natts;
    std::vector<int>         att_nums;   /* 1-based attnum for active columns */
    std::vector<Oid>         col_oids;
    arrow::FieldVector       fields;

    for (int i = 0; i < raw_ncols; i++) {
        Form_pg_attribute attr = TupleDescAttr(first_tdesc, i);
        if (attr->attisdropped) continue;
        att_nums.push_back(i + 1);
        col_oids.push_back(attr->atttypid);
        fields.push_back(
            arrow::field(NameStr(attr->attname),
                        pg_oid_to_arrow_type(attr->atttypid)));
    }
    int ncols = (int)att_nums.size();
    auto schema = arrow::schema(fields);

    auto writer_props =
        parquet::WriterProperties::Builder()
            .compression(parquet::Compression::SNAPPY)
            ->build();
    auto arrow_props =
        parquet::ArrowWriterProperties::Builder()
            .store_schema()
            ->build();

    auto writer_result =
        parquet::arrow::FileWriter::Open(*schema,
                                        arrow::default_memory_pool(),
                                        outfile,
                                        writer_props,
                                        arrow_props);
    if (!writer_result.ok())
        throw std::runtime_error(writer_result.status().ToString());
    std::unique_ptr<parquet::arrow::FileWriter> writer =
        std::move(writer_result).ValueOrDie();

    /* Flush one SPI batch to parquet --------------------------------------- */
    int64_t total = 0;

    auto flush = [&]() {
        uint64_t nrows = SPI_processed;
        if (nrows == 0) return;

        TupleDesc tdesc = SPI_tuptable->tupdesc;  /* fresh pointer each batch */

        /* Build one Arrow builder per column */
        std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders(ncols);
        for (int c = 0; c < ncols; c++)
            ARROW_THROW_NOT_OK(
                arrow::MakeBuilder(arrow::default_memory_pool(),
                                    fields[c]->type(),
                                    &builders[c]));

        for (uint64_t r = 0; r < nrows; r++) {
            HeapTuple tup = SPI_tuptable->vals[r];
            for (int c = 0; c < ncols; c++) {
                bool  is_null;
                Datum d = SPI_getbinval(tup, tdesc, att_nums[c], &is_null);
                datum_to_arrow(builders[c].get(), d, is_null, col_oids[c]);
            }
        }

        arrow::ArrayVector arrays(ncols);
        for (int c = 0; c < ncols; c++)
            ARROW_THROW_NOT_OK(builders[c]->Finish(&arrays[c]));

        auto tbl = arrow::Table::Make(schema, arrays, (int64_t)nrows);
        PARQUET_THROW_NOT_OK(writer->WriteTable(*tbl, (int64_t)nrows));
        total += (int64_t)nrows;
    };

    /* Process first batch (already fetched) */
    flush();
    SPI_freetuptable(SPI_tuptable);

    /* Fetch and process remaining batches */
    while (true) {
        SPI_cursor_fetch(portal, true, BATCH_SIZE);
        if (SPI_processed == 0) break;
        flush();
        SPI_freetuptable(SPI_tuptable);
    }

    SPI_cursor_close(portal);

    PARQUET_THROW_NOT_OK(writer->Close());
    return total;
}

extern "C" {
PG_FUNCTION_INFO_V1(parquet_write_file);
Datum
parquet_write_file(PG_FUNCTION_ARGS)
{
    char*   source_query = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char*   target_path  = text_to_cstring(PG_GETARG_TEXT_PP(1));
    int64_t row_count    = 0;

    SPI_connect();
    PG_TRY(); {
        try {
            std::shared_ptr<arrow::io::FileOutputStream> outfile;
            PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(target_path));
            row_count = do_parquet_write(source_query, outfile);
            PARQUET_THROW_NOT_OK(outfile->Close());
        } catch (const std::exception& e) {
            SPI_finish();
            ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                errmsg("parquet_write: %s", e.what())));
        }
        SPI_finish();
    } PG_CATCH(); {
        SPI_finish();
        PG_RE_THROW();
    } PG_END_TRY();

    PG_RETURN_INT64(row_count);
}
}  /* extern "C" */

extern "C" {
PG_FUNCTION_INFO_V1(parquet_write_byte);
Datum
parquet_write_byte(PG_FUNCTION_ARGS)
{
    char* source_query = text_to_cstring(PG_GETARG_TEXT_PP(0));
    std::shared_ptr<arrow::Buffer> buffer;

    SPI_connect();
    PG_TRY(); {
        try {
            auto outfile_result = arrow::io::BufferOutputStream::Create();
            if (!outfile_result.ok())
                throw std::runtime_error(outfile_result.status().ToString());
            std::shared_ptr<arrow::io::BufferOutputStream> outfile =
                std::move(outfile_result).ValueOrDie();

            do_parquet_write(source_query, outfile);

            auto buffer_result = outfile->Finish();
            if (!buffer_result.ok())
                throw std::runtime_error(buffer_result.status().ToString());
            buffer = std::move(buffer_result).ValueOrDie();
        } catch (const std::exception& e) {
            SPI_finish();
            ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                errmsg("parquet_write: %s", e.what())));
        }
        SPI_finish();
    } PG_CATCH(); {
        SPI_finish();
        PG_RE_THROW();
    } PG_END_TRY();

    bytea* result = (bytea*)palloc(VARHDRSZ + buffer->size());
    SET_VARSIZE(result, VARHDRSZ + buffer->size());
    memcpy(VARDATA(result), buffer->data(), buffer->size());
    PG_RETURN_BYTEA_P(result);
}
}  /* extern "C" */
