MODULE_big = parquet_io
EXTENSION  = parquet_io
DATA       = parquet_io--1.0.sql
OBJS       = parquet_io.o

PG_CONFIG  = pg_config
PGXS      := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Paths and flags resolved after PGXS is loaded
PG_INCDIR  := $(shell $(PG_CONFIG) --includedir-server)
ARROW_FLAGS := $(shell pkg-config --cflags arrow parquet 2>/dev/null)
ARROW_LIBS  := $(shell pkg-config --libs   arrow parquet 2>/dev/null)

override with_llvm = no
PG_CPPFLAGS += $(ARROW_FLAGS)
PG_CXXFLAGS += -std=c++20
SHLIB_LINK += $(ARROW_LIBS)

# Explicit C++ compile rule (PGXS only handles .c by default)
parquet_io.o: parquet_io.cpp
	$(CXX) $(PG_CXXFLAGS) -O2 -fPIC \
	-I$(PG_INCDIR) \
	$(PG_CPPFLAGS) \
	-c -o $@ $<
