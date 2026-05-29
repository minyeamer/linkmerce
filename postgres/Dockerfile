# ── Stage 1: build ──────────────────────────────────────────────────────────
FROM postgres:18 AS builder

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		ca-certificates lsb-release wget \
	&& wget -q \
		https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
		-O /tmp/arrow.deb \
	&& apt-get install -y /tmp/arrow.deb \
	&& rm /tmp/arrow.deb \
	&& apt-get update \
	&& apt-get install -y --no-install-recommends \
		g++ \
		make \
		pkg-config \
		postgresql-server-dev-18 \
		libparquet-dev \
		libarrow-dev \
	&& rm -rf /var/lib/apt/lists/*

COPY extension /build/extension
RUN make -C /build/extension with_llvm=no install

# ── Stage 2: runtime ────────────────────────────────────────────────────────
FROM postgres:18

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		ca-certificates lsb-release wget \
	&& wget -q \
		https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
		-O /tmp/arrow.deb \
	&& apt-get install -y /tmp/arrow.deb \
	&& rm /tmp/arrow.deb \
	&& apt-get update \
	&& apt-get install -y --no-install-recommends \
		libparquet2400 \
		libarrow2400 \
		postgresql-18-partman \
	&& rm -rf /var/lib/apt/lists/*

# Copy compiled extension into PostgreSQL lib/extension directories
COPY --from=builder /usr/lib/postgresql/18/lib/parquet_io.so \
                    /usr/lib/postgresql/18/lib/
COPY --from=builder /usr/share/postgresql/18/extension/parquet_io* \
                    /usr/share/postgresql/18/extension/

COPY init.sql /docker-entrypoint-initdb.d/init.sql
