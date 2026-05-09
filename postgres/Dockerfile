FROM postgres:13

RUN apt-get update \
	&& apt-get install -y --no-install-recommends postgresql-13-partman \
	&& rm -rf /var/lib/apt/lists/*

COPY init.sql /docker-entrypoint-initdb.d/init.sql