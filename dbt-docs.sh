#!/bin/bash

set -euo pipefail

case "${1:-}" in
  bigquery)
    cd dbt_bigquery
    dbt docs generate
    dbt docs serve --port 8091
    ;;
  postgres)
    cd dbt_postgres
    dbt docs generate
    dbt docs serve --port 8092
    ;;
  *)
    exit 0
    ;;
esac