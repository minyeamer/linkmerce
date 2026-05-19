#!/bin/bash

if [ -f "$(dirname "$0")/.env" ]; then
  export $(grep -v '^#' "$(dirname "$0")/.env" | xargs)
fi

PG_USER=${POSTGRES_USER:-linkmerce}
PG_DB=${POSTGRES_DB:-linkmerce}

container_id=$(docker ps --filter name=linkmerce-postgres --format '{{.ID}}' | head -n 1)

if [ -n "$container_id" ]; then
  docker exec -it "$container_id" psql -U "$PG_USER" -d "$PG_DB"
else
  echo "No container found with name starting with linkmerce-postgres"
  exit 1
fi