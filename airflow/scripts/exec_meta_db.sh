#!/bin/bash

container_id=$(docker ps --filter name=airflow-postgres- --format '{{.ID}}' | head -n 1)

if [ -n "$container_id" ]; then
  docker exec -it "$container_id" psql -U airflow -d airflow
else
  echo "No container found with name starting with airflow-postgres-"
  exit 1
fi