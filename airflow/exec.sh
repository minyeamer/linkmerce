#!/bin/bash

container_id=$(docker ps --filter name=airflow-airflow-apiserver- --format '{{.ID}}' | head -n 1)

if [ -n "$container_id" ]; then
  docker exec -it "$container_id" /bin/bash
else
  echo "No container found with name starting with airflow-airflow-apiserver-"
  exit 1
fi