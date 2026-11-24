#!/bin/bash

container_id=$(docker ps --filter name=linkmerce-postgres --format '{{.ID}}' | head -n 1)

if [ -n "$container_id" ]; then
  docker exec -it "$container_id" psql -U linkmerce -d linkmerce
else
  echo "No container found with name starting with linkmerce-postgres"
  exit 1
fi