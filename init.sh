#!/bin/bash

source ./.env

docker compose up -d && sleep 5 && docker exec postgres_ecommerce \
    psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    -f /sql/schema.sql
