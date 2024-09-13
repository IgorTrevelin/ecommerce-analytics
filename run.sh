#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [[ "$1" == "install" ]]; then
    cd $SCRIPT_DIR && \
    mkdir -p plugins && \
    cd plugins/ && \
    wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.7.2.Final/debezium-connector-postgres-2.7.2.Final-plugin.tar.gz && \
    tar -xzf debezium-connector-postgres-2.7.2.Final-plugin.tar.gz && \
    rm debezium-connector-postgres-2.7.2.Final-plugin.tar.gz && \
    cd $SCRIPT_DIR && \
    docker compose up -d && \
    docker exec postgres_ecommerce \
        psql -v ON_ERROR_STOP=1 \
        --username "$POSTGRES_USER" \
        --dbname "$POSTGRES_DB" \
        -f /sql/schema.sql && \
    python init_db.py && \
    sleep 10 && \
    curl    -X POST http://127.0.0.1:8083/connectors \
            -H "Content-Type: application/json" \
            -d '{
                "name": "postgresql-connector",
                "config": {
                    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                    "database.hostname": "db",
                    "database.port": "5432",
                    "database.user": "'"${POSTGRES_USER}"'",
                    "database.password": "'"${POSTGRES_PASSWORD}"'",
                    "database.dbname": "'"${POSTGRES_DB}"'",
                    "plugin.name": "pgoutput",
                    "topic.prefix": "psql"
                }
            }'

elif [[ "$1" == "agent" ]]; then

    docker compose up -d && \
    python agent.py

elif [[ "$1" == "clear" ]]; then

    docker compose down && \
    rm -rf ${SCRIPT_DIR}/plugins

fi