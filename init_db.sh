#!/bin/bash

curl -X POST http://kafka-connect:8083/connectors \
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
            "database.history.kafka.bootstrap.servers": "'"${EVENT_HUBS_HOST}:9093"'",
            "database.history.kafka.topic": "schema-changes.postgres",
            "plugin.name": "pgoutput",
            "topic.prefix": "postgres",
            "skipped.operations": "none",
            "decimal.handling.mode": "string"
        }
    }'

poetry run python init_db.py