FROM confluentinc/cp-kafka-connect:7.3.0 AS connect

WORKDIR /usr/share/java

RUN curl -sO https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.3.0.Final/debezium-connector-postgres-2.3.0.Final-plugin.tar.gz && \
    tar -xzf debezium-connector-postgres-2.3.0.Final-plugin.tar.gz && \
    rm debezium-connector-postgres-2.3.0.Final-plugin.tar.gz

EXPOSE 8083

FROM python:3.11.10-bookworm AS base

ENV POETRY_HOME=/etc/poetry
ENV PATH="$PATH:$POETRY_HOME/bin"
ENV POSTGRES_HOST=db
ENV POSTGRES_PORT=5432
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=postgres
ENV EVENT_HUBS_HOST=ehubs.servicebus.windows.net

WORKDIR /app

RUN apt-get update && apt-get install -y curl

RUN curl -sSL https://install.python-poetry.org | python3 -

COPY pyproject.toml poetry.lock init_db.py agent.py agent_state.json schema.sql init_db.sh /app/

COPY olist /app/olist

RUN chmod +x init_db.sh

RUN poetry install

FROM base AS init

CMD /app/init_db.sh

FROM base AS agent

CMD [ "poetry", "run", "python", "agent.py" ]