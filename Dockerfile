FROM python:3.11.10-bookworm AS base

ENV POETRY_HOME=/etc/poetry
ENV PATH="$PATH:$POETRY_HOME/bin"
ENV POSTGRES_HOST=db
ENV POSTGRES_PORT=5432
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=postgres

WORKDIR /app

RUN apt-get update && apt-get install -y curl

RUN curl -sSL https://install.python-poetry.org | python3 -

COPY pyproject.toml poetry.lock init_db.py agent.py agent_state.json /app/

RUN poetry install

FROM base AS init_db

CMD [ "poetry", "run", "python", "init_db.py" ]

FROM base AS agent

CMD [ "poetry", "run", "python", "agent.py" ]