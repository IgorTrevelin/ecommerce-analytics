import json
import numpy as np
from urllib.parse import quote_plus

from dotenv import dotenv_values
from sqlalchemy import text, Connection

import sys, os

sys.path.append(os.path.dirname(__file__))
from init_db import get_postgress_conn

from typing import Optional


class DBClient:
    def __init__(self, conn: Connection):
        self.conn = conn

    def _execute_query(self, query_template: str, params: Optional[tuple] = None):
        cursor = self.conn.execute(text(query_template), params)

        return cursor

    def _get_count(self, table_name: str):
        query = f"SELECT COUNT(*) FROM {table_name}"

        cursor = self._execute_query(query)
        row = cursor.fetchone()

        if row is None:
            return 0

        return row[0]

    def _get_nth_row(self, table_name: str, id_col: str, n: int):
        assert n >= 1

        query = f"""
            SELECT *
            FROM {table_name}
            ORDER BY {id_col} ASC
            OFFSET {n - 1}
            LIMIT 1
        """

        cursor = self._execute_query(query)
        row = cursor.fetchone()

        if row is None:
            return 0

        return row

    def get_product_count(self):
        return self._get_count("products")

    def get_seller_count(self):
        return self._get_count("sellers")

    def get_customer_count(self):
        return self._get_count("customers")

    # ToDo: implement methods
    def create_product(category_id: int):
        pass

    def create_product_category(category_name: str, category_name_en: str):
        pass

    def close(self):
        self.conn.close()


class StateManager:
    def __init__(self, state_file: str):
        self.state_file = state_file
        self._states = None
        self._load()

    def _load(self):
        with open(self.state_file, "r") as f:
            self._states = json.load(f)

    def get_next_state(self, current_state: str):
        state_paths = self._states.get(current_state)

        if len(state_paths) == 0:
            return "END"

        if state_paths is None:
            raise ValueError()

        states = np.array(list(map(lambda x: x["next_state"], state_paths)))
        probs = np.array(list(map(lambda x: x["prob"], state_paths)))
        prob_sum = np.sum(probs)

        return np.random.choice(states, p=probs / prob_sum)

    def is_end(self, current_state: str):
        return current_state == "END"


class SimulationAgent:
    def __init__(self, db: DBClient, sm: StateManager):
        self.db = db
        self.sm = sm

    # Todo: implement agent methods


if __name__ == "__main__":
    env_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), ".env")
    env = dotenv_values(env_file)

    host = env.get("POSTGRES_HOST", "127.0.0.1")
    port = env.get("POSTGRES_PORT", "5432")
    user = env.get("POSTGRES_USER", "root")
    password = env.get("POSTGRES_PASSWORD", "postgres")
    db = env.get("POSTGRES_DB", "postgres")

    conn = get_postgress_conn(host, port, user, quote_plus(password), db)

    db = DBClient(conn)
    sm = StateManager("agent_state.json")

    # ToDo: agent in action

    db.close()
