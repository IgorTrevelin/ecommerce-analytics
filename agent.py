import json
import numpy as np
from urllib.parse import quote_plus

from dotenv import dotenv_values
from sqlalchemy import text, Connection

import sys, os, time

sys.path.append(os.path.dirname(__file__))
from init_db import get_postgress_conn

from typing import Optional, List


class DBClient:
    def __init__(self, conn: Connection):
        self.conn = conn

    def get_login_id(self, user_type: Optional[str] = "customer"):
        if user_type == "customer":
            (user_id,) = self.conn.execute(
                text(f"SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1;")
            ).fetchone()
        elif user_type == "seller":
            (user_id,) = self.conn.execute(
                text(f"SELECT seller_id FROM sellers ORDER BY RANDOM() LIMIT 1;")
            ).fetchone()

        return user_id

    def add_customer(self, customer_unique_id: str, zip_code_id: Optional[int] = None):
        if zip_code_id is None:
            (zip_code_id,) = self.conn.execute(
                text(
                    f"SELECT zip_code_id FROM zip_code_prefixes ORDER BY RANDOM() LIMIT 1;"
                )
            ).fetchone()

        insert_query = text(
            f"""
            INSERT INTO customers (customer_unique_id, zip_code_id)
                VALUES (:customer_unique_id, :zip_code_id)
        """
        )

        cursor = self.conn.execute(
            insert_query,
            {
                "customer_unique_id": customer_unique_id,
                "zip_code_id": zip_code_id,
            },
        )
        self.conn.commit()

    def add_seller(self, zip_code_id: Optional[int] = None):
        if zip_code_id is None:
            (zip_code_id,) = self.conn.execute(
                text(
                    f"SELECT zip_code_id FROM zip_code_prefixes ORDER BY RANDOM() LIMIT 1;"
                )
            ).fetchone()

        insert_query = text(
            f"""
            INSERT INTO sellers (zip_code_id)
                VALUES (:zip_code_id)
        """
        )

        cursor = self.conn.execute(
            insert_query,
            {
                "zip_code_id": zip_code_id,
            },
        )
        self.conn.commit()

    def add_product_category(
        self,
        product_category: Optional[str] = None,
        product_category_english: Optional[str] = None,
        complement: Optional[str] = "",
    ):
        if (product_category or product_category_english) is None:
            product_category, product_category_english = self.conn.execute(
                text(
                    f"""
                    SELECT product_category, product_category_english
                    FROM product_categories
                    ORDER BY RANDOM()
                    LIMIT 1;
                """
                )
            ).fetchone()

        insert_query = text(
            f"""
            INSERT INTO product_categories (product_category, product_category_english)
                VALUES (:product_category, :product_category_english)
        """
        )

        cursor = self.conn.execute(
            insert_query,
            {
                "product_category": f"{product_category}{complement}"[-50:],
                "product_category_english": f"{product_category_english}{complement}"[
                    -50:
                ],
            },
        )
        self.conn.commit()

    def add_product(self):
        (category_id,) = self.conn.execute(
            text(
                f"""
            SELECT category_id
            FROM product_categories
            ORDER BY RANDOM()
            LIMIT 1;
        """
            )
        ).fetchone()

        insert_query = text(
            f"""
            INSERT INTO products (category_id)
                VALUES (:category_id)
        """
        )

        cursor = self.conn.execute(
            insert_query,
            {
                "category_id": category_id,
            },
        )
        self.conn.commit()

    def new_order(
        self,
        customer_id: int,
        n_products: List[int],
        prices: List[float],
        freight_values: List[float],
        product_ids: Optional[List[int]] = None,
        seller_ids: Optional[List[int]] = None,
    ):
        if seller_ids is None:
            seller_ids = self.conn.execute(
                text(
                    f"SELECT seller_id FROM sellers ORDER BY RANDOM() LIMIT {len(n_products)};"
                )
            ).fetchall()
            seller_ids = [row[0] for row in seller_ids]
        seller_ids = f"{{{','.join(map(str, [id for amount, id in zip(n_products, seller_ids) for _ in range(amount)]))}}}"

        if product_ids is None:
            product_ids = self.conn.execute(
                text(
                    f"SELECT product_id FROM products ORDER BY RANDOM() LIMIT {len(n_products)};"
                )
            ).fetchall()
            product_ids = [row[0] for row in product_ids]
        product_ids = f"{{{','.join(map(str, [id for amount, id in zip(n_products, product_ids) for _ in range(amount)]))}}}"

        fix_price = lambda x: f"{x:.2f}"

        prices = [
            price for amount, price in zip(n_products, prices) for _ in range(amount)
        ]
        prices = f"{{{','.join(map(fix_price, prices))}}}"

        freight_values = [
            freight_value
            for amount, freight_value in zip(n_products, freight_values)
            for _ in range(amount)
        ]
        freight_values = f"{{{','.join(map(fix_price, freight_values))}}}"

        query = text(
            f"""
            CALL sp_create_order_with_items(
                :customer_id,
                :products,
                :sellers,
                :prices,
                :freight_values
            );
        """
        )

        cursor = self.conn.execute(
            query,
            {
                "customer_id": customer_id,
                "products": product_ids,
                "sellers": seller_ids,
                "prices": prices,
                "freight_values": freight_values,
            },
        )

        self.conn.commit()

    def close(self):
        self.conn.close()


class StateManager:
    def __init__(self, state_file: str, seed: Optional[int] = None):
        self.state_file = state_file
        self._states = None
        self._load()

        self.rng = np.random.default_rng(seed)

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

        return self.rng.choice(states, p=probs / prob_sum)

    def is_end(self, current_state: str):
        return current_state == "END"


class SimulationAgent:
    def __init__(self, db: DBClient, sm: StateManager, seed: Optional[int] = None):
        self.db = db
        self.sm = sm

        self.rng = np.random.default_rng(seed)

        self.take_action = {
            "ANONYMOUS": self.reset,
            "REGISTRATION": self.registration,
            "LOGIN_CUSTOMER": self.login_customer,
            "LOGIN_SELLER": self.login_seller,
            "LOGIN_ADMIN": self.login_admin,
            "ADMIN_CREATE_PROD_CATEGORY": self.create_product_category,
            "CUSTOMER_PURCHASE": self.purchase,
            "SELLER_CREATE_PRODUCT": self.create_product,
            "END": lambda: None,
        }

    def reset(self):
        self.user_type = None
        self.user_id = None
        self.just_registered = False

    def registration(self, user_type: str):
        if user_type == "customer":
            self.user_id = self.db.add_customer(self.rng.bytes(16).hex())
        elif user_type == "seller":
            self.user_id = self.db.add_seller()

        self.just_registered = True

    def login_customer(self, user_id: Optional[int] = None):
        if user_id is None:
            user_id = self.db.get_login_id(user_type="customer")

        self.user_id = user_id
        self.user_type = "customer"

    def login_seller(self, user_id: Optional[int] = None):
        if user_id is None:
            user_id = self.db.get_login_id(user_type="seller")

        self.user_id = user_id
        self.user_type = "seller"

    def login_admin(self, user_id: Optional[int] = None):
        self.user_id = user_id
        self.user_type = "admin"

    def create_product_category(self):
        self.db.add_product_category(complement=self.rng.random())

    def purchase(self):
        n_prod_types = self.rng.integers(1, 5)
        n_products = self.rng.integers(1, 5, (n_prod_types,)).tolist()
        prices = (self.rng.power(a=0.33, size=(n_prod_types,)) * 1000 + 20).tolist()
        freight_values = self.rng.uniform(5, 100, size=(n_prod_types,)).tolist()

        self.db.new_order(self.user_id, n_products, prices, freight_values)

    def create_product(self):
        self.db.add_product()

    def act(self, current_state, next_state):
        params = {}
        if current_state == "REGISTRATION":
            params = {
                "user_type": "customer" if next_state == "LOGIN_CUSTOMER" else "seller"
            }
        elif "LOGIN" in current_state:
            params = {"user_id": self.user_id}

        self.take_action[current_state](**params)

    def run(self, epochs=1e6):
        current_state = "ANONYMOUS"

        if epochs is None:
            while True:
                try:
                    next_state = self.sm.get_next_state(current_state)
                    self.act(current_state, next_state)
                    current_state = (
                        "ANONYMOUS" if current_state == "END" else next_state
                    )

                    time.sleep(np.random.rand() * 5.0)
                except KeyboardInterrupt:
                    exit(0)

        for i in range(int(epochs)):
            next_state = self.sm.get_next_state(current_state)
            self.act(current_state, next_state)
            current_state = "ANONYMOUS" if current_state == "END" else next_state


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

    SimulationAgent(db, sm, seed=42).run(None)
