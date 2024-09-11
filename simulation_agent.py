import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from collections import deque
from typing import Optional
from urllib.parse import quote_plus

from dotenv import dotenv_values
from sqlalchemy import create_engine, text, Connection

import sys, os
sys.path.append(os.path.dirname(__file__))
from init_db import get_postgress_conn



class SimulationAgent:
    def __init__(
        self,
        n_products: int,
        n_sellers: int,
        n_customers: int,
        n_orders: int,
        conn: Connection,
        seed: Optional[int] = None,
    ):
        self.n_products = n_products
        self.n_sellers = n_sellers
        self.n_customers = n_customers
        self.n_orders = n_orders
        self.rng = np.random.default_rng(seed)

        self.orders = pd.DataFrame([], columns=["order_id", "customer_id", "status", "purchase_timestamp", "approved_at", "delivered_carrier_date", "delivered_customer_date", "estimated_delivery_date"])
        self.order_items = pd.DataFrame([], columns=["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"])

        self.current_time = datetime.now()

        self.n_actions = 7

    def random_action(self):
        # 0 - create new order
        # 1 - process existing order

        random_action = self.rng.integers(0, self.n_actions)
        if len(self.orders) == 0:
            random_action = 0

        if random_action == 0:
            {
                "order_id": self.n_orders,
                "customer_id": self.rng.integers(0, self.n_customers),
                "status": self.rng.integers(0, self.n_customers),
                "purchase_timestamp"
                "approved_at"
                "delivered_carrier_date"
                "delivered_customer_date"
                "estimated_delivery_date"
            }
            self.n_orders += 1


        self.current_time += timedelta(
            minutes=self.rng.integers(0, 30),
            seconds=self.rng.integers(0, 60),
        )



    def run_simulation(self):
        while True:
            self.random_action()



if __name__ == "__main__":
    env_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), ".env")
    env = dotenv_values(env_file)

    host = env.get("POSTGRES_HOST", "127.0.0.1")
    port = env.get("POSTGRES_PORT", "5432")
    user = env.get("POSTGRES_USER", "root")
    password = env.get("POSTGRES_PASSWORD", "postgres")
    db = env.get("POSTGRES_DB", "postgres")

    conn = get_postgress_conn(host, port, user, quote_plus(password), db)

    agent = SimulationAgent(n_products=10, n_sellers=10, n_customers=10, conn=conn)
    # agent.run_simulation()
    conn.close()
