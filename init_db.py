# %%

import os
import pandas as pd

from sqlalchemy import create_engine, text, Connection
from dotenv import dotenv_values

env_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), ".env")
env = dotenv_values(env_file)


def get_postgress_conn(host: str, port: str, user: str, password: str, db: str):
    url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url)
    conn = engine.connect()
    return conn


def read_table(conn: Connection, table_name: str):
    return pd.read_sql(table_name, conn)


def insert_batch(conn: Connection, df: pd.DataFrame, table_name: str):
    query = """
        INSERT INTO %s (%s)
        VALUES (%s)
    """ % (
        table_name,
        ", ".join(df.columns),
        ", ".join([f":{c}" for c in df.columns]),
    )

    try:
        data = df.to_dict(orient="records")
        conn.execute(text(query), data)
        conn.commit()
        return True
    except Exception as ex:
        conn.rollback()
        print(ex)
        return False


def load_zip_code_prefixes(conn: Connection):
    df = pd.read_csv(
        "./olist/olist_geolocation_dataset.csv",
        header=0,
        usecols=["geolocation_zip_code_prefix", "geolocation_state"],
        dtype="str",
    )

    df = df.groupby("geolocation_zip_code_prefix", as_index=False)[
        "geolocation_state"
    ].first()

    df = df.rename(
        columns={
            "geolocation_zip_code_prefix": "zip_code_prefix",
            "geolocation_state": "state",
        }
    )

    insert_batch(conn, df, "zip_code_prefixes")


def load_product_categories(conn: Connection):
    df = pd.read_csv(
        "./olist/product_category_name_translation.csv",
        header=0,
        dtype="str",
    )

    df = df.drop_duplicates()

    df = df.rename(
        columns={
            "product_category_name": "product_category",
            "product_category_name_english": "product_category_name_english",
        }
    )

    insert_batch(conn, df, "product_categories")


def load_products(conn: Connection):
    df_cats = read_table(conn, "product_categories")

    df = pd.read_csv(
        "./olist/olist_products_dataset.csv",
        header=0,
        usecols=["product_category_name"],
        dtype="str",
    )

    df = df.merge(
        df_cats,
        left_on="product_category_name",
        right_on="product_category",
        how="inner",
    )

    df = df[["category_id"]]

    insert_batch(conn, df, "products")


def load_customers(conn: Connection):
    df_zips = read_table(conn, "zip_code_prefixes")

    df = pd.read_csv(
        "./olist/olist_customers_dataset.csv",
        header=0,
        usecols=["customer_zip_code_prefix"],
        dtype="str",
    )

    df = df.merge(
        df_zips,
        left_on="customer_zip_code_prefix",
        right_on="zip_code_prefix",
        how="inner",
    )
    df = df[["zip_code_id"]]

    insert_batch(conn, df, "customers")


def load_sellers(conn: Connection):
    df_zips = read_table(conn, "zip_code_prefixes")

    df = pd.read_csv(
        "./olist/olist_sellers_dataset.csv",
        header=0,
        usecols=["seller_zip_code_prefix"],
        dtype="str",
    )

    df = df.merge(
        df_zips,
        left_on="seller_zip_code_prefix",
        right_on="zip_code_prefix",
        how="inner",
    )
    df = df[["zip_code_id"]]

    insert_batch(conn, df, "sellers")


def load_csv(filename: str):
    return pd.read_csv(f"./olist/{filename}", header=0, dtype="str")


df_cats = load_csv("product_category_name_translation.csv")
df_zip = (
    load_csv("olist_geolocation_dataset.csv")
    .groupby("geolocation_zip_code_prefix", as_index=False)["geolocation_state"]
    .first()
)
df_prods = load_csv("olist_products_dataset.csv")
df_sellers = load_csv("olist_sellers_dataset.csv")
df_customers = load_csv("olist_customers_dataset.csv")
df_orders = load_csv("olist_orders_dataset.csv")
df_order_items = load_csv("olist_order_items_dataset.csv")

df = (
    df_order_items.merge(df_orders, on="order_id", how="inner")
    .merge(df_customers, on="customer_id", how="inner")
    .merge(df_sellers, on="seller_id", how="inner")
    .merge(df_prods, on="product_id", how="inner")
    .merge(df_cats, on="product_category_name", how="inner")
    .merge(
        df_zip.rename(columns={c: f"customer_{c}" for c in df_zip.columns}),
        left_on="customer_zip_code_prefix",
        right_on="customer_geolocation_zip_code_prefix",
        how="left",
    )
    .merge(
        df_zip.rename(columns={c: f"seller_{c}" for c in df_zip.columns}),
        left_on="seller_zip_code_prefix",
        right_on="seller_geolocation_zip_code_prefix",
        how="left",
    )
)
df.head()

if __name__ == "__main__":
    host = env.get("POSTGRES_HOST", "127.0.0.1")
    port = env.get("POSTGRES_PORT", "5432")
    user = env.get("POSTGRES_USER", "root")
    password = env.get("POSTGRES_PASSWORD", "postgres")
    db = env.get("POSTGRES_DB", "postgres")

    conn = get_postgress_conn(host, port, user, password, db)
    load_zip_code_prefixes(conn)
    load_product_categories(conn)
    load_products(conn)
    load_customers(conn)
    load_sellers(conn)

    conn.close()
