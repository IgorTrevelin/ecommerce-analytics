import os
import pandas as pd
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text, Connection
from dotenv import load_dotenv


def init_database_schema(conn: Connection):
    with open(
        os.path.join(os.path.abspath(os.path.dirname(__file__)), "schema.sql"), "r"
    ) as f:
        queries = f.read()
    conn.execute(text(queries))
    conn.commit()


def load_csv(filename: str, usecols=None):
    return pd.read_csv(f"./olist/{filename}", header=0, dtype="str", usecols=usecols)


def get_postgress_conn(host: str, port: str, user: str, password: str, db: str):
    url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url)
    conn = engine.connect()
    return conn


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
        return False


def load_initial_dataset(conn: Connection):
    # load data
    df_categories = load_csv(
        "product_category_name_translation.csv",
        usecols=["product_category_name", "product_category_name_english"],
    )
    df_zip = load_csv(
        "olist_geolocation_dataset.csv",
        usecols=["geolocation_zip_code_prefix", "geolocation_state"],
    )
    df_products = load_csv(
        "olist_products_dataset.csv", usecols=["product_id", "product_category_name"]
    )
    df_sellers = load_csv(
        "olist_sellers_dataset.csv",
        usecols=["seller_id", "seller_zip_code_prefix", "seller_state"],
    )
    df_customers = load_csv(
        "olist_customers_dataset.csv",
        usecols=[
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_state",
        ],
    )
    df_orders = load_csv("olist_orders_dataset.csv")
    df_order_items = load_csv("olist_order_items_dataset.csv")

    # rename csv column names to match sql schema
    df_categories.rename(
        columns={
            "product_category_name": "product_category",
            "product_category_name_english": "product_category_english",
        },
        inplace=True,
    )
    df_zip.rename(
        columns={
            "geolocation_zip_code_prefix": "zip_code_prefix",
            "geolocation_state": "state",
        },
        inplace=True,
    )
    df_products.rename(
        columns={"product_category_name": "product_category"}, inplace=True
    )
    df_customers.rename(
        columns={
            "customer_zip_code_prefix": "zip_code_prefix",
            "customer_state": "state",
        },
        inplace=True,
    )
    df_sellers.rename(
        columns={"seller_zip_code_prefix": "zip_code_prefix", "seller_state": "state"},
        inplace=True,
    )
    df_orders.rename(
        columns={
            "order_status": "status",
            "order_purchase_timestamp": "purchase_timestamp",
            "order_approved_at": "approved_at",
            "order_delivered_carrier_date": "delivered_carrier_date",
            "order_delivered_customer_date": "delivered_customer_date",
            "order_estimated_delivery_date": "estimated_delivery_date",
        },
        inplace=True,
    )

    max_customers = 100
    customer_ids = df_customers["customer_id"].unique()[:max_customers]
    df_customers = df_customers[df_customers["customer_id"].isin(customer_ids)]
    df_orders = df_orders[df_orders["customer_id"].isin(customer_ids)]
    order_ids = df_orders["order_id"].unique()
    df_order_items = df_order_items[df_order_items["order_id"].isin(order_ids)]
    product_ids = df_order_items["product_id"].unique()
    df_products = df_products[df_products["product_id"].isin(product_ids)]
    
    # prepare data to insert into tables
    # - fix identifiers to be in accordance with the new standard defined in the schema (as serial)
    complement = df_products[["product_category"]]
    df_categories = pd.concat([df_categories, complement], axis=0)
    df_categories.drop_duplicates("product_category", inplace=True)
    df_categories.fillna(
        {
            "product_category": "unindentified",
            "product_category_english": "unindentified",
        },
        inplace=True,
    )
    # id fix
    df_categories.sort_values(by="product_category", inplace=True)
    df_categories.reset_index(drop=True, inplace=True)
    df_categories["category_id"] = df_categories.index + 1

    complement1 = df_customers[["zip_code_prefix", "state"]]
    complement2 = df_sellers[["zip_code_prefix", "state"]]
    df_zip = pd.concat([df_zip, complement1, complement2], axis=0)
    df_zip.drop_duplicates(inplace=True)
    # id fix
    df_zip.sort_values(by=["zip_code_prefix", "state"], inplace=True)
    df_zip.reset_index(drop=True, inplace=True)
    df_zip["zip_code_id"] = df_zip.index + 1

    df_products.drop_duplicates(inplace=True)
    df_products.fillna({"product_category": "unindentified"}, inplace=True)
    df_products = df_products.merge(df_categories, on="product_category", how="left")
    df_products.drop(
        columns=["product_category", "product_category_english"], inplace=True
    )
    # id fix
    df_products.sort_values(by="product_id", inplace=True)
    df_products.reset_index(drop=True, inplace=True)
    df_products["pid"] = df_products.index + 1

    df_customers.drop_duplicates(inplace=True)
    df_customers = df_customers.merge(
        df_zip, on=["zip_code_prefix", "state"], how="left"
    )
    df_customers.drop(columns=["zip_code_prefix", "state"], inplace=True)
    # id fix
    df_customers.sort_values(by="customer_id", inplace=True)
    df_customers.reset_index(drop=True, inplace=True)
    df_customers["cid"] = df_customers.index + 1

    df_sellers.drop_duplicates(inplace=True)
    df_sellers = df_sellers.merge(df_zip, on=["zip_code_prefix", "state"], how="left")
    df_sellers.drop(columns=["zip_code_prefix", "state"], inplace=True)
    # id fix
    df_sellers.sort_values(by="seller_id", inplace=True)
    df_sellers.reset_index(drop=True, inplace=True)
    df_sellers["sid"] = df_sellers.index + 1

    df_orders.fillna(
        {
            "approved_at": "2000-01-01 00:00:00",
            "delivered_carrier_date": "2000-01-01 00:00:00",
            "delivered_customer_date": "2000-01-01 00:00:00",
        },
        inplace=True,
    )
    df_orders.drop_duplicates(inplace=True)
    df_orders = df_orders.merge(df_customers, on="customer_id", how="left")
    df_orders.drop(
        columns=["customer_id", "customer_unique_id", "zip_code_id"], inplace=True
    )
    # id fix
    df_orders.sort_values(by="order_id", inplace=True)
    df_orders.reset_index(drop=True, inplace=True)
    df_orders["oid"] = df_orders.index + 1

    df_order_items.drop_duplicates(inplace=True)
    df_order_items = (
        df_order_items.merge(df_orders[["order_id", "oid"]], on="order_id", how="left")
        .merge(df_products[["product_id", "pid"]], on="product_id", how="left")
        .merge(df_sellers[["seller_id", "sid"]], on="seller_id", how="left")
    )
    df_order_items.drop(columns=["order_id", "product_id", "seller_id"], inplace=True)
    # id fix
    df_order_items.sort_values(by="order_item_id", inplace=True)
    df_order_items.reset_index(drop=True, inplace=True)

    # clean tmp columns
    df_categories.drop(columns=["category_id"], inplace=True)
    df_zip.drop(columns=["zip_code_id"], inplace=True)
    df_products.drop(columns=["product_id", "pid"], inplace=True)
    df_customers.drop(columns=["customer_id", "cid"], inplace=True)
    df_sellers.drop(columns=["seller_id", "sid"], inplace=True)
    df_orders.drop(columns=["order_id", "oid"], inplace=True)
    df_orders.rename(columns={"cid": "customer_id"}, inplace=True)
    df_order_items.rename(
        columns={"oid": "order_id", "pid": "product_id", "sid": "seller_id"},
        inplace=True,
    )

    # insert data to database
    for table_name, df in {
        "product_categories": df_categories,
        "zip_code_prefixes": df_zip,
        "products": df_products,
        "customers": df_customers,
        "sellers": df_sellers,
        "order_items": df_order_items,
        "orders": df_orders,
    }.items():
        insert_batch(conn, df, table_name)


if __name__ == "__main__":
    env_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), ".env")
    load_dotenv(env_file)

    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "root")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    db = os.getenv("POSTGRES_DB", "postgres")

    conn = get_postgress_conn(host, port, user, quote_plus(password), db)

    try:
        init_database_schema(conn)
    except:
        pass

    load_initial_dataset(conn)

    conn.close()
