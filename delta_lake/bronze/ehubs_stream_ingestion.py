# Databricks notebook source
# MAGIC %run ../make_imports

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from delta_lake.lib.hubs_stream_ingestion import HubsStreamIngestion

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
table_name = dbutils.widgets.get("table_name")
ehubs_topic = dbutils.widgets.get("ehubs_topic")
schema_filename = dbutils.widgets.get("schema_filename")
primary_key = [
    pk.strip()
    for pk in dbutils.widgets.get("primary_key").split(",")
]

# COMMAND ----------

def load_schema(schema_file: str):
    with open(f"schemas/{schema_file}", "r") as schema_file:
        return StructType.fromJson(json.load(schema_file))

# COMMAND ----------

schema = load_schema(schema_filename)

stream = HubsStreamIngestion(
    spark=spark,
    event_hubs_options={
        "event_hubs_namespace": "ehubsdata",
        "event_hubs_conn_string": dbutils.secrets.get(
            "dbsecrets",
            "eventhubs-conn-string"
        ),
        "event_hubs_topic": ehubs_topic
    },
    source_schema=schema,
    target_catalog=catalog,
    target_schema="bronze",
    target_table=table_name,
    primary_keys=primary_key,
    checkpoint_location=checkpoint_location("bronze", table_name)
)

stream.stream_upsert(trigger={"availableNow": True})
