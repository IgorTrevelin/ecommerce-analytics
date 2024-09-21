# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(".."))

from common.delta_cdf_stream_ingestion import DeltaCDFStreamIngestion
from common.utils import checkpoint_location

# COMMAND ----------

source_table = "dbs_data_eng.bronze.customers"
target_catalog = "dbs_data_eng"
target_schema = "silver"
target_table = "customers"
primary_key = ["customer_id"]
query_file = "customers.sql"
checkpoint_location_path = checkpoint_location(
    "silver",
    target_table
)

# COMMAND ----------

stream = DeltaCDFStreamIngestion(
    spark=spark,
    checkpoint_location=checkpoint_location_path,
    source_table=source_table,
    target_catalog=target_catalog,
    target_schema=target_schema,
    target_table=target_table,
    primary_key=primary_key,
    query_file=query_file
)

# COMMAND ----------

stream.upsert({"availableNow": True})

# COMMAND ----------


