# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(".."))

from common.delta_cdf_stream_ingestion import DeltaCDFStreamIngestion
from common.utils import checkpoint_location

# COMMAND ----------

source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table = dbutils.widgets.get("target_table")
source_primary_key = [pk.strip() for pk in dbutils.widgets.get("source_primary_key").split(",")]
target_primary_key = [pk.strip() for pk in dbutils.widgets.get("target_primary_key").split(",")]
query_file = dbutils.widgets.get("query_file")
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
    source_primary_key=source_primary_key,
    target_primary_key=target_primary_key,
    query_file=f"sql/{query_file}"
)

stream.upsert({"availableNow": True})
