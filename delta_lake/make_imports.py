# Databricks notebook source
import os
import sys

cwd = os.path.abspath(
    os.path.join(
        os.path.dirname(
            dbutils.notebook.entry_point
            .getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
    )
)

sys.path.append(cwd)

# COMMAND ----------

def checkpoint_location(layer: str, table: str) -> str:
    assert layer in ["bronze", "silver", "gold"]

    return f"/Volumes/dbs_data_eng/default/checkpoints/{layer}/{table}"
