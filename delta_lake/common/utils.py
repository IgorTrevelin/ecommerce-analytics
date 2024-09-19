from pyspark.sql import SparkSession

def catalog_table_exists(spark: SparkSession, catalog: str, schema: str, table: str) -> bool:
    return spark.catalog.tableExists(f"{catalog}.{schema}.{table}")

def checkpoint_location(layer: str, table: str) -> str:
    assert layer in ["bronze", "silver", "gold"]

    return f"/Volumes/dbs_data_eng/default/checkpoints/{layer}/{table}"