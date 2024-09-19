from pyspark.sql import SparkSession

def catalog_table_exists(spark: SparkSession, catalog: str, schema: str, table: str) -> bool:
    return spark.catalog.tableExists(f"{catalog}.{schema}.{table}")
