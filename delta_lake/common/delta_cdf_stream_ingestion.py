from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit
from delta.tables import DeltaTable
from .utils import table_exists

from typing import List

class DeltaCDFStreamIngestion:
    def __init__(
        self,
        spark: SparkSession,
        checkpoint_location: str,
        source_table: str,
        target_catalog: str,
        target_schema: str,
        target_table: str,
        source_primary_key: List[str],
        target_primary_key: List[str],
        query_file: str
    ):
        self.spark = spark
        self.checkpoint_location = checkpoint_location
        self.source_table = source_table
        self.target_catalog = target_catalog
        self.target_schema = target_schema
        self.target_table = target_table
        self.source_primary_key = source_primary_key
        self.target_primary_key = target_primary_key
        self.query_file = query_file

    def get_source_df_stream(self) -> DataFrame:
        return (
            self
            .spark
            .readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .table(self.source_table)
        )

    def get_source_df_updates(self) -> DataFrame:
        df_source = self.get_source_df_stream()

        query = """
            SELECT
                *,
                CASE
                    WHEN _change_type = 'update_postimage'
                        THEN 'u'
                    WHEN _change_type = 'delete'
                        THEN 'd'
                    ELSE 'i'
                END AS op,
                _commit_timestamp AS ts
            FROM {df_source}
            WHERE _change_type <> 'update_preimage'
        """
        
        df_updates = (
            self.spark.sql(query, df_source=df_source)
        )

        for col in ["_commit_version", "_commit_timestamp", "_change_type"]:
            df_updates = df_updates.drop(col)
    
        return df_updates

    @property
    def target_full_name(self):
        return f"{self.target_catalog}.{self.target_schema}.{self.target_table}"
    
    def load_query(self):
        with open(self.query_file, "r") as f:
            query = f.read()
            return query

    def _hook_query(self, query: str):
        query_parts = query.split("FROM ")

        assert len(query_parts) == 2

        return " ".join([query_parts[0], ", op, ts", "FROM ", query_parts[1]])


    def _batch_upsert(self, df_batch: DataFrame, batch_id: int):
        global_temp_name = f"df_batch_{self.target_schema}_{self.target_table}"
        df_batch.createOrReplaceGlobalTempView(global_temp_name)

        pk = ", ".join(self.source_primary_key)
        query_last = f"""
            SELECT *
            FROM global_temp.{global_temp_name}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {pk} ORDER BY ts DESC) = 1
        """
        df_last = self.spark.sql(query_last)
        df_last.createOrReplaceGlobalTempView(global_temp_name)

        target_exists = table_exists(
            self.spark,
            self.target_catalog,
            self.target_schema,
            self.target_table
        )

        df_query = self.spark.sql(
            self._hook_query(
                self.load_query().replace("{df}", f"global_temp.{global_temp_name}")
            )
        )

        if not target_exists:
            df_query.write.mode("overwrite").saveAsTable(self.target_full_name)
            return
        
        update_fields = {
            field: f"source.{field}"
            for field in df_query.columns if field not in ["op", "ts"]
        }

        (
            DeltaTable.forName(self.spark, self.target_full_name)
            .alias("target")
            .merge(
                df_query.alias("source"),
                " AND ".join([
                    f"target.{pk} = source.{pk}"
                    for pk in self.target_primary_key
                ])
            )
            .whenMatchedUpdate(
                condition="source.op = 'u'",
                set=update_fields
            )
            .whenMatchedDelete(
                condition="source.op = 'd'"
            )
            .whenNotMatchedInsert(values=update_fields)
            .execute()
        )

    def upsert(self, trigger=None):
        df_updates = self.get_source_df_updates()

        df_stream = (
            df_updates.writeStream
            .format("delta")
            .option("checkpointLocation", self.checkpoint_location)
            .foreachBatch(self._batch_upsert)
        )

        if trigger is not None:
            df_stream = (
                df_stream
                .trigger(**trigger if type(trigger) == dict else None)
            )

        df_stream.start()
