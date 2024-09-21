from typing import List, Mapping

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col, desc, row_number, when, expr
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from .utils import table_exists

class HubsStreamIngestion:
    def __init__(
        self,
        spark: SparkSession,
        event_hubs_options: Mapping[str, str],
        source_schema: StructType,
        target_catalog: str,
        target_schema: str,
        target_table: str,
        primary_keys: List[str],
        checkpoint_location: str
    ):
        self.spark = spark
        self.event_hubs_options = event_hubs_options
        self.source_schema = source_schema
        self.target_catalog = target_catalog
        self.target_schema = target_schema
        self.target_table = target_table
        self.primary_keys = primary_keys
        self.checkpoint_location = checkpoint_location


    @property
    def table_name(self):
        return f"{self.target_catalog}.{self.target_schema}.{self.target_table}"

    def _get_raw_df(self) -> DataFrame:
        event_hubs_conn_string = self.event_hubs_options.get("event_hubs_conn_string")
        event_hubs_namespace = self.event_hubs_options.get("event_hubs_namespace")
        topic_name = self.event_hubs_options.get("event_hubs_topic")

        options = {
            "kafka.bootstrap.servers": f"{event_hubs_namespace}.servicebus.windows.net:9093",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{event_hubs_conn_string}\";",
            "subscribe": topic_name,
            "startingOffsets": "earliest"
        }

        return (
            self.spark.readStream
            .format("kafka")
            .options(**options)
            .load()
            .selectExpr("CAST(value AS STRING) AS event")
        )

    def get_df_source_updates(self) -> DataFrame:
        event_schema = StructType([
            StructField("payload", StructType([
                StructField("before", self.source_schema),
                StructField("after", self.source_schema),
                StructField("ts_ms", StringType()),
                StructField("op", StringType())
            ]))
        ])

        df_raw_stream = self._get_raw_df()

        return (
            df_raw_stream
            .withColumn("event", from_json("event", event_schema))
            .withColumn("payload", when(col("event.payload.op").isin(["c", "u"]), col("event.payload.after"))
                .when(col("event.payload.op") == "d", col("event.payload.before"))
                .when(col("event.payload.op") == "t", expr("NULL"))
            )
            .withColumn("op", col("event.payload.op"))
            .where("op IN ('c', 'u', 'd', 't')")
            .withColumn("ts_ms", col("event.payload.ts_ms").cast("long"))
            .select(
                col("payload.*"),
                col("op"),
                col("ts_ms")
            )
        )

    

    def _upsert_batch(self, df_updates, batch_id):
        target_exists = table_exists(
            self.target_catalog,
            self.target_schema,
            self.target_table
        )

        df_last_truncate_table = (
            df_updates
            .where("op = 't'")
            .orderBy(desc("ts_ms"))
            .limit(1)
        )

        has_truncate_table_events = df_last_truncate_table.count() > 0

        if has_truncate_table_events:
            last_truncate_table_event_ts = df_last_truncate_table.first()["ts_ms"]
            df_updates = df_updates.where(col("ts_ms") > last_truncate_table_event_ts)

            if target_exists:
                self.spark.sql(f"TRUNCATE TABLE {self.table_name};")

        window_spec = Window.partitionBy(*self.primary_keys).orderBy(desc("ts_ms"))
        df_updates = (
            df_updates
            .withColumn("row_number", row_number().over(window_spec))
            .where("row_number = 1")
            .drop("row_number")
        )

        if not target_exists:
            (
                df_updates
                .drop("op", "ts_ms")
                .write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(self.table_name)
            )

            return

        update_fields = {field: f"source.{field}" for field in df_updates.columns if field not in ["op", "ts_ms"]}
        (
            DeltaTable.forName(self.spark, self.table_name).alias('target')
            .merge(
                df_updates.alias('source'),
                " AND ".join([f"target.{pk} = source.{pk}" for pk in self.primary_keys])
            )
            .whenMatchedUpdate(
                condition=col("source.op") == "u",
                set=update_fields
            )
            .whenMatchedDelete(
                condition=col("source.op") == "d"
            )
            .whenNotMatchedInsert(
                condition=col("source.op") == "c",
                values=update_fields
            )
            .execute()
        )

    def stream_upsert(self, trigger=None):
        df_updates = self.get_df_source_updates()

        df_stream = (
            df_updates.writeStream
            .format("delta")
            .option("checkpointLocation", self.checkpoint_location)
            .foreachBatch(self._upsert_batch)
        )

        if trigger is not None:
            df_stream = (
                df_stream
                .trigger(**trigger if type(trigger) == dict else None)
            )

        df_stream.start()
