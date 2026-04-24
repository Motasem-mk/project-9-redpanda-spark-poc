import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    when,
    to_date,
    to_timestamp,
    count
)
from pyspark.sql.types import StructType, StringType, IntegerType


print("✅ Spark consumer starting: Redpanda → PySpark → Parquet + JSON aggregation")


# --------------------------------------------------
# 1. Environment variables
# --------------------------------------------------

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "client_tickets")

OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/app/output")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/app/checkpoint")

SHUFFLE_PARTITIONS = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4")


print(f"Kafka broker: {KAFKA_BROKER}")
print(f"Kafka topic: {KAFKA_TOPIC}")
print(f"Output path: {OUTPUT_PATH}")
print(f"Checkpoint path: {CHECKPOINT_PATH}")
print(f"Spark shuffle partitions: {SHUFFLE_PARTITIONS}")


# --------------------------------------------------
# 2. Create Spark session
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("RedpandaTicketConsumer") \
    .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 3. Define ticket schema
# --------------------------------------------------

ticket_schema = StructType() \
    .add("ticket_id", StringType()) \
    .add("client_id", IntegerType()) \
    .add("created_at", StringType()) \
    .add("request", StringType()) \
    .add("request_type", StringType()) \
    .add("priority", StringType())


# --------------------------------------------------
# 4. Read stream from Redpanda/Kafka
# --------------------------------------------------

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()


# --------------------------------------------------
# 5. Parse Kafka message value from JSON
# --------------------------------------------------

df_json = df_raw.selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), ticket_schema).alias("data")) \
    .select("data.*") \
    .filter(col("ticket_id").isNotNull())


# --------------------------------------------------
# 6. Transform and enrich the ticket data
# --------------------------------------------------

df_enriched = df_json.withColumn(
    "support_team",
    when(col("request_type") == "technical", "Tech Team")
    .when(col("request_type") == "billing", "Finance Team")
    .when(col("request_type") == "account", "Accounts Team")
    .otherwise("General Team")
).withColumn(
    "created_timestamp",
    to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss")
).withColumn(
    "created_date",
    to_date(col("created_timestamp"))
)


# --------------------------------------------------
# 7. Write enriched ticket data to Parquet
# --------------------------------------------------

query_enriched = df_enriched.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", f"{OUTPUT_PATH}/enriched") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/enriched") \
    .partitionBy("created_date") \
    .start()


# --------------------------------------------------
# 8. Aggregate tickets by request type and priority
# --------------------------------------------------

df_agg = df_enriched.groupBy("request_type", "priority") \
    .agg(count("*").alias("ticket_count"))


# --------------------------------------------------
# 9. Save aggregation results to JSON using foreachBatch
# --------------------------------------------------

def save_aggregation_to_json(batch_df, epoch_id):
    output_path = f"{OUTPUT_PATH}/aggregated/epoch_{epoch_id}"

    batch_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .json(output_path)

    print(f"✅ Aggregated JSON file written to: {output_path}")


query_agg_json = df_agg.writeStream \
    .foreachBatch(save_aggregation_to_json) \
    .outputMode("complete") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/aggregated_json") \
    .start()


# --------------------------------------------------
# 10. Print aggregation to console for debugging/demo
# --------------------------------------------------

query_console = df_agg.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", False) \
    .option("numRows", 50) \
    .start()


# --------------------------------------------------
# 11. Keep all streaming queries running
# --------------------------------------------------

spark.streams.awaitAnyTermination()