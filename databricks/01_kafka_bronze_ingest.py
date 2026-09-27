# Databricks Notebook: 01_kafka_bronze_ingest
# ==============================================================================
# Phase 4: Bronze Layer Streaming Ingestion
# Ingests raw social media events from Apache Kafka into an append-only
# Delta Lake Bronze table on Databricks Unity Catalog / S3 with fault-tolerant checkpointing.
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# 1. Initialize Spark Session
spark = SparkSession.builder.appName("SocialMedia-Bronze-Ingestion").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Pipeline Configuration Parameters
# Supports both Unity Catalog Volumes and S3 storage
curr_cat = spark.catalog.currentCatalog()
curr_sch = spark.catalog.currentDatabase()
print(f"Active Catalog: '{curr_cat}' | Schema: '{curr_sch}'")

# Ensure governed Volume exists for stream checkpoints
spark.sql(f"CREATE VOLUME IF NOT EXISTS {curr_cat}.{curr_sch}.lakehouse_checkpoints")
CHECKPOINT_BASE = f"/Volumes/{curr_cat}/{curr_sch}/lakehouse_checkpoints"
CHECKPOINT_BRONZE = f"{CHECKPOINT_BASE}/bronze"

KAFKA_BOOTSTRAP_SERVERS = spark.conf.get("pipeline.kafka.bootstrap", "18.60.200.198:30094")
KAFKA_TOPIC = spark.conf.get("pipeline.kafka.topic", "twitter")

print(f"📡 Connecting to Kafka Broker: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"📋 Reading Topic: {KAFKA_TOPIC}")
print(f"💾 Checkpoint Location: {CHECKPOINT_BRONZE}")

# 3. Read Stream from Apache Kafka
# Uses spark-sql-kafka-0-10 connector
kafka_stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")          # Catch up on all historical events
    .option("failOnDataLoss", "false")              # Avoid stream crash if Kafka logs roll over
    .option("maxOffsetsPerTrigger", 10000)          # Bounded safety ceiling to protect memory during catch-up
    .load()
)

# 4. Extract Raw Kafka Fields & Add Lineage Metadata
# Enterprise Best Practice: Bronze table should NEVER parse or modify the payload.
# It stores the immutable raw message with lineage metadata for complete auditability.
bronze_df = kafka_stream_df.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("raw_payload"),
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_event_time"),
    current_timestamp().alias("_ingest_timestamp")
)

# 5. Stream Raw Events into Bronze Delta Table
# Uses Trigger.AvailableNow for efficient micro-batching on Serverless & Workflow orchestration
bronze_query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_BRONZE)
    .trigger(availableNow=True)
    .toTable("bronze_social_media_raw")
)

bronze_query.awaitTermination()
print("✅ Bronze Ingestion Complete! Data safely committed to 'bronze_social_media_raw'.")
