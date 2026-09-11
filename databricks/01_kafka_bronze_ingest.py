# Databricks Notebook: 01_kafka_bronze_ingest
# ==============================================================================
# Phase 4 & 5: Bronze Layer Streaming Ingestion
# Ingests raw social media events from Apache Kafka into an append-only
# Delta Lake Bronze table on Amazon S3 with checkpointing and fault-tolerance.
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# 1. Initialize Spark Session (Pre-configured in Databricks Runtime)
spark = SparkSession.builder.appName("SocialMedia-Bronze-Ingestion").getOrCreate()

# Reduce logging verbosity in production
spark.sparkContext.setLogLevel("WARN")

# 2. Pipeline Configuration Parameters
# In Databricks, these can be parameterized using dbutils.widgets
KAFKA_BOOTSTRAP_SERVERS = spark.conf.get("pipeline.kafka.bootstrap", "<EC2_PUBLIC_IP>:9094")
KAFKA_TOPIC = spark.conf.get("pipeline.kafka.topic", "social-media-posts")
S3_BUCKET = spark.conf.get("pipeline.s3.bucket", "s3://social-media-lakehouse")

BRONZE_DELTA_PATH = f"{S3_BUCKET}/bronze/social_media_raw"
BRONZE_CHECKPOINT_PATH = f"{S3_BUCKET}/checkpoints/bronze_ingest"

print(f"Connecting to Kafka Broker: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Reading Topic: {KAFKA_TOPIC}")
print(f"Writing Bronze Delta Table to: {BRONZE_DELTA_PATH}")

# 3. Read Stream from Apache Kafka
# Uses spark-sql-kafka-0-10 connector
kafka_stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")          # Catch up on all historical events
    .option("failOnDataLoss", "false")              # Avoid stream crash if Kafka logs roll over
    .option("maxOffsetsPerTrigger", 5000)           # Rate limiting: max 5000 records per micro-batch
    .load()
)

# 4. Extract Raw Kafka Fields & Add Ingestion Metadata
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

# 5. Write Stream into S3 Delta Lake (Bronze Layer)
# Uses append mode for immutable history
bronze_query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
    .trigger(processingTime="5 seconds")            # Micro-batch execution every 5 seconds
    .start(BRONZE_DELTA_PATH)
)

print(f"Streaming query started with Query ID: {bronze_query.id}")
# In Databricks, awaitTermination keeps the streaming job active
# bronze_query.awaitTermination()
