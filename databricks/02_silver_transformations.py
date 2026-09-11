# Databricks Notebook: 02_silver_transformations
# ==============================================================================
# Phase 4 & 5: Silver Layer Streaming Transformation & Dead-Letter Queue (DLQ)
# 
# Key Engineering Decisions:
# 1. Bounded State Store: Applies withWatermark BEFORE dropDuplicates so RocksDB
#    evicts expired state rather than leaking memory indefinitely.
# 2. Dead-Letter Queue (DLQ): Routes malformed/unparseable JSON payloads into a
#    quarantine table rather than silently dropping data.
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, BooleanType, ArrayType
)
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, current_timestamp
)

spark = SparkSession.builder.appName("SocialMedia-Silver-Transformations").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------------------------
# 1. Pipeline Paths
# ------------------------------------------------------------------------------
S3_BUCKET = spark.conf.get("pipeline.s3.bucket", "s3://social-media-lakehouse")
BRONZE_DELTA_PATH = f"{S3_BUCKET}/bronze/social_media_raw"
SILVER_DELTA_PATH = f"{S3_BUCKET}/silver/social_media_posts"
QUARANTINE_DELTA_PATH = f"{S3_BUCKET}/quarantine/corrupt_events"

SILVER_CHECKPOINT_PATH = f"{S3_BUCKET}/checkpoints/silver_transform"
QUARANTINE_CHECKPOINT_PATH = f"{S3_BUCKET}/checkpoints/silver_quarantine"

# ------------------------------------------------------------------------------
# 2. Explicit Schema Definition for Raw JSON Payloads
# ------------------------------------------------------------------------------
social_post_schema = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("username", StringType(), nullable=True),
    StructField("user_followers", LongType(), nullable=True),
    StructField("is_verified", BooleanType(), nullable=True),
    StructField("text", StringType(), nullable=True),
    StructField("primary_hashtag", StringType(), nullable=True),
    StructField("hashtags", ArrayType(StringType()), nullable=True),
    StructField("sentiment_score", DoubleType(), nullable=True),
    StructField("likes", LongType(), nullable=True),
    StructField("retweets", LongType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("device", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True)
])

# ------------------------------------------------------------------------------
# 3. Read Stream from Bronze Delta Table
# ------------------------------------------------------------------------------
bronze_stream = (
    spark.readStream
    .format("delta")
    .load(BRONZE_DELTA_PATH)
)

# Parse raw JSON payloads using explicit schema
parsed_stream = bronze_stream.withColumn(
    "parsed", from_json(col("raw_payload"), social_post_schema)
)

# ------------------------------------------------------------------------------
# 4. Operational Observability: Dead-Letter Queue (Quarantine)
# Instead of silently filtering bad data, route unparseable records to a DLQ table
# ------------------------------------------------------------------------------
corrupt_records_df = (
    parsed_stream
    .filter(col("parsed.event_id").isNull() | col("parsed.timestamp").isNull())
    .select(
        col("kafka_key"),
        col("raw_payload"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_event_time"),
        col("_ingest_timestamp"),
        current_timestamp().alias("_quarantine_timestamp")
    )
)

quarantine_query = (
    corrupt_records_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", QUARANTINE_CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start(QUARANTINE_DELTA_PATH)
)

# ------------------------------------------------------------------------------
# 5. Clean, Enrich, and Bounded Stateful Deduplication
# ------------------------------------------------------------------------------
valid_records_df = (
    parsed_stream
    .filter(col("parsed.event_id").isNotNull() & col("parsed.timestamp").isNotNull())
    .select(
        col("parsed.event_id").alias("event_id"),
        col("parsed.user_id").alias("user_id"),
        col("parsed.username").alias("username"),
        col("parsed.user_followers").alias("user_followers"),
        col("parsed.is_verified").alias("is_verified"),
        col("parsed.text").alias("text"),
        col("parsed.primary_hashtag").alias("primary_hashtag"),
        col("parsed.hashtags").alias("hashtags"),
        col("parsed.sentiment_score").alias("sentiment_score"),
        col("parsed.likes").alias("likes"),
        col("parsed.retweets").alias("retweets"),
        col("parsed.location").alias("location"),
        col("parsed.device").alias("device"),
        to_timestamp(col("parsed.timestamp")).alias("event_timestamp"),
        col("_ingest_timestamp")
    )
    .withColumn(
        "sentiment_label",
        when(col("sentiment_score") > 0.2, "POSITIVE")
        .when(col("sentiment_score") < -0.2, "NEGATIVE")
        .otherwise("NEUTRAL")
    )
    .withColumn(
        "engagement_score",
        col("likes") + (col("retweets") * 2)
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

# CRITICAL FIX: Stateful deduplication MUST have a watermark on the event time
# column applied to the exact same DataFrame, and the event time column MUST be
# included in the dropDuplicates subset. Without this, the RocksDB/HDFS state store
# retains every event_id indefinitely until the cluster runs out of memory.
silver_deduped_df = (
    valid_records_df
    .withWatermark("event_timestamp", "10 minutes")
    .dropDuplicates(["event_id", "event_timestamp"])
)

silver_query = (
    silver_deduped_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start(SILVER_DELTA_PATH)
)

print(f"Silver stream started: {silver_query.id}")
print(f"Quarantine DLQ stream started: {quarantine_query.id}")
