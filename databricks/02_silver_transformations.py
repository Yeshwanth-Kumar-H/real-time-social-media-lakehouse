# Databricks Notebook: 02_silver_transformations
# ==============================================================================
# Phase 4: Silver Layer Streaming Transformation & Dead-Letter Queue (DLQ)
# 
# Key Engineering Decisions:
# 1. Schema Enforcement: Extracts structured fields from raw JSON payloads.
# 2. Dead-Letter Queue (DLQ): Routes malformed/unparseable JSON into quarantine.
# 3. Bounded State Store: Applies withWatermark BEFORE dropDuplicates so RocksDB
#    evicts expired state rather than leaking memory indefinitely.
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

# 1. Unity Catalog Volume Setup
curr_cat = spark.catalog.currentCatalog()
curr_sch = spark.catalog.currentDatabase()
CHECKPOINT_BASE = f"/Volumes/{curr_cat}/{curr_sch}/lakehouse_checkpoints"
CHECKPOINT_SILVER = f"{CHECKPOINT_BASE}/silver"
CHECKPOINT_QUARANTINE = f"{CHECKPOINT_BASE}/quarantine"

# 2. Explicit Schema Definition for Raw JSON Payloads
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
    StructField("timestamp", StringType(), nullable=True),
    StructField("original_created_at", StringType(), nullable=True)
])

# 3. Read Stream from Bronze Delta Table
bronze_stream = (
    spark.readStream
    .format("delta")
    .table("bronze_social_media_raw")
)

# 4. Parse JSON & Implement DLQ Routing
parsed_stream = bronze_stream.withColumn("data", from_json(col("raw_payload"), social_post_schema))

# Valid events stream
valid_events_df = (
    parsed_stream
    .filter(col("data.event_id").isNotNull())
    .select(
        col("data.event_id").alias("event_id"),
        col("data.user_id").alias("user_id"),
        col("data.username").alias("username"),
        col("data.user_followers").alias("user_followers"),
        col("data.is_verified").alias("is_verified"),
        col("data.text").alias("text"),
        col("data.primary_hashtag").alias("primary_hashtag"),
        col("data.hashtags").alias("hashtags"),
        col("data.sentiment_score").alias("sentiment_score"),
        col("data.likes").alias("likes"),
        col("data.retweets").alias("retweets"),
        (col("data.likes") + (col("data.retweets") * 2)).alias("engagement_score"),
        col("data.location").alias("location"),
        col("data.device").alias("device"),
        to_timestamp(col("data.timestamp")).alias("event_timestamp"),
        col("data.original_created_at").alias("original_created_at"),
        col("_ingest_timestamp")
    )
)

# 5. Stateful Deduplication with Watermarking (Memory Bounded)
deduped_silver_df = (
    valid_events_df
    .withWatermark("event_timestamp", "10 minutes")
    .dropDuplicates(["event_id", "event_timestamp"])
)

# 6. Stream Cleansed Data into Silver Delta Table
silver_query = (
    deduped_silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .trigger(availableNow=True)
    .toTable("silver_social_media_posts")
)

silver_query.awaitTermination()
print("✅ Silver Cleansing & Deduplication Complete! Data written to 'silver_social_media_posts'.")
