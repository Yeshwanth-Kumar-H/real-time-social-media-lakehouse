# Databricks Notebook: 02_silver_transformations
# ==============================================================================
# Phase 4 & 5: Silver Layer Streaming Transformation
# Reads raw events from Bronze Delta Lake, enforces schema validation,
# filters corrupt records, handles deduplication, and enriches data
# into the cleansed Silver Layer.
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, BooleanType, ArrayType, TimestampType
)
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, current_timestamp
)

spark = SparkSession.builder.appName("SocialMedia-Silver-Transformations").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Pipeline Paths
S3_BUCKET = spark.conf.get("pipeline.s3.bucket", "s3://social-media-lakehouse")
BRONZE_DELTA_PATH = f"{S3_BUCKET}/bronze/social_media_raw"
SILVER_DELTA_PATH = f"{S3_BUCKET}/silver/social_media_posts"
SILVER_CHECKPOINT_PATH = f"{S3_BUCKET}/checkpoints/silver_transform"

# 2. Explicit Schema Definition for Raw JSON Payloads
# Prevents schema drift and avoids expensive automatic schema inference
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

# 3. Read Stream from Bronze Delta Table
bronze_stream = (
    spark.readStream
    .format("delta")
    .load(BRONZE_DELTA_PATH)
)

# 4. Parse JSON & Validate Records
parsed_stream = (
    bronze_stream
    .withColumn("parsed", from_json(col("raw_payload"), social_post_schema))
    # Filter out corrupted records where JSON parsing yielded nulls
    .filter(col("parsed.event_id").isNotNull())
)

# 5. Flatten, Clean, and Feature Enrich
silver_df = (
    parsed_stream.select(
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
    # Business Feature: Sentiment Classification
    .withColumn(
        "sentiment_label",
        when(col("sentiment_score") > 0.2, "POSITIVE")
        .when(col("sentiment_score") < -0.2, "NEGATIVE")
        .otherwise("NEUTRAL")
    )
    # Business Feature: Weighted Engagement Score
    .withColumn(
        "engagement_score",
        col("likes") + (col("retweets") * 2)
    )
    .withColumn("_silver_processed_at", current_timestamp())
)

# 6. Stream Write into Silver Delta Lake with Deduplication
# In Delta Lake streaming, dropDuplicates applies stateful deduplication
silver_deduped_df = silver_df.dropDuplicates(["event_id"])

silver_query = (
    silver_deduped_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start(SILVER_DELTA_PATH)
)

print(f"Silver stream active: {silver_query.id}")
