# Databricks Notebook: 03_gold_aggregations
# ==============================================================================
# Phase 4 & 5: Gold Layer Streaming Aggregations
# Reads cleansed events from Silver Delta Lake, applies event-time watermarking,
# computes sliding window aggregations for top trending hashtags and active users,
# and persists business-ready Gold Delta tables for the BI Dashboard.
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, window, count, avg, sum as _sum, round as _round, current_timestamp
)

spark = SparkSession.builder.appName("SocialMedia-Gold-Aggregations").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Pipeline Paths
S3_BUCKET = spark.conf.get("pipeline.s3.bucket", "s3://social-media-lakehouse")
SILVER_DELTA_PATH = f"{S3_BUCKET}/silver/social_media_posts"

GOLD_HASHTAGS_PATH = f"{S3_BUCKET}/gold/trending_hashtags"
GOLD_USERS_PATH = f"{S3_BUCKET}/gold/active_users"

CHECKPOINT_HASHTAGS = f"{S3_BUCKET}/checkpoints/gold_hashtags"
CHECKPOINT_USERS = f"{S3_BUCKET}/checkpoints/gold_users"

# 2. Read Stream from Silver Layer
silver_stream = (
    spark.readStream
    .format("delta")
    .load(SILVER_DELTA_PATH)
)

# 3. Stream 1: Top Trending Hashtags (Windowed Aggregations with Watermark)
# Event-time watermark allows handling late-arriving data up to 10 minutes late
# Explode hashtags array so each tag is counted individually
exploded_hashtags_df = (
    silver_stream
    .withWatermark("event_timestamp", "10 minutes")
    .select(
        explode(col("hashtags")).alias("hashtag"),
        col("sentiment_score"),
        col("engagement_score"),
        col("event_timestamp")
    )
    # 5-minute window sliding every 1 minute
    .groupBy(
        window(col("event_timestamp"), "5 minutes", "1 minute"),
        col("hashtag")
    )
    .agg(
        count("hashtag").alias("post_count"),
        _round(avg("sentiment_score"), 3).alias("avg_sentiment"),
        _sum("engagement_score").alias("total_engagement")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("hashtag"),
        col("post_count"),
        col("avg_sentiment"),
        col("total_engagement"),
        current_timestamp().alias("_updated_at")
    )
)

# Write Gold Hashtags Delta Table
# In Delta Lake streaming aggregations with watermarks, outputMode='append' writes finalized windows
query_hashtags = (
    exploded_hashtags_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_HASHTAGS)
    .trigger(processingTime="10 seconds")
    .start(GOLD_HASHTAGS_PATH)
)

# 4. Stream 2: Most Active Users (Windowed Aggregations with Watermark)
user_activity_df = (
    silver_stream
    .withWatermark("event_timestamp", "10 minutes")
    .groupBy(
        window(col("event_timestamp"), "5 minutes", "1 minute"),
        col("user_id"),
        col("username")
    )
    .agg(
        count("event_id").alias("posts_published"),
        _sum("likes").alias("total_likes"),
        _sum("retweets").alias("total_retweets"),
        _round(avg("sentiment_score"), 3).alias("user_avg_sentiment")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("user_id"),
        col("username"),
        col("posts_published"),
        col("total_likes"),
        col("total_retweets"),
        col("user_avg_sentiment"),
        current_timestamp().alias("_updated_at")
    )
)

# Write Gold Users Delta Table
query_users = (
    user_activity_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_USERS)
    .trigger(processingTime="10 seconds")
    .start(GOLD_USERS_PATH)
)

print(f"Gold Trending Hashtags Query active: {query_hashtags.id}")
print(f"Gold Active Users Query active: {query_users.id}")
