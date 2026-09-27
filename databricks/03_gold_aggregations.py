# Databricks Notebook: 03_gold_aggregations
# ==============================================================================
# Phase 4 & 5: Gold Layer Streaming & Business Aggregations
# Reads cleansed events from Silver Delta Lake, computes business-ready
# aggregations for Top Trending Hashtags, Engagement Velocity, and Influencer Activity.
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, count, avg, sum as _sum, round as _round
)

spark = SparkSession.builder.appName("SocialMedia-Gold-Aggregations").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Read from Cleansed Silver Delta Table
silver_df = spark.table("silver_social_media_posts")

# 2. Gold Table 1: Top Trending Hashtags & Sentiment Analysis
# Explode hashtags array so each tag is counted individually
gold_hashtags_df = (
    silver_df
    .select(
        explode(col("hashtags")).alias("hashtag"),
        col("sentiment_score"),
        col("engagement_score")
    )
    .groupBy("hashtag")
    .agg(
        count("hashtag").alias("total_posts"),
        _round(avg("sentiment_score"), 3).alias("avg_sentiment"),
        _sum("engagement_score").alias("total_engagement")
    )
    .orderBy(col("total_posts").desc())
)

# Persist to Gold Delta Table
gold_hashtags_df.write.format("delta").mode("overwrite").saveAsTable("gold_trending_hashtags")
print("✅ Gold Table 1 'gold_trending_hashtags' Created / Updated!")

# 3. Gold Table 2: Most Active Users & Influencers
gold_users_df = (
    silver_df
    .groupBy("username")
    .agg(
        count("event_id").alias("total_tweets"),
        _sum("likes").alias("total_likes"),
        _sum("retweets").alias("total_retweets"),
        _sum("engagement_score").alias("total_engagement"),
        _round(avg("sentiment_score"), 3).alias("user_avg_sentiment")
    )
    .orderBy(col("total_tweets").desc())
)

# Persist to Gold Delta Table
gold_users_df.write.format("delta").mode("overwrite").saveAsTable("gold_active_users")
print("✅ Gold Table 2 'gold_active_users' Created / Updated!")
