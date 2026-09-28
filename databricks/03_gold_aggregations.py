# Databricks Notebook: 03_gold_aggregations
# ==============================================================================
# Phase 4 & 5: Gold Layer Streaming & Business Aggregations
# Reads cleansed events from Silver Delta Lake, computes business-ready
# aggregations for Top Trending Hashtags, Engagement Velocity, and Influencer Activity.
# ==============================================================================

# COMMAND ----------
# 1. Environment & Read from Cleansed Silver Delta Table
from pyspark.sql.functions import (
    col, explode, count, avg, sum as _sum, round as _round
)

silver_df = spark.table("silver_social_media_posts")
print(f"📊 Reading from 'silver_social_media_posts' ({silver_df.count():,} rows)")

# COMMAND ----------
# 2. Gold Table 1: Top Trending Hashtags & Sentiment
# Explode hashtags array so each individual tag is aggregated
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

# COMMAND ----------
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

# COMMAND ----------
# 4. Preview Gold Trending Hashtags
display(spark.table("gold_trending_hashtags").limit(10))

# COMMAND ----------
# 5. Preview Gold Active Users
display(spark.table("gold_active_users").limit(10))
