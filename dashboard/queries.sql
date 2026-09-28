-- ==============================================================================
-- Databricks SQL Dashboard Queries: Social Media Intelligence Lakehouse
-- Powers the Real-Time Executive & Operational Lakehouse Visualizations
-- Compatible with Unity Catalog Delta Tables:
--   - bronze_social_media_raw
--   - silver_social_media_posts
--   - gold_trending_hashtags
--   - gold_active_users
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Visual 1: Top 10 Trending Hashtags (Horizontal or Vertical Bar Chart)
-- Shows top discussion topics by post volume, average sentiment, and total engagement
-- ------------------------------------------------------------------------------
SELECT 
    hashtag,
    total_posts,
    avg_sentiment,
    total_engagement
FROM gold_trending_hashtags
ORDER BY total_posts DESC
LIMIT 10;


-- ------------------------------------------------------------------------------
-- Visual 2: Top 10 Most Active Users / Accounts (Bar Chart)
-- Highlights key conversational accounts, post activity, and engagement reach
-- ------------------------------------------------------------------------------
SELECT 
    username,
    total_tweets,
    total_likes,
    total_retweets,
    total_engagement,
    user_avg_sentiment
FROM gold_active_users
ORDER BY total_tweets DESC
LIMIT 10;


-- ------------------------------------------------------------------------------
-- Visual 3: Sentiment Distribution (Donut / Pie Chart)
-- Aggregates distribution of Positive, Neutral, and Negative posts
-- ------------------------------------------------------------------------------
SELECT 
    sentiment_label,
    COUNT(*) AS post_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS percentage
FROM silver_social_media_posts
GROUP BY sentiment_label
ORDER BY post_count DESC;


-- ------------------------------------------------------------------------------
-- Visual 4: Executive KPI Counter Cards
-- Surfaces platform-wide high-level operational metrics
-- ------------------------------------------------------------------------------
SELECT 
    COUNT(DISTINCT event_id) AS total_events_processed,
    COUNT(DISTINCT username) AS unique_active_users,
    ROUND(AVG(sentiment_score), 3) AS overall_platform_sentiment,
    SUM(engagement_score) AS total_community_engagement
FROM silver_social_media_posts;


-- ------------------------------------------------------------------------------
-- Visual 5: Device Breakdown (Bar / Pie Chart)
-- Distribution of devices used to publish posts
-- ------------------------------------------------------------------------------
SELECT 
    device,
    COUNT(*) AS post_count
FROM silver_social_media_posts
GROUP BY device
ORDER BY post_count DESC;


-- ------------------------------------------------------------------------------
-- Visual 6: Top Hashtag KPI Counter Card
-- Surfaces the single leading hashtag by post count (#halalan2025)
-- ------------------------------------------------------------------------------
SELECT 
    hashtag AS top_hashtag
FROM gold_trending_hashtags
ORDER BY total_posts DESC
LIMIT 1;
