-- ==============================================================================
-- Databricks SQL Dashboard Queries: Social Media Intelligence Lakehouse
-- Powers the Real-Time Executive & Operational Lakehouse Visualizations
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 0. Register Delta Tables in Databricks Metastore / Unity Catalog
-- ------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS social_media_lakehouse;
USE social_media_lakehouse;

CREATE TABLE IF NOT EXISTS bronze_social_media
USING DELTA
LOCATION 's3://social-media-lakehouse/bronze/social_media_raw';

CREATE TABLE IF NOT EXISTS silver_social_media
USING DELTA
LOCATION 's3://social-media-lakehouse/silver/social_media_posts';

CREATE TABLE IF NOT EXISTS gold_trending_hashtags
USING DELTA
LOCATION 's3://social-media-lakehouse/gold/trending_hashtags';

CREATE TABLE IF NOT EXISTS gold_active_users
USING DELTA
LOCATION 's3://social-media-lakehouse/gold/active_users';


-- ------------------------------------------------------------------------------
-- Visual 1: Top 10 Trending Hashtags (Bar Chart)
-- Matches the dashboard shown in your target deliverable screenshot
-- ------------------------------------------------------------------------------
SELECT 
    hashtag,
    SUM(post_count) AS total_posts,
    ROUND(AVG(avg_sentiment), 2) AS sentiment_score,
    SUM(total_engagement) AS engagement_score
FROM gold_trending_hashtags
WHERE window_start >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY hashtag
ORDER BY total_posts DESC
LIMIT 10;


-- ------------------------------------------------------------------------------
-- Visual 2: Top 10 Most Active Users (Bar Chart)
-- Shows top contributing users and their reach
-- ------------------------------------------------------------------------------
SELECT 
    username,
    SUM(posts_published) AS post_count,
    SUM(total_likes) AS total_likes_received,
    SUM(total_retweets) AS total_retweets_received
FROM gold_active_users
WHERE window_start >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY username
ORDER BY post_count DESC
LIMIT 10;


-- ------------------------------------------------------------------------------
-- Visual 3: Sentiment Distribution (Donut Chart)
-- Displays breakdown of positive, neutral, and negative sentiment
-- ------------------------------------------------------------------------------
SELECT 
    sentiment_label,
    COUNT(*) AS post_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS percentage
FROM silver_social_media
WHERE event_timestamp >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY sentiment_label;


-- ------------------------------------------------------------------------------
-- Visual 4: Executive KPI Counter Cards
-- ------------------------------------------------------------------------------
SELECT 
    COUNT(DISTINCT event_id) AS total_events_processed,
    COUNT(DISTINCT username) AS unique_active_users,
    ROUND(AVG(sentiment_score), 3) AS overall_platform_sentiment
FROM silver_social_media
WHERE event_timestamp >= current_timestamp() - INTERVAL 1 HOUR;
