#!/usr/bin/env python3
"""
Social Media Event Stream Producer.
Ingests real-time live posts from Reddit / Twitter APIs, transforms them into
standardized social intelligence event payloads, and publishes to Apache Kafka.

Supports:
1. Reddit API (Default): Live streaming from subreddits (r/technology, r/datascience, r/aws, etc.)
2. Twitter/X API v2: Streaming via Recent Search endpoint with TWITTER_BEARER_TOKEN
3. Synthetic Fallback: Graceful degradation if external APIs return HTTP 429 rate limits
"""

import os
import sys
import time
import json
import uuid
import random
import signal
import logging
from datetime import datetime, timezone
from collections import deque
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Producer] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Configuration (Environment Variables)
# ------------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-service:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "social-media-posts")
DATA_SOURCE = os.getenv("DATA_SOURCE", "reddit").lower() # 'reddit', 'twitter', or 'mock'
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "3.0"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))
BASE_RETRY_DELAY_SEC = float(os.getenv("BASE_RETRY_DELAY_SEC", "1.0"))
MAX_RETRY_DELAY_SEC = float(os.getenv("MAX_RETRY_DELAY_SEC", "30.0"))

# Reddit API Configuration
REDDIT_SUBREDDITS = os.getenv("REDDIT_SUBREDDITS", "technology+datascience+programming+aws+kubernetes+artificial")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "DataEngineeringPipeline/1.0 (by /u/pipeline_bot)")

# Twitter / X API Configuration (Optional)
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")
TWITTER_QUERY = os.getenv("TWITTER_QUERY", "(#AI OR #Cloud OR #DataEngineering OR #AWS) -is:retweet lang:en")

# ------------------------------------------------------------------------------
# In-Memory Deduplication to Avoid Re-Emitting Seen Posts
# ------------------------------------------------------------------------------
SEEN_POST_IDS = deque(maxlen=2000)

# ------------------------------------------------------------------------------
# Graceful Shutdown Handler
# ------------------------------------------------------------------------------
running = True

def handle_exit_signal(signum, frame):
    global running
    logger.info(f"Received shutdown signal ({signum}). Initiating clean shutdown...")
    running = False

signal.signal(signal.SIGINT, handle_exit_signal)
signal.signal(signal.SIGTERM, handle_exit_signal)

# ------------------------------------------------------------------------------
# Kafka Connection with True Full Jitter Exponential Backoff
# ------------------------------------------------------------------------------
def create_kafka_producer(servers: str) -> KafkaProducer:
    """
    Connects to Kafka broker using exponential backoff with full jitter:
    sleep = uniform(0, min(MAX_DELAY, BASE_DELAY * 2 ** attempt))
    """
    retries = 0
    while running and retries < MAX_RETRIES:
        try:
            logger.info(f"Connecting to Kafka broker at '{servers}' (Attempt {retries + 1}/{MAX_RETRIES})...")
            producer = KafkaProducer(
                bootstrap_servers=servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info("Connection to Kafka broker established successfully.")
            return producer
        except NoBrokersAvailable:
            retries += 1
            if retries >= MAX_RETRIES:
                break
            calculated_backoff = min(MAX_RETRY_DELAY_SEC, BASE_RETRY_DELAY_SEC * (2 ** retries))
            jittered_sleep = random.uniform(0, calculated_backoff)
            logger.warning(f"Broker unavailable. Backing off for {jittered_sleep:.2f}s before retry {retries + 1}...")
            time.sleep(jittered_sleep)

    logger.error("Exceeded maximum connection retries. Unable to reach Kafka broker.")
    sys.exit(1)

# ------------------------------------------------------------------------------
# Sentiment Estimation Helper
# ------------------------------------------------------------------------------
POSITIVE_WORDS = {"great", "good", "amazing", "excellent", "love", "awesome", "fast", "powerful", "breakthrough", "success", "innovative"}
NEGATIVE_WORDS = {"bad", "terrible", "issue", "bug", "crash", "slow", "fail", "broken", "hate", "error", "drop", "down"}

def estimate_sentiment(text: str) -> float:
    """Simple, fast lexical sentiment heuristic returning score between -1.0 and +1.0."""
    words = set(text.lower().split())
    pos_count = len(words & POSITIVE_WORDS)
    neg_count = len(words & NEGATIVE_WORDS)
    total = pos_count + neg_count
    if total == 0:
        return round(random.uniform(-0.1, 0.1), 3)
    score = (pos_count - neg_count) / float(total)
    return round(score, 3)

# ------------------------------------------------------------------------------
# 1. Live Reddit API Ingest
# ------------------------------------------------------------------------------
def fetch_live_reddit_posts() -> list:
    """Fetches real live new posts from target subreddits using Reddit's JSON feed."""
    url = f"https://www.reddit.com/r/{REDDIT_SUBREDDITS}/new.json?limit=25"
    headers = {"User-Agent": REDDIT_USER_AGENT}

    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            data = response.json()
            children = data.get("data", {}).get("children", [])
            new_events = []

            for item in children:
                post = item.get("data", {})
                post_id = post.get("id")
                if not post_id or post_id in SEEN_POST_IDS:
                    continue

                SEEN_POST_IDS.append(post_id)
                subreddit = post.get("subreddit", "general")
                primary_tag = f"#{subreddit.lower()}"
                title = post.get("title", "")
                selftext = post.get("selftext", "")
                full_text = f"{title} {selftext[:150]}".strip()

                # Extract any inline hashtags or keywords
                hashtags = [word.lower() for word in full_text.split() if word.startswith("#")]
                if primary_tag.lower() not in hashtags:
                    hashtags.append(primary_tag.lower())

                # Convert Reddit created timestamp to ISO UTC string
                created_utc = post.get("created_utc", time.time())
                iso_timestamp = datetime.fromtimestamp(created_utc, timezone.utc).isoformat()

                event = {
                    "event_id": f"reddit_{post_id}",
                    "user_id": f"u_{post.get('author', 'anonymous')}",
                    "username": post.get("author", "anonymous"),
                    "user_followers": random.randint(10, 5000), # Reddit hides exact follower counts
                    "is_verified": False,
                    "text": title,
                    "primary_hashtag": primary_tag,
                    "hashtags": hashtags,
                    "sentiment_score": estimate_sentiment(full_text),
                    "likes": post.get("score", 0),
                    "retweets": post.get("num_comments", 0),
                    "location": f"r/{subreddit}",
                    "device": "Reddit API",
                    "timestamp": iso_timestamp
                }
                new_events.append(event)

            return new_events
        elif response.status_code == 429:
            logger.warning("Reddit API HTTP 429: Rate limited. Cooling down...")
            time.sleep(5)
            return []
        else:
            logger.warning(f"Reddit API returned HTTP {response.status_code}")
            return []
    except Exception as err:
        logger.error(f"Error fetching from Reddit API: {err}")
        return []

# ------------------------------------------------------------------------------
# 2. Live Twitter / X API v2 Ingest (Optional with Token)
# ------------------------------------------------------------------------------
def fetch_live_twitter_posts() -> list:
    """Fetches real live tweets using Twitter API v2 Recent Search endpoint."""
    if not TWITTER_BEARER_TOKEN:
        logger.error("TWITTER_BEARER_TOKEN not set. Falling back to Reddit.")
        return fetch_live_reddit_posts()

    url = "https://api.twitter.com/2/tweets/search/recent"
    headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
    params = {
        "query": TWITTER_QUERY,
        "max_results": 10,
        "tweet.fields": "created_at,public_metrics,author_id,entities"
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=5)
        if response.status_code == 200:
            tweets = response.json().get("data", [])
            new_events = []
            for tweet in tweets:
                tweet_id = tweet.get("id")
                if not tweet_id or tweet_id in SEEN_POST_IDS:
                    continue
                SEEN_POST_IDS.append(tweet_id)
                metrics = tweet.get("public_metrics", {})
                text = tweet.get("text", "")
                hashtags = [h.get("tag") for h in tweet.get("entities", {}).get("hashtags", [])]
                primary_tag = f"#{hashtags[0]}" if hashtags else "#tech"

                event = {
                    "event_id": f"tw_{tweet_id}",
                    "user_id": f"usr_{tweet.get('author_id')}",
                    "username": f"user_{tweet.get('author_id', 'unknown')[:6]}",
                    "user_followers": random.randint(100, 20000),
                    "is_verified": False,
                    "text": text,
                    "primary_hashtag": primary_tag,
                    "hashtags": [f"#{t}" if not t.startswith("#") else t for t in hashtags],
                    "sentiment_score": estimate_sentiment(text),
                    "likes": metrics.get("like_count", 0),
                    "retweets": metrics.get("retweet_count", 0),
                    "location": "Global",
                    "device": "Twitter Web App",
                    "timestamp": tweet.get("created_at", datetime.now(timezone.utc).isoformat())
                }
                new_events.append(event)
            return new_events
        else:
            logger.warning(f"Twitter API HTTP {response.status_code}. Falling back to Reddit.")
            return fetch_live_reddit_posts()
    except Exception as err:
        logger.error(f"Error calling Twitter API: {err}")
        return fetch_live_reddit_posts()

# ------------------------------------------------------------------------------
# 3. Synthetic Event Generator (Fallback)
# ------------------------------------------------------------------------------
def generate_synthetic_fallback() -> list:
    """Generates synthetic posts if live APIs are unreachable or offline."""
    tags = ["#ai", "#cloud", "#databricks", "#kubernetes", "#python", "#dataengineering"]
    chosen_tag = random.choice(tags)
    return [{
        "event_id": f"mock_{uuid.uuid4().hex[:8]}",
        "user_id": f"usr_{random.randint(100, 999)}",
        "username": f"dev_{random.choice(['alex', 'sam', 'jordan', 'taylor'])}",
        "user_followers": random.randint(50, 10000),
        "is_verified": False,
        "text": f"Discussing streaming architectures and real-time pipelines with {chosen_tag}.",
        "primary_hashtag": chosen_tag,
        "hashtags": [chosen_tag, "#tech"],
        "sentiment_score": round(random.uniform(-0.5, 0.8), 3),
        "likes": random.randint(0, 100),
        "retweets": random.randint(0, 25),
        "location": "Synthetic Test",
        "device": "Local Engine",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }]

# ------------------------------------------------------------------------------
# Main Producer Ingestion Loop
# ------------------------------------------------------------------------------
def main():
    logger.info("==================================================================")
    logger.info(f" Starting Social Media Stream Producer [Source: {DATA_SOURCE.upper()}]")
    logger.info(f" Target Kafka Broker: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {KAFKA_TOPIC}")
    logger.info("==================================================================")

    producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    messages_sent = 0

    try:
        while running:
            # 1. Ingest real events from requested data source
            if DATA_SOURCE == "reddit":
                events = fetch_live_reddit_posts()
            elif DATA_SOURCE == "twitter":
                events = fetch_live_twitter_posts()
            else:
                events = generate_synthetic_fallback()

            # If no new posts from API, fallback to 1 synthetic post to keep stream active
            if not events:
                events = generate_synthetic_fallback()

            # 2. Publish events to Kafka topic
            for post in events:
                if not running:
                    break

                partition_key = post["primary_hashtag"]
                future = producer.send(
                    topic=KAFKA_TOPIC,
                    key=partition_key,
                    value=post
                )

                record_metadata = future.get(timeout=10)
                messages_sent += 1

                logger.info(
                    f"[#{messages_sent}] [{post['device']}] Partition: {record_metadata.partition} "
                    f"Offset: {record_metadata.offset} | Key: {partition_key} | User: @{post['username']} | Text: {post['text'][:60]}..."
                )

            # Heartbeat touchfile for Kubernetes livenessProbe
            with open("/tmp/producer_heartbeat", "w") as f:
                f.write(str(time.time()))

            time.sleep(POLL_INTERVAL_SEC)

    except Exception as e:
        if running:
            logger.error(f"Streaming loop exception: {e}", exc_info=True)
    finally:
        logger.info("Flushing buffer and closing Kafka producer...")
        producer.flush(timeout=5)
        producer.close(timeout=5)
        logger.info(f"Producer stopped cleanly. Total live posts published: {messages_sent}")

if __name__ == "__main__":
    main()
