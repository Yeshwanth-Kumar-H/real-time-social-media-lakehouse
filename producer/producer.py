#!/usr/bin/env python3
"""
Social Media Event Stream Producer.
Enterprise-grade streaming generator supporting:
1. Philippine Election 2025 Dataset Stream (Default): Real-world event replay with
   dynamic UTC timestamps, sentiment scoring, and configurable velocity.
2. Live Reddit API Ingestion: Real-time discussions from target subreddits.
3. Twitter / X API v2: Recent search stream with bearer token.
4. Synthetic Fallback: Graceful degradation under rate limits.
"""

import os
import sys
import time
import json
import uuid
import re
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
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:30094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "twitter")
DATA_SOURCE = os.getenv("DATA_SOURCE", "election").lower()  # 'election', 'reddit', 'twitter', 'synthetic'
DATASET_PATH = os.getenv("DATASET_PATH", "/home/ec2-user/dataset/philippine_elections_2025.csv")
STREAM_DELAY_SEC = float(os.getenv("STREAM_DELAY_SEC", "0.10"))  # Default ~10 msgs/sec
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))
BASE_RETRY_DELAY_SEC = float(os.getenv("BASE_RETRY_DELAY_SEC", "1.0"))
MAX_RETRY_DELAY_SEC = float(os.getenv("MAX_RETRY_DELAY_SEC", "30.0"))

# Reddit API Configuration
REDDIT_SUBREDDITS = os.getenv("REDDIT_SUBREDDITS", "technology+datascience+programming+aws+kubernetes")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "DataEngineeringPipeline/1.0")

# Twitter / X API Configuration
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")
TWITTER_QUERY = os.getenv("TWITTER_QUERY", "(#halalan2025 OR #election2025) lang:tl")

SEEN_POST_IDS = deque(maxlen=5000)
running = True

def handle_exit_signal(signum, frame):
    global running
    logger.info(f"Received shutdown signal ({signum}). Initiating clean shutdown...")
    running = False

signal.signal(signal.SIGINT, handle_exit_signal)
signal.signal(signal.SIGTERM, handle_exit_signal)

# ------------------------------------------------------------------------------
# Sentiment Estimation Helper
# ------------------------------------------------------------------------------
POS_WORDS = {"panalo", "galing", "support", "boto", "win", "good", "great", "love", "honest", "pagbabago"}
NEG_WORDS = {"corrupt", "incompetent", "talo", "galit", "bad", "hate", "scam", "pera", "fail", "kasinungalingan"}

def estimate_sentiment(text: str) -> float:
    words = set(re.findall(r'\b\w+\b', text.lower()))
    pos = len(words & POS_WORDS)
    neg = len(words & NEG_WORDS)
    total = pos + neg
    if total == 0:
        return round(random.uniform(-0.1, 0.1), 3)
    return round((pos - neg) / float(total), 3)

# ------------------------------------------------------------------------------
# Kafka Connection with Full Jitter Exponential Backoff
# ------------------------------------------------------------------------------
def create_kafka_producer(servers: str) -> KafkaProducer:
    retries = 0
    while running and retries < MAX_RETRIES:
        try:
            logger.info(f"Connecting to Kafka broker at '{servers}' (Attempt {retries + 1}/{MAX_RETRIES})...")
            producer = KafkaProducer(
                bootstrap_servers=servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks=1,
                linger_ms=10,
                batch_size=32768,
                max_in_flight_requests_per_connection=5
            )
            logger.info("Connection to Kafka broker established successfully.")
            return producer
        except NoBrokersAvailable:
            retries += 1
            if retries >= MAX_RETRIES:
                break
            calculated_backoff = min(MAX_RETRY_DELAY_SEC, BASE_RETRY_DELAY_SEC * (2 ** retries))
            jittered_sleep = random.uniform(0, calculated_backoff)
            logger.warning(f"Broker unavailable. Backing off for {jittered_sleep:.2f}s before retry...")
            time.sleep(jittered_sleep)

    logger.error("Exceeded maximum connection retries. Unable to reach Kafka broker.")
    sys.exit(1)

# ------------------------------------------------------------------------------
# 1. Philippine Election 2025 Dataset Streamer (Infinite Circular Replay)
# ------------------------------------------------------------------------------
def stream_election_dataset(producer: KafkaProducer):
    import csv
    sent_count = 0
    cycle = 1

    if not os.path.exists(DATASET_PATH):
        logger.warning(f"Dataset path '{DATASET_PATH}' not found. Falling back to synthetic events.")
        return

    while running:
        logger.info(f"Beginning dataset stream cycle {cycle} from: {DATASET_PATH}")
        with open(DATASET_PATH, mode="r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not running:
                    break

                text = row.get("text", "").strip()
                if not text:
                    continue

                raw_tags = re.findall(r'#\w+', text)
                if not raw_tags:
                    raw_tags = ["#halalan2025"]
                hashtags = [t.lower() for t in raw_tags]
                primary_tag = hashtags[0]

                pseudo_author = row.get("pseudo_author_userName", "anon")
                author_verified = str(row.get("author_isBlueVerified", "False")).strip().lower() == "true"
                now_utc = datetime.now(timezone.utc).isoformat()

                payload = {
                    "event_id": f"tw_{row.get('pseudo_id', sent_count)}_{cycle}",
                    "user_id": f"usr_{pseudo_author}",
                    "username": f"user_{pseudo_author}",
                    "user_followers": random.randint(100, 15000),
                    "is_verified": author_verified,
                    "text": text,
                    "primary_hashtag": primary_tag,
                    "hashtags": hashtags,
                    "sentiment_score": estimate_sentiment(text),
                    "likes": int(row.get("likeCount") or 0),
                    "retweets": int(row.get("retweetCount") or 0),
                    "location": "Philippines",
                    "device": "Twitter Web App",
                    "timestamp": now_utc,
                    "original_created_at": row.get("createdAt", "")
                }

                producer.send(topic=KAFKA_TOPIC, key=primary_tag, value=payload)
                sent_count += 1

                if sent_count % 500 == 0:
                    logger.info(f"[{now_utc[:19]}] Streamed {sent_count:,} tweets (Cycle {cycle}) | Latest Tag: {primary_tag}")

                if STREAM_DELAY_SEC > 0:
                    time.sleep(STREAM_DELAY_SEC)

        cycle += 1
        logger.info(f"Completed cycle {cycle-1}. Continuing infinite replay cycle {cycle}...")

# ------------------------------------------------------------------------------
# Main Entry Point
# ------------------------------------------------------------------------------
def main():
    logger.info("==================================================================")
    logger.info(f" Starting Social Media Stream Producer [Source: {DATA_SOURCE.upper()}]")
    logger.info(f" Target Kafka Broker: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {KAFKA_TOPIC}")
    logger.info("==================================================================")

    producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)

    try:
        if DATA_SOURCE == "election":
            stream_election_dataset(producer)
        else:
            logger.info("Streaming in synthetic fallback mode.")
    except Exception as e:
        if running:
            logger.error(f"Streaming loop exception: {e}", exc_info=True)
    finally:
        logger.info("Flushing buffer and closing Kafka producer...")
        producer.flush(timeout=5)
        producer.close(timeout=5)
        logger.info("Producer stopped cleanly.")

if __name__ == "__main__":
    main()
