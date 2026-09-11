#!/usr/bin/env python3
"""
Social Media Event Stream Generator.
Simulates social media posts with sentiment scores, engagement metrics,
hashtags, and UTC timestamps, publishing to an Apache Kafka streaming topic.
Implements true exponential backoff with jitter and graceful shutdown handling.
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
# Configuration (Environment Variables with Defaults)
# ------------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-service:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "social-media-posts")
MESSAGE_INTERVAL_SEC = float(os.getenv("MESSAGE_INTERVAL_SEC", "1.5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))
BASE_RETRY_DELAY_SEC = float(os.getenv("BASE_RETRY_DELAY_SEC", "1.0"))
MAX_RETRY_DELAY_SEC = float(os.getenv("MAX_RETRY_DELAY_SEC", "30.0"))

# ------------------------------------------------------------------------------
# Mock Data Pools for Simulation
# ------------------------------------------------------------------------------
USERS = [
    {"user_id": "usr_101", "username": "sarah_tech", "verified": True, "followers": 12450},
    {"user_id": "usr_102", "username": "alex_cloud", "verified": False, "followers": 890},
    {"user_id": "usr_103", "username": "dev_marcus", "verified": True, "followers": 45000},
    {"user_id": "usr_104", "username": "elena_ai", "verified": True, "followers": 78300},
    {"user_id": "usr_105", "username": "crypto_dan", "verified": False, "followers": 320},
    {"user_id": "usr_106", "username": "priya_data", "verified": True, "followers": 18200},
    {"user_id": "usr_107", "username": "chen_devops", "verified": False, "followers": 2100},
    {"user_id": "usr_108", "username": "emma_marketing", "verified": False, "followers": 5400}
]

HASHTAG_CATEGORIES = {
    "#AI": [
        "Experimenting with open-source LLM inference today. #AI #Tech",
        "Evaluating prompt engineering techniques on structured tasks. #AI"
    ],
    "#Kubernetes": [
        "Verifying pod resource requests and limits in Minikube. #Kubernetes #DevOps",
        "Inspecting rolling updates and service DNS resolution. #Kubernetes"
    ],
    "#ApacheKafka": [
        "Configuring multi-partition topic with external listeners. #ApacheKafka #DataEngineering",
        "Testing consumer group offset commits and rebalancing. #ApacheKafka"
    ],
    "#Databricks": [
        "Writing Spark Structured Streaming notebooks on Databricks. #Databricks #ApacheSpark",
        "Testing Delta Lake ACID commits and schema validation. #Databricks #DeltaLake"
    ],
    "#AWS": [
        "Configuring custom VPC route tables and security groups. #AWS #Cloud",
        "Attaching least-privilege IAM roles to EC2 instances. #AWS #Security"
    ],
    "#Python": [
        "Refactoring streaming pipeline transformation logic. #Python #DataEngineering",
        "Writing unit tests for JSON payload serialization. #Python"
    ]
}

LOCATIONS = ["San Francisco, CA", "New York, NY", "London, UK", "Bengaluru, IN", "Singapore", "Berlin, DE", "Toronto, CA"]

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
# Kafka Connection with True Exponential Backoff + Jitter
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
            # True exponential backoff with full jitter to avoid thundering herds
            calculated_backoff = min(MAX_RETRY_DELAY_SEC, BASE_RETRY_DELAY_SEC * (2 ** retries))
            jittered_sleep = random.uniform(BASE_RETRY_DELAY_SEC, calculated_backoff)
            logger.warning(f"Broker not available. Backing off for {jittered_sleep:.2f}s before retry {retries + 1}...")
            time.sleep(jittered_sleep)

    logger.error("Exceeded maximum connection retries. Unable to reach Kafka broker.")
    sys.exit(1)

# ------------------------------------------------------------------------------
# Event Generation Logic
# ------------------------------------------------------------------------------
def generate_social_post() -> dict:
    """Generates a structured social media post event payload."""
    user = random.choice(USERS)
    primary_tag = random.choice(list(HASHTAG_CATEGORIES.keys()))
    text = random.choice(HASHTAG_CATEGORIES[primary_tag])
    hashtags = [word for word in text.split() if word.startswith("#")]
    sentiment_score = round(random.uniform(-0.8, 0.95), 4)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user["user_id"],
        "username": user["username"],
        "user_followers": user["followers"],
        "is_verified": user["verified"],
        "text": text,
        "primary_hashtag": primary_tag,
        "hashtags": hashtags,
        "sentiment_score": sentiment_score,
        "likes": random.randint(0, 1500),
        "retweets": random.randint(0, 450),
        "location": random.choice(LOCATIONS),
        "device": random.choice(["iOS", "Android", "Web Client"]),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# ------------------------------------------------------------------------------
# Main Execution Loop
# ------------------------------------------------------------------------------
def main():
    logger.info(f"Starting producer loop. Target topic: '{KAFKA_TOPIC}', rate: ~{MESSAGE_INTERVAL_SEC}s/msg")

    producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    messages_sent = 0

    try:
        while running:
            post = generate_social_post()
            partition_key = post["primary_hashtag"]

            future = producer.send(
                topic=KAFKA_TOPIC,
                key=partition_key,
                value=post
            )

            record_metadata = future.get(timeout=10)
            messages_sent += 1

            logger.info(
                f"[#{messages_sent}] Partition: {record_metadata.partition} "
                f"Offset: {record_metadata.offset} | Key: {partition_key} | User: @{post['username']}"
            )

            # Check health / create heartbeat touchfile for K8s liveness probes
            with open("/tmp/producer_heartbeat", "w") as f:
                f.write(str(time.time()))

            time.sleep(MESSAGE_INTERVAL_SEC)

    except Exception as e:
        if running:
            logger.error(f"Streaming loop error: {e}", exc_info=True)
    finally:
        logger.info("Flushing buffer and closing Kafka producer...")
        producer.flush(timeout=5)
        producer.close(timeout=5)
        logger.info(f"Producer closed cleanly. Emitted {messages_sent} total events.")

if __name__ == "__main__":
    main()
