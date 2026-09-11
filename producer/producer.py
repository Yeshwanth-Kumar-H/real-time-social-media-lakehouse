#!/usr/bin/env python3
"""
Production-Grade Social Media Event Stream Generator.
Simulates real-world high-throughput social media posts with rich attributes
(sentiment scores, engagement metrics, hashtags, timestamps) and publishes
them to an Apache Kafka streaming topic with exponential backoff and graceful shutdown.
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
# Configuration (Environment Variables with Resilient Defaults)
# ------------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "social-media-posts")
MESSAGE_INTERVAL_SEC = float(os.getenv("MESSAGE_INTERVAL_SEC", "1.5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "15"))
RETRY_DELAY_SEC = int(os.getenv("RETRY_DELAY_SEC", "5"))

# ------------------------------------------------------------------------------
# Mock Data Pools for Realistic Simulation
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
    "#AI": ["Deploying our new LLM pipeline today! #AI #Innovation", "Generative AI is transforming real-time data pipelines. #AI #Tech"],
    "#Kubernetes": ["Minikube cluster health checks passing smoothly. #Kubernetes #DevOps", "Troubleshooting pod resource limits in production. #Kubernetes"],
    "#ApacheKafka": ["Kafka event streaming handling 10k events/sec with sub-second latency! #ApacheKafka #DataEngineering", "Decoupling microservices using Kafka partitions. #ApacheKafka"],
    "#Databricks": ["Running Spark Structured Streaming jobs on Databricks Lakehouse. #Databricks #ApacheSpark", "Delta Lake ACID transactions are a game changer. #Databricks #DeltaLake"],
    "#AWS": ["Securing our AWS VPC with custom route tables and security groups. #AWS #Cloud", "Setting up S3 data lake with Medallion architecture. #AWS #DataLake"],
    "#Python": ["Writing resilient PySpark transformation jobs. #Python #BigData", "Clean code practices in Python data engineering. #Python"]
}

LOCATIONS = ["San Francisco, CA", "New York, NY", "London, UK", "Bengaluru, IN", "Singapore", "Berlin, DE", "Toronto, CA"]

# ------------------------------------------------------------------------------
# Graceful Shutdown Handler
# ------------------------------------------------------------------------------
running = True

def handle_exit_signal(signum, frame):
    global running
    logger.info(f"Received shutdown signal ({signum}). Gracefully closing producer...")
    running = False

signal.signal(signal.SIGINT, handle_exit_signal)
signal.signal(signal.SIGTERM, handle_exit_signal)

# ------------------------------------------------------------------------------
# Resilient Kafka Connection Helper
# ------------------------------------------------------------------------------
def create_kafka_producer(servers: str) -> KafkaProducer:
    """Connects to Kafka broker with exponential retry to survive startup latencies."""
    retries = 0
    while running and retries < MAX_RETRIES:
        try:
            logger.info(f"Attempting connection to Kafka broker at {servers} (Attempt {retries + 1}/{MAX_RETRIES})...")
            producer = KafkaProducer(
                bootstrap_servers=servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",              # Guarantees broker leader & ISR acknowledgment
                retries=3,               # Automatic retries on transient network errors
                max_in_flight_requests_per_connection=1 # Guarantees strict ordering per partition
            )
            logger.info("Successfully connected to Kafka broker.")
            return producer
        except NoBrokersAvailable:
            retries += 1
            logger.warning(f"Kafka broker not ready yet. Retrying in {RETRY_DELAY_SEC}s...")
            time.sleep(RETRY_DELAY_SEC)
    
    logger.error("Failed to connect to Kafka broker after maximum retries. Exiting.")
    sys.exit(1)

# ------------------------------------------------------------------------------
# Event Generation Logic
# ------------------------------------------------------------------------------
def generate_social_post() -> dict:
    """Generates a structured, production-grade social media post payload."""
    user = random.choice(USERS)
    primary_tag = random.choice(list(HASHTAG_CATEGORIES.keys()))
    text = random.choice(HASHTAG_CATEGORIES[primary_tag])
    
    # Extract all hashtags present in the text
    hashtags = [word for word in text.split() if word.startswith("#")]
    
    # Simulated sentiment score: -1.0 (very negative) to +1.0 (very positive)
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
    logger.info("Initializing Social Media Event Stream Generator...")
    logger.info(f"Target Topic: {KAFKA_TOPIC} | Interval: {MESSAGE_INTERVAL_SEC}s")

    producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    messages_sent = 0

    try:
        while running:
            post = generate_social_post()
            # Partition key by primary_hashtag to guarantee in-order delivery per topic partition
            partition_key = post["primary_hashtag"]
            
            future = producer.send(
                topic=KAFKA_TOPIC,
                key=partition_key,
                value=post
            )
            
            # Wait for acknowledgment to ensure reliable transmission
            record_metadata = future.get(timeout=10)
            messages_sent += 1

            logger.info(
                f"[#{messages_sent}] Key: {partition_key} -> Partition: {record_metadata.partition} "
                f"Offset: {record_metadata.offset} | User: @{post['username']} | Tags: {post['hashtags']}"
            )
            
            time.sleep(MESSAGE_INTERVAL_SEC)

    except Exception as e:
        if running:
            logger.error(f"Unexpected streaming error: {e}", exc_info=True)
    finally:
        logger.info("Flushing pending events and closing Kafka producer...")
        producer.flush(timeout=5)
        producer.close(timeout=5)
        logger.info(f"Producer shut down cleanly. Total events emitted: {messages_sent}")

if __name__ == "__main__":
    main()
