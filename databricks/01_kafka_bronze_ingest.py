# Databricks Notebook: 01_kafka_bronze_ingest
# ==============================================================================
# Phase 4: Bronze Layer Streaming Ingestion
# Ingests raw social media events from Apache Kafka into an append-only
# Delta Lake Bronze table on Databricks Unity Catalog with fault-tolerant checkpointing.
# ==============================================================================

# COMMAND ----------
# 1. Environment & Unity Catalog Checkpoint Setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder.appName("SocialMedia-Bronze-Ingestion").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Identify active Unity Catalog catalog and schema
curr_cat = spark.catalog.currentCatalog()
curr_sch = spark.catalog.currentDatabase()
print(f"Active Catalog: '{curr_cat}' | Schema: '{curr_sch}'")

# Ensure governed Volume exists for stream checkpoints
spark.sql(f"CREATE VOLUME IF NOT EXISTS {curr_cat}.{curr_sch}.lakehouse_checkpoints")
CHECKPOINT_BASE = f"/Volumes/{curr_cat}/{curr_sch}/lakehouse_checkpoints"
CHECKPOINT_BRONZE = f"{CHECKPOINT_BASE}/bronze_live"

print(f"✅ Governed Checkpoint Volume is ready at: {CHECKPOINT_BRONZE}")

# COMMAND ----------
# 2. Network Connectivity Validation (Broker Socket Test)
import socket

BROKER_IP = "98.130.142.79"
BROKER_PORT = 30094

s = socket.socket()
s.settimeout(5)
res = s.connect_ex((BROKER_IP, BROKER_PORT))
if res == 0:
    print(f"🎉 SUCCESS! Databricks can reach Kafka broker at {BROKER_IP}:{BROKER_PORT}")
else:
    print(f"❌ Connection failed with code {res}")
s.close()

# COMMAND ----------
# 3. Stream Raw Events from Kafka into Bronze Delta Table
# NOTE: Databricks Serverless compute enforces Trigger.AvailableNow (or Once).
# Continuous processing triggers like trigger(processingTime="...") fail with
# [INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED]. AvailableNow micro-batches all available
# messages efficiently and commits offsets with complete state guarantee.

KAFKA_BOOTSTRAP_SERVERS = f"{BROKER_IP}:{BROKER_PORT}"
KAFKA_TOPIC = "social-media-posts"

print(f"📡 Connecting to Kafka Broker: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"📋 Reading Topic: {KAFKA_TOPIC}")
print(f"💾 Checkpoint Location: {CHECKPOINT_BRONZE}")

kafka_stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")          # Ingest all historical messages from stream start
    .option("failOnDataLoss", "false")              # Protect against offset resets if pod restarts
    .option("maxOffsetsPerTrigger", 10000)          # Bounded safety ceiling per micro-batch
    .load()
)

# Extract raw Kafka payload & append lineage audit metadata
bronze_df = kafka_stream_df.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("raw_payload"),
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_event_time"),
    current_timestamp().alias("_ingest_timestamp")
)

# Stream raw events into append-only Bronze Delta Lake table
bronze_query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_BRONZE)
    .trigger(availableNow=True)
    .toTable("bronze_social_media_raw")
)

bronze_query.awaitTermination()
print("✅ Bronze Ingestion Complete! Data safely committed to 'bronze_social_media_raw'.")

# COMMAND ----------
# 4. Verify Bronze Record Count
total_records = spark.table("bronze_social_media_raw").count()
print(f"📊 Total Records in 'bronze_social_media_raw': {total_records:,}")

# COMMAND ----------
# 5. Preview Raw Bronze Records
display(spark.table("bronze_social_media_raw").orderBy(col("_ingest_timestamp").desc()).limit(10))
