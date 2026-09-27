# Real-Time Social Media Intelligence Lakehouse Platform
### *Philippine Election 2025 Trends — Enterprise Streaming Architecture*

[![Apache Kafka](https://img.shields.io/badge/Streaming-Apache%20Kafka%203.4-black?logo=apachekafka)](https://kafka.apache.org/)
[![Databricks](https://img.shields.io/badge/Compute-Databricks%20Serverless-FF3621?logo=databricks)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake%203.0-00ADD8?logo=delta)](https://delta.io/)
[![AWS](https://img.shields.io/badge/Cloud-Amazon%20Web%20Services-232F3E?logo=amazon-aws)](https://aws.amazon.com/)
[![Kubernetes](https://img.shields.io/badge/Orchestration-Kubernetes-326CE5?logo=kubernetes)](https://kubernetes.io/)

---

## 1. Executive Summary & Business Objective

During high-velocity national events (such as the **Philippine General Election 2025**), social media conversations evolve rapidly across platforms. Traditional batch ETL architectures (running overnight) deliver intelligence 12–24 hours too late.

This project delivers an **end-to-end, enterprise-grade streaming Lakehouse platform** designed to ingest, process, and analyze social media trends in real time. It captures tweet bursts, cleanses and deduplicates records with sub-second latency, maintains an append-only audit trail in Delta Lake, and powers an **Executive BI Dashboard** displaying trending hashtags, sentiment shifts, and influencer engagement.

### Verified Benchmark Performance:
- **Streaming Ingestion Velocity:** **3,057 messages/second** (217,640 tweets ingested in 71.2 seconds).
- **Scale Tested:** **299,247 live tweets** processed across 3 Kafka partitions.
- **Engagement Analyzed:** **13,517,442 user interactions** (likes, retweets, replies).
- **Top Dominant Trend:** `#halalan2025` (>200,000 posts, 8M+ engagement), followed by `#pbbcollab5theevictionnight` and `#sb19`.
- **Pipeline Orchestration:** 3-stage multi-hop DAG (`Bronze ➔ Silver ➔ Gold`) executed with zero errors in **2m 50s**.

---

## 2. End-to-End System Architecture

```mermaid
flowchart TD
    subgraph AWS_Cloud [AWS Cloud Infrastructure - ap-south-2 Hyderabad]
        subgraph VPC [Custom VPC: 10.0.0.0/16]
            IGW[Internet Gateway]
            subgraph Public_Subnet [Public Subnet: 10.0.1.0/24]
                EC2[EC2 t3.small Instance<br/>25GB gp3 + 2GB Swap Memory]
                SG[Security Group: Port 22 SSH, Port 30094 NodePort]
                
                subgraph K8s_Cluster [Kubernetes / Minikube Engine]
                    PROD[Python Event Stream Producer<br/>Dynamic Timestamps & Sentiment]
                    KAFKA[Apache Kafka Broker + Zookeeper<br/>Topic: 'twitter' | 3 Partitions]
                    PROD -->|JSON Stream| KAFKA
                end
            end
        end
        S3[Amazon S3 Landing Zone<br/>s3://social-media-lakehouse-yeshw-2026/raw_csv/]
        IAM[IAM Role: Least-Privilege Scoped Policy]
        IAM -.->|No Hardcoded Keys| EC2
    end

    subgraph Databricks_Platform [Databricks Lakehouse Platform]
        KAFKA -->|External NodePort 30094| SPARK_BRONZE[01_kafka_bronze_ingest<br/>Trigger.AvailableNow]
        
        subgraph Medallion_Architecture [Medallion Storage on Delta Lake]
            SPARK_BRONZE -->|Append-Only Raw Stream| BRONZE[(Bronze Layer<br/>bronze_social_media_raw)]
            
            BRONZE -->|Spark Structured Streaming| SPARK_SILVER[02_silver_transformations<br/>withWatermark + dropDuplicates]
            SPARK_SILVER -->|Cleansed & Deduplicated| SILVER[(Silver Layer<br/>silver_social_media_posts)]
            SPARK_SILVER -.->|Malformed Payloads| DLQ[(Quarantine DLQ<br/>quarantine_corrupt_events)]
            
            SILVER -->|Windowed Aggregations| SPARK_GOLD[03_gold_aggregations]
            SPARK_GOLD -->|Hashtag Velocity & Sentiment| GOLD_HASH[(Gold Layer<br/>gold_trending_hashtags)]
            SPARK_GOLD -->|Influencer Analytics| GOLD_USER[(Gold Layer<br/>gold_active_users)]
        end
        
        subgraph UC_Governance [Unity Catalog Governance]
            VOL[Unity Catalog Volume<br/>/Volumes/.../lakehouse_checkpoints/]
            VOL -.->|Fault-Tolerant Checkpoints| Medallion_Architecture
        end
        
        subgraph Lakeflow_Orchestration [Databricks Workflows / Jobs]
            DAG[Visual Multi-Hop Pipeline DAG<br/>Bronze ➔ Silver ➔ Gold]
            DAG -.->|Automated Execution| Medallion_Architecture
        end
        
        subgraph BI_Presentation [Databricks Lakeview AI Dashboard]
            GOLD_HASH --> DASH[Philippine Election Social Media Trends 2025<br/>KPI Cards | Horizontal Bar Charts | Active Users Table]
            GOLD_USER --> DASH
        end
    end
```

---

## 3. The Medallion Lakehouse Architecture

| Layer | Delta Table | Engineering Responsibility | Design Decisions |
| :--- | :--- | :--- | :--- |
| **Bronze** | `bronze_social_media_raw` | Raw, immutable Kafka ingestion. Never parses or modifies payloads. | Stores raw JSON blob with metadata (`kafka_partition`, `kafka_offset`, `_ingest_timestamp`) for 100% auditability and replayability. |
| **Silver** | `silver_social_media_posts` | Cleansed, typed, deduplicated streaming records. | Strict schema enforcement, Dead-Letter Queue (DLQ) routing, feature engineering (`engagement_score`), and **event-time watermarking (`withWatermark(10 mins)`) before stateful deduplication (`dropDuplicates`)**. |
| **Gold** | `gold_trending_hashtags`<br/>`gold_active_users` | Curated business-level aggregations ready for BI & executive reporting. | Hashtags array exploded, post velocity computed, lexical sentiment aggregated, and user virality tracked. |

---

## 4. Key Senior Engineering Decisions & Trade-Offs

### 1. Dual Advertised Listeners on Apache Kafka
* **The Problem:** Kafka brokers return metadata to clients telling them where partition leaders live. Inside Kubernetes, pods talk via `kafka-service:9092`. But external Databricks clusters running in the cloud cannot resolve internal Kubernetes CoreDNS names.
* **The Solution:** Dual Advertised Listeners:
  ```yaml
  KAFKA_LISTENERS: "INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094"
  KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka-service:9092,EXTERNAL://<EC2_PUBLIC_IP>:30094"
  ```
  Internal producer pods use the cluster IP; external Databricks streaming jobs connect to the public NodePort `30094`.

### 2. Bounded State Store: Watermarking Before Deduplication
* **The Problem:** In Spark Structured Streaming, doing `.dropDuplicates(["event_id"])` without a watermark forces Spark's state store (RocksDB) to retain every unique tweet ID **indefinitely**. After processing millions of tweets, the driver/executors inevitably crash with an **Out-Of-Memory (OOM)** error.
* **The Solution:**
  ```python
  deduped_silver_df = (
      valid_events_df
      .withWatermark("event_timestamp", "10 minutes")
      .dropDuplicates(["event_id", "event_timestamp"])
  )
  ```
  Spark evicts keys older than the 10-minute watermark threshold, ensuring constant bounded memory usage.

### 3. Unity Catalog Volumes for Checkpointing (Modern Governance)
* **The Problem:** Modern Databricks disables root DBFS (`/tmp/` and `dbfs:/`) for security and data exfiltration prevention (`[DBFS_DISABLED]`).
* **The Solution:** Structured streaming checkpoints are written to a governed **Unity Catalog Volume**:
  ```python
  CHECKPOINT_PATH = f"/Volumes/{curr_cat}/{curr_sch}/lakehouse_checkpoints/bronze"
  ```
  Provides access control, audit logs, and compliance without exposing cloud storage credentials.

### 4. Backpressure Safety: `maxOffsetsPerTrigger = 10000`
* **The Problem:** If a cluster restarts after a multi-hour downtime, reading an unthrottled Kafka topic with millions of backlogged events will overwhelm the executor memory heap.
* **The Solution:** `.option("maxOffsetsPerTrigger", 10000)` sets a safe processing ceiling per micro-batch, allowing rapid catch-up in stable, predictable increments.

---

## 5. Repository Structure

```text
├── databricks/                         # Databricks PySpark Lakehouse Notebooks
│   ├── 01_kafka_bronze_ingest.py       # Bronze layer raw Kafka streaming ingest
│   ├── 02_silver_transformations.py    # Silver layer schema, DLQ & deduplication
│   └── 03_gold_aggregations.py         # Gold layer analytics & business tables
├── infra/                              # AWS Cloud Infrastructure Automation
│   ├── 01_aws_infra_setup.sh           # VPC, Subnet, IGW, Route Table, SG & IAM
│   └── teardown.sh                     # Idempotent cloud resource cleanup
├── kubernetes/                         # Kubernetes Deployment Manifests
│   ├── kafka/
│   │   ├── kafka-deployment.yaml       # Kafka + Zookeeper deployment with dual listeners
│   │   ├── kafka-service.yaml          # NodePort 30094 external routing service
│   │   └── kafka-setup.sh              # Topic creation and verification script
│   └── producer/
│       ├── producer-configmap.yaml     # Pacing, broker endpoint and topic configuration
│       └── producer-deployment.yaml    # Resilient deployment with K8s health probes
├── producer/                           # Python Real-Time Event Producer
│   ├── Dockerfile                      # Hardened multi-stage non-root container
│   ├── producer.py                     # 24/7 infinite stream & high-throughput engine
│   └── requirements.txt                # Dependencies (kafka-python, requests)
├── dashboard/                          # Databricks SQL Queries & Visualizations
│   └── queries.sql                     # Executive BI analytics queries
└── README.md                           # Master Project Documentation
```

---

## 6. How to Reproduce

### 1. Provision AWS Cloud Infrastructure
```bash
chmod +x infra/01_aws_infra_setup.sh
./infra/01_aws_infra_setup.sh
```

### 2. Deploy Kafka on Kubernetes
```bash
minikube start --driver=docker
kubectl apply -f kubernetes/kafka/kafka-deployment.yaml
kubectl apply -f kubernetes/kafka/kafka-service.yaml
nohup kubectl port-forward --address 0.0.0.0 svc/kafka-service 30094:9094 > /dev/null 2>&1 &
```

### 3. Launch the Real-Time Event Streamer
```bash
python3 -m venv venv && source venv/bin/activate
pip install -r producer/requirements.txt
python3 producer/producer.py
```

### 4. Execute the Medallion Pipeline in Databricks
1. Import `databricks/01_kafka_bronze_ingest.py`, `02_silver_transformations.py`, and `03_gold_aggregations.py`.
2. Configure `pipeline.kafka.bootstrap` to `<EC2_PUBLIC_IP>:30094`.
3. Create an automated multi-task job in **Databricks Workflows** linking `Bronze ➔ Silver ➔ Gold`.
4. Open the **Lakeview Dashboard** and hit **Refresh** to monitor real-time trends!

---

## 7. Author & Technical Attribution

- **Architect & Developer:** Yeshwanth Gowda
- **Domain Focus:** Real-Time Cloud Data Engineering, Streaming Lakehouse Architectures, Distributed Computing
- **Technologies:** AWS, Kubernetes, Apache Kafka, Apache Spark, Databricks, Delta Lake, Python, SQL
