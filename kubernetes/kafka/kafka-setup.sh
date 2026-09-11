#!/usr/bin/env bash
# ==============================================================================
# Script: kafka-setup.sh
# Purpose: Initialize Kafka topic with partitioning, verify broker state,
#          and expose external listener on EC2 port 9094 for Databricks.
# ==============================================================================

set -euo pipefail

TOPIC_NAME="social-media-posts"
PARTITIONS=3
REPLICATION_FACTOR=1

echo "=================================================================="
echo " Initializing Kafka Topic: ${TOPIC_NAME}"
echo "=================================================================="

# 1. Wait for Kafka Pod to be Ready
echo "Waiting for Kafka pod to transition to Running state..."
kubectl wait --for=condition=ready pod -l app=kafka --timeout=120s

KAFKA_POD=$(kubectl get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
echo "Active Kafka Pod: ${KAFKA_POD}"

# 2. Check if topic already exists
echo "Checking existing topics..."
EXISTING_TOPICS=$(kubectl exec "${KAFKA_POD}" -c kafka-broker -- \
  /bin/kafka-topics --bootstrap-server localhost:9092 --list)

if echo "${EXISTING_TOPICS}" | grep -qw "${TOPIC_NAME}"; then
  echo "Topic '${TOPIC_NAME}' already exists. Describing topic:"
  kubectl exec "${KAFKA_POD}" -c kafka-broker -- \
    /bin/kafka-topics --bootstrap-server localhost:9092 --describe --topic "${TOPIC_NAME}"
else
  echo "Creating topic '${TOPIC_NAME}' with ${PARTITIONS} partitions..."
  kubectl exec "${KAFKA_POD}" -c kafka-broker -- \
    /bin/kafka-topics --bootstrap-server localhost:9092 \
    --create \
    --topic "${TOPIC_NAME}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}"

  echo "Topic created successfully. Describing:"
  kubectl exec "${KAFKA_POD}" -c kafka-broker -- \
    /bin/kafka-topics --bootstrap-server localhost:9092 --describe --topic "${TOPIC_NAME}"
fi

# 3. Ensure Port 9094 is forwarded to EC2 host interface for Databricks ingress
echo "Ensuring port-forwarding for external listener (Port 9094)..."
if ! pgrep -f "port-forward.*9094:9094" > /dev/null; then
  nohup kubectl port-forward --address 0.0.0.0 service/kafka-service 9094:9094 > /tmp/kafka-port-forward.log 2>&1 &
  echo "  -> Port-forwarding active on 0.0.0.0:9094 (background PID: $!)"
else
  echo "  -> Port-forwarding already active on 0.0.0.0:9094"
fi

echo "=================================================================="
echo " Kafka Broker Ready. Databricks can connect via <EC2_PUBLIC_IP>:9094"
echo "=================================================================="
