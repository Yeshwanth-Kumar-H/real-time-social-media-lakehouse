#!/usr/bin/env bash
# ==============================================================================
# Script: kafka-setup.sh
# Purpose: Initialize Kafka topic with partitioning and verify broker readiness.
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

echo "=================================================================="
echo " Kafka Broker Ready. External clients connect via <EC2_PUBLIC_IP>:30094"
echo "=================================================================="
