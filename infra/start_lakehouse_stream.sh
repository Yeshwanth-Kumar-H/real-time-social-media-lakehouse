#!/usr/bin/env bash
# ==============================================================================
# Helper Script: start_lakehouse_stream.sh
# Automates EC2 stream restart:
# 1. Detects active EC2 public IP dynamically
# 2. Ensures Docker and Minikube are running
# 3. Re-configures Kafka Advertised Listeners with the current public IP
# 4. Ensures topics 'social-media-posts' and 'twitter' exist
# 5. Starts external port-forwarding on port 30094
# 6. Starts live tweet streaming producer
# ==============================================================================

set -euo pipefail

echo "========================================================================"
echo "🚀 Starting Real-Time Social Media Intelligence Lakehouse Services"
echo "========================================================================"

# 1. Detect Public IP dynamically (IMDSv2 with IMDSv1 fallback)
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || true)
if [ -n "${TOKEN}" ]; then
  PUBLIC_IP=$(curl -s -H "X-aws-ec2-metadata-token: ${TOKEN}" http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || true)
else
  PUBLIC_IP=""
fi

if [ -z "${PUBLIC_IP}" ]; then
  PUBLIC_IP=$(curl -s https://checkip.amazonaws.com 2>/dev/null || curl -s ifconfig.me 2>/dev/null || true)
fi

echo "🌐 Active EC2 Public IP: ${PUBLIC_IP}"

# 2. Stop old processes cleanly
echo "🧹 Stopping stale processes..."
pkill -f "kubectl port-forward" 2>/dev/null || true
pkill -f "election_producer" 2>/dev/null || true
sudo fuser -k 30094/tcp 2>/dev/null || true

# 3. Ensure Docker and Minikube are running
echo "🐳 Ensuring Docker & Minikube are running..."
sudo systemctl start docker
minikube start --driver=docker

# 4. Set Kafka Advertised Listener with the current Public IP
echo "📡 Configuring Kafka Advertised Listener to ${PUBLIC_IP}:30094..."
kubectl set env deployment/kafka KAFKA_ADVERTISED_LISTENERS="INTERNAL://kafka-service:9092,EXTERNAL://${PUBLIC_IP}:30094"
echo "⏳ Waiting for Kafka pod rollout..."
kubectl rollout status deployment/kafka --timeout=60s

# 5. Ensure Topics Exist
echo "📋 Ensuring Kafka topics exist..."
kubectl exec deployment/kafka -c kafka-broker -- kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic social-media-posts --partitions 3 --replication-factor 1 2>/dev/null || true
kubectl exec deployment/kafka -c kafka-broker -- kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic twitter --partitions 3 --replication-factor 1 2>/dev/null || true

# 6. Start Port-Forwarding
echo "🔌 Exposing Kafka Port 30094 to internet..."
nohup kubectl port-forward --address 0.0.0.0 svc/kafka-service 30094:9094 > ~/port_forward.log 2>&1 &
sleep 2

# 7. Start the Producer
echo "🐦 Starting Live Tweet Streaming Producer..."
source ~/venv/bin/activate
nohup python3 -u ~/election_producer.py > ~/producer_live.log 2>&1 &
sleep 3

# 8. Verification Summary
echo "========================================================================"
echo "✅ Status Summary:"
sudo ss -tlpn | grep 30094 || echo "Warning: 30094 not listening"
ps aux | grep -E "port-forward|election_producer" | grep -v grep || true
echo "========================================================================"
echo "🎉 Stream is active and ready for Databricks ingestion at ${PUBLIC_IP}:30094!"
