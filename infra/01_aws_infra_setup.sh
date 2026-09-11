#!/usr/bin/env bash
# ==============================================================================
# Script: 01_aws_infra_setup.sh
# Purpose: Provision Production-Grade AWS VPC, Subnet, Security Groups, IAM,
#          and EC2 Compute for the Real-Time Streaming Intelligence Platform.
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# Configuration Variables
# ------------------------------------------------------------------------------
PROJECT_NAME="social-media-lakehouse"
AWS_REGION="${AWS_DEFAULT_REGION:-ap-south-2}"
VPC_CIDR="10.0.0.0/16"
PUBLIC_SUBNET_CIDR="10.0.1.0/24"
INSTANCE_TYPE="t3.medium"  # Recommended: 2 vCPU, 4GB RAM for Minikube + Kafka
KEY_NAME="${KEY_NAME:-bda_key_pair}" # Name of existing EC2 Key Pair in AWS

echo "=================================================================="
echo " Starting AWS Infrastructure Provisioning for: ${PROJECT_NAME}"
echo " Target Region: ${AWS_REGION}"
echo "=================================================================="

# ------------------------------------------------------------------------------
# Step 1: Create Custom VPC
# ------------------------------------------------------------------------------
echo "[1/7] Creating Custom VPC (${VPC_CIDR})..."
VPC_ID=$(aws ec2 create-vpc \
  --cidr-block "${VPC_CIDR}" \
  --region "${AWS_REGION}" \
  --tag-specifications "ResourceType=vpc,Tags=[{Key=Name,Value=${PROJECT_NAME}-vpc}]" \
  --query 'Vpc.VpcId' \
  --output text)

echo "  -> VPC Created: ${VPC_ID}"

# Enable DNS resolution and Hostnames within VPC (Required for K8s & internal endpoints)
aws ec2 modify-vpc-attribute --vpc-id "${VPC_ID}" --enable-dns-support "{\"Value\":true}" --region "${AWS_REGION}"
aws ec2 modify-vpc-attribute --vpc-id "${VPC_ID}" --enable-dns-hostnames "{\"Value\":true}" --region "${AWS_REGION}"

# ------------------------------------------------------------------------------
# Step 2: Create Public Subnet
# ------------------------------------------------------------------------------
echo "[2/7] Creating Public Subnet (${PUBLIC_SUBNET_CIDR})..."
AZ=$(aws ec2 describe-availability-zones \
  --region "${AWS_REGION}" \
  --query 'AvailabilityZones[0].ZoneName' \
  --output text)

SUBNET_ID=$(aws ec2 create-subnet \
  --vpc-id "${VPC_ID}" \
  --cidr-block "${PUBLIC_SUBNET_CIDR}" \
  --availability-zone "${AZ}" \
  --region "${AWS_REGION}" \
  --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=${PROJECT_NAME}-public-subnet-1}]" \
  --query 'Subnet.SubnetId' \
  --output text)

echo "  -> Subnet Created: ${SUBNET_ID} in ${AZ}"

# Auto-assign public IP addresses on launch
aws ec2 modify-subnet-attribute --subnet-id "${SUBNET_ID}" --map-public-ip-on-launch --region "${AWS_REGION}"

# ------------------------------------------------------------------------------
# Step 3: Internet Gateway (IGW) & Route Table
# ------------------------------------------------------------------------------
echo "[3/7] Creating and Attaching Internet Gateway..."
IGW_ID=$(aws ec2 create-internet-gateway \
  --region "${AWS_REGION}" \
  --tag-specifications "ResourceType=internet-gateway,Tags=[{Key=Name,Value=${PROJECT_NAME}-igw}]" \
  --query 'InternetGateway.InternetGatewayId' \
  --output text)

aws ec2 attach-internet-gateway --vpc-id "${VPC_ID}" --internet-gateway-id "${IGW_ID}" --region "${AWS_REGION}"
echo "  -> IGW Attached: ${IGW_ID}"

echo "  Configuring Public Route Table (0.0.0.0/0 -> IGW)..."
ROUTE_TABLE_ID=$(aws ec2 create-route-table \
  --vpc-id "${VPC_ID}" \
  --region "${AWS_REGION}" \
  --tag-specifications "ResourceType=route-table,Tags=[{Key=Name,Value=${PROJECT_NAME}-public-rt}]" \
  --query 'RouteTable.RouteTableId' \
  --output text)

aws ec2 create-route \
  --route-table-id "${ROUTE_TABLE_ID}" \
  --destination-cidr-block 0.0.0.0/0 \
  --gateway-id "${IGW_ID}" \
  --region "${AWS_REGION}" > /dev/null

aws ec2 associate-route-table \
  --subnet-id "${SUBNET_ID}" \
  --route-table-id "${ROUTE_TABLE_ID}" \
  --region "${AWS_REGION}" > /dev/null

echo "  -> Route Table Associated: ${ROUTE_TABLE_ID}"

# ------------------------------------------------------------------------------
# Step 4: Stateful Security Group
# ------------------------------------------------------------------------------
echo "[4/7] Creating Security Group for Compute & Kafka..."
SG_ID=$(aws ec2 create-security-group \
  --group-name "${PROJECT_NAME}-sg" \
  --description "Security Group for ${PROJECT_NAME} (SSH and Kafka External Listener)" \
  --vpc-id "${VPC_ID}" \
  --region "${AWS_REGION}" \
  --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=${PROJECT_NAME}-sg}]" \
  --query 'GroupId' \
  --output text)

echo "  -> Security Group Created: ${SG_ID}"

# Inbound Rules:
# 1. SSH (Port 22)
aws ec2 authorize-security-group-ingress \
  --group-id "${SG_ID}" \
  --protocol tcp \
  --port 22 \
  --cidr 0.0.0.0/0 \
  --region "${AWS_REGION}" > /dev/null

# 2. Kafka External Listener (Port 9092) - For Databricks Spark Streaming access
aws ec2 authorize-security-group-ingress \
  --group-id "${SG_ID}" \
  --protocol tcp \
  --port 9092 \
  --cidr 0.0.0.0/0 \
  --region "${AWS_REGION}" > /dev/null

echo "  -> Ingress Rules Configured: Port 22 (SSH), Port 9092 (Kafka)"

# ------------------------------------------------------------------------------
# Step 5: IAM Role & Instance Profile for Secure S3 Access
# ------------------------------------------------------------------------------
echo "[5/7] Creating IAM Role & Instance Profile for EC2 (Least Privilege)..."
ROLE_NAME="${PROJECT_NAME}-ec2-role"
PROFILE_NAME="${PROJECT_NAME}-ec2-profile"

# Trust Policy allowing EC2 service to assume role
TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "ec2.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}'

aws iam create-role \
  --role-name "${ROLE_NAME}" \
  --assume-role-policy-document "${TRUST_POLICY}" \
  --description "Role for ${PROJECT_NAME} EC2 instance" 2>/dev/null || true

# Attach policy granting S3 permissions (for streaming raw checkpoints or dataset sync)
aws iam attach-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess 2>/dev/null || true

aws iam create-instance-profile --instance-profile-name "${PROFILE_NAME}" 2>/dev/null || true
aws iam add-role-to-instance-profile \
  --instance-profile-name "${PROFILE_NAME}" \
  --role-name "${ROLE_NAME}" 2>/dev/null || true

echo "  -> IAM Role and Instance Profile Ready: ${PROFILE_NAME}"

# ------------------------------------------------------------------------------
# Step 6: User Data (Automated Provisioning: Docker, Swap, Minikube)
# ------------------------------------------------------------------------------
USER_DATA_FILE=$(mktemp)
cat << 'EOF' > "${USER_DATA_FILE}"
#!/bin/bash
set -e

# Update and install base packages
dnf update -y
dnf install -y git docker htop iptables

# Start and enable Docker
systemctl start docker
systemctl enable docker
usermod -aG docker ec2-user

# Configure 2GB Swap space (Crucial for memory-constrained instances running Minikube/Kafka)
if [ ! -f /swapfile ]; then
    dd if=/dev/zero of=/swapfile bs=128M count=16
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo "/swapfile swap swap defaults 0 0" >> /etc/fstab
fi

# Install kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl
mv kubectl /usr/local/bin/

# Install Minikube
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
install minikube-linux-amd64 /usr/local/bin/minikube
rm -f minikube-linux-amd64

# Mark initialization complete
touch /home/ec2-user/.infra_ready
EOF

# ------------------------------------------------------------------------------
# Step 7: Launch EC2 Instance
# ------------------------------------------------------------------------------
echo "[6/7] Finding latest Amazon Linux 2023 AMI..."
AMI_ID=$(aws ec2 describe-images \
  --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023.*-x86_64" "Name=state,Values=available" \
  --region "${AWS_REGION}" \
  --query 'reverse(sort_by(Images, &CreationDate))[0].ImageId' \
  --output text)

echo "  -> Selected AMI: ${AMI_ID}"

echo "[7/7] Launching EC2 Instance (${INSTANCE_TYPE})..."
INSTANCE_ID=$(aws ec2 run-instances \
  --image-id "${AMI_ID}" \
  --instance-type "${INSTANCE_TYPE}" \
  --key-name "${KEY_NAME}" \
  --subnet-id "${SUBNET_ID}" \
  --security-group-ids "${SG_ID}" \
  --iam-instance-profile Name="${PROFILE_NAME}" \
  --user-data "file://${USER_DATA_FILE}" \
  --block-device-mappings "[{\"DeviceName\":\"/dev/xvda\",\"Ebs\":{\"VolumeSize\":25,\"VolumeType\":\"gp3\"}}]" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${PROJECT_NAME}-node}]" \
  --region "${AWS_REGION}" \
  --query 'Instances[0].InstanceId' \
  --output text)

rm -f "${USER_DATA_FILE}"

echo "=================================================================="
echo " Provisioning Complete!"
echo " Instance ID: ${INSTANCE_ID}"
echo " Waiting for Public IP assignment..."
echo "=================================================================="

aws ec2 wait instance-running --instance-ids "${INSTANCE_ID}" --region "${AWS_REGION}"
PUBLIC_IP=$(aws ec2 describe-instances \
  --instance-ids "${INSTANCE_ID}" \
  --region "${AWS_REGION}" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' \
  --output text)

echo ""
echo ">>> EC2 Node is RUNNING at Public IP: ${PUBLIC_IP} <<<"
echo "Connect with: ssh -i path/to/${KEY_NAME}.pem ec2-user@${PUBLIC_IP}"
echo ""
