#!/usr/bin/env bash
# ==============================================================================
# Script: 01_aws_infra_setup.sh
# Purpose: Provision AWS VPC, Subnet, Security Groups, Scoped IAM, and EC2
#          for the Real-Time Streaming Intelligence Platform.
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# Configuration Variables
# ------------------------------------------------------------------------------
PROJECT_NAME="social-media-lakehouse"
AWS_REGION="${AWS_DEFAULT_REGION:-ap-south-2}"
VPC_CIDR="10.0.0.0/16"
PUBLIC_SUBNET_CIDR="10.0.1.0/24"
INSTANCE_TYPE="t3.medium"
KEY_NAME="${KEY_NAME:-bda_key_pair}"
S3_BUCKET_NAME="${S3_BUCKET_NAME:-social-media-lakehouse-${AWS_REGION}}"

echo "=================================================================="
echo " Starting AWS Infrastructure Provisioning for: ${PROJECT_NAME}"
echo " Target Region: ${AWS_REGION}"
echo " Scoped S3 Bucket: ${S3_BUCKET_NAME}"
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
# Step 4: Security Group (Internally Consistent Port Allocations)
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

# 1. Inbound SSH (Port 22)
aws ec2 authorize-security-group-ingress \
  --group-id "${SG_ID}" \
  --protocol tcp \
  --port 22 \
  --cidr 0.0.0.0/0 \
  --region "${AWS_REGION}" > /dev/null

# 2. Inbound Kafka External Listener via Kubernetes NodePort (Port 30094)
aws ec2 authorize-security-group-ingress \
  --group-id "${SG_ID}" \
  --protocol tcp \
  --port 30094 \
  --cidr 0.0.0.0/0 \
  --region "${AWS_REGION}" > /dev/null

echo "  -> Ingress Rules Configured: Port 22 (SSH), Port 30094 (Kafka NodePort)"

# ------------------------------------------------------------------------------
# Step 5: Least-Privilege Scoped IAM Role & Instance Profile
# Replaced managed AmazonS3FullAccess with resource-scoped policy document
# ------------------------------------------------------------------------------
echo "[5/7] Creating Least-Privilege IAM Role & Policy for S3 Lakehouse Access..."
ROLE_NAME="${PROJECT_NAME}-ec2-role"
PROFILE_NAME="${PROJECT_NAME}-ec2-profile"
POLICY_NAME="${PROJECT_NAME}-s3-scoped-policy"

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
  --description "Least-privilege role for ${PROJECT_NAME} EC2 node" 2>/dev/null || true

# Explicitly scoped IAM policy restricting actions exclusively to the project bucket
SCOPED_S3_POLICY=$(cat << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowBucketMetadataAndListing",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::${S3_BUCKET_NAME}"
    },
    {
      "Sid": "AllowObjectLakehouseOperations",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::${S3_BUCKET_NAME}/*"
    }
  ]
}
EOF
)

POLICY_ARN=$(aws iam create-policy \
  --policy-name "${POLICY_NAME}" \
  --policy-document "${SCOPED_S3_POLICY}" \
  --description "Scoped access to ${S3_BUCKET_NAME}" \
  --query 'Policy.Arn' \
  --output text 2>/dev/null || aws iam list-policies --query "Policies[?PolicyName=='${POLICY_NAME}'].Arn" --output text)

aws iam attach-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-arn "${POLICY_ARN}" 2>/dev/null || true

aws iam create-instance-profile --instance-profile-name "${PROFILE_NAME}" 2>/dev/null || true
aws iam add-role-to-instance-profile \
  --instance-profile-name "${PROFILE_NAME}" \
  --role-name "${ROLE_NAME}" 2>/dev/null || true

echo "  -> IAM Profile configured with strictly scoped S3 policy: ${POLICY_ARN}"

# ------------------------------------------------------------------------------
# Step 6: User Data (Automated Provisioning: Docker, Swap, Minikube)
# ------------------------------------------------------------------------------
USER_DATA_FILE=$(mktemp)
cat << 'EOF' > "${USER_DATA_FILE}"
#!/bin/bash
set -e

dnf update -y
dnf install -y git docker htop iptables

systemctl start docker
systemctl enable docker
usermod -aG docker ec2-user

# Configure 2GB Swap space to prevent OOM Killer on memory-constrained t3.medium
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
