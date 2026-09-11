#!/usr/bin/env bash
# ==============================================================================
# Script: teardown.sh
# Purpose: Clean up all AWS resources created for the Social Media Lakehouse
#          to avoid unwanted cloud charges.
# ==============================================================================

set -euo pipefail

PROJECT_NAME="social-media-lakehouse"
AWS_REGION="${AWS_DEFAULT_REGION:-ap-south-2}"

echo "=================================================================="
echo " TEARDOWN: Destroying AWS Resources for ${PROJECT_NAME} in ${AWS_REGION}"
echo "=================================================================="

# 1. Terminate EC2 Instances
echo "1. Terminating EC2 Instances tagged Name=${PROJECT_NAME}-node..."
INSTANCE_IDS=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=${PROJECT_NAME}-node" "Name=instance-state-name,Values=running,stopped,pending" \
  --region "${AWS_REGION}" \
  --query 'Reservations[*].Instances[*].InstanceId' \
  --output text)

if [ -n "${INSTANCE_IDS}" ]; then
  aws ec2 terminate-instances --instance-ids ${INSTANCE_IDS} --region "${AWS_REGION}" > /dev/null
  echo "  Waiting for instance termination..."
  aws ec2 wait instance-terminated --instance-ids ${INSTANCE_IDS} --region "${AWS_REGION}"
  echo "  -> EC2 Instances terminated."
else
  echo "  -> No running instances found."
fi

# 2. Delete IAM Role and Instance Profile
echo "2. Cleaning up IAM Instance Profile and Role..."
PROFILE_NAME="${PROJECT_NAME}-ec2-profile"
ROLE_NAME="${PROJECT_NAME}-ec2-role"

aws iam remove-role-from-instance-profile --instance-profile-name "${PROFILE_NAME}" --role-name "${ROLE_NAME}" 2>/dev/null || true
aws iam delete-instance-profile --instance-profile-name "${PROFILE_NAME}" 2>/dev/null || true
aws iam detach-role-policy --role-name "${ROLE_NAME}" --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess 2>/dev/null || true
aws iam delete-role --role-name "${ROLE_NAME}" 2>/dev/null || true
echo "  -> IAM resources deleted."

# 3. Delete Security Group
echo "3. Deleting Security Group..."
SG_ID=$(aws ec2 describe-security-groups \
  --filters "Name=group-name,Values=${PROJECT_NAME}-sg" \
  --region "${AWS_REGION}" \
  --query 'SecurityGroups[0].GroupId' \
  --output text 2>/dev/null || true)

if [ -n "${SG_ID}" ] && [ "${SG_ID}" != "None" ]; then
  aws ec2 delete-security-group --group-id "${SG_ID}" --region "${AWS_REGION}" 2>/dev/null || true
  echo "  -> Security Group deleted: ${SG_ID}"
fi

# 4. Detach and Delete Internet Gateway
echo "4. Detaching & Deleting Internet Gateway..."
VPC_ID=$(aws ec2 describe-vpcs \
  --filters "Name=tag:Name,Values=${PROJECT_NAME}-vpc" \
  --region "${AWS_REGION}" \
  --query 'Vpcs[0].VpcId' \
  --output text 2>/dev/null || true)

if [ -n "${VPC_ID}" ] && [ "${VPC_ID}" != "None" ]; then
  IGW_ID=$(aws ec2 describe-internet-gateways \
    --filters "Name=attachment.vpc-id,Values=${VPC_ID}" \
    --region "${AWS_REGION}" \
    --query 'InternetGateways[0].InternetGatewayId' \
    --output text 2>/dev/null || true)

  if [ -n "${IGW_ID}" ] && [ "${IGW_ID}" != "None" ]; then
    aws ec2 detach-internet-gateway --internet-gateway-id "${IGW_ID}" --vpc-id "${VPC_ID}" --region "${AWS_REGION}" 2>/dev/null || true
    aws ec2 delete-internet-gateway --internet-gateway-id "${IGW_ID}" --region "${AWS_REGION}" 2>/dev/null || true
    echo "  -> IGW detached & deleted: ${IGW_ID}"
  fi

  # 5. Delete Subnets
  echo "5. Deleting Subnets..."
  SUBNET_IDS=$(aws ec2 describe-subnets \
    --filters "Name=vpc-id,Values=${VPC_ID}" \
    --region "${AWS_REGION}" \
    --query 'Subnets[*].SubnetId' \
    --output text)

  for SUB in ${SUBNET_IDS}; do
    aws ec2 delete-subnet --subnet-id "${SUB}" --region "${AWS_REGION}" 2>/dev/null || true
    echo "  -> Deleted Subnet: ${SUB}"
  done

  # 6. Delete Route Tables (custom)
  echo "6. Deleting Route Tables..."
  RT_IDS=$(aws ec2 describe-route-tables \
    --filters "Name=vpc-id,Values=${VPC_ID}" "Name=tag:Name,Values=${PROJECT_NAME}-public-rt" \
    --region "${AWS_REGION}" \
    --query 'RouteTables[*].RouteTableId' \
    --output text)

  for RT in ${RT_IDS}; do
    aws ec2 delete-route-table --route-table-id "${RT}" --region "${AWS_REGION}" 2>/dev/null || true
    echo "  -> Deleted Route Table: ${RT}"
  done

  # 7. Delete VPC
  echo "7. Deleting VPC: ${VPC_ID}..."
  aws ec2 delete-vpc --vpc-id "${VPC_ID}" --region "${AWS_REGION}" 2>/dev/null || true
  echo "  -> Custom VPC deleted."
fi

echo "=================================================================="
echo " Teardown Complete! Zero charges will accrue."
echo "=================================================================="
