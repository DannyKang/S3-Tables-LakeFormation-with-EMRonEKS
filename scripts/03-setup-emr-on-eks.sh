#!/bin/bash

# EMR on EKS 클러스터 설정 스크립트 (4개 역할 포함)

set -e

# 환경 변수 로드
source .env

CLUSTER_NAME="seoul-bike-analytics-cluster"
NAMESPACE="emr-on-eks-seoul-bike"

echo "=== EMR on EKS 클러스터 설정 시작 ==="

# 1. EKS 클러스터 존재 확인
echo "1. EKS 클러스터 확인: $CLUSTER_NAME"
if ! aws eks describe-cluster --name $CLUSTER_NAME --region $REGION >/dev/null 2>&1; then
    echo "   ⚠️  EKS 클러스터가 존재하지 않습니다."
    echo "   다음 명령어로 EKS 클러스터를 생성하세요:"
    echo "   eksctl create cluster --name $CLUSTER_NAME --region $REGION --nodegroup-name workers --node-type m5.large --nodes 2"
    exit 1
else
    echo "   ✅ EKS 클러스터 확인됨"
fi

# 2. EMR on EKS 네임스페이스 생성
echo "2. EMR on EKS 네임스페이스 생성: $NAMESPACE"
kubectl create namespace $NAMESPACE || echo "   네임스페이스가 이미 존재합니다."

# 3. IAM 역할 생성 (존재하지 않는 경우)
echo "3. IAM 역할 확인 및 생성..."

# 역할 목록
ROLES=("LF_DataStewardRole" "LF_GangnamAnalyticsRole" "LF_OperationRole" "LF_MarketingPartnerRole")

for role in "${ROLES[@]}"; do
    echo "   $role 확인 중..."
    if ! aws iam get-role --role-name $role >/dev/null 2>&1; then
        echo "   $role 생성 중..."
        
        # 신뢰 정책 생성
        cat > /tmp/${role}-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "eks.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::${ACCOUNT_ID}:root"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
        
        # 역할 생성
        aws iam create-role \
            --role-name $role \
            --assume-role-policy-document file:///tmp/${role}-trust-policy.json
        
        # Lake Formation 권한 정책 연결
        aws iam attach-role-policy \
            --role-name $role \
            --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEMRContainersServiceRolePolicy
        
        aws iam attach-role-policy \
            --role-name $role \
            --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
        
        # Lake Formation 접근 정책 생성 및 연결
        cat > /tmp/${role}-lakeformation-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess",
                "lakeformation:GetWorkUnits",
                "lakeformation:StartQueryPlanning",
                "lakeformation:GetWorkUnitResults",
                "lakeformation:GetQueryState",
                "lakeformation:GetQueryStatistics"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetPartition",
                "glue:GetPartitions"
            ],
            "Resource": "*"
        }
    ]
}
EOF
        
        aws iam put-role-policy \
            --role-name $role \
            --policy-name LakeFormationAccess \
            --policy-document file:///tmp/${role}-lakeformation-policy.json
        
        echo "   ✅ $role 생성 완료"
    else
        echo "   ✅ $role 이미 존재함"
    fi
done

# 4. 서비스 계정 생성 및 IAM 역할 연결
echo "4. 서비스 계정 생성..."

# Data Steward 서비스 계정
echo "   Data Steward 서비스 계정 생성..."
eksctl create iamserviceaccount \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --name=emr-data-steward-sa \
    --namespace=$NAMESPACE \
    --attach-role-arn=arn:aws:iam::${ACCOUNT_ID}:role/LF_DataStewardRole \
    --approve || echo "   서비스 계정이 이미 존재합니다."

# Gangnam Analytics 서비스 계정
echo "   Gangnam Analytics 서비스 계정 생성..."
eksctl create iamserviceaccount \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --name=emr-gangnam-analytics-sa \
    --namespace=$NAMESPACE \
    --attach-role-arn=arn:aws:iam::${ACCOUNT_ID}:role/LF_GangnamAnalyticsRole \
    --approve || echo "   서비스 계정이 이미 존재합니다."

# Operation 서비스 계정
echo "   Operation 서비스 계정 생성..."
eksctl create iamserviceaccount \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --name=emr-operation-sa \
    --namespace=$NAMESPACE \
    --attach-role-arn=arn:aws:iam::${ACCOUNT_ID}:role/LF_OperationRole \
    --approve || echo "   서비스 계정이 이미 존재합니다."

# Marketing Partner 서비스 계정 (NEW!)
echo "   Marketing Partner 서비스 계정 생성..."
eksctl create iamserviceaccount \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --name=emr-marketing-partner-sa \
    --namespace=$NAMESPACE \
    --attach-role-arn=arn:aws:iam::${ACCOUNT_ID}:role/LF_MarketingPartnerRole \
    --approve || echo "   서비스 계정이 이미 존재합니다."

# 5. EMR Virtual Cluster 생성
echo "5. EMR Virtual Cluster 생성..."
VIRTUAL_CLUSTER_ID=$(aws emr-containers create-virtual-cluster \
    --region $REGION \
    --name seoul-bike-analytics-vc \
    --container-provider '{
        "type": "EKS",
        "id": "'$CLUSTER_NAME'",
        "info": {
            "eksInfo": {
                "namespace": "'$NAMESPACE'"
            }
        }
    }' \
    --query 'id' \
    --output text 2>/dev/null || echo "")

if [ -z "$VIRTUAL_CLUSTER_ID" ]; then
    # 기존 Virtual Cluster 확인
    VIRTUAL_CLUSTER_ID=$(aws emr-containers list-virtual-clusters \
        --region $REGION \
        --query 'virtualClusters[?name==`seoul-bike-analytics-vc` && state==`RUNNING`].id' \
        --output text)
    
    if [ -z "$VIRTUAL_CLUSTER_ID" ]; then
        echo "   ❌ Virtual Cluster 생성/조회 실패"
        exit 1
    else
        echo "   ✅ 기존 Virtual Cluster 사용: $VIRTUAL_CLUSTER_ID"
    fi
else
    echo "   ✅ Virtual Cluster 생성 완료: $VIRTUAL_CLUSTER_ID"
fi

# 6. 환경 변수 파일 업데이트
echo "6. 환경 변수 파일 업데이트..."
cat >> .env << EOF

# EMR on EKS 환경 변수
CLUSTER_NAME=$CLUSTER_NAME
NAMESPACE=$NAMESPACE
VIRTUAL_CLUSTER_ID=$VIRTUAL_CLUSTER_ID
EOF

# 7. Spark 코드 S3 업로드
echo "7. Spark 코드 S3 업로드..."
SCRIPTS_BUCKET="seoul-bike-analytics-scripts-${ACCOUNT_ID}"

# S3 버킷 생성
aws s3 mb s3://$SCRIPTS_BUCKET --region $REGION || echo "   버킷이 이미 존재합니다."

# Spark 코드 업로드
aws s3 cp spark-jobs/ s3://$SCRIPTS_BUCKET/spark-jobs/ --recursive

echo "   ✅ Spark 코드 업로드 완료: s3://$SCRIPTS_BUCKET/spark-jobs/"

# 환경 변수에 추가
cat >> .env << EOF
SCRIPTS_BUCKET=$SCRIPTS_BUCKET
EOF

echo "=== EMR on EKS 클러스터 설정 완료 ==="
echo ""
echo "설정된 리소스:"
echo "🏗️  EKS 클러스터: $CLUSTER_NAME"
echo "📦 네임스페이스: $NAMESPACE"
echo "🎭 IAM 역할: 4개 (DataSteward, GangnamAnalytics, Operation, MarketingPartner)"
echo "👤 서비스 계정: 4개"
echo "🚀 Virtual Cluster: $VIRTUAL_CLUSTER_ID"
echo "📁 Spark 코드 버킷: s3://$SCRIPTS_BUCKET"
echo ""
echo "다음 단계: ./scripts/04-run-emr-jobs.sh"
