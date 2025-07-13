#!/bin/bash

# EMR on EKS Job 실행 스크립트 (4개 역할 포함)

set -e

# 환경 변수 로드
source .env

EXECUTION_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/EMRContainers-JobExecutionRole"

echo "=== EMR on EKS Job 실행 시작 ==="
echo "Virtual Cluster ID: $VIRTUAL_CLUSTER_ID"
echo "Scripts Bucket: s3://$SCRIPTS_BUCKET"

# EMR Job Execution Role 확인/생성
echo "1. EMR Job Execution Role 확인..."
if ! aws iam get-role --role-name EMRContainers-JobExecutionRole >/dev/null 2>&1; then
    echo "   EMR Job Execution Role 생성 중..."
    
    # 신뢰 정책
    cat > /tmp/emr-execution-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "emr-containers.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
    
    # 역할 생성
    aws iam create-role \
        --role-name EMRContainers-JobExecutionRole \
        --assume-role-policy-document file:///tmp/emr-execution-trust-policy.json
    
    # 정책 연결
    aws iam attach-role-policy \
        --role-name EMRContainers-JobExecutionRole \
        --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEMRContainersServiceRolePolicy
    
    aws iam attach-role-policy \
        --role-name EMRContainers-JobExecutionRole \
        --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
    
    aws iam attach-role-policy \
        --role-name EMRContainers-JobExecutionRole \
        --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2
    
    echo "   ✅ EMR Job Execution Role 생성 완료"
else
    echo "   ✅ EMR Job Execution Role 이미 존재함"
fi

# 2. Data Steward Role Job 실행
echo "2. Data Steward 분석 Job 실행..."
DATA_STEWARD_JOB_ID=$(aws emr-containers start-job-run \
    --region $REGION \
    --virtual-cluster-id $VIRTUAL_CLUSTER_ID \
    --name "data-steward-seoul-bike-analysis-$(date +%Y%m%d-%H%M%S)" \
    --execution-role-arn $EXECUTION_ROLE_ARN \
    --release-label "emr-7.2.0-latest" \
    --job-driver '{
        "sparkSubmitJobDriver": {
            "entryPoint": "s3://'$SCRIPTS_BUCKET'/spark-jobs/data-steward-analysis.py",
            "sparkSubmitParameters": "--conf spark.executor.instances=2 --conf spark.executor.memory=4G --conf spark.executor.cores=2 --conf spark.driver.memory=2G"
        }
    }' \
    --configuration-overrides '{
        "applicationConfiguration": [
            {
                "classification": "spark-defaults",
                "properties": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.kubernetes.container.image": "public.ecr.aws/emr-on-eks/spark/emr-7.2.0:latest",
                    "spark.kubernetes.authenticate.driver.serviceAccountName": "emr-data-steward-sa"
                }
            }
        ],
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://'$SCRIPTS_BUCKET'/logs/"
            }
        }
    }' \
    --tags team=data-steward,project=seoul-bike-analytics \
    --query 'id' \
    --output text)

echo "   ✅ Data Steward Job 시작: $DATA_STEWARD_JOB_ID"

# 3. Gangnam Analytics Role Job 실행
echo "3. Gangnam Analytics 분석 Job 실행..."
GANGNAM_JOB_ID=$(aws emr-containers start-job-run \
    --region $REGION \
    --virtual-cluster-id $VIRTUAL_CLUSTER_ID \
    --name "gangnam-analytics-seoul-bike-analysis-$(date +%Y%m%d-%H%M%S)" \
    --execution-role-arn $EXECUTION_ROLE_ARN \
    --release-label "emr-7.2.0-latest" \
    --job-driver '{
        "sparkSubmitJobDriver": {
            "entryPoint": "s3://'$SCRIPTS_BUCKET'/spark-jobs/gangnam-analytics.py",
            "sparkSubmitParameters": "--conf spark.executor.instances=1 --conf spark.executor.memory=2G --conf spark.executor.cores=1 --conf spark.driver.memory=1G"
        }
    }' \
    --configuration-overrides '{
        "applicationConfiguration": [
            {
                "classification": "spark-defaults",
                "properties": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.kubernetes.container.image": "public.ecr.aws/emr-on-eks/spark/emr-7.2.0:latest",
                    "spark.kubernetes.authenticate.driver.serviceAccountName": "emr-gangnam-analytics-sa"
                }
            }
        ],
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://'$SCRIPTS_BUCKET'/logs/"
            }
        }
    }' \
    --tags team=gangnam-analytics,project=seoul-bike-analytics \
    --query 'id' \
    --output text)

echo "   ✅ Gangnam Analytics Job 시작: $GANGNAM_JOB_ID"

# 4. Operation Role Job 실행
echo "4. Operation 분석 Job 실행..."
OPERATION_JOB_ID=$(aws emr-containers start-job-run \
    --region $REGION \
    --virtual-cluster-id $VIRTUAL_CLUSTER_ID \
    --name "operation-seoul-bike-analysis-$(date +%Y%m%d-%H%M%S)" \
    --execution-role-arn $EXECUTION_ROLE_ARN \
    --release-label "emr-7.2.0-latest" \
    --job-driver '{
        "sparkSubmitJobDriver": {
            "entryPoint": "s3://'$SCRIPTS_BUCKET'/spark-jobs/operation-analysis.py",
            "sparkSubmitParameters": "--conf spark.executor.instances=1 --conf spark.executor.memory=2G --conf spark.executor.cores=1 --conf spark.driver.memory=1G"
        }
    }' \
    --configuration-overrides '{
        "applicationConfiguration": [
            {
                "classification": "spark-defaults",
                "properties": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.kubernetes.container.image": "public.ecr.aws/emr-on-eks/spark/emr-7.2.0:latest",
                    "spark.kubernetes.authenticate.driver.serviceAccountName": "emr-operation-sa"
                }
            }
        ],
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://'$SCRIPTS_BUCKET'/logs/"
            }
        }
    }' \
    --tags team=operation,project=seoul-bike-analytics \
    --query 'id' \
    --output text)

echo "   ✅ Operation Job 시작: $OPERATION_JOB_ID"

# 5. Marketing Partner Role Job 실행 (NEW!)
echo "5. Marketing Partner 분석 Job 실행..."
MARKETING_JOB_ID=$(aws emr-containers start-job-run \
    --region $REGION \
    --virtual-cluster-id $VIRTUAL_CLUSTER_ID \
    --name "marketing-partner-seoul-bike-analysis-$(date +%Y%m%d-%H%M%S)" \
    --execution-role-arn $EXECUTION_ROLE_ARN \
    --release-label "emr-7.2.0-latest" \
    --job-driver '{
        "sparkSubmitJobDriver": {
            "entryPoint": "s3://'$SCRIPTS_BUCKET'/spark-jobs/marketing-partner-analysis.py",
            "sparkSubmitParameters": "--conf spark.executor.instances=1 --conf spark.executor.memory=2G --conf spark.executor.cores=1 --conf spark.driver.memory=1G"
        }
    }' \
    --configuration-overrides '{
        "applicationConfiguration": [
            {
                "classification": "spark-defaults",
                "properties": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.kubernetes.container.image": "public.ecr.aws/emr-on-eks/spark/emr-7.2.0:latest",
                    "spark.kubernetes.authenticate.driver.serviceAccountName": "emr-marketing-partner-sa"
                }
            }
        ],
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://'$SCRIPTS_BUCKET'/logs/"
            }
        }
    }' \
    --tags team=marketing-partner,project=seoul-bike-analytics \
    --query 'id' \
    --output text)

echo "   ✅ Marketing Partner Job 시작: $MARKETING_JOB_ID"

# 6. Job ID를 환경 변수 파일에 저장
echo "6. Job ID 저장..."
cat >> .env << EOF

# EMR Job IDs
DATA_STEWARD_JOB_ID=$DATA_STEWARD_JOB_ID
GANGNAM_JOB_ID=$GANGNAM_JOB_ID
OPERATION_JOB_ID=$OPERATION_JOB_ID
MARKETING_JOB_ID=$MARKETING_JOB_ID
EOF

echo "=== 모든 EMR on EKS Job 실행 완료 ==="
echo ""
echo "실행된 Job 목록:"
echo "📊 Data Steward: $DATA_STEWARD_JOB_ID"
echo "🏢 Gangnam Analytics: $GANGNAM_JOB_ID"
echo "⚙️  Operation: $OPERATION_JOB_ID"
echo "🎯 Marketing Partner: $MARKETING_JOB_ID"
echo ""
echo "Job 상태 확인:"
echo "aws emr-containers list-job-runs --virtual-cluster-id $VIRTUAL_CLUSTER_ID --region $REGION"
echo ""
echo "개별 Job 상태 확인:"
echo "aws emr-containers describe-job-run --virtual-cluster-id $VIRTUAL_CLUSTER_ID --id <JOB_ID> --region $REGION"
echo ""
echo "다음 단계: ./scripts/05-verify-and-analyze.sh (Job 완료 후 실행)"
