#!/bin/bash

# 샘플 데이터 설정 스크립트
# S3 Tables 버킷 생성 및 샘플 데이터 업로드

set -e

REGION="ap-northeast-2"
TABLE_BUCKET_NAME="seoul-bike-rental-data-202506"
NAMESPACE="bike_db"
TABLE_NAME="bike_rental_data"
SAMPLE_DATA_FILE="sample-data/seoul-bike-rental-sample.csv"

echo "=== 샘플 데이터 설정 시작 ==="

# 1. S3 Tables 버킷 생성
echo "1. S3 Tables 버킷 생성: $TABLE_BUCKET_NAME"
aws s3tables create-table-bucket \
    --region $REGION \
    --name $TABLE_BUCKET_NAME \
    --table-bucket-configuration '{
        "locationConstraint": {
            "bucketLocationConstraint": "'$REGION'"
        }
    }' || echo "버킷이 이미 존재하거나 생성 중입니다."

# 2. AWS Analytics Services 통합 활성화 (콘솔에서 수동으로 해야 함)
echo "2. AWS Analytics Services 통합 활성화"
echo "   ⚠️  S3 콘솔에서 수동으로 활성화해야 합니다:"
echo "   1. S3 콘솔 → Table buckets → $TABLE_BUCKET_NAME"
echo "   2. Properties 탭 → AWS analytics services integration → Enable"

# 3. 네임스페이스 생성
echo "3. 네임스페이스 생성: $NAMESPACE"
aws s3tables create-namespace \
    --region $REGION \
    --table-bucket-arn "arn:aws:s3tables:$REGION:$(aws sts get-caller-identity --query Account --output text):bucket/$TABLE_BUCKET_NAME" \
    --namespace "$NAMESPACE" || echo "네임스페이스가 이미 존재합니다."

# 4. 테이블 생성
echo "4. 테이블 생성: $TABLE_NAME"
aws s3tables create-table \
    --region $REGION \
    --table-bucket-arn "arn:aws:s3tables:$REGION:$(aws sts get-caller-identity --query Account --output text):bucket/$TABLE_BUCKET_NAME" \
    --namespace "$NAMESPACE" \
    --name "$TABLE_NAME" \
    --format "ICEBERG" || echo "테이블이 이미 존재합니다."

# 5. 샘플 데이터를 임시 S3 버킷에 업로드
TEMP_BUCKET="seoul-bike-temp-data-$(date +%s)"
echo "5. 임시 S3 버킷 생성 및 샘플 데이터 업로드: $TEMP_BUCKET"

aws s3 mb s3://$TEMP_BUCKET --region $REGION
aws s3 cp $SAMPLE_DATA_FILE s3://$TEMP_BUCKET/sample-data/

echo "6. 샘플 데이터 위치: s3://$TEMP_BUCKET/sample-data/"

# 6. Spark Job을 통한 데이터 로드 안내
echo "7. 데이터 로드 안내"
echo "   다음 단계에서 EMR on EKS Spark Job을 통해 샘플 데이터를 S3 Tables로 로드합니다."
echo "   임시 데이터 위치: s3://$TEMP_BUCKET/sample-data/"

# 7. 환경 변수 파일 생성
cat > .env << EOF
# S3 Tables 환경 변수
TABLE_BUCKET_NAME=$TABLE_BUCKET_NAME
NAMESPACE=$NAMESPACE
TABLE_NAME=$TABLE_NAME
TEMP_BUCKET=$TEMP_BUCKET
REGION=$REGION
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
EOF

echo "8. 환경 변수 파일 생성: .env"

echo "=== 샘플 데이터 설정 완료 ==="
echo ""
echo "다음 단계:"
echo "1. S3 콘솔에서 AWS Analytics Services 통합 활성화"
echo "2. Lake Formation 권한 설정: ./scripts/02-setup-lakeformation-permissions.sh"
echo ""
echo "생성된 리소스:"
echo "- S3 Tables 버킷: $TABLE_BUCKET_NAME"
echo "- 네임스페이스: $NAMESPACE"
echo "- 테이블: $TABLE_NAME"
echo "- 임시 S3 버킷: $TEMP_BUCKET"
