#!/bin/bash

# S3 Tables 버킷 생성 스크립트
# 고유한 버킷명 생성으로 다중 사용자 환경 지원

set -e

REGION="ap-northeast-2"
NAMESPACE="bike_db"
TABLE_NAME="bike_rental_data"

# 계정 ID 확인
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# 기존 .env 파일이 있으면 기존 버킷 재사용
if [ -f ".env" ] && grep -q "TABLE_BUCKET_NAME=" .env; then
    echo "기존 .env 파일 발견. 기존 버킷 정보를 로드합니다..."
    source .env
    echo "기존 버킷 재사용: $TABLE_BUCKET_NAME"
    REUSE_EXISTING=true
else
    # 새로운 버킷명 생성
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    TABLE_BUCKET_NAME="seoul-bike-demo-${ACCOUNT_ID}-${TIMESTAMP}"
    REUSE_EXISTING=false
fi

echo "=== S3 Tables 버킷 생성 시작 ==="
echo "계정 ID: $ACCOUNT_ID"
echo "리전: $REGION"
echo "버킷명: $TABLE_BUCKET_NAME"
echo "네임스페이스: $NAMESPACE"
echo "테이블명: $TABLE_NAME"
echo ""

# 1. S3 Tables 버킷 생성
echo "1. S3 Tables 버킷 생성 중..."
aws s3tables create-table-bucket \
    --name $TABLE_BUCKET_NAME \
    --region $REGION

echo "✅ S3 Tables 버킷 생성 완료: $TABLE_BUCKET_NAME"

# 2. 네임스페이스 생성
echo ""
echo "2. 네임스페이스 생성 중..."
BUCKET_ARN="arn:aws:s3tables:$REGION:$ACCOUNT_ID:bucket/$TABLE_BUCKET_NAME"

aws s3tables create-namespace \
    --table-bucket-arn $BUCKET_ARN \
    --namespace $NAMESPACE

echo "✅ 네임스페이스 생성 완료: $NAMESPACE"

# 3. 기존 테이블 삭제 (존재하는 경우)
echo ""
echo "3. 기존 테이블 확인 및 삭제 중..."

# 테이블 존재 여부 확인
if aws s3tables get-table \
    --table-bucket-arn $BUCKET_ARN \
    --namespace $NAMESPACE \
    --name $TABLE_NAME \
    --region $REGION >/dev/null 2>&1; then
    
    echo "⚠️  기존 테이블 발견. 삭제 중..."
    aws s3tables delete-table \
        --table-bucket-arn $BUCKET_ARN \
        --namespace $NAMESPACE \
        --name $TABLE_NAME \
        --region $REGION
    
    echo "✅ 기존 테이블 삭제 완료"
    
    # 삭제 완료 대기 (약간의 지연)
    echo "   삭제 완료 대기 중..."
    sleep 5
else
    echo "ℹ️  기존 테이블이 없습니다. 새로 생성합니다."
fi

# 4. S3 Tables API를 통한 테이블 생성
echo ""
echo "4. S3 Tables API를 통한 테이블 생성 중..."

# S3 Tables 카탈로그 이름 설정
S3_TABLES_CATALOG="s3tablescatalog/${TABLE_BUCKET_NAME}"

echo "📋 생성할 테이블 스키마:"
echo "┌─────────────────────┬─────────────┬─────────────────────────────┐"
echo "│ 컬럼명              │ 데이터 타입 │ 설명                        │"
echo "├─────────────────────┼─────────────┼─────────────────────────────┤"
echo "│ rental_id           │ STRING      │ 대여 ID                     │"
echo "│ station_id          │ STRING      │ 정거장 ID                   │"
echo "│ station_name        │ STRING      │ 정거장명                    │"
echo "│ rental_date         │ STRING      │ 대여 일시                   │"
echo "│ return_date         │ STRING      │ 반납 일시                   │"
echo "│ usage_min           │ INT         │ 사용 시간(분)               │"
echo "│ distance_meter      │ DOUBLE      │ 이동 거리(미터)             │"
echo "│ birth_year          │ STRING      │ 출생년도                    │"
echo "│ gender              │ STRING      │ 성별                        │"
echo "│ user_type           │ STRING      │ 사용자 유형                 │"
echo "│ district            │ STRING      │ 구                          │"
echo "└─────────────────────┴─────────────┴─────────────────────────────┘"

echo ""
echo "S3 Tables API를 사용하여 스키마 포함 테이블 생성 중..."

echo "📋 S3 Tables API 메타데이터 스키마:"
echo "   • 11개 컬럼 정의"
echo "   • 필수 필드: rental_id, station_id, station_name, rental_date, return_date"
echo "   • 선택 필드: usage_min, distance_meter, birth_year, gender, user_type, district"
echo "   • Apache Iceberg 형식"

TABLE_RESPONSE=$(aws s3tables create-table \
    --table-bucket-arn $BUCKET_ARN \
    --namespace $NAMESPACE \
    --name $TABLE_NAME \
    --format ICEBERG \
    --metadata '{
        "iceberg": {
            "schema": {
                "fields": [
                    {"name": "rental_id", "type": "string", "required": true},
                    {"name": "station_id", "type": "string", "required": true},
                    {"name": "station_name", "type": "string", "required": true},
                    {"name": "rental_date", "type": "string", "required": true},
                    {"name": "return_date", "type": "string", "required": true},
                    {"name": "usage_min", "type": "int", "required": false},
                    {"name": "distance_meter", "type": "double", "required": false},
                    {"name": "birth_year", "type": "string", "required": false},
                    {"name": "gender", "type": "string", "required": false},
                    {"name": "user_type", "type": "string", "required": false},
                    {"name": "district", "type": "string", "required": false}
                ]
            }
        }
    }' \
    --region $REGION)

if [ $? -eq 0 ]; then
    echo "✅ 테이블 생성 완료: $NAMESPACE.$TABLE_NAME"
    TABLE_ARN=$(echo $TABLE_RESPONSE | jq -r '.tableARN')
    echo "테이블 ARN: $TABLE_ARN"
else
    echo "❌ 테이블 생성 실패"
    exit 1
fi

# 5. 환경 설정 파일 생성
echo ""
echo "5. 환경 설정 파일 생성 중..."

# Athena 결과 버킷 확인/생성
ATHENA_RESULTS_BUCKET="aws-athena-query-results-${ACCOUNT_ID}-${REGION}"
if ! aws s3 ls "s3://${ATHENA_RESULTS_BUCKET}" >/dev/null 2>&1; then
    echo "Athena 결과 버킷 생성 중: ${ATHENA_RESULTS_BUCKET}"
    aws s3 mb "s3://${ATHENA_RESULTS_BUCKET}" --region $REGION
fi

# 생성 정보 변수 설정
CREATED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
CREATED_BY="$(aws sts get-caller-identity --query Arn --output text)"

# CREATE TABLE 쿼리를 파일로 저장 (참고용)
cat > create_s3_table_reference.sql << EOF
-- S3 Tables 테이블 Athena 쿼리 참고용 (실제로는 S3 Tables API 사용)
-- 이 쿼리는 S3 Tables 카탈로그가 Athena에 등록된 후 사용 가능합니다.

SHOW TABLES IN "${S3_TABLES_CATALOG}".${NAMESPACE};

DESCRIBE "${S3_TABLES_CATALOG}".${NAMESPACE}.${TABLE_NAME};

SELECT COUNT(*) FROM "${S3_TABLES_CATALOG}".${NAMESPACE}.${TABLE_NAME};

-- 샘플 데이터 조회
SELECT * FROM "${S3_TABLES_CATALOG}".${NAMESPACE}.${TABLE_NAME} LIMIT 10;
EOF

cat > .env << EOF
# AWS 환경 설정 (자동 생성됨 - 수정하지 마세요)
ACCOUNT_ID=$ACCOUNT_ID
REGION=$REGION

# S3 Tables 설정 (자동 생성됨 - 수정하지 마세요)
TABLE_BUCKET_NAME=$TABLE_BUCKET_NAME
BUCKET_ARN=$BUCKET_ARN
NAMESPACE=$NAMESPACE
TABLE_NAME=$TABLE_NAME
S3_TABLES_CATALOG=$S3_TABLES_CATALOG

# 테이블 스키마 정보 (S3 Tables API로 생성됨)
TABLE_COLUMNS="rental_id,station_id,station_name,rental_date,return_date,usage_min,distance_meter,birth_year,gender,user_type,district"
TABLE_SCHEMA_VERSION="v4_s3_tables_api_with_metadata"
TABLE_CREATION_METHOD="S3_TABLES_API"

# S3 Tables 카탈로그 정보
S3_TABLES_CATALOG="s3tablescatalog/${TABLE_BUCKET_NAME}"

# Athena 설정
ATHENA_RESULTS_BUCKET=$ATHENA_RESULTS_BUCKET

# 참고용 쿼리 파일
ATHENA_REFERENCE_QUERIES="create_s3_table_reference.sql"

# 생성 정보
CREATED_AT="$CREATED_AT"
CREATED_BY="$CREATED_BY"
EOF

echo "✅ 환경 설정 파일 생성 완료: .env"
echo "✅ Athena 참고 쿼리 파일 생성 완료: create_s3_table_reference.sql"

# 6. 설정 요약 출력
echo ""
echo "=== S3 Tables 버킷 및 테이블 생성 완료 ==="
echo ""
echo "📋 생성된 리소스 요약:"
echo "┌─────────────────────────┬─────────────────────────────────────┐"
echo "│ 리소스                  │ 값                                  │"
echo "├─────────────────────────┼─────────────────────────────────────┤"
echo "│ S3 Tables 버킷          │ $TABLE_BUCKET_NAME                  │"
echo "│ 버킷 ARN                │ $BUCKET_ARN                         │"
echo "│ 네임스페이스            │ $NAMESPACE                          │"
echo "│ 테이블                  │ $TABLE_NAME                         │"
echo "│ 테이블 스키마           │ 11개 컬럼 (정의된 타입)             │"
echo "│ 리전                    │ $REGION                             │"
echo "│ Athena 결과 버킷        │ $ATHENA_RESULTS_BUCKET              │"
echo "│ S3 Tables 카탈로그      │ $S3_TABLES_CATALOG                  │"
echo "└─────────────────────────┴─────────────────────────────────────┘"
echo ""
echo "📊 테이블 스키마 정보:"
echo "   • rental_id (STRING) - 대여 ID"
echo "   • station_id (STRING) - 정거장 ID"
echo "   • station_name (STRING) - 정거장명"
echo "   • rental_date (STRING) - 대여 일시"
echo "   • return_date (STRING) - 반납 일시"
echo "   • usage_min (INT) - 사용 시간(분)"
echo "   • distance_meter (DOUBLE) - 이동 거리(미터)"
echo "   • birth_year (STRING) - 출생년도"
echo "   • gender (STRING) - 성별"
echo "   • user_type (STRING) - 사용자 유형"
echo "   • district (STRING) - 구"
echo ""
echo "🔧 환경 설정:"
echo "   • 설정 파일: .env"
echo "   • CREATE TABLE 쿼리: create_s3_table.sql"
echo "   • 다른 스크립트들이 이 설정을 자동으로 사용합니다"
echo "   • 테이블 스키마 정보 포함"
echo ""
echo "📋 **S3 Tables API로 생성된 테이블**:"
echo "   • 버킷: ${TABLE_BUCKET_NAME}"
echo "   • 네임스페이스: ${NAMESPACE}"
echo "   • 테이블: ${TABLE_NAME}"
echo "   • 형식: Apache Iceberg"
echo "   • 스키마: 11개 컬럼 (메타데이터 포함)"
echo ""
echo "📋 **Athena에서 테이블 확인**:"
echo "   SHOW TABLES IN \"${S3_TABLES_CATALOG}\".${NAMESPACE};"
echo "   DESCRIBE \"${S3_TABLES_CATALOG}\".${NAMESPACE}.${TABLE_NAME};"
echo ""
echo "⚠️  중요사항:"
echo "   • .env 파일을 삭제하지 마세요"
echo "   • 버킷명은 계정별로 고유하게 생성됩니다"
echo "   • 테이블은 S3 Tables API로 스키마 포함하여 생성되었습니다"
echo "   • Athena 참고 쿼리는 create_s3_table_reference.sql 파일에 저장되었습니다"
echo "   • S3 Tables 카탈로그는 Athena에서 바로 사용 가능합니다"
echo ""
echo "✅ 다음 단계: ./scripts/02-load-data-to-s3-tables.sh"
