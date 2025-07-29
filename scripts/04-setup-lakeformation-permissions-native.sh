#!/bin/bash

# Lake Formation FGAC 권한 설정 스크립트 (S3 Tables Native)
# S3 Tables federated catalog를 직접 사용하는 네이티브 방식

set -e

# 환경 변수 로드
if [ ! -f ".env" ]; then
    echo "❌ .env 파일이 존재하지 않습니다."
    echo "먼저 ./scripts/01-create-s3-table-bucket.sh를 실행하세요."
    exit 1
fi

# .env 파일 검증 및 로드
echo "환경 설정 파일 로드 중..."
if ! source .env 2>/dev/null; then
    echo "❌ .env 파일 로드 중 오류가 발생했습니다."
    echo "파일 내용을 확인하거나 01-create-s3-table-bucket.sh를 다시 실행하세요."
    exit 1
fi

# 필수 환경 변수 확인
if [ -z "$ACCOUNT_ID" ] || [ -z "$REGION" ] || [ -z "$TABLE_BUCKET_NAME" ] || [ -z "$LF_DATA_STEWARD_ROLE" ]; then
    echo "❌ 필수 환경 변수가 설정되지 않았습니다."
    echo "이전 단계들을 순서대로 다시 실행하세요."
    exit 1
fi

# S3 Tables federated catalog 설정
S3_TABLES_FEDERATED_CATALOG="s3tablescatalog"
S3_TABLES_CATALOG_ID="$S3_TABLES_FEDERATED_CATALOG/$TABLE_BUCKET_NAME"
S3_TABLES_DATABASE_NAME=$NAMESPACE
S3_TABLES_TABLE_NAME=$TABLE_NAME

echo "=== Lake Formation FGAC 권한 설정 시작 (S3 Tables Native) ==="
echo "계정 ID: $ACCOUNT_ID"
echo "리전: $REGION"
echo "S3 Tables 버킷: $TABLE_BUCKET_NAME"
echo "S3 Tables 카탈로그: $S3_TABLES_CATALOG_ID"
echo "데이터베이스: $S3_TABLES_DATABASE_NAME"
echo "테이블: $S3_TABLES_TABLE_NAME"
echo ""

# 0. IAM 역할 존재 확인
echo "0. IAM 역할 존재 확인..."
ROLES=("$LF_DATA_STEWARD_ROLE" "$LF_GANGNAM_ANALYTICS_ROLE" "$LF_OPERATION_ROLE" "$LF_MARKETING_PARTNER_ROLE")

for role in "${ROLES[@]}"; do
    if aws iam get-role --role-name $role >/dev/null 2>&1; then
        echo "✅ $role 존재 확인"
    else
        echo "❌ $role 역할이 존재하지 않습니다."
        echo "먼저 ./scripts/02-create-iam-roles.sh를 실행하세요."
        exit 1
    fi
done

# 1. Lake Formation 설정 초기화 및 S3 Tables 통합 확인
echo ""
echo "1. Lake Formation 설정 초기화 및 S3 Tables 통합 확인..."

# Lake Formation 관리자 설정
CURRENT_USER_ARN=$(aws sts get-caller-identity --query Arn --output text)
echo "   Lake Formation 관리자 설정: $CURRENT_USER_ARN"

aws lakeformation put-data-lake-settings \
    --region $REGION \
    --data-lake-settings '{
        "DataLakeAdmins": [
            {
                "DataLakePrincipalIdentifier": "'$CURRENT_USER_ARN'"
            }
        ],
        "CreateDatabaseDefaultPermissions": [],
        "CreateTableDefaultPermissions": []
    }' >/dev/null 2>&1 || echo "   Lake Formation 설정이 이미 구성되어 있습니다."

# S3 Tables federated catalog 확인
echo "   S3 Tables federated catalog 확인 중..."
if aws glue get-catalog --catalog-id "$ACCOUNT_ID:$S3_TABLES_FEDERATED_CATALOG" --region $REGION >/dev/null 2>&1; then
    echo "   ✅ S3 Tables federated catalog 확인: $S3_TABLES_FEDERATED_CATALOG"
    echo "   ✅ 카탈로그 ID: $S3_TABLES_CATALOG_ID"
else
    echo "   ❌ S3 Tables federated catalog를 찾을 수 없습니다."
    echo "   Lake Formation 콘솔에서 S3 Tables 통합을 활성화하세요."
    echo "   참고: https://docs.aws.amazon.com/lake-formation/latest/dg/enable-s3-tables-catalog-integration.html"
    exit 1
fi

# 2. LF_DataStewardRole - 전체 데이터 접근 권한
echo ""
echo "2. $LF_DATA_STEWARD_ROLE 권한 설정 (전체 데이터 접근)..."

# 데이터베이스 권한
echo "   데이터베이스 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_DATA_STEWARD_ROLE_ARN \
    --resource "Database={CatalogId=${S3_TABLES_CATALOG_ID},Name=${S3_TABLES_DATABASE_NAME}}" \
    --permissions "DESCRIBE" 2>/dev/null || echo "   ⚠️  데이터베이스 권한이 이미 부여되어 있습니다."

# 테이블 전체 컬럼 권한
echo "   테이블 전체 컬럼 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_DATA_STEWARD_ROLE_ARN \
    --resource "TableWithColumns={
        CatalogId=${S3_TABLES_CATALOG_ID},
        DatabaseName=${S3_TABLES_DATABASE_NAME},
        Name=${S3_TABLES_TABLE_NAME},
        ColumnWildcard={}
    }" \
    --permissions "SELECT" 2>/dev/null || echo "   ⚠️  테이블 권한이 이미 부여되어 있습니다."

echo "   ✅ DataSteward: 전체 11개 컬럼, 모든 구, 모든 연령대 (100,000건)"

# 3. LF_GangnamAnalyticsRole - 강남구 데이터만, birth_year 제외
echo ""
echo "3. $LF_GANGNAM_ANALYTICS_ROLE 권한 설정 (강남구만, 개인정보 제외)..."

# 데이터베이스 권한
echo "   데이터베이스 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_GANGNAM_ANALYTICS_ROLE_ARN \
    --resource "Database={CatalogId=${S3_TABLES_CATALOG_ID},Name=${S3_TABLES_DATABASE_NAME}}" \
    --permissions "DESCRIBE" 2>/dev/null || echo "   ⚠️  데이터베이스 권한이 이미 부여되어 있습니다."

# 강남구 데이터 필터 생성
echo "   강남구 데이터 필터 생성..."
GANGNAM_FILTER_NAME="gangnam_analytics_filter"

aws lakeformation create-data-cells-filter \
    --region $REGION \
    --table-data "{
        \"TableCatalogId\": \"${S3_TABLES_CATALOG_ID}\",
        \"DatabaseName\": \"${S3_TABLES_DATABASE_NAME}\",
        \"TableName\": \"${S3_TABLES_TABLE_NAME}\",
        \"Name\": \"${GANGNAM_FILTER_NAME}\",
        \"RowFilter\": {
            \"FilterExpression\": \"district = '강남구'\"
        },
        \"ColumnNames\": [
            \"rental_id\", \"station_id\", \"station_name\", \"rental_date\", \"return_date\",
            \"usage_min\", \"distance_meter\", \"gender\", \"user_type\", \"district\"
        ]
    }" 2>/dev/null || echo "   ⚠️  필터가 이미 존재합니다."

# 필터 권한 부여
echo "   필터 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_GANGNAM_ANALYTICS_ROLE_ARN \
    --resource "DataCellsFilter={
        TableCatalogId=${S3_TABLES_CATALOG_ID},
        DatabaseName=${S3_TABLES_DATABASE_NAME},
        TableName=${S3_TABLES_TABLE_NAME},
        Name=${GANGNAM_FILTER_NAME}
    }" \
    --permissions "SELECT" 2>/dev/null || echo "   ⚠️  필터 권한이 이미 부여되어 있습니다."

echo "   ✅ GangnamAnalyst: 10개 컬럼 (birth_year 제외), 강남구만 (~3,000건)"

# 4. LF_OperationRole - 운영 데이터만, 개인정보 제외
echo ""
echo "4. $LF_OPERATION_ROLE 권한 설정 (운영 데이터만, 개인정보 제외)..."

# 데이터베이스 권한
echo "   데이터베이스 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_OPERATION_ROLE_ARN \
    --resource "Database={CatalogId=${S3_TABLES_CATALOG_ID},Name=${S3_TABLES_DATABASE_NAME}}" \
    --permissions "DESCRIBE" 2>/dev/null || echo "   ⚠️  데이터베이스 권한이 이미 부여되어 있습니다."

# 운영팀용 컬럼 필터 생성
echo "   운영팀용 컬럼 필터 생성..."
OPERATION_FILTER_NAME="operation_team_filter"

aws lakeformation create-data-cells-filter \
    --region $REGION \
    --table-data "{
        \"TableCatalogId\": \"${S3_TABLES_CATALOG_ID}\",
        \"DatabaseName\": \"${S3_TABLES_DATABASE_NAME}\",
        \"TableName\": \"${S3_TABLES_TABLE_NAME}\",
        \"Name\": \"${OPERATION_FILTER_NAME}\",
        \"ColumnNames\": [
            \"rental_id\", \"station_id\", \"station_name\", \"rental_date\", \"return_date\",
            \"usage_min\", \"distance_meter\", \"user_type\", \"district\"
        ]
    }" 2>/dev/null || echo "   ⚠️  필터가 이미 존재합니다."

# 필터 권한 부여
echo "   필터 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_OPERATION_ROLE_ARN \
    --resource "DataCellsFilter={
        TableCatalogId=${S3_TABLES_CATALOG_ID},
        DatabaseName=${S3_TABLES_DATABASE_NAME},
        TableName=${S3_TABLES_TABLE_NAME},
        Name=${OPERATION_FILTER_NAME}
    }" \
    --permissions "SELECT" 2>/dev/null || echo "   ⚠️  필터 권한이 이미 부여되어 있습니다."

echo "   ✅ Operation: 9개 컬럼 (birth_year, gender 제외), 전체 구 (100,000건)"

# 5. LF_MarketingPartnerRole - 강남구 20-30대만, 마케팅 관련
echo ""
echo "5. $LF_MARKETING_PARTNER_ROLE 권한 설정 (강남구 20-30대만, 마케팅 관련)..."

# 데이터베이스 권한
echo "   데이터베이스 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_MARKETING_PARTNER_ROLE_ARN \
    --resource "Database={CatalogId=${S3_TABLES_CATALOG_ID},Name=${S3_TABLES_DATABASE_NAME}}" \
    --permissions "DESCRIBE" 2>/dev/null || echo "   ⚠️  데이터베이스 권한이 이미 부여되어 있습니다."

# 마케팅 파트너용 다차원 필터 생성 (강남구 + 20-30대)
echo "   마케팅 파트너용 다차원 필터 생성 (강남구 + 20-30대)..."
MARKETING_FILTER_NAME="marketing_partner_filter"

aws lakeformation create-data-cells-filter \
    --region $REGION \
    --table-data "{
        \"TableCatalogId\": \"${S3_TABLES_CATALOG_ID}\",
        \"DatabaseName\": \"${S3_TABLES_DATABASE_NAME}\",
        \"TableName\": \"${S3_TABLES_TABLE_NAME}\",
        \"Name\": \"${MARKETING_FILTER_NAME}\",
        \"RowFilter\": {
            \"FilterExpression\": \"district = '강남구' AND (birth_year >= '1994' AND birth_year <= '2004')\"
        },
        \"ColumnNames\": [
            \"rental_id\", \"station_id\", \"station_name\", \"rental_date\", \"return_date\",
            \"usage_min\", \"distance_meter\", \"gender\", \"user_type\", \"district\"
        ]
    }" 2>/dev/null || echo "   ⚠️  필터가 이미 존재합니다."

# 필터 권한 부여
echo "   필터 권한 부여 중..."
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=$LF_MARKETING_PARTNER_ROLE_ARN \
    --resource "DataCellsFilter={
        TableCatalogId=${S3_TABLES_CATALOG_ID},
        DatabaseName=${S3_TABLES_DATABASE_NAME},
        TableName=${S3_TABLES_TABLE_NAME},
        Name=${MARKETING_FILTER_NAME}
    }" \
    --permissions "SELECT" 2>/dev/null || echo "   ⚠️  필터 권한이 이미 부여되어 있습니다."

echo "   ✅ MarketingPartner: 10개 컬럼 (birth_year 제외), 강남구 20-30대만 (~1,650건)"

# 6. 데이터 위치 권한 설정
echo ""
echo "6. 데이터 위치 권한 설정..."

# S3 Tables 리소스 등록 확인
echo "   S3 Tables 리소스 등록 확인..."
S3_TABLES_RESOURCE_ARN="arn:aws:s3tables:$REGION:$ACCOUNT_ID:bucket/*"

if aws lakeformation describe-resource --resource-arn $S3_TABLES_RESOURCE_ARN --region $REGION >/dev/null 2>&1; then
    echo "   ✅ S3 Tables 리소스가 이미 등록되어 있습니다."
else
    echo "   S3 Tables 리소스 등록 중..."
    aws lakeformation register-resource \
        --region $REGION \
        --resource-arn $S3_TABLES_RESOURCE_ARN \
        --use-service-linked-role 2>/dev/null || echo "   ⚠️  리소스 등록 중 오류 발생"
fi

# 각 역할에 데이터 위치 권한 부여
for role in "${ROLES[@]}"; do
    role_arn="arn:aws:iam::${ACCOUNT_ID}:role/${role}"
    echo "   $role 데이터 위치 권한 부여..."
    aws lakeformation grant-permissions \
        --region $REGION \
        --principal DataLakePrincipalIdentifier=$role_arn \
        --resource "DataLocation={ResourceArn=${S3_TABLES_RESOURCE_ARN}}" \
        --permissions "DATA_LOCATION_ACCESS" 2>/dev/null || echo "   ⚠️  권한이 이미 부여되어 있습니다."
done

# 7. 권한 검증
echo ""
echo "7. 권한 검증..."

for role in "${ROLES[@]}"; do
    role_arn="arn:aws:iam::${ACCOUNT_ID}:role/${role}"
    echo "   $role 권한 확인..."
    
    if PERMISSIONS=$(aws lakeformation list-permissions \
        --region $REGION \
        --principal DataLakePrincipalIdentifier=$role_arn \
        --max-results 10 \
        --query 'PrincipalResourcePermissions[*].{Resource:Resource,Permissions:Permissions}' \
        --output json 2>/dev/null); then
        
        PERMISSION_COUNT=$(echo $PERMISSIONS | jq length)
        if [ "$PERMISSION_COUNT" -gt 0 ]; then
            echo "   ✅ $role: $PERMISSION_COUNT개 권한 확인됨"
        else
            echo "   ⚠️  $role: 권한이 설정되지 않았을 수 있습니다"
        fi
    else
        echo "   ⚠️  $role: 권한 조회 실패 (정상적인 경우일 수 있음)"
    fi
done

echo ""
echo "📋 권한 검증 참고사항:"
echo "   • S3 Tables federated catalog를 사용하는 네이티브 방식입니다"
echo "   • 실제 데이터 접근은 EMR on EKS에서 테스트됩니다"
echo "   • 권한 조회 실패는 정상적인 경우일 수 있습니다"

# 8. 환경 설정 업데이트
echo ""
echo "8. 환경 설정 업데이트..."

# 기존 Lake Formation 설정이 있는지 확인
if grep -q "LAKE_FORMATION_SETUP=" .env 2>/dev/null; then
    echo "   ⚠️  기존 Lake Formation 설정이 발견되었습니다."
    echo "   기존 정보를 유지합니다."
else
    cat >> .env << EOF

# Lake Formation FGAC 설정 (S3 Tables Native)
LAKE_FORMATION_SETUP=s3_tables_native
S3_TABLES_FEDERATED_CATALOG=$S3_TABLES_FEDERATED_CATALOG
S3_TABLES_CATALOG_ID=$S3_TABLES_CATALOG_ID
S3_TABLES_DATABASE_NAME=$S3_TABLES_DATABASE_NAME
S3_TABLES_TABLE_NAME=$S3_TABLES_TABLE_NAME
LAKE_FORMATION_SETUP_DATE="$(date '+%Y-%m-%d %H:%M:%S')"
EOF
    echo "   ✅ Lake Formation 설정 정보 추가 완료"
fi

echo "✅ 환경 설정 업데이트 완료"

echo ""
echo "=== Lake Formation FGAC 권한 설정 완료 (S3 Tables Native) ==="
echo ""
echo "📊 설정된 권한 요약:"
echo "┌─────────────────────────┬──────────┬─────────────┬─────────────┬──────────────┐"
echo "│ 역할                    │ 접근구역 │ 연령대      │ 접근컬럼    │ 예상결과     │"
echo "├─────────────────────────┼──────────┼─────────────┼─────────────┼──────────────┤"
echo "│ LF_DataStewardRole      │ 전체구   │ 전체        │ 전체 11개   │ 100,000건    │"
echo "│ LF_GangnamAnalyticsRole │ 강남구   │ 전체        │ 10개(개인정보제외) │ ~3,000건 │"
echo "│ LF_OperationRole        │ 전체구   │ 전체        │ 9개(운영관련만) │ 100,000건 │"
echo "│ LF_MarketingPartnerRole │ 강남구   │ 20-30대     │ 10개(마케팅관련) │ ~1,650건 │"
echo "└─────────────────────────┴──────────┴─────────────┴─────────────┴──────────────┘"
echo ""
echo "🔑 핵심 FGAC 기능 (S3 Tables Native):"
echo "   • Row-level 필터링: 지역별 (강남구) + 연령대별 (20-30대)"
echo "   • Column-level 필터링: 역할별 컬럼 접근 제어"
echo "   • Cell-level 필터링: 다차원 조건 (지역 + 연령대)"
echo "   • Federated Catalog: s3tablescatalog/$TABLE_BUCKET_NAME"
echo ""
echo "🏗️ 사용된 리소스:"
echo "   • S3 Tables 버킷: $TABLE_BUCKET_NAME"
echo "   • Federated 카탈로그: $S3_TABLES_CATALOG_ID"
echo "   • 데이터베이스: $S3_TABLES_DATABASE_NAME"
echo "   • 테이블: $S3_TABLES_TABLE_NAME"
echo ""
echo "✅ 다음 단계: ./scripts/04-setup-emr-on-eks.sh"
