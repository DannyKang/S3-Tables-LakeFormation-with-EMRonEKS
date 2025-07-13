#!/bin/bash

# Lake Formation FGAC 권한 설정 스크립트 (Marketing Partner 포함)
# 4개 역할별 세밀한 데이터 접근 제어 구현

set -e

# 환경 변수 로드
source .env

CATALOG_ID="${ACCOUNT_ID}:s3tablescatalog/${TABLE_BUCKET_NAME}"
DATABASE_NAME=$NAMESPACE
TABLE_NAME=$TABLE_NAME

echo "=== Lake Formation FGAC 권한 설정 시작 ==="
echo "카탈로그 ID: $CATALOG_ID"
echo "데이터베이스: $DATABASE_NAME"
echo "테이블: $TABLE_NAME"

# 1. LF_DataStewardRole - 전체 데이터 접근 권한
echo "1. LF_DataStewardRole 권한 설정..."

# 카탈로그 권한
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/LF_DataStewardRole \
    --resource "Catalog={Id=${CATALOG_ID}}" \
    --permissions "ALL,ALTER,CREATE_DATABASE,DESCRIBE,DROP"

# 데이터베이스 권한
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/LF_DataStewardRole \
    --resource "Database={CatalogId=${CATALOG_ID},Name=${DATABASE_NAME}}" \
    --permissions "ALL,ALTER,CREATE_TABLE,DESCRIBE,DROP"

# 테이블 권한
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/LF_DataStewardRole \
    --resource "Table={CatalogId=${CATALOG_ID},DatabaseName=${DATABASE_NAME},Name=${TABLE_NAME}}" \
    --permissions "ALL,ALTER,DELETE,DESCRIBE,INSERT"

# 컬럼 권한 (전체)
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/LF_DataStewardRole \
    --resource "TableWithColumns={CatalogId=${CATALOG_ID},DatabaseName=${DATABASE_NAME},Name=${TABLE_NAME},ColumnWildcard={}}" \
    --permissions "SELECT"

# 2. LF_GangnamAnalyticsRole - 강남구 데이터만, 개인정보 제외
echo "2. LF_GangnamAnalyticsRole 권한 설정..."

# 카탈로그 권한
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/LF_GangnamAnalyticsRole \
    --resource "Catalog={Id=${CATALOG_ID}}" \
    --permissions "DESCRIBE"

# 데이터베이스 권한
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/LF_GangnamAnalyticsRole \
    --resource "Database={CatalogId=${CATALOG_ID},Name=${DATABASE_NAME}}" \
    --permissions "DESCRIBE"

# 강남구 데이터 필터 생성
echo "   강남구 데이터 필터 생성..."
aws lakeformation create-data-cells-filter \
    --region $REGION \
    --table-data '{
        "TableCatalogId": "'${CATALOG_ID}'",
        "DatabaseName": "'${DATABASE_NAME}'",
        "TableName": "'${TABLE_NAME}'",
        "Name": "GangnamDistrictFilter",
        "RowFilter": {
            "FilterExpression": "district = '\''강남구'\''"
        },
        "ColumnNames": [
            "rental_id", "station_id", "station_name", "district", 
            "rental_date", "return_date", "user_type", "age_group", 
            "gender", "rental_duration", "payment_amount"
        ]
    }' || echo "   필터가 이미 존재합니다."

# 필터 권한 부여
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/LF_GangnamAnalyticsRole \
    --resource "DataCellsFilter={TableCatalogId=${CATALOG_ID},DatabaseName=${DATABASE_NAME},TableName=${TABLE_NAME},Name=GangnamDistrictFilter}" \
    --permissions "SELECT"

# 3. LF_OperationRole - 운영 데이터만, 결제정보/개인정보 제외
echo "3. LF_OperationRole 권한 설정..."

# 카탈로그 권한
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/LF_OperationRole \
    --resource "Catalog={Id=${CATALOG_ID}}" \
    --permissions "DESCRIBE"

# 데이터베이스 권한
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/LF_OperationRole \
    --resource "Database={CatalogId=${CATALOG_ID},Name=${DATABASE_NAME}}" \
    --permissions "DESCRIBE"

# 운영팀용 컬럼 필터 생성
echo "   운영팀용 컬럼 필터 생성..."
aws lakeformation create-data-cells-filter \
    --region $REGION \
    --table-data '{
        "TableCatalogId": "'${CATALOG_ID}'",
        "DatabaseName": "'${DATABASE_NAME}'",
        "TableName": "'${TABLE_NAME}'",
        "Name": "OperationDataFilter",
        "ColumnNames": [
            "rental_id", "station_id", "station_name", "district", 
            "rental_date", "return_date", "user_type", "rental_duration"
        ]
    }' || echo "   필터가 이미 존재합니다."

# 필터 권한 부여
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/LF_OperationRole \
    --resource "DataCellsFilter={TableCatalogId=${CATALOG_ID},DatabaseName=${DATABASE_NAME},TableName=${TABLE_NAME},Name=OperationDataFilter}" \
    --permissions "SELECT"

# 4. LF_MarketingPartnerRole - 강남구 20대만, 마케팅 관련 컬럼만
echo "4. LF_MarketingPartnerRole 권한 설정 (NEW!)..."

# 카탈로그 권한
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/LF_MarketingPartnerRole \
    --resource "Catalog={Id=${CATALOG_ID}}" \
    --permissions "DESCRIBE"

# 데이터베이스 권한
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/LF_MarketingPartnerRole \
    --resource "Database={CatalogId=${CATALOG_ID},Name=${DATABASE_NAME}}" \
    --permissions "DESCRIBE"

# 마케팅 파트너용 다차원 필터 생성 (강남구 + 20대)
echo "   마케팅 파트너용 다차원 필터 생성 (강남구 + 20대)..."
aws lakeformation create-data-cells-filter \
    --region $REGION \
    --table-data '{
        "TableCatalogId": "'${CATALOG_ID}'",
        "DatabaseName": "'${DATABASE_NAME}'",
        "TableName": "'${TABLE_NAME}'",
        "Name": "MarketingPartnerFilter",
        "RowFilter": {
            "FilterExpression": "district = '\''강남구'\'' AND age_group = '\''20대'\''"
        },
        "ColumnNames": [
            "rental_id", "station_id", "station_name", "district", 
            "rental_date", "return_date", "user_type", "age_group", "gender"
        ]
    }' || echo "   필터가 이미 존재합니다."

# 필터 권한 부여
aws lakeformation grant-permissions \
    --region $REGION \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/LF_MarketingPartnerRole \
    --resource "DataCellsFilter={TableCatalogId=${CATALOG_ID},DatabaseName=${DATABASE_NAME},TableName=${TABLE_NAME},Name=MarketingPartnerFilter}" \
    --permissions "SELECT"

# 5. 데이터 위치 권한 설정
echo "5. 데이터 위치 권한 설정..."

# S3 Tables 리소스 등록 (이미 되어있을 수 있음)
aws lakeformation register-resource \
    --region $REGION \
    --resource-arn "arn:aws:s3tables:${REGION}:${ACCOUNT_ID}:bucket/*" \
    --role-arn "arn:aws:iam::${ACCOUNT_ID}:role/service-role/S3TablesRoleForLakeFormation" || echo "   리소스가 이미 등록되어 있습니다."

# 각 역할에 데이터 위치 접근 권한 부여
for role in "LF_DataStewardRole" "LF_GangnamAnalyticsRole" "LF_OperationRole" "LF_MarketingPartnerRole"; do
    echo "   $role 데이터 위치 권한 부여..."
    aws lakeformation grant-permissions \
        --region $REGION \
        --principal DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT_ID}:role/$role \
        --resource "DataLocation={CatalogId=${ACCOUNT_ID},ResourceArn=arn:aws:s3tables:${REGION}:${ACCOUNT_ID}:bucket/*}" \
        --permissions "DATA_LOCATION_ACCESS" || echo "   권한이 이미 부여되어 있습니다."
done

echo "=== Lake Formation FGAC 권한 설정 완료 ==="
echo ""
echo "설정된 권한 요약:"
echo "📊 LF_DataStewardRole: 전체 데이터 (12개 컬럼, 모든 지역/연령대)"
echo "🏢 LF_GangnamAnalyticsRole: 강남구 데이터 (11개 컬럼, user_id 제외)"
echo "⚙️  LF_OperationRole: 운영 데이터 (8개 컬럼, 결제/개인정보 제외)"
echo "🎯 LF_MarketingPartnerRole: 강남구 20대 (9개 컬럼, 마케팅 관련만)"
echo ""
echo "다음 단계: ./scripts/03-setup-emr-on-eks.sh"
