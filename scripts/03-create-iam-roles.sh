#!/bin/bash

# Lake Formation FGAC용 IAM 역할 생성 스크립트
# 4개 역할별 기본 신뢰 정책 및 권한 설정 (IRSA는 04단계에서 추가)

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
if [ -z "$ACCOUNT_ID" ] || [ -z "$REGION" ] || [ -z "$TABLE_BUCKET_NAME" ]; then
    echo "❌ 필수 환경 변수가 설정되지 않았습니다."
    echo "01-create-s3-table-bucket.sh를 다시 실행하세요."
    exit 1
fi

echo "=== Lake Formation FGAC IAM 역할 생성 시작 ==="
echo "계정 ID: $ACCOUNT_ID"
echo "리전: $REGION"
echo "S3 Tables 버킷: $TABLE_BUCKET_NAME"
echo ""
echo "ℹ️  IRSA 신뢰 정책은 04-setup-emr-on-eks.sh에서 추가됩니다."
echo ""

# 1. 기본 신뢰 정책 생성
echo "1. 기본 신뢰 정책 생성..."

cat > /tmp/trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": [
                    "emr-containers.amazonaws.com",
                    "glue.amazonaws.com"
                ]
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::$ACCOUNT_ID:root"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

# 2. 기본 권한 정책 생성 (동적 버킷명 사용)
echo "2. 기본 권한 정책 생성..."

cat > /tmp/base-policy.json << EOF
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
                "s3tables:GetTable",
                "s3tables:GetTableMetadata",
                "s3tables:GetTablePolicy",
                "s3tables:ListTables"
            ],
            "Resource": [
                "arn:aws:s3tables:$REGION:$ACCOUNT_ID:bucket/$TABLE_BUCKET_NAME",
                "arn:aws:s3tables:$REGION:$ACCOUNT_ID:bucket/$TABLE_BUCKET_NAME/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartition",
                "glue:GetPartitions"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::$TABLE_BUCKET_NAME",
                "arn:aws:s3:::$TABLE_BUCKET_NAME/*"
            ]
        }
    ]
}
EOF

# 3. IAM 역할 생성
echo ""
echo "3. IAM 역할 생성 중..."

ROLES=(
    "LF_DataStewardRole:데이터 관리자 - 전체 데이터 접근"
    "LF_GangnamAnalyticsRole:강남구 분석가 - 강남구 데이터만"
    "LF_OperationRole:운영팀 - 운영 데이터만"
    "LF_MarketingPartnerRole:마케팅 파트너 - 강남구 20-30대만"
)

for role_info in "${ROLES[@]}"; do
    IFS=':' read -r role_name role_desc <<< "$role_info"
    
    echo ""
    echo "   $role_name 생성 중..."
    echo "   설명: $role_desc"
    
    # 역할 생성 또는 업데이트
    if aws iam get-role --role-name $role_name >/dev/null 2>&1; then
        echo "   ⚠️  역할이 이미 존재합니다. 신뢰 정책을 업데이트합니다."
        aws iam update-assume-role-policy --role-name $role_name --policy-document file:///tmp/trust-policy.json
    else
        # 역할 생성
        aws iam create-role \
            --role-name $role_name \
            --assume-role-policy-document file:///tmp/trust-policy.json \
            --description "$role_desc" \
            --max-session-duration 3600
        
        echo "   ✅ $role_name 생성 완료"
    fi
    
    # 기본 정책 연결
    if ! aws iam get-role-policy --role-name $role_name --policy-name "BasePolicy" >/dev/null 2>&1; then
        aws iam put-role-policy \
            --role-name $role_name \
            --policy-name "BasePolicy" \
            --policy-document file:///tmp/base-policy.json
        echo "   ✅ 기본 정책 연결 완료"
    else
        echo "   ⚠️  기본 정책이 이미 연결되어 있습니다."
    fi
done

# 4. 환경 변수 파일 업데이트
echo ""
echo "4. 환경 변수 파일 업데이트..."

# 기존 Lake Formation 역할 정보 제거 (중복 방지)
if grep -q "# Lake Formation IAM 역할" .env; then
    # 기존 정보가 있으면 업데이트하지 않음
    echo "   ⚠️  기존 Lake Formation 설정이 발견되었습니다. 기존 정보를 유지합니다."
else
    # 역할 ARN 정보 추가
    cat >> .env << EOF

# Lake Formation IAM 역할
LF_DATA_STEWARD_ROLE=LF_DataStewardRole
LF_GANGNAM_ANALYTICS_ROLE=LF_GangnamAnalyticsRole
LF_OPERATION_ROLE=LF_OperationRole
LF_MARKETING_PARTNER_ROLE=LF_MarketingPartnerRole

# 역할 ARN
LF_DATA_STEWARD_ROLE_ARN=arn:aws:iam::$ACCOUNT_ID:role/LF_DataStewardRole
LF_GANGNAM_ANALYTICS_ROLE_ARN=arn:aws:iam::$ACCOUNT_ID:role/LF_GangnamAnalyticsRole
LF_OPERATION_ROLE_ARN=arn:aws:iam::$ACCOUNT_ID:role/LF_OperationRole
LF_MARKETING_PARTNER_ROLE_ARN=arn:aws:iam::$ACCOUNT_ID:role/LF_MarketingPartnerRole

# EMR 서비스 역할 (04단계에서 생성됨)
EMR_EKS_SERVICE_ROLE=EMRContainers-JobExecutionRole
EOF

    echo "   ✅ 환경 변수 파일 업데이트 완료"
fi

# 5. 임시 파일 정리
echo ""
echo "5. 임시 파일 정리..."
rm -f /tmp/trust-policy.json
rm -f /tmp/base-policy.json
echo "   ✅ 임시 파일 정리 완료"

echo ""
echo "=== Lake Formation FGAC IAM 역할 생성 완료 ==="
echo ""
echo "📋 생성된 역할 요약:"
echo "┌─────────────────────────┬─────────────────────────────────────┐"
echo "│ 역할명                  │ 설명                                │"
echo "├─────────────────────────┼─────────────────────────────────────┤"
echo "│ LF_DataStewardRole      │ 데이터 관리자 - 전체 데이터 접근    │"
echo "│ LF_GangnamAnalyticsRole │ 강남구 분석가 - 강남구 데이터만     │"
echo "│ LF_OperationRole        │ 운영팀 - 운영 데이터만              │"
echo "│ LF_MarketingPartnerRole │ 마케팅 파트너 - 강남구 20-30대만    │"
echo "└─────────────────────────┴─────────────────────────────────────┘"
echo ""
echo "🔑 현재 신뢰 정책: AWS 서비스 및 계정 루트만"
echo "   • IRSA 신뢰 정책은 04-setup-emr-on-eks.sh에서 추가됩니다"
echo ""
echo "✅ 다음 단계: ./scripts/03-setup-lakeformation-permissions-native.sh"
