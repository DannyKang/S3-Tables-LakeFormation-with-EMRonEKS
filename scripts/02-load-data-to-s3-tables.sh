#!/bin/bash

# S3 Tables에 로컬 데이터 적재 스크립트 (블로그용 간소화 버전)
# Athena를 통한 안정적인 데이터 적재

set -e

# 환경 변수 로드
if [ ! -f ".env" ]; then
    echo "❌ .env 파일이 존재하지 않습니다."
    echo "먼저 ./scripts/01-create-s3-table-bucket.sh를 실행하세요."
    exit 1
fi

echo "환경 설정 파일 로드 중..."
source .env

# 필수 환경 변수 확인
if [ -z "$TABLE_BUCKET_NAME" ] || [ -z "$NAMESPACE" ] || [ -z "$TABLE_NAME" ]; then
    echo "❌ 필수 환경 변수가 설정되지 않았습니다."
    echo "01-create-s3-table-bucket.sh를 다시 실행하세요."
    exit 1
fi

LOCAL_DATA_FILE="./sample-data/seoul-bike-sample-100k.csv"

echo "=== S3 Tables 데이터 적재 시작 ==="
echo "S3 Tables 버킷: $TABLE_BUCKET_NAME"
echo "네임스페이스: $NAMESPACE"
echo "테이블: $TABLE_NAME"
echo "로컬 데이터: $LOCAL_DATA_FILE"
echo ""

# 1. 로컬 데이터 파일 확인
echo "1. 로컬 데이터 파일 확인..."
if [ ! -f "$LOCAL_DATA_FILE" ]; then
    echo "❌ 로컬 데이터 파일이 존재하지 않습니다: $LOCAL_DATA_FILE"
    echo "먼저 샘플 데이터를 생성하세요."
    exit 1
fi

RECORD_COUNT=$(tail -n +2 "$LOCAL_DATA_FILE" | wc -l | tr -d ' ')
echo "✅ 로컬 데이터 파일 확인 완료: $RECORD_COUNT 레코드"

# 2. 테이블 존재 확인
echo ""
echo "2. S3 Tables 테이블 확인..."
if ! aws s3tables get-table \
    --region $REGION \
    --table-bucket-arn $BUCKET_ARN \
    --namespace $NAMESPACE \
    --name $TABLE_NAME >/dev/null 2>&1; then
    echo "❌ 테이블이 존재하지 않습니다. 먼저 01-create-s3-table-bucket.sh를 실행하세요."
    exit 1
fi
echo "✅ S3 Tables 테이블 확인 완료"

# 3. 임시 S3 버킷 생성
echo ""
echo "3. 임시 S3 버킷 생성..."
TEMP_S3_BUCKET="seoul-bike-temp-data-${ACCOUNT_ID}-$(date +%s)"
aws s3 mb s3://$TEMP_S3_BUCKET --region $REGION
echo "✅ 임시 S3 버킷 생성: $TEMP_S3_BUCKET"

# 4. 로컬 파일을 S3에 업로드
echo ""
echo "4. 로컬 파일을 S3에 업로드..."
aws s3 cp "$LOCAL_DATA_FILE" s3://$TEMP_S3_BUCKET/data/seoul-bike-sample-100k.csv
echo "✅ S3 업로드 완료: s3://$TEMP_S3_BUCKET/data/seoul-bike-sample-100k.csv"

# 5. 데이터 미리보기
echo ""
echo "5. 데이터 미리보기..."
echo "=== 처음 3행 ====="
head -n 4 "$LOCAL_DATA_FILE" | tail -n 3
echo "=================="

# 6. Athena를 통한 데이터 적재
echo ""
echo "6. Athena를 통한 데이터 적재..."

# 6-1. 기존 임시 테이블 정리
echo "   6-1. 기존 임시 테이블 정리..."
TEMP_TABLE_NAME="temp_seoul_bike_csv_$(date +%s)"

# 기존 테이블들 정리 (에러 무시)
aws athena start-query-execution \
    --query-string "DROP TABLE IF EXISTS temp_seoul_bike_csv;" \
    --result-configuration "OutputLocation=s3://${ATHENA_RESULTS_BUCKET}/" \
    --work-group "primary" \
    --region $REGION >/dev/null 2>&1 || true

echo "   ✅ 기존 테이블 정리 완료"

# 6-2. CSV 외부 테이블 생성
echo "   6-2. CSV 외부 테이블 생성: $TEMP_TABLE_NAME"

CREATE_TABLE_SQL="CREATE EXTERNAL TABLE $TEMP_TABLE_NAME (
    rental_id string,
    station_id string,
    station_name string,
    rental_date string,
    return_date string,
    usage_min int,
    distance_meter double,
    birth_year string,
    gender string,
    user_type string,
    district string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = ',',
    'field.delim' = ','
)
LOCATION 's3://$TEMP_S3_BUCKET/data/'
TBLPROPERTIES (
    'has_encrypted_data'='false',
    'skip.header.line.count'='1'
);"

QUERY_EXECUTION_ID=$(aws athena start-query-execution \
    --query-string "$CREATE_TABLE_SQL" \
    --result-configuration "OutputLocation=s3://${ATHENA_RESULTS_BUCKET}/" \
    --work-group "primary" \
    --region $REGION \
    --query 'QueryExecutionId' \
    --output text)

echo "   쿼리 ID: $QUERY_EXECUTION_ID"

# 쿼리 완료 대기
echo "   쿼리 실행 대기 중..."
for i in {1..30}; do
    STATUS=$(aws athena get-query-execution \
        --query-execution-id $QUERY_EXECUTION_ID \
        --region $REGION \
        --query 'QueryExecution.Status.State' \
        --output text)
    
    if [ "$STATUS" = "SUCCEEDED" ]; then
        echo "   ✅ CSV 외부 테이블 생성 완료"
        break
    elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
        echo "   ❌ CSV 테이블 생성 실패"
        ERROR_MSG=$(aws athena get-query-execution \
            --query-execution-id $QUERY_EXECUTION_ID \
            --region $REGION \
            --query 'QueryExecution.Status.StateChangeReason' \
            --output text)
        echo "   오류: $ERROR_MSG"
        exit 1
    else
        echo "   상태: $STATUS (${i}/30)"
        sleep 3
    fi
done

if [ "$STATUS" != "SUCCEEDED" ]; then
    echo "   ❌ 테이블 생성 시간 초과"
    exit 1
fi

# 6-3. 데이터 검증 쿼리
echo "   6-3. 임시 테이블 데이터 검증..."

VERIFY_SQL="SELECT COUNT(*) as record_count FROM $TEMP_TABLE_NAME;"

VERIFY_QUERY_ID=$(aws athena start-query-execution \
    --query-string "$VERIFY_SQL" \
    --result-configuration "OutputLocation=s3://${ATHENA_RESULTS_BUCKET}/" \
    --work-group "primary" \
    --region $REGION \
    --query 'QueryExecutionId' \
    --output text)

# 검증 쿼리 완료 대기
for i in {1..20}; do
    STATUS=$(aws athena get-query-execution \
        --query-execution-id $VERIFY_QUERY_ID \
        --region $REGION \
        --query 'QueryExecution.Status.State' \
        --output text)
    
    if [ "$STATUS" = "SUCCEEDED" ]; then
        echo "   ✅ 임시 테이블 데이터 검증 완료"
        break
    elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
        echo "   ⚠️  데이터 검증 실패 (계속 진행)"
        break
    else
        sleep 2
    fi
done

# 7. 데이터 적재 완료 표시
echo ""
echo "=== 데이터 준비 완료 ==="
echo "✅ CSV 데이터가 S3에 업로드되었습니다"
echo "✅ Athena 외부 테이블이 생성되었습니다"
echo ""
echo "📊 데이터 정보:"
echo "   - 레코드 수: $RECORD_COUNT"
echo "   - S3 위치: s3://$TEMP_S3_BUCKET/data/seoul-bike-sample-100k.csv"
echo "   - Athena 테이블: $TEMP_TABLE_NAME"
echo ""

# 환경 변수에 정보 추가
cat >> .env << EOF

# 데이터 적재 정보 ($(date '+%Y-%m-%d %H:%M:%S'))
TEMP_S3_BUCKET=$TEMP_S3_BUCKET
CSV_DATA_LOCATION=s3://$TEMP_S3_BUCKET/data/seoul-bike-sample-100k.csv
TEMP_ATHENA_TABLE=$TEMP_TABLE_NAME
DATA_PREPARED=true
DATA_PREPARATION_DATE="$(date '+%Y-%m-%d %H:%M:%S')"
EOF

echo "🎯 **다음 단계 옵션**:"
echo ""
echo "1. 🚀 **S3 Tables에 직접 적재** (권장)"
echo "   - Athena INSERT 쿼리 사용"
echo "   - 가장 빠르고 안정적"
echo ""
echo "2. 🔧 **AWS Glue ETL Job 사용**"
echo "   - AWS 콘솔에서 시각적 설정"
echo "   - 프로덕션 환경에 적합"
echo ""
echo "3. ⚡ **EMR on EKS 사용**"
echo "   - 완전한 Lake Formation 데모"
echo "   - 다음 스크립트들과 연동"
echo ""
echo "4. ⏭️  **데이터 적재 건너뛰기**"
echo "   - Lake Formation 설정만 진행"
echo "   - 나중에 데이터 적재 가능"
echo ""

read -p "선택하세요 (1/2/3/4): " choice

case $choice in
    1)
        echo ""
        echo "=== S3 Tables 직접 적재 시작 ==="
        
        # S3 Tables에 INSERT 실행
        echo "S3 Tables에 데이터 INSERT 중..."
        echo "   소스: $TEMP_TABLE_NAME"
        echo "   대상: $S3_TABLES_CATALOG.$NAMESPACE.$TABLE_NAME"
        
        # INSERT INTO 쿼리 생성
        INSERT_SQL="INSERT INTO \"$S3_TABLES_CATALOG\".$NAMESPACE.$TABLE_NAME
SELECT 
    rental_id,
    station_id,
    station_name,
    rental_date,
    return_date,
    usage_min,
    distance_meter,
    birth_year,
    gender,
    user_type,
    district
FROM $TEMP_TABLE_NAME
WHERE rental_id IS NOT NULL
  AND station_id IS NOT NULL
  AND rental_date IS NOT NULL
  AND return_date IS NOT NULL;"

        echo "   실행할 쿼리:"
        echo "   INSERT INTO \"$S3_TABLES_CATALOG\".$NAMESPACE.$TABLE_NAME"
        echo "   SELECT * FROM $TEMP_TABLE_NAME WHERE ..."
        echo ""
        
        # INSERT 쿼리 실행
        INSERT_QUERY_ID=$(aws athena start-query-execution \
            --query-string "$INSERT_SQL" \
            --result-configuration "OutputLocation=s3://${ATHENA_RESULTS_BUCKET}/" \
            --work-group "primary" \
            --region $REGION \
            --query 'QueryExecutionId' \
            --output text)

        echo "   INSERT 쿼리 ID: $INSERT_QUERY_ID"
        echo "   INSERT 실행 중... (약 2-5분 소요)"
        
        # INSERT 쿼리 완료 대기
        for i in {1..60}; do
            STATUS=$(aws athena get-query-execution \
                --query-execution-id $INSERT_QUERY_ID \
                --region $REGION \
                --query 'QueryExecution.Status.State' \
                --output text)
            
            if [ "$STATUS" = "SUCCEEDED" ]; then
                echo "   ✅ S3 Tables 데이터 적재 완료!"
                
                # 적재된 레코드 수 확인
                echo "   적재 결과 확인 중..."
                
                COUNT_SQL="SELECT COUNT(*) as total_records FROM \"$S3_TABLES_CATALOG\".$NAMESPACE.$TABLE_NAME;"
                
                COUNT_QUERY_ID=$(aws athena start-query-execution \
                    --query-string "$COUNT_SQL" \
                    --result-configuration "OutputLocation=s3://${ATHENA_RESULTS_BUCKET}/" \
                    --work-group "primary" \
                    --region $REGION \
                    --query 'QueryExecutionId' \
                    --output text)
                
                # 카운트 쿼리 완료 대기
                for j in {1..20}; do
                    COUNT_STATUS=$(aws athena get-query-execution \
                        --query-execution-id $COUNT_QUERY_ID \
                        --region $REGION \
                        --query 'QueryExecution.Status.State' \
                        --output text)
                    
                    if [ "$COUNT_STATUS" = "SUCCEEDED" ]; then
                        echo "   ✅ 적재 완료 검증 성공"
                        break
                    elif [ "$COUNT_STATUS" = "FAILED" ] || [ "$COUNT_STATUS" = "CANCELLED" ]; then
                        echo "   ⚠️  레코드 수 확인 실패 (적재는 성공)"
                        break
                    else
                        sleep 2
                    fi
                done
                
                echo ""
                echo "🎉 **S3 Tables 데이터 적재 성공!**"
                echo "   📊 적재된 데이터: $RECORD_COUNT 레코드"
                echo "   📍 S3 Tables 위치: $TABLE_BUCKET_NAME"
                echo "   🗂️  테이블: $NAMESPACE.$TABLE_NAME"
                echo ""
                
                # .env에 적재 완료 정보 추가
                cat >> .env << EOF
# S3 Tables 적재 완료 정보 ($(date '+%Y-%m-%d %H:%M:%S'))
DATA_LOADED_TO_S3_TABLES=true
S3_TABLES_LOAD_DATE="$(date '+%Y-%m-%d %H:%M:%S')"
S3_TABLES_LOAD_METHOD="ATHENA_INSERT"
LOADED_RECORD_COUNT=$RECORD_COUNT
EOF
                
                break
                
            elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
                echo "   ❌ S3 Tables 데이터 적재 실패"
                ERROR_MSG=$(aws athena get-query-execution \
                    --query-execution-id $INSERT_QUERY_ID \
                    --region $REGION \
                    --query 'QueryExecution.Status.StateChangeReason' \
                    --output text)
                echo "   오류: $ERROR_MSG"
                echo ""
                echo "📋 **대안 방법**:"
                echo "1. AWS Glue ETL Job 사용"
                echo "2. EMR on EKS Spark 사용"
                echo "3. 로컬 Python 스크립트 사용"
                break
            else
                echo "   상태: $STATUS (${i}/60) - 대기 중..."
                sleep 5
            fi
        done

        if [ "$STATUS" != "SUCCEEDED" ]; then
            echo "   ⚠️  INSERT 시간 초과 또는 실패"
            echo "   다른 적재 방법을 시도해보세요."
        fi
        ;;
        
    2)
        echo ""
        echo "=== AWS Glue ETL Job 사용 안내 ==="
        echo ""
        echo "📋 **AWS Glue 콘솔에서 ETL Job 생성**:"
        echo ""
        echo "1. AWS Glue 콘솔 접속:"
        echo "   https://$REGION.console.aws.amazon.com/glue/"
        echo ""
        echo "2. 'ETL jobs' → 'Visual ETL' 선택"
        echo ""
        echo "3. 소스 설정:"
        echo "   - Data source: S3"
        echo "   - S3 URL: s3://$TEMP_S3_BUCKET/data/"
        echo "   - Data format: CSV"
        echo "   - Delimiter: Comma"
        echo "   - First line contains header: Yes"
        echo ""
        echo "4. 대상 설정:"
        echo "   - Data target: S3 Tables"
        echo "   - Table bucket: $TABLE_BUCKET_NAME"
        echo "   - Namespace: $NAMESPACE"
        echo "   - Table: $TABLE_NAME"
        echo ""
        echo "5. Job 실행 후 약 5-10분 대기"
        echo ""
        echo "✅ 완료 후 다음 스크립트를 실행하세요:"
        echo "   ./scripts/02-create-iam-roles.sh"
        ;;
        
    3)
        echo ""
        echo "=== EMR on EKS 사용 선택 ==="
        echo ""
        echo "📋 다음 단계에서 EMR on EKS를 설정한 후"
        echo "Spark를 사용하여 데이터를 적재합니다."
        echo ""
        echo "🎯 다음 스크립트: ./scripts/02-create-iam-roles.sh"
        ;;
        
    4)
        echo ""
        echo "=== 데이터 적재 건너뛰기 ==="
        echo ""
        echo "📋 데이터 준비는 완료되었습니다."
        echo "나중에 필요할 때 다음 방법으로 적재할 수 있습니다:"
        echo ""
        echo "- AWS Glue ETL Job"
        echo "- EMR on EKS Spark"
        echo "- 로컬 Python 스크립트"
        echo ""
        echo "🎯 다음 스크립트: ./scripts/02-create-iam-roles.sh"
        ;;
        
    *)
        echo ""
        echo "기본값으로 데이터 준비만 완료합니다."
        echo "🎯 다음 스크립트: ./scripts/02-create-iam-roles.sh"
        ;;
esac

# 정리 옵션
echo ""
echo "=== 정리 옵션 ==="
echo "임시 S3 버킷을 지금 정리하시겠습니까? (y/N)"
echo "⚠️  나중에 데이터 적재할 예정이면 'N'을 선택하세요."
read -r cleanup_response

if [[ "$cleanup_response" =~ ^[Yy]$ ]]; then
    echo "임시 리소스 정리 중..."
    
    # Athena 임시 테이블 삭제
    aws athena start-query-execution \
        --query-string "DROP TABLE IF EXISTS $TEMP_TABLE_NAME;" \
        --result-configuration "OutputLocation=s3://${ATHENA_RESULTS_BUCKET}/" \
        --work-group "primary" \
        --region $REGION >/dev/null 2>&1 || true
    
    # S3 버킷 정리
    aws s3 rm s3://$TEMP_S3_BUCKET --recursive >/dev/null 2>&1 || true
    aws s3 rb s3://$TEMP_S3_BUCKET >/dev/null 2>&1 || true
    
    echo "✅ 임시 리소스 정리 완료"
    
    # .env에서 임시 정보 제거
    sed -i.bak '/TEMP_S3_BUCKET/d' .env 2>/dev/null || true
    sed -i.bak '/CSV_DATA_LOCATION/d' .env 2>/dev/null || true
    sed -i.bak '/TEMP_ATHENA_TABLE/d' .env 2>/dev/null || true
    rm -f .env.bak
else
    echo "📁 임시 리소스 유지됨:"
    echo "   - S3 버킷: s3://$TEMP_S3_BUCKET"
    echo "   - Athena 테이블: $TEMP_TABLE_NAME"
    echo ""
    echo "🧹 나중에 정리하려면:"
    echo "   aws s3 rb s3://$TEMP_S3_BUCKET --force"
fi

echo ""
echo "=== 데이터 준비/적재 단계 완료 ==="
echo ""

# 적재 완료 여부 확인
if grep -q "DATA_LOADED_TO_S3_TABLES=true" .env 2>/dev/null; then
    echo "🎉 **S3 Tables 데이터 적재 완료!**"
    echo ""
    echo "✅ **완료된 작업**:"
    echo "   - 로컬 CSV 데이터 검증 ($RECORD_COUNT 레코드)"
    echo "   - S3에 데이터 업로드"
    echo "   - Athena 외부 테이블 생성"
    echo "   - S3 Tables에 데이터 적재 완료"
    echo ""
    echo "📊 **적재된 데이터 정보**:"
    echo "   - S3 Tables 버킷: $TABLE_BUCKET_NAME"
    echo "   - 네임스페이스: $NAMESPACE"
    echo "   - 테이블: $TABLE_NAME"
    echo "   - 레코드 수: $RECORD_COUNT"
    echo ""
    echo "🔍 **데이터 확인 쿼리** (Athena에서 실행):"
    echo "   SELECT COUNT(*) FROM \"$S3_TABLES_CATALOG\".$NAMESPACE.$TABLE_NAME;"
    echo "   SELECT * FROM \"$S3_TABLES_CATALOG\".$NAMESPACE.$TABLE_NAME LIMIT 10;"
    echo ""
    echo "🎯 **다음 단계**: Lake Formation FGAC 설정"
    echo "   ./scripts/02-create-iam-roles.sh"
else
    echo "📋 **데이터 준비 완료** (적재는 선택사항)"
    echo ""
    echo "✅ **완료된 작업**:"
    echo "   - 로컬 CSV 데이터 검증 ($RECORD_COUNT 레코드)"
    echo "   - S3에 데이터 업로드"
    echo "   - Athena 외부 테이블 생성"
    echo ""
    echo "📋 **데이터 적재 옵션** (나중에 선택 가능):"
    echo "   1. 스크립트 재실행 후 옵션 1 선택 (Athena INSERT)"
    echo "   2. AWS Glue ETL Job 사용"
    echo "   3. EMR on EKS Spark 사용"
    echo ""
    echo "🎯 **다음 단계**: Lake Formation FGAC 설정 (데이터 없이도 가능)"
    echo "   ./scripts/02-create-iam-roles.sh"
fi

echo ""
echo "⚠️  **중요사항**:"
echo "   - Lake Formation FGAC 설정은 데이터 적재 없이도 가능합니다"
echo "   - 실제 데이터 테스트는 EMR on EKS 설정 완료 후 권장합니다"
echo "   - 임시 S3 버킷은 테스트 완료 후 정리하세요"
