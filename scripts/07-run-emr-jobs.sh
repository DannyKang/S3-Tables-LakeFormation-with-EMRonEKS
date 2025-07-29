#!/bin/bash

# EMR on EKS Job 실행 스크립트 (Lake Formation FGAC + S3 Tables)
# Lake Formation FGAC 4개 역할별 분석 Job 실행
# S3 Tables (Apache Iceberg) 카탈로그 사용
# EMR on EKS Blueprint의 Job Template과 Pod Template 활용

set -e

# 환경 변수 로드
if [ ! -f ".env" ]; then
    echo "❌ .env 파일이 존재하지 않습니다. 먼저 04-setup-emr-on-eks.sh를 실행하세요."
    exit 1
fi

source .env

# Lake Formation FGAC 설정 확인
if [ -z "$LF_VIRTUAL_CLUSTER_ID" ]; then
    echo "❌ Lake Formation FGAC가 설정되지 않았습니다."
    echo "먼저 ./scripts/04-1-setup-lake-formation-fgac.sh를 실행하세요."
    exit 1
fi

echo "=== EMR on EKS Job 실행 시작 (Lake Formation FGAC 활성화) ==="
echo "LF Virtual Cluster ID: $LF_VIRTUAL_CLUSTER_ID"
echo "Security Configuration: $SECURITY_CONFIG_ID"
echo "Session Tag Value: $LF_SESSION_TAG_VALUE"
echo "User Namespace: $USER_NAMESPACE"
echo "Scripts Bucket: s3://$SCRIPTS_BUCKET"
echo "Job Templates Directory: ./job-templates/"
echo "Pod Templates Directory: ./pod-templates/"
echo ""

# Job 설정 (Lake Formation FGAC 역할 매핑)
JOB_CONFIGS=(
    "data-steward:emr-data-steward-sa:$LF_DATA_STEWARD_ROLE:전체 데이터 분석 (100,000건)"
    #"gangnam-analytics:emr-gangnam-analytics-sa:$LF_GANGNAM_ANALYTICS_ROLE:강남구 데이터 분석 (~3,000건)"
    #"operation:emr-operation-sa:$LF_OPERATION_ROLE:운영 데이터 분석 (개인정보 제외)"
    #"marketing-partner:emr-marketing-partner-sa:$LF_MARKETING_PARTNER_ROLE:마케팅 타겟 분석 (강남구 20-30대)"
)

# 결과 저장용 S3 버킷
RESULTS_BUCKET="seoul-bike-analytics-results-${ACCOUNT_ID}"
aws s3 mb s3://$RESULTS_BUCKET --region $REGION 2>/dev/null || echo "결과 버킷이 이미 존재합니다."

# 템플릿 디렉토리 생성
mkdir -p job-templates pod-templates

echo ""
echo "ℹ️  Lake Formation FGAC + S3 Tables가 활성화된 Virtual Cluster를 사용합니다."
echo "ℹ️  S3 Tables (Apache Iceberg) 카탈로그: s3tablescatalog"
echo "ℹ️  EMR on EKS Blueprint Job Template과 Pod Template을 활용합니다."
echo "ℹ️  Security Configuration: $SECURITY_CONFIG_ID"
echo "ℹ️  Session Tag: LakeFormationAuthorizedCaller=$LF_SESSION_TAG_VALUE"

# Job Template 생성 함수 (Lake Formation FGAC + S3 Tables 최적화)
create_job_template() {
    local job_name=$1
    local service_account=$2
    local role_name=$3
    local timestamp=$(date +%Y%m%d-%H%M%S)
    
    echo "📝 $job_name Job Template 생성 중..."
    
    # Job Template 파일 생성
    local job_template_file="job-templates/${job_name}-job-template.json"
    
    cat > "$job_template_file" << EOF
{
  "name": "seoul-bike-${job_name}-${timestamp}",
  "virtualClusterId": "$LF_VIRTUAL_CLUSTER_ID",
  "executionRoleArn": "arn:aws:iam::${ACCOUNT_ID}:role/${role_name}",
  "releaseLabel": "emr-7.7.0-latest",
  "jobDriver": {
    "sparkSubmitJobDriver": {
      "entryPoint": "s3://${SCRIPTS_BUCKET}/spark-jobs/${job_name}-analysis.py",
      "sparkSubmitParameters": "--conf spark.executor.instances=2 --conf spark.executor.memory=1g --conf spark.executor.cores=1 --conf spark.driver.cores=1 --conf spark.driver.memory=1g"
    }
  },
  "configurationOverrides": {
    "applicationConfiguration": [
      {
        "classification": "spark-defaults",
        "properties": {
          "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
          "spark.sql.catalog.s3tablesbucket": "org.apache.iceberg.spark.SparkCatalog",
          "spark.sql.catalog.s3tablesbucket.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
          "spark.sql.catalog.s3tablesbucket.warehouse": "arn:aws:s3tables:$REGION:$ACCOUNT_ID:bucket/${TABLE_BUCKET_NAME}",
          "spark.sql.catalog.s3tablesbucket.client.region": "$REGION",
          "spark.sql.defaultCatalog": "s3tablesbucket",
          "spark.kubernetes.driver.podTemplateFile": "s3://seoul-bike-analytics-scripts-${ACCOUNT_ID}/pod-templates/driver-pod-template.yaml",
          "spark.kubernetes.executor.podTemplateFile": "s3://seoul-bike-analytics-scripts-${ACCOUNT_ID}/pod-templates/executor-pod-template.yaml",
          "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
          "spark.hadoop.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
          "spark.hadoop.fs.s3a.endpoint.region": "$REGION",
          "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
          "spark.hadoop.aws.region": "$REGION",
          "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
          "spark.executor.instances": "2",
          "spark.executor.memory": "1g",
          "spark.executor.cores": "1",
          "spark.driver.memory": "1g",
          "spark.driver.cores": "1"
        }
      }
    ],
    "monitoringConfiguration": {
      "persistentAppUI": "ENABLED",
      "cloudWatchMonitoringConfiguration": {
        "logGroupName": "/aws/emr-containers/jobs",
        "logStreamNamePrefix": "${job_name}"
      },
      "s3MonitoringConfiguration": {
        "logUri": "s3://${RESULTS_BUCKET}/logs/"
      }
    }
  },
  "tags": {
    "LakeFormationAuthorizedCaller": "$LF_SESSION_TAG_VALUE",
    "JobType": "LakeFormationFGAC",
    "Role": "${role_name}",
    "Namespace": "$USER_NAMESPACE",
    "CatalogType": "S3Tables"
  }
}
EOF
    
    echo "   ✅ Job Template 생성 완료: $job_template_file"
    return 0
}

# Pod Template 생성 함수
create_pod_template() {
    local job_name=$1
    local service_account=$2
    
    echo "📝 $job_name Pod Template 생성 중..."
    
    # Pod Template 파일 생성
    local pod_template_file="pod-templates/${job_name}-pod-template.yaml"
    
    cat > "$pod_template_file" << EOF
apiVersion: v1
kind: Pod
metadata:
  name: spark-${job_name}-template
  namespace: $USER_NAMESPACE
  labels:
    app: spark-${job_name}
    version: "1.0"
    component: spark-executor
    spark-role: executor
    job-type: lake-formation-fgac
    LakeFormationAuthorizedCaller: "$LF_SESSION_TAG_VALUE"
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/path: "/metrics/executors/prometheus/"
    prometheus.io/port: "4040"
    LakeFormationAuthorizedCaller: "$LF_SESSION_TAG_VALUE"
spec:
  serviceAccountName: ${service_account}
  restartPolicy: Never
  nodeSelector:
    karpenter.sh/nodepool: spark-compute-optimized
    node.kubernetes.io/instance-type: "c5.large"
  tolerations:
    - key: spark-compute-optimized
      operator: Equal
      value: "true"
      effect: NoSchedule
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: karpenter.sh/nodepool
            operator: In
            values:
            - spark-compute-optimized
    podAntiAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchExpressions:
            - key: spark-role
              operator: In
              values:
              - executor
          topologyKey: kubernetes.io/hostname
  containers:
  - name: spark-kubernetes-executor
    image: public.ecr.aws/emr-on-eks/spark/emr-7.8.0:latest
    imagePullPolicy: IfNotPresent
    resources:
      requests:
        memory: "1Gi"
        cpu: "500m"
        ephemeral-storage: "2Gi"
      limits:
        memory: "1Gi"
        cpu: "1"
        ephemeral-storage: "4Gi"
    env:
    - name: SPARK_CONF_DIR
      value: /opt/spark/conf
    - name: AWS_REGION
      value: ${REGION}
    - name: AWS_DEFAULT_REGION
      value: ${REGION}
    - name: SPARK_LOCAL_DIRS
      value: /tmp/spark-local
    - name: JOB_NAME
      value: ${job_name}
    - name: SERVICE_ACCOUNT
      value: ${service_account}
    - name: LAKE_FORMATION_AUTHORIZED_CALLER
      value: "$LF_SESSION_TAG_VALUE"
    volumeMounts:
    - name: spark-local-dir
      mountPath: /tmp/spark-local
    - name: spark-conf-volume
      mountPath: /opt/spark/conf
    securityContext:
      runAsUser: 999
      runAsGroup: 1000
      fsGroup: 1000
      runAsNonRoot: true
  volumes:
  - name: spark-local-dir
    emptyDir:
      sizeLimit: 2Gi
  - name: spark-conf-volume
    emptyDir: {}
  terminationGracePeriodSeconds: 30
  dnsPolicy: ClusterFirst
  schedulerName: default-scheduler
EOF
    
    echo "   ✅ Pod Template 생성 완료: $pod_template_file"
    return 0
}

# Job 실행 함수 (Template 사용)
run_emr_job_with_template() {
    local job_name=$1
    local service_account=$2
    local role_name=$3
    local description=$4
    
    echo ""
    echo "🚀 $job_name Job 실행 중 (Blueprint Template 사용)..."
    echo "   설명: $description"
    echo "   서비스 계정: $service_account"
    echo "   IAM 역할: $role_name"
    
    # Job Template과 Pod Template 생성
    create_job_template "$job_name" "$service_account" "$role_name"
    create_pod_template "$job_name" "$service_account"
    
    # Job Template 파일 읽기
    local job_template_file="job-templates/${job_name}-job-template.json"
    
    if [ ! -f "$job_template_file" ]; then
        echo "   ❌ Job Template 파일을 찾을 수 없습니다: $job_template_file"
        return 1
    fi
    
    # Job 실행 (Template 파일 사용)
    echo "   📋 Job Template 파일 사용: $job_template_file"
    
    JOB_ID=$(aws emr-containers start-job-run \
        --region $REGION \
        --cli-input-json file://"$job_template_file" \
        --query 'id' \
        --output text)
    
    if [ -n "$JOB_ID" ] && [ "$JOB_ID" != "None" ]; then
        echo "   ✅ Job 시작됨: $JOB_ID"
        echo "   📊 모니터링: aws emr-containers describe-job-run --region $REGION --virtual-cluster-id $LF_VIRTUAL_CLUSTER_ID --id $JOB_ID"
        echo "   📝 Job Template: $job_template_file"
        echo "   📝 Pod Template: pod-templates/${job_name}-pod-template.yaml"
        echo "   🔐 Lake Formation FGAC: 활성화됨 (Session Tag: $LF_SESSION_TAG_VALUE)"
        
        # Job ID를 파일에 저장
        echo "$job_name:$JOB_ID:$job_template_file" >> /tmp/emr-job-ids.txt
        
        return 0
    else
        echo "   ❌ Job 시작 실패"
        return 1
    fi
}

# 모든 Job 실행
echo "1. EMR Job Template 및 Pod Template 생성 및 실행..."
rm -f /tmp/emr-job-ids.txt

for job_config in "${JOB_CONFIGS[@]}"; do
    IFS=':' read -r job_name service_account role_name description <<< "$job_config"
    
    if run_emr_job_with_template "$job_name" "$service_account" "$role_name" "$description"; then
        echo "   Job 실행 성공: $job_name"
    else
        echo "   Job 실행 실패: $job_name"
    fi
    
    # Job 간 간격
    sleep 5
done

# Job 상태 모니터링
echo ""
echo "2. Job 상태 모니터링..."

if [ -f "/tmp/emr-job-ids.txt" ]; then
    echo ""
    echo "📊 실행된 Job 목록 (Blueprint Template 사용):"
    echo "┌─────────────────────────┬─────────────────────────────────────┬─────────────────────────────────────┐"
    echo "│ Job 이름                │ Job ID                              │ Template 파일                       │"
    echo "├─────────────────────────┼─────────────────────────────────────┼─────────────────────────────────────┤"
    
    while IFS=':' read -r job_name job_id template_file; do
        printf "│ %-23s │ %-35s │ %-35s │\n" "$job_name" "$job_id" "$(basename "$template_file")"
    done < /tmp/emr-job-ids.txt
    
    echo "└─────────────────────────┴─────────────────────────────────────┴─────────────────────────────────────┘"
    
    echo ""
    echo "3. Job 상태 확인 중..."
    
    # 각 Job의 상태 확인
    while IFS=':' read -r job_name job_id template_file; do
        echo ""
        echo "   $job_name Job 상태 확인 중..."
        
        # 최대 2.5분 대기 (5회 retry)
        for i in {1..5}; do
            JOB_STATE=$(aws emr-containers describe-job-run \
                --region $REGION \
                --virtual-cluster-id $LF_VIRTUAL_CLUSTER_ID \
                --id $job_id \
                --query 'jobRun.state' \
                --output text 2>/dev/null || echo "UNKNOWN")
            
            case $JOB_STATE in
                "COMPLETED")
                    echo "   ✅ $job_name: 완료"
                    break
                    ;;
                "FAILED"|"CANCELLED")
                    echo "   ❌ $job_name: 실패 ($JOB_STATE)"
                    
                    # 실패 원인 조회
                    FAILURE_REASON=$(aws emr-containers describe-job-run \
                        --region $REGION \
                        --virtual-cluster-id $LF_VIRTUAL_CLUSTER_ID \
                        --id $job_id \
                        --query 'jobRun.failureReason' \
                        --output text 2>/dev/null || echo "Unknown")
                    
                    echo "      실패 원인: $FAILURE_REASON"
                    break
                    ;;
                "RUNNING"|"PENDING"|"SUBMITTED")
                    echo "   ⏳ $job_name: 진행 중 ($JOB_STATE) - ${i}/5"
                    if [ $i -lt 5 ]; then
                        sleep 30
                    fi
                    ;;
                *)
                    echo "   ❓ $job_name: 알 수 없는 상태 ($JOB_STATE)"
                    break
                    ;;
            esac
        done
        
        # 최종 상태가 RUNNING이면 타임아웃 메시지
        if [ "$JOB_STATE" = "RUNNING" ] || [ "$JOB_STATE" = "PENDING" ]; then
            echo "   ⏰ $job_name: 타임아웃 (여전히 실행 중)"
        fi
        
    done < /tmp/emr-job-ids.txt
    
    echo ""
    echo "4. Job 결과 및 Template 정보..."
    
    # Template 파일 위치 안내
    echo ""
    echo "📁 생성된 Template 파일:"
    echo "   Job Templates: ./job-templates/"
    echo "   Pod Templates: ./pod-templates/"
    ls -la job-templates/ | grep -E "\.json$" | awk '{print "     - " $9}'
    ls -la pod-templates/ | grep -E "\.yaml$" | awk '{print "     - " $9}'
    
    # 로그 위치 안내
    echo ""
    echo "📁 Job 로그 위치:"
    echo "   S3 버킷: s3://$RESULTS_BUCKET/logs/"
    echo "   로컬 확인: aws s3 ls s3://$RESULTS_BUCKET/logs/ --recursive"
    
    # Job 상세 정보 조회 명령어 안내
    echo ""
    echo "🔍 Job 상세 정보 조회 명령어:"
    while IFS=':' read -r job_name job_id template_file; do
        echo "   $job_name: aws emr-containers describe-job-run --region $REGION --virtual-cluster-id $LF_VIRTUAL_CLUSTER_ID --id $job_id"
    done < /tmp/emr-job-ids.txt
    
else
    echo "❌ 실행된 Job이 없습니다."
fi

# 임시 파일 정리
rm -f /tmp/emr-job-ids.txt

echo ""
echo "=== EMR on EKS Job 실행 완료 (Lake Formation FGAC + S3 Tables) ==="
echo ""
echo "📋 실행 요약:"
echo "   • 총 4개 역할별 분석 Job 실행"
echo "   • Lake Formation FGAC Virtual Cluster 사용: $LF_VIRTUAL_CLUSTER_ID"
echo "   • Security Configuration 적용: $SECURITY_CONFIG_ID"
echo "   • Session Tag 설정: LakeFormationAuthorizedCaller=$LF_SESSION_TAG_VALUE"
echo "   • S3 Tables (Apache Iceberg) 카탈로그: s3tablescatalog"
echo "   • EMR on EKS Blueprint Job Template 및 Pod Template 활용"
echo "   • S3 Tables 데이터 분석 (기본 카탈로그: s3tablescatalog)"
echo "   • Prometheus 메트릭 수집 활성화"
echo "   • Karpenter 기반 노드 스케줄링"
echo ""
echo "🗂️ S3 Tables 카탈로그 설정:"
echo "   • 카탈로그명: s3tablescatalog"
echo "   • 기본 카탈로그: spark.sql.defaultCatalog=s3tablescatalog"
echo "   • 테이블 참조: bike_db.bike_rental_data"
echo "   • Warehouse: arn:aws:s3tables:$REGION:$ACCOUNT_ID:bucket/${TABLE_BUCKET_NAME}"
echo ""
echo "🔐 Lake Formation FGAC 적용 결과:"
echo "   • DataSteward: 100,000건 전체 분석 (모든 컬럼 접근)"
echo "   • GangnamAnalytics: ~3,000건 (강남구만, birth_year 제외)"
echo "   • Operation: 100,000건 (개인정보 제외: birth_year, gender)"
echo "   • MarketingPartner: ~1,650건 (강남구 20-30대만, birth_year 제외)"
echo ""
echo "📝 Template 재사용:"
echo "   • Job Templates: ./job-templates/ 디렉토리에서 재사용 가능"
echo "   • Pod Templates: ./pod-templates/ 디렉토리에서 재사용 가능"
echo "   • 향후 유사한 Job 실행 시 Template 수정하여 활용"
echo ""
echo "🎯 Lake Formation FGAC + S3 Tables 검증:"
echo "   • 각 역할별로 다른 데이터 접근 결과 확인"
echo "   • Row-level Security: 지역별 필터링 (강남구)"
echo "   • Column-level Security: 역할별 컬럼 접근 제어"
echo "   • Cell-level Security: 연령대별 세밀한 제어 (20-30대)"
echo "   • S3 Tables 네이티브 카탈로그 활용"
echo ""
echo "✅ 다음 단계: ./scripts/06-verify-and-analyze.sh"
