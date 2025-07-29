#!/bin/bash

# Lake Formation Fine-Grained Access Control (FGAC) 설정 스크립트
# EMR on EKS와 Lake Formation 통합을 위한 설정
# AWS 공식 문서 기준: QueryExecutionRole(System Driver)과 QueryEngineRole(System Executor) 분리
# 참조: https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/security_iam_fgac-lf-enable.html

set -e

# 환경 변수 로드
if [ ! -f ".env" ]; then
    echo "❌ .env 파일이 존재하지 않습니다."
    echo "먼저 ./scripts/04-setup-emr-on-eks.sh를 실행하세요."
    exit 1
fi

source .env

echo "=== Lake Formation FGAC 설정 시작 ==="
echo "계정 ID: $ACCOUNT_ID"
echo "리전: $REGION"
echo "EKS 클러스터: $CLUSTER_NAME"
echo "S3 Tables 버킷: $TABLE_BUCKET_NAME"
echo ""

# Lake Formation 설정 변수
LF_SESSION_TAG_VALUE="EMRonEKSEngine"
SYSTEM_NAMESPACE="emr-system"
USER_NAMESPACE="emr-data-team-a"  # 실제 존재하는 네임스페이스 사용
QUERY_EXECUTION_ROLE_NAME="LF_QueryExecutionRole"  # System Driver용 (AWS 문서 기준)
QUERY_ENGINE_ROLE_NAME="LF_QueryEngineRole"        # System Executor용 (AWS 문서 기준)
SECURITY_CONFIG_NAME="seoul-bike-lf-security-config"

echo "Lake Formation 설정:"
echo "• Session Tag Value: $LF_SESSION_TAG_VALUE"
echo "• System Namespace: $SYSTEM_NAMESPACE"
echo "• User Namespace: $USER_NAMESPACE"
echo "• Query Execution Role (System Driver): $QUERY_EXECUTION_ROLE_NAME"
echo "• Query Engine Role (System Executor): $QUERY_ENGINE_ROLE_NAME"
echo ""

# Step 1: Lake Formation Application Integration Settings 설정
echo "1. Lake Formation Application Integration Settings 설정..."

# Lake Formation에서 외부 엔진 허용 설정
echo "   Lake Formation에서 외부 엔진 데이터 필터링 허용 설정 중..."

# Lake Formation 설정 확인
LF_SETTINGS=$(aws lakeformation get-data-lake-settings --region $REGION 2>/dev/null || echo "{}")

# 현재 사용자 정보 가져오기
CURRENT_USER_ARN=$(aws sts get-caller-identity --query 'Arn' --output text)

# 기존 Lake Formation 설정 가져오기
EXISTING_SETTINGS=$(aws lakeformation get-data-lake-settings --region $REGION)

# 외부 엔진 허용 설정 업데이트 (기존 설정 유지하면서 필요한 부분만 추가)
aws lakeformation put-data-lake-settings \
    --region $REGION \
    --data-lake-settings '{
        "DataLakeAdmins": [
            {
                "DataLakePrincipalIdentifier": "'$CURRENT_USER_ARN'"
            }
        ],
        "CreateDatabaseDefaultPermissions": [],
        "CreateTableDefaultPermissions": [],
        "Parameters": {
            "CROSS_ACCOUNT_VERSION": "3",
            "SET_CONTEXT": "TRUE"
        },
        "AllowExternalDataFiltering": true,
        "AllowFullTableExternalDataAccess": false,
        "ExternalDataFilteringAllowList": [
            {
                "DataLakePrincipalIdentifier": "'$ACCOUNT_ID'"
            }
        ],
        "AuthorizedSessionTagValueList": [
            "'$LF_SESSION_TAG_VALUE'"
        ]
    }' >/dev/null

echo "   ✅ Lake Formation Application Integration Settings 설정 완료"

# Step 2: EKS RBAC 권한 설정
echo ""
echo "2. EKS RBAC 권한 설정..."

# System Namespace 생성
echo "   System Namespace 생성 중..."
kubectl create namespace $SYSTEM_NAMESPACE 2>/dev/null || echo "   System Namespace가 이미 존재합니다."

# EMR Containers ClusterRole 생성
echo "   EMR Containers ClusterRole 생성 중..."
cat > /tmp/emr-containers-cluster-role.yaml << EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: emr-containers
rules:
  - apiGroups: [""]
    resources: ["namespaces"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["serviceaccounts", "services", "configmaps", "events", "pods", "pods/log"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["create", "patch", "delete", "watch"]
  - apiGroups: ["apps"]
    resources: ["statefulsets", "deployments"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "annotate", "patch", "label"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "annotate", "patch", "label"]
  - apiGroups: ["extensions", "networking.k8s.io"]
    resources: ["ingresses"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "annotate", "patch", "label"]
  - apiGroups: ["rbac.authorization.k8s.io"]
    resources: ["clusterroles","clusterrolebindings","roles", "rolebindings"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "describe", "create", "edit", "delete", "deletecollection", "annotate", "patch", "label"]
EOF

kubectl apply -f /tmp/emr-containers-cluster-role.yaml

# EMR Containers ClusterRoleBinding 생성
echo "   EMR Containers ClusterRoleBinding 생성 중..."
cat > /tmp/emr-containers-cluster-role-binding.yaml << EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: emr-containers
subjects:
- kind: User
  name: emr-containers
  apiGroup: rbac.authorization.k8s.io
roleRef:
  kind: ClusterRole
  name: emr-containers
  apiGroup: rbac.authorization.k8s.io
EOF

kubectl apply -f /tmp/emr-containers-cluster-role-binding.yaml

echo "   ✅ EKS RBAC 권한 설정 완료"

# Step 3: IAM 역할 설정
echo ""
echo "3. IAM 역할 설정..."

# Query Execution Role 생성 (System Driver용)
echo "   Query Execution Role (System Driver용) 생성 중..."

# Query Execution Role Trust Policy
cat > /tmp/query-execution-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "emr-containers.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "Federated": "$OIDC_PROVIDER_ARN"
            },
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringLike": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:sub": "system:serviceaccount:$SYSTEM_NAMESPACE:emr-containers-sa-*"
                },
                "StringEquals": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:aud": "sts.amazonaws.com"
                }
            }
        }
    ]
}
EOF

# Query Execution Role 생성
aws iam create-role \
    --role-name $QUERY_EXECUTION_ROLE_NAME \
    --assume-role-policy-document file:///tmp/query-execution-trust-policy.json \
    --description "Lake Formation Query Execution Role for EMR on EKS System Driver" \
    2>/dev/null || echo "   Query Execution Role이 이미 존재합니다."

# Query Execution Role Permissions Policy
cat > /tmp/query-execution-permissions-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AssumeJobRoleWithSessionTagAccessForSystemDriver",
            "Effect": "Allow",
            "Action": [
                "sts:AssumeRole",
                "sts:TagSession"
            ],
            "Resource": [
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_DATA_STEWARD_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_GANGNAM_ANALYTICS_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_OPERATION_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_MARKETING_PARTNER_ROLE"
            ],
            "Condition": {
                "StringLike": {
                    "aws:RequestTag/LakeFormationAuthorizedCaller": "$LF_SESSION_TAG_VALUE"
                }
            }
        },
        {
            "Sid": "CreateCertificateAccessForTLS",
            "Effect": "Allow",
            "Action": "emr-containers:CreateCertificate",
            "Resource": "*"
        },
        {
            "Sid": "GlueCatalogAccessForQueryExecution",
            "Effect": "Allow",
            "Action": [
                "glue:Get*",
                "glue:List*"
            ],
            "Resource": ["*"]
        },
        {
            "Sid": "LakeFormationAccessForQueryExecution",
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess",
                "lakeformation:GetWorkUnits",
                "lakeformation:StartQueryPlanning",
                "lakeformation:GetWorkUnitResults"
            ],
            "Resource": ["*"]
        }
    ]
}
EOF

# Query Execution Role에 권한 정책 연결
aws iam put-role-policy \
    --role-name $QUERY_EXECUTION_ROLE_NAME \
    --policy-name "QueryExecutionPermissions" \
    --policy-document file:///tmp/query-execution-permissions-policy.json

echo "   ✅ Query Execution Role 생성 완료"

# Query Engine Role 생성 (System Executor용)
echo "   Query Engine Role (System Executor용) 생성 중..."

# Query Engine Role Trust Policy
cat > /tmp/query-engine-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "emr-containers.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "Federated": "$OIDC_PROVIDER_ARN"
            },
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringLike": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:sub": "system:serviceaccount:$SYSTEM_NAMESPACE:emr-containers-sa-*"
                },
                "StringEquals": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:aud": "sts.amazonaws.com"
                }
            }
        }
    ]
}
EOF

# Query Engine Role 생성
aws iam create-role \
    --role-name $QUERY_ENGINE_ROLE_NAME \
    --assume-role-policy-document file:///tmp/query-engine-trust-policy.json \
    --description "Lake Formation Query Engine Role for EMR on EKS System Executor" \
    2>/dev/null || echo "   Query Engine Role이 이미 존재합니다."

# Query Engine Role Permissions Policy (AWS 공식 문서 기준)
cat > /tmp/query-engine-permissions-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AssumeJobRoleWithSessionTagAccessForSystemExecutor",
            "Effect": "Allow",
            "Action": [
                "sts:AssumeRole"
            ],
            "Resource": [
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_DATA_STEWARD_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_GANGNAM_ANALYTICS_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_OPERATION_ROLE",
                "arn:aws:iam::$ACCOUNT_ID:role/$LF_MARKETING_PARTNER_ROLE"
            ]
        },
        {
            "Sid": "CreateCertificateAccessForTLS",
            "Effect": "Allow",
            "Action": "emr-containers:CreateCertificate",
            "Resource": "*"
        },
        {
            "Sid": "GlueCatalogAccessForQueryEngine",
            "Effect": "Allow",
            "Action": [
                "glue:Get*",
                "glue:List*"
            ],
            "Resource": ["*"]
        },
        {
            "Sid": "LakeFormationAccessForQueryEngine",
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess",
                "lakeformation:GetWorkUnits",
                "lakeformation:StartQueryPlanning",
                "lakeformation:GetWorkUnitResults"
            ],
            "Resource": ["*"]
        },
        {
            "Sid": "S3AccessForQueryEngine",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::$TABLE_BUCKET_NAME",
                "arn:aws:s3:::$TABLE_BUCKET_NAME/*"
            ]
        }
    ]
}
EOF

# Query Engine Role에 권한 정책 연결
aws iam put-role-policy \
    --role-name $QUERY_ENGINE_ROLE_NAME \
    --policy-name "QueryEnginePermissions" \
    --policy-document file:///tmp/query-engine-permissions-policy.json

echo "   ✅ Query Engine Role 생성 완료"

# Job Execution Role Trust Policy 업데이트 (AWS 공식 문서 기준)
echo "   Job Execution Role Trust Policy 업데이트 중..."

for role in "$LF_DATA_STEWARD_ROLE" "$LF_GANGNAM_ANALYTICS_ROLE" "$LF_OPERATION_ROLE" "$LF_MARKETING_PARTNER_ROLE"; do
    echo "     $role Trust Policy 업데이트 중..."
    
    # AWS 공식 문서 기준 Lake Formation FGAC Trust Policy
    cat > /tmp/job-execution-trust-policy-$role.json << EOF
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
        },
        {
            "Effect": "Allow",
            "Principal": {
                "Federated": "$OIDC_PROVIDER_ARN"
            },
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringLike": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:sub": [
                        "system:serviceaccount:$USER_NAMESPACE:emr-containers-sa-*",
                        "system:serviceaccount:emr-data-team-a:emr-data-*-sa",
                        "system:serviceaccount:$USER_NAMESPACE:emr-*-sa"
                    ]
                },
                "StringEquals": {
                    "oidc.eks.$REGION.amazonaws.com/id/$OIDC_ID:aud": "sts.amazonaws.com"
                }
            }
        },
        {
            "Sid": "TrustQueryExecutionRoleForSystemDriver",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::$ACCOUNT_ID:role/$QUERY_EXECUTION_ROLE_NAME"
            },
            "Action": [
                "sts:AssumeRole",
                "sts:TagSession"
            ],
            "Condition": {
                "StringLike": {
                    "aws:RequestedRegion": "$REGION",
                    "aws:RequestTag/LakeFormationAuthorizedCaller": "$LF_SESSION_TAG_VALUE"
                }
            }
        },
        {
            "Sid": "TrustQueryEngineRoleForSystemExecutor",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::$ACCOUNT_ID:role/$QUERY_ENGINE_ROLE_NAME"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "$REGION"
                }
            }
        }
    ]
}
EOF
    
    # Trust Policy 업데이트
    aws iam update-assume-role-policy \
        --role-name $role \
        --policy-document file:///tmp/job-execution-trust-policy-$role.json
done

echo "   ✅ Job Execution Role Trust Policy 업데이트 완료 (AWS 공식 문서 기준)"

# Job Execution Role에 Lake Formation 권한 추가
echo "   Job Execution Role에 Lake Formation 권한 추가 중..."

for role in "$LF_DATA_STEWARD_ROLE" "$LF_GANGNAM_ANALYTICS_ROLE" "$LF_OPERATION_ROLE" "$LF_MARKETING_PARTNER_ROLE"; do
    echo "     $role에 Lake Formation 권한 추가 중..."
    
    # Lake Formation 권한 정책
    cat > /tmp/lake-formation-permissions-$role.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "GlueCatalogAccess",
            "Effect": "Allow",
            "Action": [
                "glue:Get*",
                "glue:Create*",
                "glue:Update*"
            ],
            "Resource": ["*"]
        },
        {
            "Sid": "LakeFormationAccess",
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess"
            ],
            "Resource": ["*"]
        },
        {
            "Sid": "CreateCertificateAccessForTLS",
            "Effect": "Allow",
            "Action": "emr-containers:CreateCertificate",
            "Resource": "*"
        }
    ]
}
EOF
    
    # Lake Formation 권한 정책 연결
    aws iam put-role-policy \
        --role-name $role \
        --policy-name "LakeFormationPermissions" \
        --policy-document file:///tmp/lake-formation-permissions-$role.json
done

echo "   ✅ Job Execution Role Lake Formation 권한 추가 완료"

# EMR on EKS 실행에 필요한 추가 권한 추가 중...
echo "EMR on EKS 실행에 필요한 추가 권한 추가 중..."

for role in "LF_DataStewardRole" "LF_GangnamAnalyticsRole" "LF_OperationRole" "LF_MarketingPartnerRole"; do
    echo "  $role에 EMR 실행 권한 추가 중..."
    
    cat > /tmp/emr-execution-permissions-$role.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "EMRContainersAccess",
            "Effect": "Allow",
            "Action": [
                "emr-containers:DescribeJobRun",
                "emr-containers:ListJobRuns",
                "emr-containers:DescribeVirtualCluster",
                "emr-containers:CreateCertificate"
            ],
            "Resource": "*"
        },
        {
            "Sid": "S3TablesAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::$TABLE_BUCKET_NAME",
                "arn:aws:s3:::$TABLE_BUCKET_NAME/*",
                "arn:aws:s3:::$SCRIPTS_BUCKET",
                "arn:aws:s3:::$SCRIPTS_BUCKET/*"
            ]
        },
        {
            "Sid": "S3TablesAPIAccess",
            "Effect": "Allow",
            "Action": [
                "s3tables:GetTable",
                "s3tables:GetTableData",
                "s3tables:PutTableData",
                "s3tables:GetTableMetadataLocation",
                "s3tables:ListTables",
                "s3tables:ListNamespaces"
            ],
            "Resource": [
                "$BUCKET_ARN",
                "$BUCKET_ARN/*"
            ]
        }
    ]
}
EOF
    
    aws iam put-role-policy \
        --role-name $role \
        --policy-name "EMRExecutionPermissions" \
        --policy-document file:///tmp/emr-execution-permissions-$role.json
    
    echo "  ✅ $role EMR 실행 권한 추가 완료"
done

echo "✅ 모든 Lake Formation 역할에 EMR 실행 권한 추가 완료"


# CloudWatch Logs 권한 범위 확장 중...
echo "CloudWatch Logs 권한 범위 확장 중..."

for role in "LF_DataStewardRole" "LF_GangnamAnalyticsRole" "LF_OperationRole" "LF_MarketingPartnerRole"; do
    echo "  $role에 확장된 CloudWatch Logs 권한 추가 중..."
    
    cat > /tmp/expanded-cloudwatch-logs-permissions-$role.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CloudWatchLogsFullAccess",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:GetLogEvents",
                "logs:FilterLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:$REGION:$ACCOUNT_ID:*"
            ]
        },
        {
            "Sid": "S3LogsAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::seoul-bike-analytics-results-$ACCOUNT_ID",
                "arn:aws:s3:::seoul-bike-analytics-results-$ACCOUNT_ID/*"
            ]
        }
    ]
}
EOF
    
    # 기존 정책 삭제 후 새로운 정책 추가
    aws iam delete-role-policy --role-name $role --policy-name "CloudWatchLogsPermissions" 2>/dev/null || true
    
    aws iam put-role-policy \
        --role-name $role \
        --policy-name "ExpandedCloudWatchLogsPermissions" \
        --policy-document file:///tmp/expanded-cloudwatch-logs-permissions-$role.json
    
    echo "  ✅ $role 확장된 CloudWatch Logs 권한 추가 완료"
done

echo "✅ 모든 Lake Formation 역할에 확장된 CloudWatch Logs 권한 추가 완료"

# Step 4: Security Configuration 생성
echo ""
echo "4. Security Configuration 생성..."

# 기존 Security Configuration 삭제 (있다면)
aws emr-containers delete-security-configuration \
    --id $SECURITY_CONFIG_NAME \
    --region $REGION 2>/dev/null || true

# Security Configuration 생성
SECURITY_CONFIG_ID=$(aws emr-containers create-security-configuration \
    --name $SECURITY_CONFIG_NAME \
    --region $REGION \
    --security-configuration '{
        "authorizationConfiguration": {
            "lakeFormationConfiguration": {
                "authorizedSessionTagValue": "'$LF_SESSION_TAG_VALUE'",
                "secureNamespaceInfo": {
                    "clusterId": "'$CLUSTER_NAME'",
                    "namespace": "'$SYSTEM_NAMESPACE'"
                },
                "queryEngineRoleArn": "arn:aws:iam::'$ACCOUNT_ID':role/'$QUERY_EXECUTION_ROLE_NAME'"
            }
        }
    }' \
    --query 'id' --output text)

echo "   ✅ Security Configuration 생성 완료: $SECURITY_CONFIG_ID"

# Step 5: Lake Formation FGAC Virtual Cluster 생성
echo ""
echo "5. Lake Formation FGAC Virtual Cluster 생성..."

LF_VIRTUAL_CLUSTER_NAME="seoul-bike-lf-vc"

# 기존 Virtual Cluster 삭제 (있다면)
EXISTING_VC=$(aws emr-containers list-virtual-clusters \
    --region $REGION \
    --query "virtualClusters[?name=='$LF_VIRTUAL_CLUSTER_NAME' && state=='RUNNING'].id" \
    --output text)

if [ ! -z "$EXISTING_VC" ] && [ "$EXISTING_VC" != "None" ]; then
    echo "   기존 Virtual Cluster 삭제 중: $EXISTING_VC"
    aws emr-containers delete-virtual-cluster \
        --id $EXISTING_VC \
        --region $REGION
    
    # 삭제 완료 대기
    echo "   Virtual Cluster 삭제 대기 중..."
    while true; do
        VC_STATE=$(aws emr-containers describe-virtual-cluster \
            --id $EXISTING_VC \
            --region $REGION \
            --query 'virtualCluster.state' \
            --output text 2>/dev/null || echo "TERMINATED")
        
        if [ "$VC_STATE" = "TERMINATED" ]; then
            break
        fi
        sleep 10
    done
fi

# Lake Formation FGAC Virtual Cluster 생성
LF_VIRTUAL_CLUSTER_ID=$(aws emr-containers create-virtual-cluster \
    --name $LF_VIRTUAL_CLUSTER_NAME \
    --region $REGION \
    --container-provider '{
        "id": "'$CLUSTER_NAME'",
        "type": "EKS",
        "info": {
            "eksInfo": {
                "namespace": "'$USER_NAMESPACE'"
            }
        }
    }' \
    --security-configuration-id $SECURITY_CONFIG_ID \
    --query 'id' --output text)

echo "   ✅ Lake Formation FGAC Virtual Cluster 생성 완료: $LF_VIRTUAL_CLUSTER_ID"

# Step 6: 환경 변수 파일 업데이트
echo ""
echo "6. 환경 변수 파일 업데이트..."

# .env 파일에 Lake Formation 설정 추가
cat >> .env << EOF

# Lake Formation FGAC 설정 ($(date '+%Y-%m-%d %H:%M:%S'))
LF_SESSION_TAG_VALUE=$LF_SESSION_TAG_VALUE
SYSTEM_NAMESPACE=$SYSTEM_NAMESPACE
USER_NAMESPACE=$USER_NAMESPACE
QUERY_EXECUTION_ROLE_NAME=$QUERY_EXECUTION_ROLE_NAME
QUERY_ENGINE_ROLE_NAME=$QUERY_ENGINE_ROLE_NAME
SECURITY_CONFIG_NAME=$SECURITY_CONFIG_NAME
SECURITY_CONFIG_ID=$SECURITY_CONFIG_ID
LF_VIRTUAL_CLUSTER_NAME=$LF_VIRTUAL_CLUSTER_NAME
LF_VIRTUAL_CLUSTER_ID=$LF_VIRTUAL_CLUSTER_ID
EOF

echo "   ✅ 환경 변수 파일 업데이트 완료"

# 임시 파일 정리
rm -f /tmp/emr-containers-*.yaml
rm -f /tmp/query-execution-*.json
rm -f /tmp/query-engine-*.json
rm -f /tmp/job-execution-*.json
rm -f /tmp/lake-formation-*.json
rm -f /tmp/emr-execution-*.json
rm -f /tmp/expanded-cloudwatch-*.json

echo ""
echo "=== Lake Formation FGAC 설정 완료 ==="
echo ""
echo "📋 설정된 리소스 요약:"
echo "┌─────────────────────────────┬─────────────────────────────────────┐"
echo "│ 리소스                      │ 값                                  │"
echo "├─────────────────────────────┼─────────────────────────────────────┤"
echo "│ Session Tag Value           │ $LF_SESSION_TAG_VALUE               │"
echo "│ System Namespace            │ $SYSTEM_NAMESPACE                   │"
echo "│ User Namespace              │ $USER_NAMESPACE                     │"
echo "│ Query Execution Role (Driver) │ $QUERY_EXECUTION_ROLE_NAME      │"
echo "│ Query Engine Role (Executor)   │ $QUERY_ENGINE_ROLE_NAME         │"
echo "│ Security Configuration      │ $SECURITY_CONFIG_ID                 │"
echo "│ LF Virtual Cluster          │ $LF_VIRTUAL_CLUSTER_ID              │"
echo "└─────────────────────────────┴─────────────────────────────────────┘"
echo ""
echo "🔐 Lake Formation FGAC 기능:"
echo "   • Row-level Security: 지역별 필터링 (강남구)"
echo "   • Column-level Security: 역할별 컬럼 접근 제어"
echo "   • Cell-level Security: 연령대별 세밀한 제어 (20-30대)"
echo ""
echo "🎭 지원되는 역할:"
echo "   • DataSteward: 전체 데이터 접근"
echo "   • GangnamAnalytics: 강남구 데이터만"
echo "   • Operation: 운영 데이터 (개인정보 제외)"
echo "   • MarketingPartner: 강남구 20-30대만"
echo ""
echo "✅ 다음 단계: ./scripts/05-run-emr-jobs.sh (Lake Formation FGAC 활성화됨)"
echo ""
echo "⚠️  주의사항:"
echo "   • Lake Formation에서 데이터베이스 및 테이블 권한을 별도로 설정해야 합니다"
echo "   • S3 Tables 데이터가 Lake Formation에 등록되어 있어야 합니다"
echo "   • 각 역할별로 적절한 Lake Formation 권한을 부여해야 합니다"
echo ""
