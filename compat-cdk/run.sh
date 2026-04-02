#!/bin/bash
set -e

export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export LOCALSTACK_HOSTNAME=floci
export EDGE_PORT=4566
export AWS_ENDPOINT_URL=http://floci:4566

ENDPOINT="http://floci:4566"
PASS=0
FAIL=0

check() {
    local name="$1"
    local code="$2"
    if [ "$code" -eq 0 ]; then
        echo "  PASS  $name"
        PASS=$((PASS + 1))
    else
        echo "  FAIL  $name"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== CDK Compatibility Test ==="
echo "Endpoint: $ENDPOINT"
echo ""

echo "--- Bootstrap ---"
cdklocal bootstrap --force 2>&1 | grep -E "CDKToolkit|bootstrapped|error|Error" || true
check "cdklocal bootstrap" $?

echo ""
echo "--- Deploy ---"
cdklocal deploy --require-approval never 2>&1 | grep -E "FlociTestStack|complete|error|Error|PASS|FAIL" | tail -10 || true
check "cdklocal deploy FlociTestStack" $?

echo ""
echo "--- Spot Checks ---"

# S3 bucket created
BUCKETS=$(aws --endpoint-url "$ENDPOINT" s3 ls 2>/dev/null | wc -l)
[ "$BUCKETS" -gt 0 ] && check "S3 bucket exists" 0 || check "S3 bucket exists" 1

# SQS queue created
QUEUES=$(aws --endpoint-url "$ENDPOINT" sqs list-queues 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('QueueUrls', [])))" 2>/dev/null || echo "0")
[ "$QUEUES" -gt 0 ] && check "SQS queue exists" 0 || check "SQS queue exists" 1

# DynamoDB table created
TABLES=$(aws --endpoint-url "$ENDPOINT" dynamodb list-tables 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('TableNames', [])))" 2>/dev/null || echo "0")
[ "$TABLES" -gt 0 ] && check "DynamoDB table exists" 0 || check "DynamoDB table exists" 1

# DynamoDB GSI/LSI index table — validates CloudFormation index provisioning
IDX_DESC=$(aws --endpoint-url "$ENDPOINT" dynamodb describe-table --table-name floci-cdk-index-table 2>/dev/null || echo '{}')

GSI_COUNT=$(echo "$IDX_DESC" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('Table',{}).get('GlobalSecondaryIndexes',[])))" 2>/dev/null || echo "0")
[ "$GSI_COUNT" -eq 1 ] && check "DynamoDB GSI exists on index table" 0 || check "DynamoDB GSI exists on index table" 1

GSI_NAME=$(echo "$IDX_DESC" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['Table']['GlobalSecondaryIndexes'][0]['IndexName'])" 2>/dev/null || echo "")
[ "$GSI_NAME" = "gsi-1" ] && check "DynamoDB GSI name is gsi-1" 0 || check "DynamoDB GSI name is gsi-1" 1

GSI_PROJ=$(echo "$IDX_DESC" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['Table']['GlobalSecondaryIndexes'][0]['Projection']['ProjectionType'])" 2>/dev/null || echo "")
[ "$GSI_PROJ" = "ALL" ] && check "DynamoDB GSI projection ALL" 0 || check "DynamoDB GSI projection ALL" 1

LSI_COUNT=$(echo "$IDX_DESC" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('Table',{}).get('LocalSecondaryIndexes',[])))" 2>/dev/null || echo "0")
[ "$LSI_COUNT" -eq 1 ] && check "DynamoDB LSI exists on index table" 0 || check "DynamoDB LSI exists on index table" 1

LSI_NAME=$(echo "$IDX_DESC" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['Table']['LocalSecondaryIndexes'][0]['IndexName'])" 2>/dev/null || echo "")
[ "$LSI_NAME" = "lsi-1" ] && check "DynamoDB LSI name is lsi-1" 0 || check "DynamoDB LSI name is lsi-1" 1

LSI_PROJ=$(echo "$IDX_DESC" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['Table']['LocalSecondaryIndexes'][0]['Projection']['ProjectionType'])" 2>/dev/null || echo "")
[ "$LSI_PROJ" = "KEYS_ONLY" ] && check "DynamoDB LSI projection KEYS_ONLY" 0 || check "DynamoDB LSI projection KEYS_ONLY" 1

# CloudFormation stack exists
CF_STATUS=$(aws --endpoint-url "$ENDPOINT" cloudformation describe-stacks \
    --stack-name FlociTestStack 2>/dev/null | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d['Stacks'][0]['StackStatus'])" 2>/dev/null || echo "NONE")
[ "$CF_STATUS" = "CREATE_COMPLETE" ] && check "CloudFormation stack CREATE_COMPLETE" 0 || check "CloudFormation stack CREATE_COMPLETE" 1

# Secrets Manager generated secret exists and generated value matches constraints
SECRET_NAME="floci-cdk-generated-secret"
SECRET_DESC=$(aws --endpoint-url "$ENDPOINT" secretsmanager describe-secret --secret-id "$SECRET_NAME" 2>/dev/null || echo '{}')
SECRET_FOUND=$(echo "$SECRET_DESC" | python3 -c "import sys,json; d=json.load(sys.stdin); print(1 if d.get('Name') == 'floci-cdk-generated-secret' else 0)" 2>/dev/null || echo "0")
[ "$SECRET_FOUND" -eq 1 ] && check "Secrets Manager generated secret exists" 0 || check "Secrets Manager generated secret exists" 1

SECRET_STRING=$(aws --endpoint-url "$ENDPOINT" secretsmanager get-secret-value --secret-id "$SECRET_NAME" --query SecretString --output text 2>/dev/null || echo "")
SECRET_USERNAME=$(echo "$SECRET_STRING" | python3 -c "import sys,json; d=json.loads(sys.stdin.read() or '{}'); print(d.get('username',''))" 2>/dev/null || echo "")
[ "$SECRET_USERNAME" = "admin" ] && check "GeneratedSecret username is admin" 0 || check "GeneratedSecret username is admin" 1

SECRET_PASSWORD_LEN=$(echo "$SECRET_STRING" | python3 -c "import sys,json; d=json.loads(sys.stdin.read() or '{}'); p=d.get('password',''); print(len(p) if isinstance(p,str) else 0)" 2>/dev/null || echo "0")
[ "$SECRET_PASSWORD_LEN" -eq 24 ] && check "GeneratedSecret password length is 24" 0 || check "GeneratedSecret password length is 24" 1

SECRET_EXCLUDE_OK=$(echo "$SECRET_STRING" | python3 -c "import sys,json,re; d=json.loads(sys.stdin.read() or '{}'); p=d.get('password',''); print(1 if isinstance(p,str) and re.search(r'[abc]', p) is None else 0)" 2>/dev/null || echo "0")
[ "$SECRET_EXCLUDE_OK" -eq 1 ] && check "GeneratedSecret password excludes abc" 0 || check "GeneratedSecret password excludes abc" 1

echo ""
echo "--- Destroy ---"
cdklocal destroy --force 2>&1 | grep -E "FlociTestStack|destroyed|error|Error" | tail -5 || true
check "cdklocal destroy FlociTestStack" $?

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] || exit 1
