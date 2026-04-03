#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${FLOCI_ENDPOINT:-http://localhost:4566}"
AWS="aws --endpoint-url=$ENDPOINT --region us-east-1 --no-cli-pager"

# Route all AWS SDK calls (including STS/IAM used by Terraform backend) to the emulator
export AWS_ENDPOINT_URL="$ENDPOINT"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="us-east-1"
PASS=0
FAIL=0

ok()   { echo "  PASS  $1"; PASS=$((PASS+1)); }
fail() { echo "  FAIL  $1 — $2"; FAIL=$((FAIL+1)); }

echo "=== Terraform Compatibility ==="
echo "Endpoint: $ENDPOINT"
echo ""

# -- Pre-requisites: S3 state bucket + DynamoDB lock table ---------------------
echo "--- Setup: state bucket & lock table ---"

if $AWS s3api head-bucket --bucket tfstate 2>/dev/null; then
  echo "  INFO  S3 state bucket already exists"
else
  $AWS s3api create-bucket --bucket tfstate 2>/dev/null && \
    echo "  INFO  Created S3 state bucket"
fi

if $AWS dynamodb describe-table --table-name tflock 2>/dev/null | grep -q ACTIVE; then
  echo "  INFO  DynamoDB lock table already exists"
else
  $AWS dynamodb create-table \
    --table-name tflock \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --no-cli-pager 2>/dev/null && \
    echo "  INFO  Created DynamoDB lock table"
fi

# Build backend.hcl with resolved endpoint
cat > /tmp/floci-backend.hcl <<EOF
bucket = "tfstate"
key    = "floci-compat.tfstate"
region = "us-east-1"

endpoint                    = "${ENDPOINT}"
access_key                  = "test"
secret_key                  = "test"
skip_credentials_validation = true
skip_region_validation      = true
force_path_style            = true

dynamodb_endpoint = "${ENDPOINT}"
dynamodb_table    = "tflock"
EOF

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# Clean any previous state
rm -rf .terraform .terraform.lock.hcl terraform.tfstate* 2>/dev/null || true

# -- terraform init ------------------------------------------------------------
echo ""
echo "--- terraform init ---"
if terraform init -backend-config=/tmp/floci-backend.hcl \
    -var="endpoint=${ENDPOINT}" -input=false -no-color 2>&1 | \
    grep -E "(Successfully|Error|error)" | head -5; then
  ok "terraform init"
else
  fail "terraform init" "see output above"
  exit 1
fi

# -- terraform validate --------------------------------------------------------
echo ""
echo "--- terraform validate ---"
if terraform validate -no-color 2>&1 | grep -q "Success"; then
  ok "terraform validate"
else
  fail "terraform validate" "$(terraform validate -no-color 2>&1 | head -3)"
  exit 1
fi

# -- terraform plan ------------------------------------------------------------
echo ""
echo "--- terraform plan ---"
PLAN_OUT=$(terraform plan -var="endpoint=${ENDPOINT}" \
    -input=false -no-color 2>&1)
PLAN_EXIT=$?
CHANGES=$(echo "$PLAN_OUT" | grep -E "^Plan:" | head -1)
if [ $PLAN_EXIT -eq 0 ]; then
  ok "terraform plan ($CHANGES)"
else
  echo "$PLAN_OUT" | tail -20
  fail "terraform plan" "exit code $PLAN_EXIT"
  exit 1
fi

# -- terraform apply -----------------------------------------------------------
echo ""
echo "--- terraform apply ---"
terraform apply -var="endpoint=${ENDPOINT}" \
    -input=false -auto-approve -no-color 2>&1 | tee /tmp/tf-apply.log
APPLY_EXIT=${PIPESTATUS[0]}
if [ $APPLY_EXIT -eq 0 ]; then
  APPLIED=$(grep -E "^Apply complete" /tmp/tf-apply.log | head -1)
  ok "terraform apply ($APPLIED)"
else
  grep -E "(Error|error)" /tmp/tf-apply.log | head -20
  fail "terraform apply" "exit code $APPLY_EXIT"
fi

# -- Spot-check: verify resources exist ----------------------------------------
echo ""
echo "--- Spot checks ---"

$AWS s3api head-bucket --bucket floci-compat-app 2>/dev/null && \
  ok "S3 bucket created" || fail "S3 bucket created" "head-bucket returned error"

$AWS sqs get-queue-url --queue-name floci-compat-jobs 2>/dev/null | grep -q QueueUrl && \
  ok "SQS queue created" || fail "SQS queue created" "queue not found"

$AWS sns list-topics 2>/dev/null | grep -q "floci-compat-events" && \
  ok "SNS topic created" || fail "SNS topic created" "topic not found"

$AWS dynamodb describe-table --table-name floci-compat-items 2>/dev/null | grep -q ACTIVE && \
  ok "DynamoDB table created" || fail "DynamoDB table created" "table not found"

$AWS ssm get-parameter --name /floci-compat/db-url 2>/dev/null | grep -q "jdbc:" && \
  ok "SSM parameter created" || fail "SSM parameter created" "parameter not found"

$AWS secretsmanager describe-secret --secret-id "floci-compat/db-creds" 2>/dev/null | grep -q "floci-compat" && \
  ok "Secrets Manager secret created" || fail "Secrets Manager secret created" "secret not found"

# -- terraform destroy ---------------------------------------------------------
echo ""
echo "--- terraform destroy ---"
terraform destroy -var="endpoint=${ENDPOINT}" \
    -input=false -auto-approve -no-color 2>&1 | tee /tmp/tf-destroy.log
DESTROY_EXIT=${PIPESTATUS[0]}
if [ $DESTROY_EXIT -eq 0 ]; then
  DESTROYED=$(grep -E "^Destroy complete" /tmp/tf-destroy.log | head -1)
  ok "terraform destroy ($DESTROYED)"
else
  grep -E "(Error|error)" /tmp/tf-destroy.log | head -20
  fail "terraform destroy" "exit code $DESTROY_EXIT"
fi

# -- Summary -------------------------------------------------------------------
echo ""
echo "=================================================="
echo "Results: $PASS passed, $FAIL failed"
[ $FAIL -eq 0 ]
