#!/usr/bin/env bash
# Floci SDK Test — AWS CLI
#
# Runs against the Floci AWS emulator. Configure via:
#   FLOCI_ENDPOINT=http://localhost:4566  (default)
#
# To run specific groups:
#   ./test_all.sh ssm sqs s3
#   FLOCI_TESTS=ssm,sqs ./test_all.sh

set -euo pipefail

ENDPOINT="${FLOCI_ENDPOINT:-http://localhost:4566}"
REGION="us-east-1"
AWS_ACCESS_KEY_ID="test"
AWS_SECRET_ACCESS_KEY="test"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

PASSED=0
FAILED=0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

aws_cmd() {
    aws --endpoint-url "$ENDPOINT" --region "$REGION" --output json "$@" 2>&1
}

check() {
    local name="$1"
    local ok="$2"
    local msg="${3:-}"
    if [ "$ok" = "true" ]; then
        PASSED=$((PASSED + 1))
        printf "  PASS  %s\n" "$name"
    else
        FAILED=$((FAILED + 1))
        printf "  FAIL  %s\n" "$name"
        [ -n "$msg" ] && printf "        -> %s\n" "$msg"
    fi
}

run_if() {
    local group="$1"
    shift
    if [ ${#ENABLED[@]} -eq 0 ] || [[ " ${ENABLED[*]} " == *" $group "* ]]; then
        "$@"
    fi
}

# ---------------------------------------------------------------------------
# SSM
# ---------------------------------------------------------------------------

run_ssm() {
    echo "--- SSM Tests ---"
    local name="/cli-sdk-test/param"
    local value="param-value-awscli"

    local out rc
    out=$(aws_cmd ssm put-parameter --name "$name" --value "$value" --type String --overwrite 2>&1) && rc=0 || rc=1
    ver=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin).get('Version',0))" 2>/dev/null || echo 0)
    check "SSM PutParameter" "$( [ "$ver" -gt 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd ssm get-parameter --name "$name" --no-with-decryption 2>&1) && rc=0 || rc=1
    got=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin)['Parameter']['Value'])" 2>/dev/null || echo "")
    check "SSM GetParameter" "$( [ "$got" = "$value" ] && echo true || echo false )" "$out"

    out=$(aws_cmd ssm get-parameters-by-path --path "/cli-sdk-test" 2>&1) && rc=0 || rc=1
    found=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print('true' if any(p['Name']=='$name' for p in d.get('Parameters',[])) else 'false')" 2>/dev/null || echo false)
    check "SSM GetParametersByPath" "$found" "$out"

    out=$(aws_cmd ssm add-tags-to-resource --resource-type Parameter --resource-id "$name" --tags Key=env,Value=test 2>&1) && rc=0 || rc=1
    check "SSM AddTagsToResource" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd ssm list-tags-for-resource --resource-type Parameter --resource-id "$name" 2>&1) && rc=0 || rc=1
    found=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print('true' if any(t['Key']=='env' and t['Value']=='test' for t in d.get('TagList',[])) else 'false')" 2>/dev/null || echo false)
    check "SSM ListTagsForResource" "$found" "$out"

    out=$(aws_cmd ssm delete-parameter --name "$name" 2>&1) && rc=0 || rc=1
    check "SSM DeleteParameter" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"
}

# ---------------------------------------------------------------------------
# SQS
# ---------------------------------------------------------------------------

run_sqs() {
    echo "--- SQS Tests ---"
    local queue_name="cli-sdk-test-queue"

    local out rc url
    out=$(aws_cmd sqs create-queue --queue-name "$queue_name" 2>&1) && rc=0 || rc=1
    url=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin).get('QueueUrl',''))" 2>/dev/null || echo "")
    check "SQS CreateQueue" "$( [ -n "$url" ] && echo true || echo false )" "$out"

    out=$(aws_cmd sqs list-queues --queue-name-prefix "cli-sdk-test" 2>&1) && rc=0 || rc=1
    found=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print('true' if any('$queue_name' in u for u in d.get('QueueUrls',[])) else 'false')" 2>/dev/null || echo false)
    check "SQS ListQueues" "$found" "$out"

    local body="hello-from-cli"
    out=$(aws_cmd sqs send-message --queue-url "$url" --message-body "$body" 2>&1) && rc=0 || rc=1
    mid=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin).get('MessageId',''))" 2>/dev/null || echo "")
    check "SQS SendMessage" "$( [ -n "$mid" ] && echo true || echo false )" "$out"

    out=$(aws_cmd sqs receive-message --queue-url "$url" --max-number-of-messages 1 --wait-time-seconds 1 2>&1) && rc=0 || rc=1
    recv_body=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); msgs=d.get('Messages',[]); print(msgs[0]['Body'] if msgs else '')" 2>/dev/null || echo "")
    receipt=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); msgs=d.get('Messages',[]); print(msgs[0]['ReceiptHandle'] if msgs else '')" 2>/dev/null || echo "")
    check "SQS ReceiveMessage" "$( [ "$recv_body" = "$body" ] && echo true || echo false )" "$out"

    if [ -n "$receipt" ]; then
        out=$(aws_cmd sqs delete-message --queue-url "$url" --receipt-handle "$receipt" 2>&1) && rc=0 || rc=1
        check "SQS DeleteMessage" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"
    fi

    out=$(aws_cmd sqs get-queue-attributes --queue-url "$url" --attribute-names ApproximateNumberOfMessages 2>&1) && rc=0 || rc=1
    check "SQS GetQueueAttributes" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd sqs delete-queue --queue-url "$url" 2>&1) && rc=0 || rc=1
    check "SQS DeleteQueue" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"
}

# ---------------------------------------------------------------------------
# SNS
# ---------------------------------------------------------------------------

run_sns() {
    echo "--- SNS Tests ---"
    local topic_name="cli-sdk-test-topic"

    local out rc arn
    out=$(aws_cmd sns create-topic --name "$topic_name" 2>&1) && rc=0 || rc=1
    arn=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin).get('TopicArn',''))" 2>/dev/null || echo "")
    check "SNS CreateTopic" "$( [ -n "$arn" ] && echo true || echo false )" "$out"

    out=$(aws_cmd sns list-topics 2>&1) && rc=0 || rc=1
    found=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print('true' if any('$topic_name' in t.get('TopicArn','') for t in d.get('Topics',[])) else 'false')" 2>/dev/null || echo false)
    check "SNS ListTopics" "$found" "$out"

    out=$(aws_cmd sns get-topic-attributes --topic-arn "$arn" 2>&1) && rc=0 || rc=1
    check "SNS GetTopicAttributes" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd sns publish --topic-arn "$arn" --message "hello-cli" 2>&1) && rc=0 || rc=1
    msg_id=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin).get('MessageId',''))" 2>/dev/null || echo "")
    check "SNS Publish" "$( [ -n "$msg_id" ] && echo true || echo false )" "$out"

    out=$(aws_cmd sns delete-topic --topic-arn "$arn" 2>&1) && rc=0 || rc=1
    check "SNS DeleteTopic" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"
}

# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

run_s3() {
    echo "--- S3 Tests ---"
    local bucket="cli-sdk-test-bucket-$$"

    local out rc
    out=$(aws_cmd s3api create-bucket --bucket "$bucket" 2>&1) && rc=0 || rc=1
    check "S3 CreateBucket" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd s3api list-buckets 2>&1) && rc=0 || rc=1
    found=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print('true' if any(b['Name']=='$bucket' for b in d.get('Buckets',[])) else 'false')" 2>/dev/null || echo false)
    check "S3 ListBuckets" "$found" "$out"

    local key="cli-test-object.txt"
    local body="hello-s3-cli"
    local body_file
    body_file=$(mktemp)
    printf '%s' "$body" > "$body_file"
    out=$(aws_cmd s3api put-object \
        --bucket "$bucket" \
        --key "$key" \
        --metadata owner=team-a,env=dev \
        --storage-class STANDARD_IA \
        --body "$body_file" 2>&1) && rc=0 || rc=1
    check "S3 PutObject" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    local get_tmp
    get_tmp=$(mktemp)
    out=$(aws_cmd s3api get-object --bucket "$bucket" --key "$key" "$get_tmp" 2>&1) && rc=0 || rc=1
    local downloaded get_owner get_storage get_checksum
    downloaded=$(cat "$get_tmp" 2>/dev/null || echo "")
    get_owner=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('Metadata',{}).get('owner',''))" 2>/dev/null || echo "")
    get_storage=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('StorageClass',''))" 2>/dev/null || echo "")
    get_checksum=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ChecksumSHA256',''))" 2>/dev/null || echo "")
    check "S3 GetObject" "$( [ $rc -eq 0 ] && [ "$downloaded" = "$body" ] && echo true || echo false )" "$out"
    check "S3 GetObject metadata header parity" "$( [ "$get_owner" = "team-a" ] && echo true || echo false )" "$out"
    check "S3 GetObject storage class header parity" "$( [ "$get_storage" = "STANDARD_IA" ] && echo true || echo false )" "$out"
    check "S3 GetObject checksum header parity" "$( [ -n "$get_checksum" ] && echo true || echo false )" "$out"
    rm -f "$get_tmp"

    out=$(aws_cmd s3api head-object --bucket "$bucket" --key "$key" 2>&1) && rc=0 || rc=1
    local head_owner head_storage head_checksum head_len
    head_owner=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('Metadata',{}).get('owner',''))" 2>/dev/null || echo "")
    head_storage=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('StorageClass',''))" 2>/dev/null || echo "")
    head_checksum=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ChecksumSHA256',''))" 2>/dev/null || echo "")
    head_len=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ContentLength',0))" 2>/dev/null || echo 0)
    check "S3 HeadObject" "$( [ $rc -eq 0 ] && [ "$head_len" = "${#body}" ] && echo true || echo false )" "$out"
    check "S3 HeadObject metadata parity" "$( [ $rc -eq 0 ] && [ "$head_owner" = "$get_owner" ] && echo true || echo false )" "$out"
    check "S3 HeadObject storage class parity" "$( [ $rc -eq 0 ] && [ "$head_storage" = "$get_storage" ] && echo true || echo false )" "$out"
    check "S3 HeadObject checksum parity" "$( [ $rc -eq 0 ] && [ "$head_checksum" = "$get_checksum" ] && echo true || echo false )" "$out"

    out=$(aws_cmd s3api get-object-attributes \
        --bucket "$bucket" \
        --key "$key" \
        --object-attributes ETag ObjectSize StorageClass Checksum 2>&1) && rc=0 || rc=1
    local attr_size attr_storage attr_etag attr_checksum_type
    attr_size=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ObjectSize',0))" 2>/dev/null || echo 0)
    attr_storage=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('StorageClass',''))" 2>/dev/null || echo "")
    attr_etag=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ETag',''))" 2>/dev/null || echo "")
    attr_checksum_type=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('Checksum',{}).get('ChecksumType',''))" 2>/dev/null || echo "")
    check "S3 GetObjectAttributes" "$( [ $rc -eq 0 ] && [ "$attr_size" = "${#body}" ] && [ "$attr_storage" = "STANDARD_IA" ] && [ -n "$attr_etag" ] && [ "$attr_checksum_type" = "FULL_OBJECT" ] && echo true || echo false )" "$out"

    out=$(aws_cmd s3api list-objects-v2 --bucket "$bucket" 2>&1) && rc=0 || rc=1
    found=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print('true' if any(o['Key']=='$key' for o in d.get('Contents',[])) else 'false')" 2>/dev/null || echo false)
    check "S3 ListObjectsV2" "$found" "$out"

    out=$(aws_cmd s3api put-object-tagging --bucket "$bucket" --key "$key" --tagging 'TagSet=[{Key=env,Value=test}]' 2>&1) && rc=0 || rc=1
    check "S3 PutObjectTagging" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd s3api get-object-tagging --bucket "$bucket" --key "$key" 2>&1) && rc=0 || rc=1
    found=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print('true' if any(t['Key']=='env' and t['Value']=='test' for t in d.get('TagSet',[])) else 'false')" 2>/dev/null || echo false)
    check "S3 GetObjectTagging" "$found" "$out"

    out=$(aws_cmd s3api copy-object --bucket "$bucket" --copy-source "$bucket/$key" --key "${key}.copy" 2>&1) && rc=0 || rc=1
    check "S3 CopyObject" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd s3api copy-object \
        --bucket "$bucket" \
        --copy-source "$bucket/$key" \
        --key "${key}.replace" \
        --metadata-directive REPLACE \
        --metadata owner=team-b \
        --content-type application/json \
        --storage-class GLACIER 2>&1) && rc=0 || rc=1
    check "S3 CopyObject metadata replace" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd s3api head-object --bucket "$bucket" --key "${key}.replace" 2>&1) && rc=0 || rc=1
    local replace_owner replace_storage replace_content_type
    replace_owner=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('Metadata',{}).get('owner',''))" 2>/dev/null || echo "")
    replace_storage=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('StorageClass',''))" 2>/dev/null || echo "")
    replace_content_type=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ContentType',''))" 2>/dev/null || echo "")
    check "S3 CopyObject replaced metadata visible" "$( [ "$replace_owner" = "team-b" ] && [ "$replace_storage" = "GLACIER" ] && [ "$replace_content_type" = "application/json" ] && echo true || echo false )" "$out"

    out=$(aws_cmd s3api put-bucket-versioning \
        --bucket "$bucket" \
        --versioning-configuration Status=Enabled 2>&1) && rc=0 || rc=1
    check "S3 PutBucketVersioning" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    local version_key="versioned-object.txt"
    local version_one="old"
    local version_two="newer!"
    local v1 v2 version_one_file version_two_file
    version_one_file=$(mktemp)
    version_two_file=$(mktemp)
    printf '%s' "$version_one" > "$version_one_file"
    printf '%s' "$version_two" > "$version_two_file"
    out=$(aws_cmd s3api put-object --bucket "$bucket" --key "$version_key" --body "$version_one_file" 2>&1) && rc=0 || rc=1
    v1=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('VersionId',''))" 2>/dev/null || echo "")
    check "S3 PutObject version one" "$( [ $rc -eq 0 ] && [ -n "$v1" ] && echo true || echo false )" "$out"

    out=$(aws_cmd s3api put-object --bucket "$bucket" --key "$version_key" --body "$version_two_file" 2>&1) && rc=0 || rc=1
    v2=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('VersionId',''))" 2>/dev/null || echo "")
    check "S3 PutObject version two" "$( [ $rc -eq 0 ] && [ -n "$v2" ] && [ "$v1" != "$v2" ] && echo true || echo false )" "$out"

    out=$(aws_cmd s3api get-object-attributes \
        --bucket "$bucket" \
        --key "$version_key" \
        --version-id "$v1" \
        --object-attributes ObjectSize ETag StorageClass 2>&1) && rc=0 || rc=1
    local version_one_size
    version_one_size=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ObjectSize',0))" 2>/dev/null || echo 0)
    check "S3 GetObjectAttributes version one" "$( [ $rc -eq 0 ] && [ "$version_one_size" = "${#version_one}" ] && echo true || echo false )" "$out"

    out=$(aws_cmd s3api get-object-attributes \
        --bucket "$bucket" \
        --key "$version_key" \
        --version-id "$v2" \
        --object-attributes ObjectSize ETag StorageClass 2>&1) && rc=0 || rc=1
    local version_two_size
    version_two_size=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ObjectSize',0))" 2>/dev/null || echo 0)
    check "S3 GetObjectAttributes version two" "$( [ $rc -eq 0 ] && [ "$version_two_size" = "${#version_two}" ] && echo true || echo false )" "$out"

    local multipart_key="multipart-object.bin"
    local upload_id part1_path part2_path part1_etag part2_etag multipart_file
    out=$(aws_cmd s3api create-multipart-upload \
        --bucket "$bucket" \
        --key "$multipart_key" \
        --metadata owner=team-a \
        --storage-class STANDARD_IA 2>&1) && rc=0 || rc=1
    upload_id=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('UploadId',''))" 2>/dev/null || echo "")
    check "S3 CreateMultipartUpload" "$( [ $rc -eq 0 ] && [ -n "$upload_id" ] && echo true || echo false )" "$out"

    part1_path=$(mktemp)
    part2_path=$(mktemp)
    printf 'part-one' > "$part1_path"
    printf 'part-two' > "$part2_path"

    out=$(aws_cmd s3api upload-part --bucket "$bucket" --key "$multipart_key" --upload-id "$upload_id" --part-number 1 --body "$part1_path" 2>&1) && rc=0 || rc=1
    part1_etag=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ETag',''))" 2>/dev/null || echo "")
    check "S3 UploadPart one" "$( [ $rc -eq 0 ] && [ -n "$part1_etag" ] && echo true || echo false )" "$out"

    out=$(aws_cmd s3api upload-part --bucket "$bucket" --key "$multipart_key" --upload-id "$upload_id" --part-number 2 --body "$part2_path" 2>&1) && rc=0 || rc=1
    part2_etag=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ETag',''))" 2>/dev/null || echo "")
    check "S3 UploadPart two" "$( [ $rc -eq 0 ] && [ -n "$part2_etag" ] && echo true || echo false )" "$out"

    multipart_file=$(mktemp)
    cat > "$multipart_file" <<EOF
{
  "Parts": [
    { "ETag": $part1_etag, "PartNumber": 1 },
    { "ETag": $part2_etag, "PartNumber": 2 }
  ]
}
EOF

    out=$(aws_cmd s3api complete-multipart-upload \
        --bucket "$bucket" \
        --key "$multipart_key" \
        --upload-id "$upload_id" \
        --multipart-upload "file://$multipart_file" 2>&1) && rc=0 || rc=1
    check "S3 CompleteMultipartUpload" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd s3api get-object-attributes \
        --bucket "$bucket" \
        --key "$multipart_key" \
        --object-attributes ObjectParts Checksum StorageClass \
        --max-parts 1 2>&1) && rc=0 || rc=1
    local mp_storage mp_checksum_type mp_parts_count mp_truncated mp_next_marker mp_first_part
    mp_storage=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('StorageClass',''))" 2>/dev/null || echo "")
    mp_checksum_type=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('Checksum',{}).get('ChecksumType',''))" 2>/dev/null || echo "")
    mp_parts_count=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); parts=d.get('ObjectParts',{}); print(parts.get('PartsCount', parts.get('TotalPartsCount', 0)))" 2>/dev/null || echo 0)
    mp_truncated=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(str(d.get('ObjectParts',{}).get('IsTruncated',False)).lower())" 2>/dev/null || echo false)
    mp_next_marker=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ObjectParts',{}).get('NextPartNumberMarker',0))" 2>/dev/null || echo 0)
    mp_first_part=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); parts=d.get('ObjectParts',{}).get('Parts',[]); print(parts[0].get('PartNumber',0) if parts else 0)" 2>/dev/null || echo 0)
    check "S3 GetObjectAttributes multipart object parts" "$( [ $rc -eq 0 ] && [ "$mp_storage" = "STANDARD_IA" ] && [ "$mp_checksum_type" = "COMPOSITE" ] && [ "$mp_parts_count" = "2" ] && [ "$mp_truncated" = "true" ] && [ "$mp_next_marker" = "1" ] && [ "$mp_first_part" = "1" ] && echo true || echo false )" "$out"

    local invalid_url="${ENDPOINT}/${bucket}/${key}?attributes"
    out=$(curl -sS -i -H 'x-amz-object-attributes: ETag,UnknownThing' "$invalid_url" 2>&1) && rc=0 || rc=1
    local invalid_ok
    invalid_ok=$(echo "$out" | python3 -c "import sys; txt=sys.stdin.read(); ok=(' 400 ' in txt or 'HTTP/1.1 400' in txt or 'HTTP/2 400' in txt) and 'InvalidArgument' in txt; print('true' if ok else 'false')" 2>/dev/null || echo false)
    check "S3 GetObjectAttributes invalid selector" "$invalid_ok" "$out"

    out=$(aws_cmd s3api delete-object --bucket "$bucket" --key "$key" 2>&1) && rc=0 || rc=1
    check "S3 DeleteObject" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    aws_cmd s3api delete-object --bucket "$bucket" --key "${key}.replace" >/dev/null 2>&1 || true
    out=$(aws_cmd s3api delete-object --bucket "$bucket" --key "${key}.copy" 2>&1) && rc=0 || rc=1
    aws_cmd s3api delete-object --bucket "$bucket" --key "$version_key" >/dev/null 2>&1 || true
    aws_cmd s3api delete-object --bucket "$bucket" --key "$multipart_key" >/dev/null 2>&1 || true
    aws_cmd s3api delete-bucket --bucket "$bucket" >/dev/null 2>&1 || true
    rm -f "$body_file" "$version_one_file" "$version_two_file" "$part1_path" "$part2_path" "$multipart_file"
    check "S3 DeleteBucket" "true"
}

# ---------------------------------------------------------------------------
# DynamoDB
# ---------------------------------------------------------------------------

run_dynamodb() {
    echo "--- DynamoDB Tests ---"
    local table="cli-sdk-test-table"

    local out rc
    out=$(aws_cmd dynamodb create-table \
        --table-name "$table" \
        --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
        --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
        --billing-mode PAY_PER_REQUEST 2>&1) && rc=0 || rc=1
    check "DynamoDB CreateTable" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    aws_cmd dynamodb wait table-exists --table-name "$table" >/dev/null 2>&1 || true

    out=$(aws_cmd dynamodb put-item --table-name "$table" \
        --item '{"pk":{"S":"item1"},"sk":{"S":"sort1"},"value":{"S":"hello"}}' 2>&1) && rc=0 || rc=1
    check "DynamoDB PutItem" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd dynamodb get-item --table-name "$table" \
        --key '{"pk":{"S":"item1"},"sk":{"S":"sort1"}}' 2>&1) && rc=0 || rc=1
    val=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('Item',{}).get('value',{}).get('S',''))" 2>/dev/null || echo "")
    check "DynamoDB GetItem" "$( [ "$val" = "hello" ] && echo true || echo false )" "$out"

    out=$(aws_cmd dynamodb update-item --table-name "$table" \
        --key '{"pk":{"S":"item1"},"sk":{"S":"sort1"}}' \
        --update-expression "SET #v = :v" \
        --expression-attribute-names '{"#v":"value"}' \
        --expression-attribute-values '{":v":{"S":"updated"}}' 2>&1) && rc=0 || rc=1
    check "DynamoDB UpdateItem" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd dynamodb query --table-name "$table" \
        --key-condition-expression "pk = :pk" \
        --expression-attribute-values '{":pk":{"S":"item1"}}' 2>&1) && rc=0 || rc=1
    cnt=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin).get('Count',0))" 2>/dev/null || echo 0)
    check "DynamoDB Query" "$( [ "$cnt" -gt 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd dynamodb delete-item --table-name "$table" \
        --key '{"pk":{"S":"item1"},"sk":{"S":"sort1"}}' 2>&1) && rc=0 || rc=1
    check "DynamoDB DeleteItem" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd dynamodb delete-table --table-name "$table" 2>&1) && rc=0 || rc=1
    check "DynamoDB DeleteTable" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"
}

# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------

run_iam() {
    echo "--- IAM Tests ---"
    local user="cli-sdk-test-user"
    local role="cli-sdk-test-role"

    local out rc arn
    out=$(aws_cmd iam create-user --user-name "$user" 2>&1) && rc=0 || rc=1
    check "IAM CreateUser" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd iam list-users 2>&1) && rc=0 || rc=1
    found=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print('true' if any(u['UserName']=='$user' for u in d.get('Users',[])) else 'false')" 2>/dev/null || echo false)
    check "IAM ListUsers" "$found" "$out"

    local trust='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    out=$(aws_cmd iam create-role --role-name "$role" --assume-role-policy-document "$trust" 2>&1) && rc=0 || rc=1
    arn=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin).get('Role',{}).get('Arn',''))" 2>/dev/null || echo "")
    check "IAM CreateRole" "$( [ -n "$arn" ] && echo true || echo false )" "$out"

    out=$(aws_cmd iam get-role --role-name "$role" 2>&1) && rc=0 || rc=1
    check "IAM GetRole" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd iam delete-role --role-name "$role" 2>&1) && rc=0 || rc=1
    check "IAM DeleteRole" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd iam delete-user --user-name "$user" 2>&1) && rc=0 || rc=1
    check "IAM DeleteUser" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"
}

# ---------------------------------------------------------------------------
# STS
# ---------------------------------------------------------------------------

run_sts() {
    echo "--- STS Tests ---"

    local out rc
    out=$(aws_cmd sts get-caller-identity 2>&1) && rc=0 || rc=1
    acct=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin).get('Account',''))" 2>/dev/null || echo "")
    check "STS GetCallerIdentity" "$( [ -n "$acct" ] && echo true || echo false )" "$out"
}

# ---------------------------------------------------------------------------
# Secrets Manager
# ---------------------------------------------------------------------------

run_secretsmanager() {
    echo "--- Secrets Manager Tests ---"
    local name="cli-sdk-test/secret"
    local value='{"user":"admin","pass":"s3cr3t"}'

    local out rc arn
    out=$(aws_cmd secretsmanager create-secret --name "$name" --secret-string "$value" 2>&1) && rc=0 || rc=1
    arn=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin).get('ARN',''))" 2>/dev/null || echo "")
    check "SecretsManager CreateSecret" "$( [ -n "$arn" ] && echo true || echo false )" "$out"

    out=$(aws_cmd secretsmanager get-secret-value --secret-id "$name" 2>&1) && rc=0 || rc=1
    got=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin).get('SecretString',''))" 2>/dev/null || echo "")
    check "SecretsManager GetSecretValue" "$( [ "$got" = "$value" ] && echo true || echo false )" "$out"

    out=$(aws_cmd secretsmanager put-secret-value --secret-id "$name" --secret-string '{"user":"admin","pass":"new"}' 2>&1) && rc=0 || rc=1
    check "SecretsManager PutSecretValue" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd secretsmanager list-secrets 2>&1) && rc=0 || rc=1
    found=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print('true' if any(s['Name']=='$name' for s in d.get('SecretList',[])) else 'false')" 2>/dev/null || echo false)
    check "SecretsManager ListSecrets" "$found" "$out"

    out=$(aws_cmd secretsmanager tag-resource --secret-id "$name" --tags Key=env,Value=test 2>&1) && rc=0 || rc=1
    check "SecretsManager TagResource" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    out=$(aws_cmd secretsmanager delete-secret --secret-id "$name" --force-delete-without-recovery 2>&1) && rc=0 || rc=1
    check "SecretsManager DeleteSecret" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"
}

# ---------------------------------------------------------------------------
# KMS
# ---------------------------------------------------------------------------

run_kms() {
    echo "--- KMS Tests ---"

    local out rc key_id
    out=$(aws_cmd kms create-key --description "cli-sdk-test-key" 2>&1) && rc=0 || rc=1
    key_id=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin).get('KeyMetadata',{}).get('KeyId',''))" 2>/dev/null || echo "")
    check "KMS CreateKey" "$( [ -n "$key_id" ] && echo true || echo false )" "$out"

    local alias="alias/cli-sdk-test"
    out=$(aws_cmd kms create-alias --alias-name "$alias" --target-key-id "$key_id" 2>&1) && rc=0 || rc=1
    check "KMS CreateAlias" "$( [ $rc -eq 0 ] && echo true || echo false )" "$out"

    local plaintext
    plaintext=$(echo -n "hello-kms" | base64)
    out=$(aws_cmd kms encrypt --key-id "$key_id" --plaintext "$plaintext" 2>&1) && rc=0 || rc=1
    ciphertext=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin).get('CiphertextBlob',''))" 2>/dev/null || echo "")
    check "KMS Encrypt" "$( [ -n "$ciphertext" ] && echo true || echo false )" "$out"

    out=$(aws_cmd kms decrypt --ciphertext-blob "$ciphertext" 2>&1) && rc=0 || rc=1
    decrypted=$(echo "$out" | python3 -c "import sys,json,base64; d=json.load(sys.stdin); print(base64.b64decode(d.get('Plaintext','')).decode())" 2>/dev/null || echo "")
    check "KMS Decrypt" "$( [ "$decrypted" = "hello-kms" ] && echo true || echo false )" "$out"

    out=$(aws_cmd kms list-aliases 2>&1) && rc=0 || rc=1
    found=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print('true' if any(a.get('AliasName')=='$alias' for a in d.get('Aliases',[])) else 'false')" 2>/dev/null || echo false)
    check "KMS ListAliases" "$found" "$out"

    aws_cmd kms delete-alias --alias-name "$alias" >/dev/null 2>&1 || true
    aws_cmd kms schedule-key-deletion --key-id "$key_id" --pending-window-in-days 7 >/dev/null 2>&1 || true
}

# ---------------------------------------------------------------------------
# Group registry and entry point
# ---------------------------------------------------------------------------

ALL_GROUPS=(ssm sqs sns s3 dynamodb iam sts secretsmanager kms)

resolve_enabled() {
    local names=()
    for arg in "$@"; do
        IFS=',' read -ra parts <<< "$arg"
        for part in "${parts[@]}"; do
            part="${part// /}"
            [ -n "$part" ] && names+=("${part,,}")
        done
    done
    if [ ${#names[@]} -gt 0 ]; then
        echo "${names[@]}"
        return
    fi
    if [ -n "${FLOCI_TESTS:-}" ]; then
        IFS=',' read -ra parts <<< "$FLOCI_TESTS"
        for part in "${parts[@]}"; do
            part="${part// /}"
            [ -n "$part" ] && names+=("${part,,}")
        done
        [ ${#names[@]} -gt 0 ] && echo "${names[@]}" && return
    fi
    echo ""
}

main() {
    echo "=== Floci SDK Test (AWS CLI) ==="
    echo ""

    local enabled_str
    enabled_str=$(resolve_enabled "$@")
    read -ra ENABLED <<< "$enabled_str"

    if [ ${#ENABLED[@]} -gt 0 ]; then
        echo "Running groups: ${ENABLED[*]}"
        echo ""
    fi

    for group in "${ALL_GROUPS[@]}"; do
        if [ ${#ENABLED[@]} -eq 0 ] || [[ " ${ENABLED[*]} " == *" $group "* ]]; then
            "run_${group//-/_}"
            echo ""
        fi
    done

    echo "=== Results: $PASSED passed, $FAILED failed ==="
    [ "$FAILED" -gt 0 ] && exit 1 || exit 0
}

main "$@"
