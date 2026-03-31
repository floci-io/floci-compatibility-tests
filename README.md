# floci-compatibility-tests

Compatibility test suite for [Floci](https://github.com/hectorvent/floci) — a local AWS emulator.

Verifies that standard AWS tooling (SDKs, CDK, OpenTofu/Terraform) works correctly against the emulator without modification. Tests run against a live Floci instance and use real AWS SDK clients — no mocks.

## Modules

| Module | Language / Tool | AWS SDK | SDK Version |
|---|---|---|---|
| [`sdk-test-java`](sdk-test-java/) | Java 17 | AWS SDK for Java v2 | 2.31.8 |
| [`sdk-test-go`](sdk-test-go/) | Go 1.24 | AWS SDK for Go v2 | 1.41.4 |
| [`sdk-test-node`](sdk-test-node/) | Node.js | AWS SDK for JavaScript v3 | 3.1003.0 |
| [`sdk-test-python`](sdk-test-python/) | Python 3 | boto3 | 1.37.1 |
| [`sdk-test-rust`](sdk-test-rust/) | Rust | AWS SDK for Rust | 1.8.15 |
| [`sdk-test-awscli`](sdk-test-awscli/) | Bash | AWS CLI v2 | 2.22.35 |
| [`compat-cdk`](compat-cdk/) | TypeScript | AWS CDK v2 | 2.171.1 |
| [`compat-opentofu`](compat-opentofu/) | HCL | OpenTofu (Terraform-compatible) with AWS provider | ~5.0 |
| [`compat-terraform`](compat-terraform/) | HCL | Terraform with AWS provider | ~6.0 |

## Prerequisites

- **Floci running** on `http://localhost:4566` (or set `FLOCI_ENDPOINT`)
- **Docker** — required for Lambda invocation tests (containers are pulled automatically)

Per-module requirements:

| Module | Requirements |
|---|---|
| `sdk-test-java` | Java 17+, Maven |
| `sdk-test-go` | Go 1.24+ |
| `sdk-test-node` | Node.js 18+, npm |
| `sdk-test-python` | Python 3.9+, pip |
| `sdk-test-rust` | Rust (stable), Cargo |
| `sdk-test-awscli` | AWS CLI v2, bash |
| `compat-cdk` | Node.js 18+, `aws-cdk-local` (`npm i -g aws-cdk-local`) |
| `compat-opentofu` | OpenTofu CLI, AWS CLI |
| `compat-terraform` | Terraform CLI, AWS CLI |

## Quick Start

Start Floci first (from the `aws-local` repo):

```bash
docker compose up -d
```

Then run any test module:

```bash
# Java SDK
cd sdk-test-java && mvn compile exec:java

# Go SDK
cd sdk-test-go && go run main.go

# Node.js SDK
cd sdk-test-node && npm install && node test-all.mjs

# Python SDK
cd sdk-test-python && pip install -r requirements.txt && python test_all.py

# AWS CDK
cd compat-cdk && npm install && ./run.sh

# Rust SDK
cd sdk-test-rust && cargo run

# AWS CLI
cd sdk-test-awscli && ./test_all.sh

# OpenTofu
cd compat-opentofu && ./run.sh

# Terraform
cd compat-terraform && ./run.sh
```

## Configuration

All modules read the endpoint from the `FLOCI_ENDPOINT` environment variable (default: `http://localhost:4566`).

```bash
FLOCI_ENDPOINT=http://my-host:4566 go run main.go
FLOCI_ENDPOINT=http://my-host:4566 python test_all.py
FLOCI_ENDPOINT=http://my-host:4566 node test-all.mjs
```

AWS credentials are always `test` / `test` / `us-east-1` — the emulator accepts any value.

## Services Covered

### Java SDK (`sdk-test-java`)

537 assertions across 38 test groups::

| Group flag | Description |
|---|---|
| `ssm` | Parameter Store — put, get, label, history, path, tags |
| `sqs` | Queues, send/receive/delete, DLQ, message move, visibility |
| `sqs-esm` | SQS → Lambda event source mapping, disable/enable |
| `sns` | Topics, subscriptions, publish, SQS delivery, attributes |
| `s3` | Buckets, objects, tagging, copy, batch delete |
| `s3-object-lock` | COMPLIANCE/GOVERNANCE modes, legal hold, default retention |
| `s3-advanced` | Bucket policy, CORS, lifecycle, ACL, encryption, S3 Select, virtual host |
| `dynamodb` | Tables, CRUD, batch, TTL, tags, streams |
| `dynamodb-advanced` | GSI, pagination, condition expressions, transactions, TTL expiry |
| `dynamodb-lsi` | Local secondary indexes |
| `dynamodb-streams` | INSERT/MODIFY/REMOVE records, stream types, shard iterator |
| `lambda` | Create/invoke/update/delete functions, dry-run, async |
| `lambda-invoke` | Synchronous RequestResponse invocation with Docker |
| `lambda-http` | Direct Lambda URL HTTP invocation |
| `lambda-warmpool` | 110 sequential invocations, warm container reuse |
| `lambda-concurrent` | 10 000 invocations across 10 threads (~290 req/s) |
| `apigateway` | REST APIs, resources, methods, integrations, stages, authorizers, usage plans, domain names |
| `apigateway-execute` | AWS_PROXY Lambda integration, stage invocation, MOCK integrations |
| `apigatewayv2` | HTTP API create/integrate/route |
| `s3-notifications` | S3 → SQS and S3 → SNS event notifications |
| `iam` | Users, groups, roles, policies, access keys, instance profiles |
| `sts` | GetCallerIdentity, AssumeRole, GetSessionToken, federation |
| `iam-perf` | Throughput (2000 ops/sec), latency p99, concurrent correctness |
| `elasticache` | IAM auth token generation and validation |
| `elasticache-mgmt` | Replication groups, users, password auth, ModifyUser |
| `elasticache-lettuce` | Redis data-plane via Lettuce — PING, SET/GET, password/IAM auth |
| `rds-mgmt` | PostgreSQL instances, JDBC connect, DDL, DML, IAM enable |
| `rds-cluster` | MySQL clusters, JDBC cluster/instance endpoints, full DML |
| `rds-iam` | RDS IAM token auth, JDBC with generated token |
| `eventbridge` | Event buses, rules, SQS targets, PutEvents, enable/disable |
| `kinesis` | Streams, shards, PutRecord/GetRecords, consumers, encryption, split |
| `cloudwatch-logs` | Log groups/streams, PutLogEvents, GetLogEvents, FilterLogEvents, retention |
| `cloudwatch-metrics` | PutMetricData, ListMetrics, GetMetricStatistics, alarms, SetAlarmState |
| `secretsmanager` | Create/get/put/list/rotate/delete secrets, versioning, tags |
| `kms` | Keys, aliases, encrypt/decrypt, data keys, sign/verify, re-encrypt |
| `cognito` | User pools, clients, AdminCreateUser, InitiateAuth, GetUser |
| `cognito-oauth` | Resource server CRUD, confidential clients, `/oauth2/token`, OIDC discovery, JWKS/JWT verification |
| `stepfunctions` | State machines, executions, history |

Run a subset by passing group names:

```bash
# CLI argument
mvn compile exec:java -Dexec.args="sqs s3 dynamodb"

# Environment variable (comma-separated)
FLOCI_TESTS=sqs,s3 mvn compile exec:java
```

### Rust SDK (`sdk-test-rust`)

Covers: SSM, SQS, S3, STS, KMS, Secrets Manager.

```bash
cd sdk-test-rust
cargo run              # all groups
cargo run -- ssm sqs s3  # specific groups
```

### AWS CLI (`sdk-test-awscli`)

Covers: SSM, SQS, SNS, S3, DynamoDB, IAM, STS, Secrets Manager, KMS.

```bash
cd sdk-test-awscli
./test_all.sh              # all groups
./test_all.sh sqs s3       # specific groups
FLOCI_TESTS=kms ./test_all.sh
```

### Go SDK (`sdk-test-go`)

Covers: SSM, SQS, SNS, S3, DynamoDB, Lambda, IAM, STS, Secrets Manager, KMS, Kinesis, CloudWatch Metrics.

```bash
cd sdk-test-go
go run main.go              # all groups
go run main.go ssm sqs s3  # specific groups
```

### Node.js SDK (`sdk-test-node`)

Covers: SSM, SQS, SNS, S3, DynamoDB, Lambda, IAM, STS, Secrets Manager, KMS, Kinesis, CloudWatch Metrics, Cognito.

```bash
cd sdk-test-node
npm install
node test-all.mjs              # all groups
node test-all.mjs sqs,s3       # specific groups
FLOCI_TESTS=kms node test-all.mjs
```

### Python SDK (`sdk-test-python`)

Covers: SSM, SQS, SNS, S3, DynamoDB, Lambda, IAM, STS, Secrets Manager, KMS, Kinesis, CloudWatch Metrics, Cognito.

```bash
cd sdk-test-python
pip install -r requirements.txt
python test_all.py              # all groups
python test_all.py sqs s3       # specific groups
FLOCI_TESTS=kms python test_all.py
```

### AWS CDK (`compat-cdk`)

Deploys a real CDK stack (`FlociTestStack`) via `cdklocal` (aws-cdk-local) and verifies the created resources:

- S3 Bucket
- SQS Queue (`floci-cdk-test-queue`)
- DynamoDB Table (`floci-cdk-test-table`)

Steps: bootstrap → deploy → spot-check → destroy.

```bash
cd compat-cdk
npm install
npm install -g aws-cdk-local   # one-time
./run.sh
```

Requires the emulator to be reachable as `http://floci:4566` (Docker network), or edit `run.sh` to point to `http://localhost:4566`.

### OpenTofu (`compat-opentofu`)

Runs a full `tofu init → validate → plan → apply → spot-check → destroy` cycle using the AWS provider pointed at the emulator. Provisions:

- S3 bucket with versioning
- SQS queue + DLQ with redrive policy
- SNS topic + SQS subscription
- DynamoDB table with GSI and TTL
- IAM role for Lambda
- SSM parameters (String + SecureString)
- Secrets Manager secret

Uses S3 remote state + DynamoDB lock table (both created automatically in the emulator before the run).

```bash
cd compat-opentofu
./run.sh                                      # localhost:4566
FLOCI_ENDPOINT=http://my-host:4566 ./run.sh  # custom endpoint
```

Requires `tofu` and `aws` CLIs on `PATH`.

### Terraform (`compat-terraform`)

Runs a full `terraform init → validate → plan → apply → spot-check → destroy` cycle using the AWS provider v6 pointed at the emulator. Provisions the same resources as the OpenTofu module:

- S3 bucket with versioning
- SQS queue + DLQ with redrive policy
- SNS topic + SQS subscription
- DynamoDB table with GSI and TTL
- IAM role for Lambda
- SSM parameters (String + SecureString)
- Secrets Manager secret

Uses S3 remote state + DynamoDB lock table (both created automatically in the emulator before the run).

```bash
cd compat-terraform
./run.sh                                      # localhost:4566
FLOCI_ENDPOINT=http://my-host:4566 ./run.sh  # custom endpoint
```

Requires `terraform` and `aws` CLIs on `PATH`.

## Running with Docker

Each module includes a `Dockerfile` for isolated, dependency-free execution:

```bash
# Java
docker build -t floci-sdk-java sdk-test-java/
docker run --rm --network host floci-sdk-java

# Go
docker build -t floci-sdk-go sdk-test-go/
docker run --rm --network host floci-sdk-go

# Node.js
docker build -t floci-sdk-node sdk-test-node/
docker run --rm --network host floci-sdk-node

# Python
docker build -t floci-sdk-python sdk-test-python/
docker run --rm --network host floci-sdk-python

# Rust
docker build -t floci-sdk-rust sdk-test-rust/
docker run --rm --network host floci-sdk-rust

# AWS CLI
docker build -t floci-sdk-awscli sdk-test-awscli/
docker run --rm --network host floci-sdk-awscli
```

`--network host` makes `localhost:4566` accessible inside the container. On macOS/Windows, use `host.docker.internal` instead:

```bash
docker run --rm -e FLOCI_ENDPOINT=http://host.docker.internal:4566 floci-sdk-java
docker run --rm -e FLOCI_ENDPOINT=http://host.docker.internal:4566 floci-sdk-rust
```

## Exit Codes

All runners exit `0` on full pass and `1` if any test fails — suitable for use in CI pipelines.
