# sdk-test-java

Compatibility tests for [Floci](https://github.com/hectorvent/floci) using the **AWS SDK for Java v2 (2.31.8)**.

Runs 507 assertions across 37 test groups against a live Floci instance — no mocks.

## Services Covered

| Group | Description |
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

## Requirements

- Java 17+
- Maven

## Running

```bash
# All groups
mvn compile exec:java

# Specific groups (CLI args)
mvn compile exec:java -Dexec.args="sqs s3 dynamodb"

# Specific groups (env var, comma-separated)
FLOCI_TESTS=sqs,s3 mvn compile exec:java
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `FLOCI_ENDPOINT` | `http://localhost:4566` | Floci emulator endpoint |

AWS credentials are always `test` / `test` / `us-east-1`.

## Docker

```bash
docker build -t floci-sdk-java .
docker run --rm --network host floci-sdk-java

# Custom endpoint (macOS/Windows)
docker run --rm -e FLOCI_ENDPOINT=http://host.docker.internal:4566 floci-sdk-java
```
