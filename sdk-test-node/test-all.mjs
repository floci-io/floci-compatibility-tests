/**
 * Floci Node.js SDK test suite — @aws-sdk/client-* v3
 * Covers: SSM, SQS, SNS, S3, DynamoDB, Lambda, IAM, STS, Secrets Manager, KMS, Kinesis,
 *         CloudWatch Metrics, Cognito, Cognito OAuth
 *
 * Usage:
 *   node test-all.mjs [suite1,suite2,...]
 *   FLOCI_TESTS=sqs,s3 node test-all.mjs
 */

import { SSMClient, PutParameterCommand, GetParameterCommand, DeleteParameterCommand, GetParametersByPathCommand, DescribeParametersCommand } from "@aws-sdk/client-ssm";
import { SQSClient, CreateQueueCommand, SendMessageCommand, ReceiveMessageCommand, DeleteMessageCommand, GetQueueAttributesCommand, DeleteQueueCommand, SendMessageBatchCommand, SetQueueAttributesCommand } from "@aws-sdk/client-sqs";
import { SNSClient, CreateTopicCommand, SubscribeCommand, PublishCommand, ListTopicsCommand, ListSubscriptionsByTopicCommand, UnsubscribeCommand, DeleteTopicCommand, GetSubscriptionAttributesCommand, SetSubscriptionAttributesCommand, PublishBatchCommand } from "@aws-sdk/client-sns";
import { S3Client, CreateBucketCommand, PutObjectCommand, GetObjectCommand, DeleteObjectCommand, ListObjectsV2Command, DeleteBucketCommand, HeadObjectCommand, HeadBucketCommand, ListBucketsCommand, CopyObjectCommand, GetBucketLocationCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient, CreateTableCommand, PutItemCommand, GetItemCommand, DeleteItemCommand, ScanCommand, QueryCommand, UpdateItemCommand, DeleteTableCommand, ListTablesCommand, DescribeTableCommand } from "@aws-sdk/client-dynamodb";
import { LambdaClient, CreateFunctionCommand, GetFunctionCommand, ListFunctionsCommand, DeleteFunctionCommand, CreateAliasCommand, GetAliasCommand, ListAliasesCommand, UpdateAliasCommand, DeleteAliasCommand, PublishVersionCommand } from "@aws-sdk/client-lambda";
import { IAMClient, CreateRoleCommand, GetRoleCommand, DeleteRoleCommand, ListRolesCommand, CreatePolicyCommand, DeletePolicyCommand, AttachRolePolicyCommand, DetachRolePolicyCommand } from "@aws-sdk/client-iam";
import { STSClient, GetCallerIdentityCommand, AssumeRoleCommand } from "@aws-sdk/client-sts";
import { SecretsManagerClient, CreateSecretCommand, GetSecretValueCommand, UpdateSecretCommand, DeleteSecretCommand, ListSecretsCommand } from "@aws-sdk/client-secrets-manager";
import { KMSClient, CreateKeyCommand, ListKeysCommand, DescribeKeyCommand, EncryptCommand, DecryptCommand, GenerateDataKeyCommand } from "@aws-sdk/client-kms";
import { KinesisClient, CreateStreamCommand, DescribeStreamCommand, PutRecordCommand, GetShardIteratorCommand, GetRecordsCommand, DeleteStreamCommand, ListStreamsCommand } from "@aws-sdk/client-kinesis";
import { CloudWatchClient, PutMetricDataCommand, GetMetricStatisticsCommand, ListMetricsCommand, PutMetricAlarmCommand, DescribeAlarmsCommand, DeleteAlarmsCommand } from "@aws-sdk/client-cloudwatch";
import { createPublicKey, createVerify } from "node:crypto";
import { CognitoIdentityProviderClient, CreateUserPoolCommand, CreateUserPoolClientCommand, CreateResourceServerCommand, DescribeResourceServerCommand, ListResourceServersCommand, UpdateResourceServerCommand, DeleteResourceServerCommand, DeleteUserPoolClientCommand, AdminCreateUserCommand, AdminSetUserPasswordCommand, InitiateAuthCommand, RespondToAuthChallengeCommand, SignUpCommand, ConfirmSignUpCommand, AdminGetUserCommand, ListUsersCommand, DeleteUserPoolCommand, CreateGroupCommand, GetGroupCommand, ListGroupsCommand, DeleteGroupCommand, AdminAddUserToGroupCommand, AdminRemoveUserFromGroupCommand, AdminListGroupsForUserCommand } from "@aws-sdk/client-cognito-identity-provider";

const ENDPOINT = process.env.FLOCI_ENDPOINT || "http://localhost:4566";
const REGION = "us-east-1";
const ACCOUNT = "000000000000";
const CREDS = { accessKeyId: "test", secretAccessKey: "test" };

const CLIENT_CONFIG = {
  endpoint: ENDPOINT,
  region: REGION,
  credentials: CREDS,
  forcePathStyle: true,
};

function makeClient(ClientClass, extra = {}) {
  return new ClientClass({ ...CLIENT_CONFIG, ...extra });
}

let passed = 0;
let failed = 0;

function check(name, ok, err = null) {
  if (ok) {
    passed++;
    console.log(`  PASS  ${name}`);
  } else {
    failed++;
    console.log(`  FAIL  ${name}${err ? ": " + err : ""}`);
  }
}

async function tryOk(name, fn) {
  try {
    await fn();
    check(name, true);
  } catch (e) {
    check(name, false, e.message || e.name);
  }
}

async function tryFail(name, fn) {
  try {
    await fn();
    check(name, false, "Expected error but got success");
  } catch (e) {
    check(name, true);
  }
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function decodeJwtPart(token, index) {
  const parts = token.split(".");
  if (parts.length !== 3) {
    throw new Error("JWT must have exactly 3 parts");
  }
  return JSON.parse(Buffer.from(parts[index], "base64url").toString("utf8"));
}

function scopeContains(scopeClaim, expectedScope) {
  return typeof scopeClaim === "string"
    && scopeClaim.split(/\s+/).some(scope => scope === expectedScope);
}

function isPublicClientRejectionError(error) {
  const message = (error?.message || "").toLowerCase();
  return message.includes("secret")
    || message.includes("client_credentials")
    || message.includes("public client")
    || message.includes("confidential client");
}

async function readJsonResponse(response) {
  const body = await response.text();
  let json = {};
  if (body) {
    try {
      json = JSON.parse(body);
    } catch {
      json = {};
    }
  }
  return { status: response.status, body, json };
}

async function discoverOpenIdConfiguration(poolId) {
  const candidates = [
    `${ENDPOINT}/${poolId}/.well-known/openid-configuration`,
    `${ENDPOINT}/.well-known/openid-configuration`,
  ];
  const failures = [];

  for (const candidate of candidates) {
    const response = await fetch(candidate);
    if (!response.ok) {
      failures.push(`${candidate} -> HTTP ${response.status}`);
      continue;
    }

    const json = await response.json();
    if (!json.issuer || !json.token_endpoint || !json.jwks_uri) {
      failures.push(`${candidate} -> missing issuer/token_endpoint/jwks_uri`);
      continue;
    }

    return {
      discoveryUrl: candidate,
      issuer: json.issuer,
      tokenEndpoint: new URL(json.token_endpoint, candidate).toString(),
      jwksUri: new URL(json.jwks_uri, candidate).toString(),
    };
  }

  throw new Error(`No valid OIDC discovery document found: ${failures.join("; ")}`);
}

async function postOAuthForm(url, form, headers = {}) {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      ...headers,
    },
    body: new URLSearchParams(form),
  });
  return readJsonResponse(response);
}

async function requestConfidentialClientToken(tokenEndpoint, clientId, clientSecret, scope) {
  const basic = Buffer.from(`${clientId}:${clientSecret}`, "utf8").toString("base64");
  return postOAuthForm(tokenEndpoint, {
    grant_type: "client_credentials",
    scope,
  }, {
    Authorization: `Basic ${basic}`,
  });
}

async function requestPublicClientToken(tokenEndpoint, clientId, scope) {
  return postOAuthForm(tokenEndpoint, {
    grant_type: "client_credentials",
    client_id: clientId,
    scope,
  });
}

async function fetchJwk(jwksUri, kid) {
  const response = await fetch(jwksUri);
  if (!response.ok) {
    throw new Error(`JWKS request failed with HTTP ${response.status}`);
  }

  const json = await response.json();
  if (!Array.isArray(json.keys)) {
    throw new Error("JWKS keys array missing");
  }

  const key = json.keys.find(item => item.kid === kid);
  if (!key) {
    throw new Error(`No JWK found for kid ${kid}`);
  }

  return key;
}

function verifyRs256(token, jwk) {
  if (jwk.kty !== "RSA") {
    throw new Error(`Expected RSA JWK but got ${jwk.kty}`);
  }

  const parts = token.split(".");
  if (parts.length !== 3) {
    throw new Error("JWT must have exactly 3 parts");
  }

  const publicKey = createPublicKey({
    key: { kty: "RSA", n: jwk.n, e: jwk.e },
    format: "jwk",
  });
  const verifier = createVerify("RSA-SHA256");
  verifier.update(`${parts[0]}.${parts[1]}`);
  verifier.end();
  return verifier.verify(publicKey, Buffer.from(parts[2], "base64url"));
}

// ─────────────────────────── SSM ───────────────────────────
async function testSsm() {
  console.log("\n=== SSM ===");
  const ssm = makeClient(SSMClient);

  await tryOk("PutParameter String", () =>
    ssm.send(new PutParameterCommand({ Name: "/floci/node/p1", Value: "hello", Type: "String" })));

  await tryOk("PutParameter SecureString", () =>
    ssm.send(new PutParameterCommand({ Name: "/floci/node/secret", Value: "s3cr3t", Type: "SecureString" })));

  await tryOk("GetParameter", async () => {
    const r = await ssm.send(new GetParameterCommand({ Name: "/floci/node/p1" }));
    check("GetParameter value", r.Parameter.Value === "hello");
  });

  await tryOk("GetParameter WithDecryption", async () => {
    const r = await ssm.send(new GetParameterCommand({ Name: "/floci/node/secret", WithDecryption: true }));
    check("SecureString decrypted", r.Parameter.Value === "s3cr3t");
  });

  await tryOk("GetParametersByPath", async () => {
    const r = await ssm.send(new GetParametersByPathCommand({ Path: "/floci/node", Recursive: true }));
    check("GetParametersByPath count", r.Parameters.length >= 2);
  });

  await tryOk("DescribeParameters", async () => {
    const r = await ssm.send(new DescribeParametersCommand({}));
    check("DescribeParameters has results", r.Parameters.length >= 2);
  });

  await tryOk("PutParameter Overwrite", () =>
    ssm.send(new PutParameterCommand({ Name: "/floci/node/p1", Value: "updated", Type: "String", Overwrite: true })));

  await tryOk("GetParameter after overwrite", async () => {
    const r = await ssm.send(new GetParameterCommand({ Name: "/floci/node/p1" }));
    check("SSM overwrite value", r.Parameter.Value === "updated");
  });

  await tryFail("GetParameter missing", () =>
    ssm.send(new GetParameterCommand({ Name: "/floci/node/missing" })));

  await tryOk("DeleteParameter", () =>
    ssm.send(new DeleteParameterCommand({ Name: "/floci/node/p1" })));

  await tryOk("DeleteParameter SecureString", () =>
    ssm.send(new DeleteParameterCommand({ Name: "/floci/node/secret" })));
}

// ─────────────────────────── SQS ───────────────────────────
async function testSqs() {
  console.log("\n=== SQS ===");
  const sqs = makeClient(SQSClient);

  let queueUrl;
  await tryOk("CreateQueue", async () => {
    const r = await sqs.send(new CreateQueueCommand({ QueueName: "floci-node-test" }));
    queueUrl = r.QueueUrl;
    check("Queue URL returned", !!queueUrl);
  });

  await tryOk("GetQueueAttributes", async () => {
    const r = await sqs.send(new GetQueueAttributesCommand({
      QueueUrl: queueUrl, AttributeNames: ["All"]
    }));
    check("QueueArn present", !!r.Attributes.QueueArn);
  });

  let receiptHandle;
  await tryOk("SendMessage", async () => {
    await sqs.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: "node-test-message" }));
  });

  await tryOk("ReceiveMessage", async () => {
    const r = await sqs.send(new ReceiveMessageCommand({
      QueueUrl: queueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: 1
    }));
    check("Message received", r.Messages && r.Messages.length > 0);
    if (r.Messages && r.Messages.length > 0) {
      check("Message body correct", r.Messages[0].Body === "node-test-message");
      receiptHandle = r.Messages[0].ReceiptHandle;
    }
  });

  await tryOk("DeleteMessage", () =>
    sqs.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: receiptHandle })));

  await tryOk("SendMessageBatch", async () => {
    await sqs.send(new SendMessageBatchCommand({
      QueueUrl: queueUrl,
      Entries: [
        { Id: "1", MessageBody: "batch-msg-1" },
        { Id: "2", MessageBody: "batch-msg-2" },
      ]
    }));
  });

  await tryOk("CreateFIFO queue", async () => {
    const r = await sqs.send(new CreateQueueCommand({
      QueueName: "floci-node-fifo.fifo",
      Attributes: { FifoQueue: "true", ContentBasedDeduplication: "true" }
    }));
    check("FIFO URL returned", r.QueueUrl.endsWith(".fifo"));
  });

  await tryOk("SetQueueAttributes", () =>
    sqs.send(new SetQueueAttributesCommand({
      QueueUrl: queueUrl,
      Attributes: { VisibilityTimeout: "60" }
    })));

  await tryOk("DeleteQueue", () =>
    sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl })));

  await tryOk("Delete FIFO queue", async () => {
    const fifoUrl = `${ENDPOINT}/${ACCOUNT}/floci-node-fifo.fifo`;
    await sqs.send(new DeleteQueueCommand({ QueueUrl: fifoUrl }));
  });
}

// ─────────────────────────── SNS ───────────────────────────
async function testSns() {
  console.log("\n=== SNS ===");
  const sns = makeClient(SNSClient);
  const sqs = makeClient(SQSClient);

  // Create backing SQS queue for subscription
  const queueResult = await sqs.send(new CreateQueueCommand({ QueueName: "floci-node-sns-target" }));
  const queueUrl = queueResult.QueueUrl;
  const queueAttr = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: queueUrl, AttributeNames: ["QueueArn"] }));
  const queueArn = queueAttr.Attributes.QueueArn;

  let topicArn, subscriptionArn;

  await tryOk("CreateTopic", async () => {
    const r = await sns.send(new CreateTopicCommand({ Name: "floci-node-topic" }));
    topicArn = r.TopicArn;
    check("TopicArn returned", !!topicArn);
  });

  await tryOk("ListTopics", async () => {
    const r = await sns.send(new ListTopicsCommand({}));
    check("Topic in list", r.Topics.some(t => t.TopicArn === topicArn));
  });

  await tryOk("Subscribe SQS", async () => {
    const r = await sns.send(new SubscribeCommand({ TopicArn: topicArn, Protocol: "sqs", Endpoint: queueArn }));
    subscriptionArn = r.SubscriptionArn;
    check("SubscriptionArn returned", !!subscriptionArn);
  });

  await tryOk("ListSubscriptionsByTopic", async () => {
    const r = await sns.send(new ListSubscriptionsByTopicCommand({ TopicArn: topicArn }));
    check("Subscription in list", r.Subscriptions.length > 0);
  });

  await tryOk("GetSubscriptionAttributes", async () => {
    const r = await sns.send(new GetSubscriptionAttributesCommand({ SubscriptionArn: subscriptionArn }));
    check("Protocol attribute", r.Attributes.Protocol === "sqs");
    check("TopicArn attribute", r.Attributes.TopicArn === topicArn);
  });

  await tryOk("SetSubscriptionAttributes", () =>
    sns.send(new SetSubscriptionAttributesCommand({
      SubscriptionArn: subscriptionArn,
      AttributeName: "RawMessageDelivery",
      AttributeValue: "true"
    })));

  await tryOk("Publish", async () => {
    const r = await sns.send(new PublishCommand({ TopicArn: topicArn, Message: "node-test-sns" }));
    check("MessageId returned", !!r.MessageId);
  });

  await tryOk("PublishBatch", async () => {
    const r = await sns.send(new PublishBatchCommand({
      TopicArn: topicArn,
      PublishBatchRequestEntries: [
        { Id: "1", Message: "batch-msg-1" },
        { Id: "2", Message: "batch-msg-2" },
      ]
    }));
    check("Successful count", r.Successful.length === 2);
  });

  await tryOk("Unsubscribe", () =>
    sns.send(new UnsubscribeCommand({ SubscriptionArn: subscriptionArn })));

  await tryOk("DeleteTopic", () =>
    sns.send(new DeleteTopicCommand({ TopicArn: topicArn })));

  await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
}

// ─────────────────────────── S3 ───────────────────────────
async function testS3() {
  console.log("\n=== S3 ===");
  const s3 = makeClient(S3Client, { forcePathStyle: true });
  const euS3 = makeClient(S3Client, { forcePathStyle: true, region: "eu-central-1" });

  const bucket = "floci-node-test-bucket";

  await tryOk("CreateBucket", async () => {
    const r = await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    check("CreateBucket Location header mapped", r.Location === `/${bucket}`);
  });

  // CreateBucket with LocationConstraint (regression: issue #11)
  const euBucket = "floci-node-test-bucket-eu";
  await tryOk("CreateBucket with LocationConstraint", async () => {
    const r = await s3.send(new CreateBucketCommand({
      Bucket: euBucket,
      CreateBucketConfiguration: { LocationConstraint: "eu-central-1" },
    }));
    check("CreateBucket with LocationConstraint maps Location", r.Location === `/${euBucket}`);
  });

  await tryOk("GetBucketLocation", async () => {
    const r = await s3.send(new GetBucketLocationCommand({ Bucket: euBucket }));
    check("LocationConstraint is eu-central-1", r.LocationConstraint === "eu-central-1");
  });

  const signedRegionBucket = "floci-node-test-bucket-signed-region";
  await tryOk("CreateBucket uses signing region when body empty", async () => {
    const r = await euS3.send(new CreateBucketCommand({ Bucket: signedRegionBucket }));
    check("CreateBucket uses signing region Location", r.Location === `/${signedRegionBucket}`);
    const head = await euS3.send(new HeadBucketCommand({ Bucket: signedRegionBucket }));
    check("HeadBucket exposes stored region", head.BucketRegion === "eu-central-1");
    const loc = await euS3.send(new GetBucketLocationCommand({ Bucket: signedRegionBucket }));
    check("Empty-body CreateBucket stores signing region", loc.LocationConstraint === "eu-central-1");
  });

  try {
    await s3.send(new CreateBucketCommand({
      Bucket: "floci-node-invalid-location-bucket",
      CreateBucketConfiguration: { LocationConstraint: "us-east-1" },
    }));
    check("CreateBucket rejects explicit us-east-1 LocationConstraint", false, "Expected InvalidLocationConstraint");
  } catch (e) {
    check(
      "CreateBucket rejects explicit us-east-1 LocationConstraint",
      e?.name === "InvalidLocationConstraint" || `${e?.message || ""}`.includes("InvalidLocationConstraint"),
      e?.message || e?.name
    );
  }

  await tryOk("ListBuckets", async () => {
    const r = await s3.send(new ListBucketsCommand({}));
    check("Bucket in list", r.Buckets.some(b => b.Name === bucket));
  });

  await tryOk("PutObject", () =>
    s3.send(new PutObjectCommand({ Bucket: bucket, Key: "test.txt", Body: "hello from node" })));

  await tryOk("HeadObject", async () => {
    const r = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: "test.txt" }));
    check("ContentLength > 0", r.ContentLength > 0);
    check("LastModified second precision", r.LastModified instanceof Date && r.LastModified.getMilliseconds() === 0);
  });

  await tryOk("GetObject", async () => {
    const r = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: "test.txt" }));
    const body = await r.Body.transformToString();
    check("GetObject content", body === "hello from node");
  });

  await tryOk("CopyObject", () =>
    s3.send(new CopyObjectCommand({ CopySource: `${bucket}/test.txt`, Bucket: bucket, Key: "test-copy.txt" })));

  // CopyObject with non-ASCII (multibyte) key — regression: issue #93
  const nonAsciiKey = "src/テスト画像.png";
  const nonAsciiDst = "dst/テスト画像.png";
  await tryOk("CopyObject non-ASCII key", async () => {
    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: nonAsciiKey, Body: Buffer.from("non-ascii content") }));
    await s3.send(new CopyObjectCommand({ CopySource: `${bucket}/${encodeURIComponent(nonAsciiKey)}`, Bucket: bucket, Key: nonAsciiDst }));
    const r = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: nonAsciiDst }));
    const body = await r.Body.transformToString();
    check("CopyObject non-ASCII content", body === "non-ascii content");
    await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: nonAsciiKey }));
    await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: nonAsciiDst }));
  });

  await tryOk("ListObjectsV2", async () => {
    const r = await s3.send(new ListObjectsV2Command({ Bucket: bucket }));
    check("Objects listed", r.KeyCount >= 2);
  });

  // Large object upload (25 MB) — validates fix for upload size limit
  await tryOk("PutObject 25 MB", () =>
    s3.send(new PutObjectCommand({
      Bucket: bucket,
      Key: "large-object-25mb.bin",
      Body: Buffer.alloc(25 * 1024 * 1024),
      ContentType: "application/octet-stream",
    })));

  await tryOk("HeadObject 25 MB content-length", async () => {
    const r = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: "large-object-25mb.bin" }));
    check("ContentLength is 25 MB", r.ContentLength === 25 * 1024 * 1024);
  });

  await tryOk("DeleteObject 25 MB", () =>
    s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: "large-object-25mb.bin" })));

  await tryOk("DeleteObject", () =>
    s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: "test.txt" })));

  await tryOk("DeleteObject copy", () =>
    s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: "test-copy.txt" })));

  await tryOk("DeleteBucket", () =>
    s3.send(new DeleteBucketCommand({ Bucket: bucket })));
  await tryOk("DeleteBucket signed region", () =>
    euS3.send(new DeleteBucketCommand({ Bucket: signedRegionBucket })));
  await tryOk("DeleteBucket eu", () =>
    s3.send(new DeleteBucketCommand({ Bucket: euBucket })));

  await tryFail("GetObject missing", () =>
    s3.send(new GetObjectCommand({ Bucket: bucket, Key: "missing.txt" })));
}

// ─────────────────────────── DynamoDB ───────────────────────────
async function testDynamoDb() {
  console.log("\n=== DynamoDB ===");
  const dynamo = makeClient(DynamoDBClient);

  const table = "floci-node-table";

  await tryOk("CreateTable", () =>
    dynamo.send(new CreateTableCommand({
      TableName: table,
      KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }, { AttributeName: "sk", KeyType: "RANGE" }],
      AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }, { AttributeName: "sk", AttributeType: "S" }],
      BillingMode: "PAY_PER_REQUEST",
    })));

  await tryOk("DescribeTable", async () => {
    const r = await dynamo.send(new DescribeTableCommand({ TableName: table }));
    check("Table active", r.Table.TableStatus === "ACTIVE");
  });

  await tryOk("ListTables", async () => {
    const r = await dynamo.send(new ListTablesCommand({}));
    check("Table in list", r.TableNames.includes(table));
  });

  await tryOk("PutItem", () =>
    dynamo.send(new PutItemCommand({
      TableName: table,
      Item: { pk: { S: "user#1" }, sk: { S: "profile" }, name: { S: "Alice" }, age: { N: "30" } }
    })));

  await tryOk("GetItem", async () => {
    const r = await dynamo.send(new GetItemCommand({
      TableName: table,
      Key: { pk: { S: "user#1" }, sk: { S: "profile" } }
    }));
    check("Name attribute", r.Item?.name?.S === "Alice");
  });

  await tryOk("UpdateItem", () =>
    dynamo.send(new UpdateItemCommand({
      TableName: table,
      Key: { pk: { S: "user#1" }, sk: { S: "profile" } },
      UpdateExpression: "SET #n = :v",
      ExpressionAttributeNames: { "#n": "name" },
      ExpressionAttributeValues: { ":v": { S: "Bob" } }
    })));

  await tryOk("GetItem after update", async () => {
    const r = await dynamo.send(new GetItemCommand({
      TableName: table,
      Key: { pk: { S: "user#1" }, sk: { S: "profile" } }
    }));
    check("Updated name", r.Item?.name?.S === "Bob");
  });

  await tryOk("PutItem second", () =>
    dynamo.send(new PutItemCommand({
      TableName: table,
      Item: { pk: { S: "user#2" }, sk: { S: "profile" }, name: { S: "Carol" }, age: { N: "25" } }
    })));

  await tryOk("Scan", async () => {
    const r = await dynamo.send(new ScanCommand({ TableName: table }));
    check("Scan count", r.Count >= 2);
  });

  await tryOk("Query", async () => {
    const r = await dynamo.send(new QueryCommand({
      TableName: table,
      KeyConditionExpression: "pk = :pk",
      ExpressionAttributeValues: { ":pk": { S: "user#1" } }
    }));
    check("Query count", r.Count >= 1);
  });

  await tryOk("DeleteItem", () =>
    dynamo.send(new DeleteItemCommand({
      TableName: table,
      Key: { pk: { S: "user#1" }, sk: { S: "profile" } }
    })));

  await tryOk("DeleteItem second", () =>
    dynamo.send(new DeleteItemCommand({
      TableName: table,
      Key: { pk: { S: "user#2" }, sk: { S: "profile" } }
    })));

  await tryOk("DeleteTable", () =>
    dynamo.send(new DeleteTableCommand({ TableName: table })));
}

// ───────────────────── DynamoDB GSI/LSI ─────────────────────
// Validates CloudFormation index provisioning
async function testDynamoDbGsi() {
  console.log("\n=== DynamoDB GSI/LSI ===");
  const dynamo = makeClient(DynamoDBClient);
  const table = "floci-node-gsi-table";

  // CreateTable with GSI and LSI
  await tryOk("CreateTable with GSI+LSI", () =>
    dynamo.send(new CreateTableCommand({
      TableName: table,
      KeySchema: [
        { AttributeName: "pk", KeyType: "HASH" },
        { AttributeName: "sk", KeyType: "RANGE" },
      ],
      AttributeDefinitions: [
        { AttributeName: "pk", AttributeType: "S" },
        { AttributeName: "sk", AttributeType: "S" },
        { AttributeName: "gsiPk", AttributeType: "S" },
        { AttributeName: "lsiSk", AttributeType: "S" },
      ],
      GlobalSecondaryIndexes: [{
        IndexName: "gsi-1",
        KeySchema: [
          { AttributeName: "gsiPk", KeyType: "HASH" },
          { AttributeName: "sk", KeyType: "RANGE" },
        ],
        Projection: { ProjectionType: "ALL" },
        ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
      }],
      LocalSecondaryIndexes: [{
        IndexName: "lsi-1",
        KeySchema: [
          { AttributeName: "pk", KeyType: "HASH" },
          { AttributeName: "lsiSk", KeyType: "RANGE" },
        ],
        Projection: { ProjectionType: "KEYS_ONLY" },
      }],
      ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
    })));

  // DescribeTable — verify indexes
  try {
    const desc = await dynamo.send(new DescribeTableCommand({ TableName: table }));
    const gsis = desc.Table.GlobalSecondaryIndexes || [];
    const lsis = desc.Table.LocalSecondaryIndexes || [];
    check("GSI count", gsis.length === 1);
    check("GSI name", gsis[0]?.IndexName === "gsi-1");
    check("GSI projection", gsis[0]?.Projection?.ProjectionType === "ALL");
    check("LSI count", lsis.length === 1);
    check("LSI name", lsis[0]?.IndexName === "lsi-1");
    check("LSI projection", lsis[0]?.Projection?.ProjectionType === "KEYS_ONLY");
  } catch (e) {
    check("DescribeTable GSI/LSI", false, e.message);
  }

  // PutItem — 2 with gsiPk, 1 sparse
  await tryOk("PutItem gsi-1", () =>
    dynamo.send(new PutItemCommand({
      TableName: table,
      Item: { pk: { S: "item-1" }, sk: { S: "rev-1" }, gsiPk: { S: "group-A" }, lsiSk: { S: "2024-01-01" } },
    })));
  await tryOk("PutItem gsi-2", () =>
    dynamo.send(new PutItemCommand({
      TableName: table,
      Item: { pk: { S: "item-2" }, sk: { S: "rev-1" }, gsiPk: { S: "group-A" }, lsiSk: { S: "2024-01-02" } },
    })));
  await tryOk("PutItem sparse", () =>
    dynamo.send(new PutItemCommand({
      TableName: table,
      Item: { pk: { S: "item-3" }, sk: { S: "rev-1" }, data: { S: "no-gsi-attrs" } },
    })));

  // Query GSI — should return 2 items with gsiPk="group-A"
  try {
    const resp = await dynamo.send(new QueryCommand({
      TableName: table,
      IndexName: "gsi-1",
      KeyConditionExpression: "gsiPk = :gpk",
      ExpressionAttributeValues: { ":gpk": { S: "group-A" } },
    }));
    check("GSI Query returns 2 items", resp.Count === 2);
    const pks = new Set(resp.Items.map(i => i.pk.S));
    check("GSI sparse excludes item-3", !pks.has("item-3"));
  } catch (e) {
    check("GSI Query", false, e.message);
  }

  // Query LSI — pk="item-1", lsiSk > "2024-01-00"
  try {
    const resp = await dynamo.send(new QueryCommand({
      TableName: table,
      IndexName: "lsi-1",
      KeyConditionExpression: "pk = :pk AND lsiSk > :d",
      ExpressionAttributeValues: { ":pk": { S: "item-1" }, ":d": { S: "2024-01-00" } },
    }));
    check("LSI Query returns 1 item", resp.Count === 1);
  } catch (e) {
    check("LSI Query", false, e.message);
  }

  // Cleanup
  await tryOk("DeleteTable", () =>
    dynamo.send(new DeleteTableCommand({ TableName: table })));
}

// ─────────────────────────── Lambda ───────────────────────────
async function testLambda() {
  console.log("\n=== Lambda ===");
  const lambda = makeClient(LambdaClient);

  const fnName = "floci-node-fn";
  // Minimal Node.js handler as base64 zip
  const handlerCode = "exports.handler = async (event) => ({ statusCode: 200, body: 'ok' });";
  const zipBase64 = await makeNodeZip(handlerCode);

  await tryOk("CreateFunction", async () => {
    await lambda.send(new CreateFunctionCommand({
      FunctionName: fnName,
      Runtime: "nodejs18.x",
      Role: `arn:aws:iam::${ACCOUNT}:role/lambda-role`,
      Handler: "index.handler",
      Code: { ZipFile: Buffer.from(zipBase64, "base64") },
    }));
  });

  await tryOk("GetFunction", async () => {
    const r = await lambda.send(new GetFunctionCommand({ FunctionName: fnName }));
    check("FunctionName matches", r.Configuration.FunctionName === fnName);
  });

  await tryOk("ListFunctions", async () => {
    const r = await lambda.send(new ListFunctionsCommand({}));
    check("Function in list", r.Functions.some(f => f.FunctionName === fnName));
  });

  await tryOk("PublishVersion", async () => {
    const r = await lambda.send(new PublishVersionCommand({ FunctionName: fnName, Description: "v1" }));
    check("Version returned", !!r.Version);
  });

  await tryOk("CreateAlias", async () => {
    const r = await lambda.send(new CreateAliasCommand({
      FunctionName: fnName, Name: "live", FunctionVersion: "$LATEST", Description: "live alias"
    }));
    check("AliasArn returned", !!r.AliasArn);
  });

  await tryOk("GetAlias", async () => {
    const r = await lambda.send(new GetAliasCommand({ FunctionName: fnName, Name: "live" }));
    check("Alias name matches", r.Name === "live");
  });

  await tryOk("ListAliases", async () => {
    const r = await lambda.send(new ListAliasesCommand({ FunctionName: fnName }));
    check("Alias in list", r.Aliases.some(a => a.Name === "live"));
  });

  await tryOk("UpdateAlias", async () => {
    const r = await lambda.send(new UpdateAliasCommand({
      FunctionName: fnName, Name: "live", Description: "updated description"
    }));
    check("Updated description", r.Description === "updated description");
  });

  await tryOk("DeleteAlias", () =>
    lambda.send(new DeleteAliasCommand({ FunctionName: fnName, Name: "live" })));

  await tryFail("GetAlias after delete", () =>
    lambda.send(new GetAliasCommand({ FunctionName: fnName, Name: "live" })));

  await tryOk("DeleteFunction", () =>
    lambda.send(new DeleteFunctionCommand({ FunctionName: fnName })));

  await tryFail("GetFunction after delete", () =>
    lambda.send(new GetFunctionCommand({ FunctionName: fnName })));
}

// Creates a minimal zip with an index.js handler (base64 encoded)
async function makeNodeZip(code) {
  // Hardcoded minimal ZIP with a single file "index.js"
  // This is a pre-built zip: zip containing index.js with "exports.handler = ..."
  // For test purposes, we use the Python-style approach of a very small valid zip
  const { createHash } = await import("crypto");
  const content = Buffer.from(code);
  const filename = "index.js";

  // Build a minimal ZIP manually
  const buf = buildMinimalZip(filename, content);
  return buf.toString("base64");
}

function buildMinimalZip(filename, content) {
  const filenameBytes = Buffer.from(filename);
  const compressedData = content; // store uncompressed (method=0)

  const crc = crc32(content);

  // Local file header
  const localHeader = Buffer.alloc(30 + filenameBytes.length);
  localHeader.writeUInt32LE(0x04034b50, 0); // signature
  localHeader.writeUInt16LE(20, 4);          // version needed
  localHeader.writeUInt16LE(0, 6);           // flags
  localHeader.writeUInt16LE(0, 8);           // compression: store
  localHeader.writeUInt16LE(0, 10);          // mod time
  localHeader.writeUInt16LE(0, 12);          // mod date
  localHeader.writeUInt32LE(crc, 14);        // crc32
  localHeader.writeUInt32LE(content.length, 18); // compressed size
  localHeader.writeUInt32LE(content.length, 22); // uncompressed size
  localHeader.writeUInt16LE(filenameBytes.length, 26); // filename len
  localHeader.writeUInt16LE(0, 28);          // extra field len
  filenameBytes.copy(localHeader, 30);

  const centralDir = Buffer.alloc(46 + filenameBytes.length);
  centralDir.writeUInt32LE(0x02014b50, 0);   // signature
  centralDir.writeUInt16LE(20, 4);           // version made by
  centralDir.writeUInt16LE(20, 6);           // version needed
  centralDir.writeUInt16LE(0, 8);            // flags
  centralDir.writeUInt16LE(0, 10);           // compression
  centralDir.writeUInt16LE(0, 12);           // mod time
  centralDir.writeUInt16LE(0, 14);           // mod date
  centralDir.writeUInt32LE(crc, 16);         // crc32
  centralDir.writeUInt32LE(content.length, 20); // compressed size
  centralDir.writeUInt32LE(content.length, 24); // uncompressed size
  centralDir.writeUInt16LE(filenameBytes.length, 28); // filename len
  centralDir.writeUInt16LE(0, 30);           // extra
  centralDir.writeUInt16LE(0, 32);           // comment
  centralDir.writeUInt16LE(0, 34);           // disk start
  centralDir.writeUInt16LE(0, 36);           // int attributes
  centralDir.writeUInt32LE(0, 38);           // ext attributes
  centralDir.writeUInt32LE(0, 42);           // local header offset
  filenameBytes.copy(centralDir, 46);

  const centralDirOffset = localHeader.length + content.length;
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);         // signature
  eocd.writeUInt16LE(0, 4);                  // disk num
  eocd.writeUInt16LE(0, 6);                  // disk with cd
  eocd.writeUInt16LE(1, 8);                  // total entries on disk
  eocd.writeUInt16LE(1, 10);                 // total entries
  eocd.writeUInt32LE(centralDir.length, 12); // cd size
  eocd.writeUInt32LE(centralDirOffset, 16);  // cd offset
  eocd.writeUInt16LE(0, 20);                 // comment len

  return Buffer.concat([localHeader, content, centralDir, eocd]);
}

function crc32(buf) {
  const table = makeCrcTable();
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < buf.length; i++) {
    crc = (crc >>> 8) ^ table[(crc ^ buf[i]) & 0xFF];
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

function makeCrcTable() {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) {
      c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    }
    table[i] = c;
  }
  return table;
}

// ─────────────────────────── IAM ───────────────────────────
async function testIam() {
  console.log("\n=== IAM ===");
  const iam = makeClient(IAMClient, { endpoint: ENDPOINT });

  const roleName = "floci-node-role";
  const policyDoc = JSON.stringify({
    Version: "2012-10-17",
    Statement: [{ Effect: "Allow", Principal: { Service: "lambda.amazonaws.com" }, Action: "sts:AssumeRole" }]
  });

  let roleArn;
  await tryOk("CreateRole", async () => {
    const r = await iam.send(new CreateRoleCommand({ RoleName: roleName, AssumeRolePolicyDocument: policyDoc }));
    roleArn = r.Role.Arn;
    check("RoleArn returned", !!roleArn);
  });

  await tryOk("GetRole", async () => {
    const r = await iam.send(new GetRoleCommand({ RoleName: roleName }));
    check("RoleName matches", r.Role.RoleName === roleName);
  });

  await tryOk("ListRoles", async () => {
    const r = await iam.send(new ListRolesCommand({}));
    check("Role in list", r.Roles.some(role => role.RoleName === roleName));
  });

  let policyArn;
  await tryOk("CreatePolicy", async () => {
    const r = await iam.send(new CreatePolicyCommand({
      PolicyName: "floci-node-policy",
      PolicyDocument: JSON.stringify({
        Version: "2012-10-17",
        Statement: [{ Effect: "Allow", Action: "s3:GetObject", Resource: "*" }]
      })
    }));
    policyArn = r.Policy.Arn;
    check("PolicyArn returned", !!policyArn);
  });

  await tryOk("AttachRolePolicy", () =>
    iam.send(new AttachRolePolicyCommand({ RoleName: roleName, PolicyArn: policyArn })));

  await tryOk("DetachRolePolicy", () =>
    iam.send(new DetachRolePolicyCommand({ RoleName: roleName, PolicyArn: policyArn })));

  await tryOk("DeletePolicy", () =>
    iam.send(new DeletePolicyCommand({ PolicyArn: policyArn })));

  await tryOk("DeleteRole", () =>
    iam.send(new DeleteRoleCommand({ RoleName: roleName })));
}

// ─────────────────────────── STS ───────────────────────────
async function testSts() {
  console.log("\n=== STS ===");
  const sts = makeClient(STSClient);

  await tryOk("GetCallerIdentity", async () => {
    const r = await sts.send(new GetCallerIdentityCommand({}));
    check("Account returned", !!r.Account);
    check("UserId returned", !!r.UserId);
  });

  await tryOk("AssumeRole", async () => {
    const r = await sts.send(new AssumeRoleCommand({
      RoleArn: `arn:aws:iam::${ACCOUNT}:role/test-role`,
      RoleSessionName: "test-session"
    }));
    check("Credentials returned", !!r.Credentials?.AccessKeyId);
  });
}

// ─────────────────────────── Secrets Manager ───────────────────────────
async function testSecretsManager() {
  console.log("\n=== Secrets Manager ===");
  const sm = makeClient(SecretsManagerClient);

  const secretName = "floci/node/secret";
  let secretArn;

  await tryOk("CreateSecret", async () => {
    const r = await sm.send(new CreateSecretCommand({ Name: secretName, SecretString: '{"key":"value"}' }));
    secretArn = r.ARN;
    check("SecretARN returned", !!secretArn);
  });

  await tryOk("GetSecretValue", async () => {
    const r = await sm.send(new GetSecretValueCommand({ SecretId: secretName }));
    check("SecretString returned", r.SecretString === '{"key":"value"}');
  });

  await tryOk("UpdateSecret", () =>
    sm.send(new UpdateSecretCommand({ SecretId: secretName, SecretString: '{"key":"updated"}' })));

  await tryOk("GetSecretValue after update", async () => {
    const r = await sm.send(new GetSecretValueCommand({ SecretId: secretName }));
    check("Updated secret", r.SecretString === '{"key":"updated"}');
  });

  await tryOk("ListSecrets", async () => {
    const r = await sm.send(new ListSecretsCommand({}));
    check("Secret in list", r.SecretList.some(s => s.Name === secretName));
  });

  await tryOk("DeleteSecret", () =>
    sm.send(new DeleteSecretCommand({ SecretId: secretName, ForceDeleteWithoutRecovery: true })));

  await tryFail("GetSecretValue after delete", () =>
    sm.send(new GetSecretValueCommand({ SecretId: secretName })));
}

// ─────────────────────────── KMS ───────────────────────────
async function testKms() {
  console.log("\n=== KMS ===");
  const kms = makeClient(KMSClient);

  let keyId;
  await tryOk("CreateKey", async () => {
    const r = await kms.send(new CreateKeyCommand({ Description: "floci-node-key" }));
    keyId = r.KeyMetadata.KeyId;
    check("KeyId returned", !!keyId);
  });

  await tryOk("DescribeKey", async () => {
    const r = await kms.send(new DescribeKeyCommand({ KeyId: keyId }));
    check("Key enabled", r.KeyMetadata.Enabled);
  });

  await tryOk("ListKeys", async () => {
    const r = await kms.send(new ListKeysCommand({}));
    check("Key in list", r.Keys.some(k => k.KeyId === keyId));
  });

  let ciphertext;
  await tryOk("Encrypt", async () => {
    const r = await kms.send(new EncryptCommand({
      KeyId: keyId,
      Plaintext: Buffer.from("hello from node")
    }));
    ciphertext = r.CiphertextBlob;
    check("Ciphertext returned", !!ciphertext);
  });

  await tryOk("Decrypt", async () => {
    const r = await kms.send(new DecryptCommand({ CiphertextBlob: ciphertext }));
    const plaintext = Buffer.from(r.Plaintext).toString();
    check("Decrypt correct", plaintext === "hello from node");
  });

  await tryOk("GenerateDataKey", async () => {
    const r = await kms.send(new GenerateDataKeyCommand({ KeyId: keyId, KeySpec: "AES_256" }));
    check("Plaintext key returned", !!r.Plaintext);
    check("CiphertextBlob returned", !!r.CiphertextBlob);
  });
}

// ─────────────────────────── Kinesis ───────────────────────────
async function testKinesis() {
  console.log("\n=== Kinesis ===");
  const kinesis = makeClient(KinesisClient);

  const streamName = "floci-node-stream";

  await tryOk("CreateStream", () =>
    kinesis.send(new CreateStreamCommand({ StreamName: streamName, ShardCount: 1 })));

  await tryOk("DescribeStream", async () => {
    const r = await kinesis.send(new DescribeStreamCommand({ StreamName: streamName }));
    check("Stream active", ["ACTIVE", "CREATING"].includes(r.StreamDescription.StreamStatus));
  });

  await tryOk("ListStreams", async () => {
    const r = await kinesis.send(new ListStreamsCommand({}));
    check("Stream in list", r.StreamNames.includes(streamName));
  });

  let sequenceNumber;
  await tryOk("PutRecord", async () => {
    const r = await kinesis.send(new PutRecordCommand({
      StreamName: streamName,
      Data: Buffer.from("kinesis-node-record"),
      PartitionKey: "pk1"
    }));
    sequenceNumber = r.SequenceNumber;
    check("SequenceNumber returned", !!sequenceNumber);
  });

  await tryOk("GetShardIterator + GetRecords", async () => {
    const descr = await kinesis.send(new DescribeStreamCommand({ StreamName: streamName }));
    const shardId = descr.StreamDescription.Shards[0].ShardId;
    const iterResult = await kinesis.send(new GetShardIteratorCommand({
      StreamName: streamName,
      ShardId: shardId,
      ShardIteratorType: "TRIM_HORIZON"
    }));
    const recordsResult = await kinesis.send(new GetRecordsCommand({
      ShardIterator: iterResult.ShardIterator,
      Limit: 10
    }));
    check("Records returned", recordsResult.Records.length > 0);
    const data = Buffer.from(recordsResult.Records[0].Data).toString();
    check("Record data correct", data === "kinesis-node-record");
  });

  await tryOk("DeleteStream", () =>
    kinesis.send(new DeleteStreamCommand({ StreamName: streamName })));
}

// ─────────────────────────── CloudWatch Metrics ───────────────────────────
async function testCloudWatch() {
  console.log("\n=== CloudWatch Metrics ===");
  const cw = makeClient(CloudWatchClient);

  await tryOk("PutMetricData", () =>
    cw.send(new PutMetricDataCommand({
      Namespace: "Floci/NodeTest",
      MetricData: [{ MetricName: "TestMetric", Value: 42.0, Unit: "Count" }]
    })));

  await tryOk("PutMetricData batch", () =>
    cw.send(new PutMetricDataCommand({
      Namespace: "Floci/NodeTest",
      MetricData: [
        { MetricName: "Latency", Value: 100, Unit: "Milliseconds" },
        { MetricName: "Errors", Value: 0, Unit: "Count" },
      ]
    })));

  await tryOk("ListMetrics", async () => {
    const r = await cw.send(new ListMetricsCommand({ Namespace: "Floci/NodeTest" }));
    check("Metrics listed", r.Metrics.length > 0);
  });

  await tryOk("GetMetricStatistics", async () => {
    const now = new Date();
    const start = new Date(now.getTime() - 3600000);
    const r = await cw.send(new GetMetricStatisticsCommand({
      Namespace: "Floci/NodeTest",
      MetricName: "TestMetric",
      StartTime: start,
      EndTime: now,
      Period: 3600,
      Statistics: ["Sum", "Average", "Maximum"]
    }));
    check("Datapoints returned", r.Datapoints.length >= 0);
  });

  await tryOk("PutMetricAlarm", () =>
    cw.send(new PutMetricAlarmCommand({
      AlarmName: "floci-node-alarm",
      MetricName: "TestMetric",
      Namespace: "Floci/NodeTest",
      Statistic: "Average",
      Period: 60,
      EvaluationPeriods: 1,
      Threshold: 100,
      ComparisonOperator: "GreaterThanThreshold"
    })));

  await tryOk("DescribeAlarms", async () => {
    const r = await cw.send(new DescribeAlarmsCommand({ AlarmNames: ["floci-node-alarm"] }));
    check("Alarm found", r.MetricAlarms.some(a => a.AlarmName === "floci-node-alarm"));
  });

  await tryOk("DeleteAlarms", () =>
    cw.send(new DeleteAlarmsCommand({ AlarmNames: ["floci-node-alarm"] })));
}

// ─────────────────────────── Cognito ───────────────────────────
async function testCognito() {
  console.log("\n=== Cognito ===");
  const cognito = makeClient(CognitoIdentityProviderClient);

  let poolId, clientId;

  await tryOk("CreateUserPool", async () => {
    const r = await cognito.send(new CreateUserPoolCommand({ PoolName: "floci-node-pool" }));
    poolId = r.UserPool.Id;
    check("Pool ID returned", !!poolId);
  });

  await tryOk("CreateUserPoolClient", async () => {
    const r = await cognito.send(new CreateUserPoolClientCommand({
      UserPoolId: poolId, ClientName: "floci-node-client"
    }));
    clientId = r.UserPoolClient.ClientId;
    check("ClientId returned", !!clientId);
  });

  await tryOk("AdminCreateUser", async () => {
    const r = await cognito.send(new AdminCreateUserCommand({
      UserPoolId: poolId, Username: "nodeuser",
      TemporaryPassword: "Temp123!",
      UserAttributes: [{ Name: "email", Value: "nodeuser@example.com" }]
    }));
    check("User created", r.User.Username === "nodeuser");
  });

  await tryOk("AdminSetUserPassword", () =>
    cognito.send(new AdminSetUserPasswordCommand({
      UserPoolId: poolId, Username: "nodeuser",
      Password: "Perm456!", Permanent: true
    })));

  await tryOk("AdminGetUser", async () => {
    const r = await cognito.send(new AdminGetUserCommand({ UserPoolId: poolId, Username: "nodeuser" }));
    check("UserStatus confirmed", r.UserStatus === "CONFIRMED");
  });

  await tryOk("ListUsers", async () => {
    const r = await cognito.send(new ListUsersCommand({ UserPoolId: poolId }));
    check("User in list", r.Users.some(u => u.Username === "nodeuser"));
  });

  await tryOk("InitiateAuth USER_PASSWORD_AUTH", async () => {
    const r = await cognito.send(new InitiateAuthCommand({
      AuthFlow: "USER_PASSWORD_AUTH",
      AuthParameters: { USERNAME: "nodeuser", PASSWORD: "Perm456!" },
      ClientId: clientId
    }));
    check("AccessToken returned", !!r.AuthenticationResult?.AccessToken);
    check("IdToken returned", !!r.AuthenticationResult?.IdToken);
  });

  await tryOk("SignUp + ConfirmSignUp", async () => {
    await cognito.send(new SignUpCommand({
      ClientId: clientId,
      Username: "nodeuser2",
      Password: "Pass789!",
      UserAttributes: [{ Name: "email", Value: "nodeuser2@example.com" }]
    }));
    await cognito.send(new ConfirmSignUpCommand({
      ClientId: clientId, Username: "nodeuser2", ConfirmationCode: "123456"
    }));
    check("SignUp+Confirm succeeded", true);
  });

  // ── Groups ──────────────────────────────────────────────────────

  await tryOk("CreateGroup", async () => {
    const r = await cognito.send(new CreateGroupCommand({
      UserPoolId: poolId, GroupName: "test-group",
      Description: "Test group", Precedence: 1
    }));
    check("Group name returned", r.Group.GroupName === "test-group");
  });

  await tryOk("GetGroup", async () => {
    const r = await cognito.send(new GetGroupCommand({
      UserPoolId: poolId, GroupName: "test-group"
    }));
    check("GetGroup name", r.Group.GroupName === "test-group");
    check("GetGroup description", r.Group.Description === "Test group");
    check("GetGroup precedence", r.Group.Precedence === 1);
  });

  await tryFail("CreateGroup duplicate", () =>
    cognito.send(new CreateGroupCommand({
      UserPoolId: poolId, GroupName: "test-group"
    })));

  await tryOk("ListGroups", async () => {
    const r = await cognito.send(new ListGroupsCommand({ UserPoolId: poolId }));
    check("Group in list", r.Groups.some(g => g.GroupName === "test-group"));
  });

  await tryOk("AdminAddUserToGroup", () =>
    cognito.send(new AdminAddUserToGroupCommand({
      UserPoolId: poolId, GroupName: "test-group", Username: "nodeuser"
    })));

  await tryOk("AdminListGroupsForUser", async () => {
    const r = await cognito.send(new AdminListGroupsForUserCommand({
      UserPoolId: poolId, Username: "nodeuser"
    }));
    check("User has group", r.Groups.some(g => g.GroupName === "test-group"));
  });

  await tryOk("InitiateAuth groups in JWT", async () => {
    const r = await cognito.send(new InitiateAuthCommand({
      AuthFlow: "USER_PASSWORD_AUTH",
      AuthParameters: { USERNAME: "nodeuser", PASSWORD: "Perm456!" },
      ClientId: clientId
    }));
    const payload = decodeJwtPart(r.AuthenticationResult.AccessToken, 1);
    check("JWT cognito:groups present", Array.isArray(payload["cognito:groups"]));
    check("JWT cognito:groups contains test-group", payload["cognito:groups"]?.includes("test-group"));
  });

  await tryOk("AdminRemoveUserFromGroup", () =>
    cognito.send(new AdminRemoveUserFromGroupCommand({
      UserPoolId: poolId, GroupName: "test-group", Username: "nodeuser"
    })));

  await tryOk("AdminListGroupsForUser empty", async () => {
    const r = await cognito.send(new AdminListGroupsForUserCommand({
      UserPoolId: poolId, Username: "nodeuser"
    }));
    check("No groups after removal", r.Groups.length === 0);
  });

  await tryOk("DeleteGroup", () =>
    cognito.send(new DeleteGroupCommand({
      UserPoolId: poolId, GroupName: "test-group"
    })));

  await tryFail("GetGroup not found", () =>
    cognito.send(new GetGroupCommand({
      UserPoolId: poolId, GroupName: "test-group"
    })));

  await tryOk("JWKS endpoint", async () => {
    const resp = await fetch(`${ENDPOINT}/${poolId}/.well-known/jwks.json`);
    check("JWKS status 200", resp.ok);
    const json = await resp.json();
    check("Keys array present", Array.isArray(json.keys) && json.keys.length > 0);
    check("Key type RSA", json.keys[0].kty === "RSA");
    check("Algorithm RS256", json.keys[0].alg === "RS256");
  });

  await tryOk("DeleteUserPool", () =>
    cognito.send(new DeleteUserPoolCommand({ UserPoolId: poolId })));
}

// ─────────────────────────── Cognito OAuth ───────────────────────────
async function testCognitoOAuth() {
  console.log("\n=== Cognito OAuth ===");
  const cognito = makeClient(CognitoIdentityProviderClient);

  const suffix = `${Date.now()}`;
  const resourceServerId = `https://compat.floci.test/resource/${suffix}`;
  const readScope = `${resourceServerId}/read`;
  const adminScope = `${resourceServerId}/admin`;

  let poolId = null;
  let confidentialClientId = null;
  let confidentialClientSecret = null;
  let publicClientId = null;
  let publicClientRejectedAtCreate = false;
  let discovery = null;
  let accessToken = null;

  try {
    const poolResp = await cognito.send(new CreateUserPoolCommand({
      PoolName: `floci-node-oauth-pool-${suffix}`,
    }));
    poolId = poolResp.UserPool?.Id;
    check("Cognito OAuth CreateUserPool", !!poolId);
  } catch (e) {
    check("Cognito OAuth CreateUserPool", false, e.message || e.name);
    return;
  }

  try {
    const createResp = await cognito.send(new CreateResourceServerCommand({
      UserPoolId: poolId,
      Identifier: resourceServerId,
      Name: "compat-resource-server",
      Scopes: [
        { ScopeName: "read", ScopeDescription: "Read access" },
        { ScopeName: "write", ScopeDescription: "Write access" },
      ],
    }));
    check("Cognito OAuth CreateResourceServer",
      createResp.ResourceServer?.Identifier === resourceServerId
        && Array.isArray(createResp.ResourceServer?.Scopes)
        && createResp.ResourceServer.Scopes.length === 2);
  } catch (e) {
    check("Cognito OAuth CreateResourceServer", false, e.message || e.name);
    await tryOk("Cognito OAuth DeleteUserPool", () =>
      cognito.send(new DeleteUserPoolCommand({ UserPoolId: poolId })));
    return;
  }

  await tryOk("Cognito OAuth DescribeResourceServer", async () => {
    const resp = await cognito.send(new DescribeResourceServerCommand({
      UserPoolId: poolId,
      Identifier: resourceServerId,
    }));
    const scopes = resp.ResourceServer?.Scopes?.map(scope => scope.ScopeName) || [];
    check("Cognito OAuth DescribeResourceServer values",
      resp.ResourceServer?.Name === "compat-resource-server"
        && scopes.includes("read")
        && scopes.includes("write"));
  });

  await tryOk("Cognito OAuth ListResourceServers", async () => {
    const resp = await cognito.send(new ListResourceServersCommand({
      UserPoolId: poolId,
      MaxResults: 60,
    }));
    check("Cognito OAuth ListResourceServers values",
      resp.ResourceServers?.some(server => server.Identifier === resourceServerId));
  });

  await tryOk("Cognito OAuth UpdateResourceServer", async () => {
    await cognito.send(new UpdateResourceServerCommand({
      UserPoolId: poolId,
      Identifier: resourceServerId,
      Name: "compat-resource-server-updated",
      Scopes: [
        { ScopeName: "read", ScopeDescription: "Read access updated" },
        { ScopeName: "admin", ScopeDescription: "Admin access" },
      ],
    }));

    const resp = await cognito.send(new DescribeResourceServerCommand({
      UserPoolId: poolId,
      Identifier: resourceServerId,
    }));
    const scopes = resp.ResourceServer?.Scopes?.map(scope => scope.ScopeName) || [];
    check("Cognito OAuth UpdateResourceServer values",
      resp.ResourceServer?.Name === "compat-resource-server-updated"
        && scopes.includes("read")
        && scopes.includes("admin")
        && !scopes.includes("write"));
  });

  try {
    const resp = await cognito.send(new CreateUserPoolClientCommand({
      UserPoolId: poolId,
      ClientName: `compat-confidential-client-${suffix}`,
      GenerateSecret: true,
      AllowedOAuthFlowsUserPoolClient: true,
      AllowedOAuthFlows: ["client_credentials"],
      AllowedOAuthScopes: [readScope, adminScope],
    }));
    confidentialClientId = resp.UserPoolClient?.ClientId;
    confidentialClientSecret = resp.UserPoolClient?.ClientSecret;
    check("Cognito OAuth Create confidential client",
      !!confidentialClientId && !!confidentialClientSecret);
  } catch (e) {
    check("Cognito OAuth Create confidential client", false, e.message || e.name);
  }

  try {
    const resp = await cognito.send(new CreateUserPoolClientCommand({
      UserPoolId: poolId,
      ClientName: `compat-public-client-${suffix}`,
      AllowedOAuthFlowsUserPoolClient: true,
      AllowedOAuthFlows: ["client_credentials"],
      AllowedOAuthScopes: [readScope, adminScope],
    }));
    publicClientId = resp.UserPoolClient?.ClientId;
    check("Cognito OAuth Create public client",
      !!publicClientId && !resp.UserPoolClient?.ClientSecret);
  } catch (e) {
    publicClientRejectedAtCreate = isPublicClientRejectionError(e);
    check("Cognito OAuth Public client rejected", publicClientRejectedAtCreate, publicClientRejectedAtCreate ? null : (e.message || e.name));
  }

  await tryOk("Cognito OAuth OIDC discovery", async () => {
    discovery = await discoverOpenIdConfiguration(poolId);
    check("Cognito OAuth OIDC token endpoint", discovery.tokenEndpoint.endsWith("/oauth2/token"));
    check("Cognito OAuth OIDC JWKS URI", discovery.jwksUri.endsWith("/.well-known/jwks.json"));
    check("Cognito OAuth OIDC issuer", !!discovery.issuer);
  });

  if (discovery && confidentialClientId && confidentialClientSecret) {
    await tryOk("Cognito OAuth /oauth2/token happy path", async () => {
      const resp = await requestConfidentialClientToken(
        discovery.tokenEndpoint,
        confidentialClientId,
        confidentialClientSecret,
        readScope,
      );
      check("Cognito OAuth /oauth2/token status 200", resp.status === 200, resp.body);
      accessToken = resp.json.access_token;
      check("Cognito OAuth access token returned", !!accessToken, resp.body);
      check("Cognito OAuth token type Bearer", resp.json.token_type?.toLowerCase() === "bearer", resp.body);
      check("Cognito OAuth expires_in present", Number(resp.json.expires_in) > 0, resp.body);
      check("Cognito OAuth granted scope", !resp.json.scope || scopeContains(resp.json.scope, readScope), resp.body);
    });
  }

  if (accessToken && discovery) {
    await tryOk("Cognito OAuth JWT inspection", async () => {
      const header = decodeJwtPart(accessToken, 0);
      const payload = decodeJwtPart(accessToken, 1);
      check("Cognito OAuth JWT alg is RS256", header.alg === "RS256");
      check("Cognito OAuth JWT kid present", !!header.kid);
      check("Cognito OAuth JWT issuer matches discovery", payload.iss === discovery.issuer);
      check("Cognito OAuth JWT client_id matches app client", payload.client_id === confidentialClientId);
      check("Cognito OAuth JWT scope claim", scopeContains(payload.scope, readScope));
    });

    await tryOk("Cognito OAuth RS256 JWT verification against JWKS", async () => {
      const kid = decodeJwtPart(accessToken, 0).kid;
      const jwk = await fetchJwk(discovery.jwksUri, kid);
      check("Cognito OAuth RS256 JWT verification against JWKS",
        verifyRs256(accessToken, jwk));
    });
  }

  if (!publicClientRejectedAtCreate && discovery && publicClientId) {
    await tryOk("Cognito OAuth Public client rejected", async () => {
      const resp = await requestPublicClientToken(
        discovery.tokenEndpoint,
        publicClientId,
        readScope,
      );
      check("Cognito OAuth Public client rejected status",
        resp.status >= 400 && resp.status < 500, resp.body);
      check("Cognito OAuth Public client rejection reason",
        ["invalid_client", "unauthorized_client"].includes(resp.json.error), resp.body);
    });
  }

  if (discovery && confidentialClientId && confidentialClientSecret) {
    await tryOk("Cognito OAuth unknown scope rejected", async () => {
      const resp = await requestConfidentialClientToken(
        discovery.tokenEndpoint,
        confidentialClientId,
        confidentialClientSecret,
        `${resourceServerId}/unknown`,
      );
      check("Cognito OAuth unknown scope status 400", resp.status === 400, resp.body);
      check("Cognito OAuth unknown scope error", resp.json.error === "invalid_scope", resp.body);
    });
  }

  if (confidentialClientId) {
    await tryOk("Cognito OAuth Delete confidential client", () =>
      cognito.send(new DeleteUserPoolClientCommand({
        UserPoolId: poolId,
        ClientId: confidentialClientId,
      })));
    confidentialClientId = null;
  }

  if (publicClientId) {
    await tryOk("Cognito OAuth Delete public client", () =>
      cognito.send(new DeleteUserPoolClientCommand({
        UserPoolId: poolId,
        ClientId: publicClientId,
      })));
    publicClientId = null;
  }

  await tryOk("Cognito OAuth DeleteResourceServer", async () => {
    await cognito.send(new DeleteResourceServerCommand({
      UserPoolId: poolId,
      Identifier: resourceServerId,
    }));
    try {
      await cognito.send(new DescribeResourceServerCommand({
        UserPoolId: poolId,
        Identifier: resourceServerId,
      }));
      check("Cognito OAuth DescribeResourceServer after delete", false, "Expected missing resource server");
    } catch {
      check("Cognito OAuth DescribeResourceServer after delete", true);
    }
  });

  await tryOk("Cognito OAuth DeleteUserPool", () =>
    cognito.send(new DeleteUserPoolCommand({ UserPoolId: poolId })));
}

// ─────────────────────────── Runner ───────────────────────────
const ALL_SUITES = {
  ssm: testSsm,
  sqs: testSqs,
  sns: testSns,
  s3: testS3,
  dynamodb: testDynamoDb,
  "dynamodb-gsi": testDynamoDbGsi,
  lambda: testLambda,
  iam: testIam,
  sts: testSts,
  secretsmanager: testSecretsManager,
  kms: testKms,
  kinesis: testKinesis,
  cloudwatch: testCloudWatch,
  cognito: testCognito,
  "cognito-oauth": testCognitoOAuth,
};

async function main() {
  const arg = process.argv[2] || process.env.FLOCI_TESTS;
  const selected = arg
    ? arg.split(",").map(s => s.trim().toLowerCase())
    : Object.keys(ALL_SUITES);

  console.log(`Floci Node.js SDK Tests — endpoint: ${ENDPOINT}`);
  console.log(`Running suites: ${selected.join(", ")}`);

  for (const name of selected) {
    if (!ALL_SUITES[name]) {
      console.log(`Unknown suite: ${name}`);
      continue;
    }
    try {
      await ALL_SUITES[name]();
    } catch (e) {
      console.log(`\n  ERROR in suite ${name}: ${e.message}`);
    }
  }

  console.log(`\n${"=".repeat(50)}`);
  console.log(`Results: ${passed} passed, ${failed} failed`);
  if (failed > 0) {
    process.exit(1);
  }
}

main().catch(e => {
  console.error("Fatal:", e);
  process.exit(1);
});
