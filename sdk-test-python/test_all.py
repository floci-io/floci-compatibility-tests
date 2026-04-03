#!/usr/bin/env python3
"""
Floci SDK Test — Python (boto3)

Runs against the Floci AWS emulator. Configure the endpoint via:
  FLOCI_ENDPOINT=http://localhost:4566  (default)

To run only specific groups:
  FLOCI_TESTS=sqs,s3 python test_all.py
  python test_all.py sqs s3
"""

import os
import sys
import io
import json
import time
import base64
import zipfile
import datetime

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, ParamValidationError

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ENDPOINT = os.environ.get("FLOCI_ENDPOINT", "http://localhost:4566")
REGION = "us-east-1"
CREDS = dict(aws_access_key_id="test", aws_secret_access_key="test")
CLIENT_CONFIG = Config(retries={"max_attempts": 1})

passed = 0
failed = 0


def client(service):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        region_name=REGION,
        config=CLIENT_CONFIG,
        **CREDS,
    )


def check(name, ok, error=None):
    global passed, failed
    if ok:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name}")
        if error:
            print(f"        -> {error}")


def minimal_zip():
    """Return bytes of a minimal Node.js Lambda ZIP."""
    code = (
        "exports.handler = async (event) => {\n"
        "  console.log('[handler] event:', JSON.stringify(event));\n"
        "  const name = (event && event.name) ? event.name : 'World';\n"
        "  return { statusCode: 200, body: JSON.stringify({ message: `Hello, ${name}!`, input: event }) };\n"
        "};\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.js", code)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# SSM
# ---------------------------------------------------------------------------

def run_ssm():
    print("--- SSM Tests ---")
    ssm = client("ssm")
    name = "/py-sdk-test/param"
    value = "param-value-boto3"

    try:
        r = ssm.put_parameter(Name=name, Value=value, Type="String", Overwrite=True)
        check("SSM PutParameter", r["Version"] > 0)
    except Exception as e:
        check("SSM PutParameter", False, e)

    try:
        r = ssm.get_parameter(Name=name, WithDecryption=False)
        check("SSM GetParameter", r["Parameter"]["Value"] == value)
    except Exception as e:
        check("SSM GetParameter", False, e)

    try:
        ssm.label_parameter_version(Name=name, Labels=["py-label"], ParameterVersion=1)
        check("SSM LabelParameterVersion", True)
    except Exception as e:
        check("SSM LabelParameterVersion", False, e)

    try:
        r = ssm.get_parameter_history(Name=name, WithDecryption=False)
        found = any(p["Value"] == value for p in r["Parameters"])
        check("SSM GetParameterHistory", found)
    except Exception as e:
        check("SSM GetParameterHistory", False, e)

    try:
        r = ssm.get_parameters(Names=[name])
        found = any(p["Name"] == name and p["Value"] == value for p in r["Parameters"])
        check("SSM GetParameters", found)
    except Exception as e:
        check("SSM GetParameters", False, e)

    try:
        r = ssm.describe_parameters()
        found = any(p["Name"] == name for p in r["Parameters"])
        check("SSM DescribeParameters", found)
    except Exception as e:
        check("SSM DescribeParameters", False, e)

    try:
        r = ssm.get_parameters_by_path(Path="/py-sdk-test", Recursive=False)
        found = any(p["Name"] == name for p in r["Parameters"])
        check("SSM GetParametersByPath", found)
    except Exception as e:
        check("SSM GetParametersByPath", False, e)

    try:
        ssm.add_tags_to_resource(
            ResourceType="Parameter",
            ResourceId=name,
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "backend"}],
        )
        check("SSM AddTagsToResource", True)
    except Exception as e:
        check("SSM AddTagsToResource", False, e)

    try:
        r = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=name)
        tags = {t["Key"]: t["Value"] for t in r["TagList"]}
        check("SSM ListTagsForResource", tags.get("env") == "test" and tags.get("team") == "backend")
    except Exception as e:
        check("SSM ListTagsForResource", False, e)

    try:
        ssm.remove_tags_from_resource(ResourceType="Parameter", ResourceId=name, TagKeys=["team"])
        r = ssm.list_tags_for_resource(ResourceType="Parameter", ResourceId=name)
        tags = {t["Key"]: t["Value"] for t in r["TagList"]}
        check("SSM RemoveTagsFromResource", tags.get("env") == "test" and "team" not in tags)
    except Exception as e:
        check("SSM RemoveTagsFromResource", False, e)

    try:
        ssm.delete_parameter(Name=name)
        try:
            ssm.get_parameter(Name=name, WithDecryption=False)
            check("SSM DeleteParameter", False)
        except ClientError as ce:
            check("SSM DeleteParameter", ce.response["Error"]["Code"] == "ParameterNotFound")
    except Exception as e:
        check("SSM DeleteParameter", False, e)

    try:
        p1, p2 = "/py-sdk-test/p1", "/py-sdk-test/p2"
        ssm.put_parameter(Name=p1, Value="v1", Type="String", Overwrite=True)
        ssm.put_parameter(Name=p2, Value="v2", Type="String", Overwrite=True)
        r = ssm.delete_parameters(Names=[p1, p2])
        check("SSM DeleteParameters", len(r["DeletedParameters"]) == 2)
    except Exception as e:
        check("SSM DeleteParameters", False, e)


# ---------------------------------------------------------------------------
# SQS
# ---------------------------------------------------------------------------

def run_sqs():
    print("--- SQS Tests ---")
    sqs = client("sqs")
    queue_url = None

    try:
        r = sqs.create_queue(QueueName="py-sdk-test-queue")
        queue_url = r["QueueUrl"]
        check("SQS CreateQueue", "py-sdk-test-queue" in queue_url)
    except Exception as e:
        check("SQS CreateQueue", False, e)
        return

    try:
        r = sqs.get_queue_url(QueueName="py-sdk-test-queue")
        check("SQS GetQueueUrl", r.get("QueueUrl") == queue_url)
    except Exception as e:
        check("SQS GetQueueUrl", False, e)

    try:
        r = sqs.list_queues(QueueNamePrefix="py-sdk-test-queue")
        check("SQS ListQueues", any("py-sdk-test-queue" in u for u in r.get("QueueUrls", [])))
    except Exception as e:
        check("SQS ListQueues", False, e)

    try:
        r = sqs.send_message(QueueUrl=queue_url, MessageBody="Hello from boto3!")
        check("SQS SendMessage", bool(r.get("MessageId")))
    except Exception as e:
        check("SQS SendMessage", False, e)

    receipt = None
    try:
        r = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
        msgs = r.get("Messages", [])
        ok = len(msgs) == 1 and msgs[0]["Body"] == "Hello from boto3!"
        receipt = msgs[0]["ReceiptHandle"] if msgs else None
        check("SQS ReceiveMessage", ok)
    except Exception as e:
        check("SQS ReceiveMessage", False, e)

    try:
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
        check("SQS DeleteMessage", True)
    except Exception as e:
        check("SQS DeleteMessage", False, e)

    try:
        r = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
        check("SQS Queue empty after delete", len(r.get("Messages", [])) == 0)
    except Exception as e:
        check("SQS Queue empty after delete", False, e)

    try:
        r = sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": "m1", "MessageBody": "Batch 1"},
                {"Id": "m2", "MessageBody": "Batch 2"},
                {"Id": "m3", "MessageBody": "Batch 3"},
            ],
        )
        check("SQS SendMessageBatch", len(r["Successful"]) == 3 and len(r.get("Failed", [])) == 0)
    except Exception as e:
        check("SQS SendMessageBatch", False, e)

    try:
        r = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=3)
        msgs = r.get("Messages", [])
        entries = [{"Id": f"d{i}", "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)]
        r2 = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        check("SQS DeleteMessageBatch", len(r2["Successful"]) == 3)
    except Exception as e:
        check("SQS DeleteMessageBatch", False, e)

    try:
        r = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
        check("SQS Queue empty after batch delete", len(r.get("Messages", [])) == 0)
    except Exception as e:
        check("SQS Queue empty after batch delete", False, e)

    try:
        sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"VisibilityTimeout": "60"})
        r = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["VisibilityTimeout"])
        check("SQS SetQueueAttributes", r["Attributes"].get("VisibilityTimeout") == "60")
    except Exception as e:
        check("SQS SetQueueAttributes", False, e)

    try:
        sqs.tag_queue(QueueUrl=queue_url, Tags={"env": "test", "team": "backend"})
        check("SQS TagQueue", True)
    except Exception as e:
        check("SQS TagQueue", False, e)

    try:
        r = sqs.list_queue_tags(QueueUrl=queue_url)
        tags = r.get("Tags", {})
        check("SQS ListQueueTags", tags.get("env") == "test" and tags.get("team") == "backend")
    except Exception as e:
        check("SQS ListQueueTags", False, e)

    try:
        sqs.untag_queue(QueueUrl=queue_url, TagKeys=["team"])
        r = sqs.list_queue_tags(QueueUrl=queue_url)
        tags = r.get("Tags", {})
        check("SQS UntagQueue", tags.get("env") == "test" and "team" not in tags)
    except Exception as e:
        check("SQS UntagQueue", False, e)

    # Message attributes
    try:
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="msg-attrs",
            MessageAttributes={"myattr": {"DataType": "String", "StringValue": "myval"}},
        )
        r = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=1, MessageAttributeNames=["All"]
        )
        msgs = r.get("Messages", [])
        ok = (
            len(msgs) == 1
            and msgs[0].get("MessageAttributes", {}).get("myattr", {}).get("StringValue") == "myval"
        )
        check("SQS MessageAttributes", ok)
        if msgs:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msgs[0]["ReceiptHandle"])
    except Exception as e:
        check("SQS MessageAttributes", False, e)

    # Long polling
    try:
        start = time.time()
        sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
        elapsed = time.time() - start
        check("SQS Long Polling", elapsed >= 1.8)
    except Exception as e:
        check("SQS Long Polling", False, e)

    # DLQ routing
    dlq_url = None
    try:
        dlq_url = sqs.create_queue(QueueName="py-sdk-test-dlq")["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        redrive = json.dumps({"maxReceiveCount": "2", "deadLetterTargetArn": dlq_arn})
        sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"RedrivePolicy": redrive})

        sqs.send_message(QueueUrl=queue_url, MessageBody="dlq-test")
        m1 = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)["Messages"][0]
        sqs.change_message_visibility(QueueUrl=queue_url, ReceiptHandle=m1["ReceiptHandle"], VisibilityTimeout=0)
        m2 = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)["Messages"][0]
        sqs.change_message_visibility(QueueUrl=queue_url, ReceiptHandle=m2["ReceiptHandle"], VisibilityTimeout=0)
        r3 = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
        check("SQS DLQ Main Queue Empty", len(r3.get("Messages", [])) == 0)

        dlq_recv = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=1)
        dlq_msgs = dlq_recv.get("Messages", [])
        check("SQS DLQ Message Moved", len(dlq_msgs) == 1 and dlq_msgs[0]["Body"] == "dlq-test")
        if dlq_msgs:
            sqs.delete_message(QueueUrl=dlq_url, ReceiptHandle=dlq_msgs[0]["ReceiptHandle"])
    except Exception as e:
        check("SQS DLQ Routing", False, e)

    try:
        r = sqs.list_dead_letter_source_queues(QueueUrl=dlq_url)
        check("SQS ListDeadLetterSourceQueues", any(queue_url == u for u in r.get("queueUrls", [])))
    except Exception as e:
        check("SQS ListDeadLetterSourceQueues", False, e)

    try:
        sqs.delete_queue(QueueUrl=queue_url)
        if dlq_url:
            sqs.delete_queue(QueueUrl=dlq_url)
        check("SQS DeleteQueue", True)
    except Exception as e:
        check("SQS DeleteQueue", False, e)


# ---------------------------------------------------------------------------
# SNS
# ---------------------------------------------------------------------------

def run_sns():
    print("--- SNS Tests ---")
    sns = client("sns")
    sqs = client("sqs")

    ts = int(time.time() * 1000)
    topic_name = f"py-sns-test-{ts}"
    topic_arn = None
    queue_url = None
    sub_arn = None

    try:
        r = sns.create_topic(Name=topic_name)
        topic_arn = r["TopicArn"]
        check("SNS CreateTopic", topic_name in topic_arn)
    except Exception as e:
        check("SNS CreateTopic", False, e)
        return

    try:
        r = sns.list_topics()
        check("SNS ListTopics", any(t["TopicArn"] == topic_arn for t in r["Topics"]))
    except Exception as e:
        check("SNS ListTopics", False, e)

    try:
        r = sns.get_topic_attributes(TopicArn=topic_arn)
        check("SNS GetTopicAttributes", "TopicArn" in r["Attributes"])
    except Exception as e:
        check("SNS GetTopicAttributes", False, e)

    try:
        q_name = f"sns-py-queue-{ts}"
        queue_url = sqs.create_queue(QueueName=q_name)["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        r = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
        sub_arn = r["SubscriptionArn"]
        check("SNS Subscribe SQS", bool(sub_arn))
    except Exception as e:
        check("SNS Subscribe SQS", False, e)

    try:
        r = sns.list_subscriptions_by_topic(TopicArn=topic_arn)
        check("SNS ListSubscriptionsByTopic", any(s["SubscriptionArn"] == sub_arn for s in r["Subscriptions"]))
    except Exception as e:
        check("SNS ListSubscriptionsByTopic", False, e)

    try:
        r = sns.publish(TopicArn=topic_arn, Message="hello from boto3 sns", Subject="test")
        check("SNS Publish", bool(r.get("MessageId")))
    except Exception as e:
        check("SNS Publish", False, e)

    try:
        time.sleep(0.5)
        r = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
        msgs = r.get("Messages", [])
        check("SNS SQS Delivery", len(msgs) > 0)
        if msgs:
            check("SNS Message Body contains content", "hello from boto3 sns" in msgs[0]["Body"])
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msgs[0]["ReceiptHandle"])
    except Exception as e:
        check("SNS SQS Delivery", False, e)

    try:
        sns.publish(
            TopicArn=topic_arn,
            Message="msg with attrs",
            MessageAttributes={"my-attr": {"DataType": "String", "StringValue": "my-value"}},
        )
        time.sleep(0.5)
        r = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=2)
        msgs = r.get("Messages", [])
        check("SNS Delivery with Attrs", len(msgs) > 0)
        if msgs:
            check("SNS Attr present in body", "my-value" in msgs[0]["Body"])
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msgs[0]["ReceiptHandle"])
    except Exception as e:
        check("SNS Delivery with Attrs", False, e)

    try:
        sns.unsubscribe(SubscriptionArn=sub_arn)
        check("SNS Unsubscribe", True)
    except Exception as e:
        check("SNS Unsubscribe", False, e)

    try:
        sns.delete_topic(TopicArn=topic_arn)
        check("SNS DeleteTopic", True)
    except Exception as e:
        check("SNS DeleteTopic", False, e)

    if queue_url:
        try:
            sqs.delete_queue(QueueUrl=queue_url)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

def run_s3():
    print("--- S3 Tests ---")
    s3 = client("s3")
    bucket = "py-sdk-test-bucket"
    key = "test-file.txt"
    content = b"Hello from boto3!"

    try:
        s3.create_bucket(Bucket=bucket)
        check("S3 CreateBucket", True)
    except Exception as e:
        check("S3 CreateBucket", False, e)
        return

    # CreateBucket with LocationConstraint (regression: issue #11)
    eu_bucket = "py-sdk-test-bucket-eu"
    try:
        s3.create_bucket(
            Bucket=eu_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
        check("S3 CreateBucket with LocationConstraint", True)
    except Exception as e:
        check("S3 CreateBucket with LocationConstraint", False, e)

    try:
        r = s3.get_bucket_location(Bucket=eu_bucket)
        check("S3 GetBucketLocation (eu-central-1)", r.get("LocationConstraint") == "eu-central-1")
    except Exception as e:
        check("S3 GetBucketLocation (eu-central-1)", False, e)

    try:
        r = s3.list_buckets()
        check("S3 ListBuckets", any(b["Name"] == bucket for b in r["Buckets"]))
    except Exception as e:
        check("S3 ListBuckets", False, e)

    try:
        s3.put_object(Bucket=bucket, Key=key, Body=content, ContentType="text/plain")
        check("S3 PutObject", True)
    except Exception as e:
        check("S3 PutObject", False, e)

    try:
        r = s3.list_objects_v2(Bucket=bucket)
        check("S3 ListObjects", any(o["Key"] == key for o in r.get("Contents", [])))
    except Exception as e:
        check("S3 ListObjects", False, e)

    try:
        r = s3.get_object(Bucket=bucket, Key=key)
        data = r["Body"].read()
        check("S3 GetObject", data == content)
    except Exception as e:
        check("S3 GetObject", False, e)

    try:
        r = s3.head_object(Bucket=bucket, Key=key)
        check("S3 HeadObject", r["ContentLength"] == len(content))
        check("S3 HeadObject LastModified second precision",
              r["LastModified"].microsecond == 0)
    except Exception as e:
        check("S3 HeadObject", False, e)

    try:
        s3.head_bucket(Bucket=bucket)
        check("S3 HeadBucket", True)
    except Exception as e:
        check("S3 HeadBucket", False, e)

    try:
        s3.head_bucket(Bucket="non-existent-bucket-py-xyz")
        check("S3 HeadBucket non-existent", False)
    except ClientError as e:
        check("S3 HeadBucket non-existent", e.response["ResponseMetadata"]["HTTPStatusCode"] == 404)
    except Exception as e:
        check("S3 HeadBucket non-existent", False, e)

    try:
        r = s3.get_bucket_location(Bucket=bucket)
        check("S3 GetBucketLocation", "LocationConstraint" in r)
    except Exception as e:
        check("S3 GetBucketLocation", False, e)

    try:
        s3.put_object_tagging(
            Bucket=bucket, Key=key,
            Tagging={"TagSet": [{"Key": "env", "Value": "test"}, {"Key": "project", "Value": "floci"}]},
        )
        check("S3 PutObjectTagging", True)
    except Exception as e:
        check("S3 PutObjectTagging", False, e)

    try:
        r = s3.get_object_tagging(Bucket=bucket, Key=key)
        tags = {t["Key"]: t["Value"] for t in r["TagSet"]}
        check("S3 GetObjectTagging", tags.get("env") == "test" and tags.get("project") == "floci")
    except Exception as e:
        check("S3 GetObjectTagging", False, e)

    try:
        s3.delete_object_tagging(Bucket=bucket, Key=key)
        r = s3.get_object_tagging(Bucket=bucket, Key=key)
        check("S3 DeleteObjectTagging", len(r["TagSet"]) == 0)
    except Exception as e:
        check("S3 DeleteObjectTagging", False, e)

    try:
        s3.put_bucket_tagging(
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "team", "Value": "backend"}, {"Key": "cost-center", "Value": "123"}]},
        )
        check("S3 PutBucketTagging", True)
    except Exception as e:
        check("S3 PutBucketTagging", False, e)

    try:
        r = s3.get_bucket_tagging(Bucket=bucket)
        tags = {t["Key"]: t["Value"] for t in r["TagSet"]}
        check("S3 GetBucketTagging", tags.get("team") == "backend" and tags.get("cost-center") == "123")
    except Exception as e:
        check("S3 GetBucketTagging", False, e)

    try:
        s3.delete_bucket_tagging(Bucket=bucket)
        try:
            r = s3.get_bucket_tagging(Bucket=bucket)
            check("S3 DeleteBucketTagging", len(r.get("TagSet", [])) == 0)
        except ClientError:
            check("S3 DeleteBucketTagging", True)
    except Exception as e:
        check("S3 DeleteBucketTagging", False, e)

    non_ascii_src = "src/テスト画像.png"
    non_ascii_dst = "dst/テスト画像.png"
    try:
        s3.put_object(Bucket=bucket, Key=non_ascii_src, Body=b"non-ascii content")
        r = s3.copy_object(
            CopySource={"Bucket": bucket, "Key": non_ascii_src},
            Bucket=bucket, Key=non_ascii_dst,
        )
        check("S3 CopyObject non-ASCII key", bool(r.get("CopyObjectResult")))
    except Exception as e:
        check("S3 CopyObject non-ASCII key", False, e)

    try:
        r = s3.get_object(Bucket=bucket, Key=non_ascii_dst)
        check("S3 GetObject non-ASCII copy", r["Body"].read() == b"non-ascii content")
    except Exception as e:
        check("S3 GetObject non-ASCII copy", False, e)

    try:
        s3.delete_object(Bucket=bucket, Key=non_ascii_src)
        s3.delete_object(Bucket=bucket, Key=non_ascii_dst)
    except Exception:
        pass

    dest_bucket = "py-sdk-test-bucket-copy"
    dest_key = "copied-file.txt"
    try:
        s3.create_bucket(Bucket=dest_bucket)
        r = s3.copy_object(
            CopySource={"Bucket": bucket, "Key": key},
            Bucket=dest_bucket, Key=dest_key,
        )
        check("S3 CopyObject cross-bucket", bool(r.get("CopyObjectResult")))
    except Exception as e:
        check("S3 CopyObject cross-bucket", False, e)

    try:
        r = s3.get_object(Bucket=dest_bucket, Key=dest_key)
        data = r["Body"].read()
        check("S3 GetObject from copy", data == content)
    except Exception as e:
        check("S3 GetObject from copy", False, e)

    try:
        s3.delete_object(Bucket=dest_bucket, Key=dest_key)
        s3.delete_bucket(Bucket=dest_bucket)
        check("S3 Delete copy bucket", True)
    except Exception as e:
        check("S3 Delete copy bucket", False, e)

    try:
        for i in range(1, 4):
            s3.put_object(Bucket=bucket, Key=f"batch-{i}.txt", Body=f"batch {i}".encode())
        r = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": f"batch-{i}.txt"} for i in range(1, 4)]},
        )
        check("S3 DeleteObjects batch", len(r.get("Deleted", [])) == 3)
    except Exception as e:
        check("S3 DeleteObjects batch", False, e)

    try:
        r = s3.list_objects_v2(Bucket=bucket)
        contents = r.get("Contents", [])
        check("S3 Verify batch delete", len(contents) == 1 and contents[0]["Key"] == key)
    except Exception as e:
        check("S3 Verify batch delete", False, e)

    try:
        s3.delete_object(Bucket=bucket, Key=key)
        check("S3 DeleteObject", True)
    except Exception as e:
        check("S3 DeleteObject", False, e)

    try:
        r = s3.list_objects_v2(Bucket=bucket)
        check("S3 Object deleted", len(r.get("Contents", [])) == 0)
    except Exception as e:
        check("S3 Object deleted", False, e)

    # Large object upload (25 MB) — validates fix for upload size limit
    large_key = "large-object-25mb.bin"
    large_payload = b'\x00' * (25 * 1024 * 1024)
    try:
        s3.put_object(Bucket=bucket, Key=large_key, Body=large_payload, ContentType="application/octet-stream")
        check("S3 PutObject 25 MB", True)
    except Exception as e:
        check("S3 PutObject 25 MB", False, e)

    try:
        r = s3.head_object(Bucket=bucket, Key=large_key)
        check("S3 HeadObject 25 MB content-length", r["ContentLength"] == 25 * 1024 * 1024)
    except Exception as e:
        check("S3 HeadObject 25 MB content-length", False, e)

    try:
        s3.delete_object(Bucket=bucket, Key=large_key)
    except Exception:
        pass

    try:
        s3.delete_bucket(Bucket=eu_bucket)
    except Exception:
        pass

    try:
        s3.delete_bucket(Bucket=bucket)
        check("S3 DeleteBucket", True)
    except Exception as e:
        check("S3 DeleteBucket", False, e)


# ---------------------------------------------------------------------------
# DynamoDB
# ---------------------------------------------------------------------------

def run_dynamodb():
    print("--- DynamoDB Tests ---")
    ddb = client("dynamodb")
    table = "py-sdk-test-table"
    table_arn = None

    try:
        r = ddb.create_table(
            TableName=table,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        table_arn = r["TableDescription"]["TableArn"]
        check("DDB CreateTable", r["TableDescription"]["TableStatus"] == "ACTIVE")
    except Exception as e:
        check("DDB CreateTable", False, e)
        return

    try:
        r = ddb.describe_table(TableName=table)
        check("DDB DescribeTable", r["Table"]["TableName"] == table)
    except Exception as e:
        check("DDB DescribeTable", False, e)

    try:
        r = ddb.list_tables()
        check("DDB ListTables", table in r["TableNames"])
    except Exception as e:
        check("DDB ListTables", False, e)

    try:
        for i in range(1, 4):
            ddb.put_item(
                TableName=table,
                Item={
                    "pk": {"S": "user-1"},
                    "sk": {"S": f"item-{i}"},
                    "data": {"S": f"value-{i}"},
                },
            )
        ddb.put_item(
            TableName=table,
            Item={"pk": {"S": "user-2"}, "sk": {"S": "item-1"}, "data": {"S": "other-value"}},
        )
        check("DDB PutItem", True)
    except Exception as e:
        check("DDB PutItem", False, e)

    try:
        r = ddb.get_item(
            TableName=table,
            Key={"pk": {"S": "user-1"}, "sk": {"S": "item-2"}},
        )
        check("DDB GetItem", r.get("Item", {}).get("data", {}).get("S") == "value-2")
    except Exception as e:
        check("DDB GetItem", False, e)

    try:
        r = ddb.update_item(
            TableName=table,
            Key={"pk": {"S": "user-1"}, "sk": {"S": "item-1"}},
            UpdateExpression="SET #d = :newVal",
            ExpressionAttributeNames={"#d": "data"},
            ExpressionAttributeValues={":newVal": {"S": "updated-value"}},
            ReturnValues="ALL_NEW",
        )
        check("DDB UpdateItem", r["Attributes"]["data"]["S"] == "updated-value")
    except Exception as e:
        check("DDB UpdateItem", False, e)

    try:
        r = ddb.query(
            TableName=table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "user-1"}},
        )
        check("DDB Query", r["Count"] == 3)
    except Exception as e:
        check("DDB Query", False, e)

    try:
        r = ddb.scan(TableName=table)
        check("DDB Scan", r["Count"] == 4)
    except Exception as e:
        check("DDB Scan", False, e)

    try:
        ddb.batch_write_item(
            RequestItems={
                table: [
                    {"PutRequest": {"Item": {"pk": {"S": "user-3"}, "sk": {"S": "item-1"}, "data": {"S": "batch-1"}}}},
                    {"PutRequest": {"Item": {"pk": {"S": "user-3"}, "sk": {"S": "item-2"}, "data": {"S": "batch-2"}}}},
                ]
            }
        )
        r = ddb.scan(TableName=table)
        check("DDB BatchWriteItem", r["Count"] == 6)
    except Exception as e:
        check("DDB BatchWriteItem", False, e)

    try:
        r = ddb.batch_get_item(
            RequestItems={
                table: {
                    "Keys": [
                        {"pk": {"S": "user-1"}, "sk": {"S": "item-1"}},
                        {"pk": {"S": "user-3"}, "sk": {"S": "item-2"}},
                    ]
                }
            }
        )
        items = r["Responses"].get(table, [])
        check("DDB BatchGetItem", len(items) == 2)
    except Exception as e:
        check("DDB BatchGetItem", False, e)

    try:
        r = ddb.update_table(
            TableName=table,
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )
        check("DDB UpdateTable", r["TableDescription"]["ProvisionedThroughput"]["ReadCapacityUnits"] == 10)
    except Exception as e:
        check("DDB UpdateTable", False, e)

    try:
        r = ddb.describe_time_to_live(TableName=table)
        check("DDB DescribeTimeToLive", r["TimeToLiveDescription"]["TimeToLiveStatus"] == "DISABLED")
    except Exception as e:
        check("DDB DescribeTimeToLive", False, e)

    try:
        ddb.tag_resource(
            ResourceArn=table_arn,
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "backend"}],
        )
        check("DDB TagResource", True)
    except Exception as e:
        check("DDB TagResource", False, e)

    try:
        r = ddb.list_tags_of_resource(ResourceArn=table_arn)
        tags = {t["Key"]: t["Value"] for t in r["Tags"]}
        check("DDB ListTagsOfResource", tags.get("env") == "test" and tags.get("team") == "backend")
    except Exception as e:
        check("DDB ListTagsOfResource", False, e)

    try:
        ddb.untag_resource(ResourceArn=table_arn, TagKeys=["team"])
        r = ddb.list_tags_of_resource(ResourceArn=table_arn)
        tags = {t["Key"]: t["Value"] for t in r["Tags"]}
        check("DDB UntagResource", tags.get("env") == "test" and "team" not in tags)
    except Exception as e:
        check("DDB UntagResource", False, e)

    try:
        ddb.batch_write_item(
            RequestItems={
                table: [
                    {"DeleteRequest": {"Key": {"pk": {"S": "user-3"}, "sk": {"S": "item-1"}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": "user-3"}, "sk": {"S": "item-2"}}}},
                ]
            }
        )
        r = ddb.scan(TableName=table)
        check("DDB BatchWriteItem delete", r["Count"] == 4)
    except Exception as e:
        check("DDB BatchWriteItem delete", False, e)

    try:
        ddb.delete_item(TableName=table, Key={"pk": {"S": "user-2"}, "sk": {"S": "item-1"}})
        check("DDB DeleteItem", True)
    except Exception as e:
        check("DDB DeleteItem", False, e)

    try:
        ddb.delete_table(TableName=table)
        r = ddb.list_tables()
        check("DDB DeleteTable", table not in r["TableNames"])
    except Exception as e:
        check("DDB DeleteTable", False, e)


# ---------------------------------------------------------------------------
# DynamoDB GSI/LSI (validates CloudFormation index provisioning)
# ---------------------------------------------------------------------------

def run_dynamodb_gsi():
    print("--- DynamoDB GSI/LSI Tests ---")
    ddb = client("dynamodb")
    table = "py-sdk-gsi-table"

    # CreateTable with GSI and LSI
    try:
        ddb.create_table(
            TableName=table,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsiPk", "AttributeType": "S"},
                {"AttributeName": "lsiSk", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi-1",
                    "KeySchema": [
                        {"AttributeName": "gsiPk", "KeyType": "HASH"},
                        {"AttributeName": "sk", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                }
            ],
            LocalSecondaryIndexes=[
                {
                    "IndexName": "lsi-1",
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "lsiSk", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "KEYS_ONLY"},
                }
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        check("DDB GSI/LSI CreateTable", True)
    except Exception as e:
        check("DDB GSI/LSI CreateTable", False, e)
        return

    # DescribeTable — verify indexes exist
    try:
        desc = ddb.describe_table(TableName=table)["Table"]
        gsis = desc.get("GlobalSecondaryIndexes", [])
        lsis = desc.get("LocalSecondaryIndexes", [])
        check("DDB GSI count", len(gsis) == 1)
        check("DDB GSI name", gsis[0]["IndexName"] == "gsi-1" if gsis else False)
        check("DDB GSI projection", gsis[0]["Projection"]["ProjectionType"] == "ALL" if gsis else False)
        check("DDB LSI count", len(lsis) == 1)
        check("DDB LSI name", lsis[0]["IndexName"] == "lsi-1" if lsis else False)
        check("DDB LSI projection", lsis[0]["Projection"]["ProjectionType"] == "KEYS_ONLY" if lsis else False)
    except Exception as e:
        check("DDB GSI/LSI DescribeTable", False, e)

    # PutItem — 2 items with gsiPk, 1 sparse (no gsiPk)
    try:
        ddb.put_item(TableName=table, Item={
            "pk": {"S": "item-1"}, "sk": {"S": "rev-1"},
            "gsiPk": {"S": "group-A"}, "lsiSk": {"S": "2024-01-01"},
        })
        ddb.put_item(TableName=table, Item={
            "pk": {"S": "item-2"}, "sk": {"S": "rev-1"},
            "gsiPk": {"S": "group-A"}, "lsiSk": {"S": "2024-01-02"},
        })
        ddb.put_item(TableName=table, Item={
            "pk": {"S": "item-3"}, "sk": {"S": "rev-1"},
            "data": {"S": "no-gsi-attrs"},
        })
        check("DDB GSI PutItem", True)
    except Exception as e:
        check("DDB GSI PutItem", False, e)

    # Query GSI — should return only the 2 items with gsiPk="group-A"
    try:
        resp = ddb.query(
            TableName=table,
            IndexName="gsi-1",
            KeyConditionExpression="gsiPk = :gpk",
            ExpressionAttributeValues={":gpk": {"S": "group-A"}},
        )
        check("DDB GSI Query returns 2 items", resp["Count"] == 2)
        pks = {item["pk"]["S"] for item in resp["Items"]}
        check("DDB GSI sparse excludes item-3", "item-3" not in pks)
    except Exception as e:
        check("DDB GSI Query", False, e)

    # Query LSI — pk="item-1", lsiSk > "2024-01-00"
    try:
        resp = ddb.query(
            TableName=table,
            IndexName="lsi-1",
            KeyConditionExpression="pk = :pk AND lsiSk > :d",
            ExpressionAttributeValues={
                ":pk": {"S": "item-1"},
                ":d": {"S": "2024-01-00"},
            },
        )
        check("DDB LSI Query returns 1 item", resp["Count"] == 1)
    except Exception as e:
        check("DDB LSI Query", False, e)

    # Cleanup
    try:
        ddb.delete_table(TableName=table)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Lambda (management plane)
# ---------------------------------------------------------------------------

def run_lambda():
    print("--- Lambda Tests ---")
    lmb = client("lambda")
    fn = "py-sdk-test-fn"
    role = "arn:aws:iam::000000000000:role/lambda-role"
    zip_bytes = minimal_zip()

    fn_arn = None
    try:
        r = lmb.create_function(
            FunctionName=fn,
            Runtime="nodejs20.x",
            Role=role,
            Handler="index.handler",
            Timeout=30,
            MemorySize=256,
            Code={"ZipFile": zip_bytes},
        )
        fn_arn = r["FunctionArn"]
        check("Lambda CreateFunction",
              r["FunctionName"] == fn and fn_arn and fn in fn_arn and r["State"] == "Active")
    except Exception as e:
        check("Lambda CreateFunction", False, e)
        return

    try:
        r = lmb.get_function(FunctionName=fn)
        check("Lambda GetFunction", r["Configuration"]["FunctionName"] == fn and r["Configuration"]["Role"] == role)
    except Exception as e:
        check("Lambda GetFunction", False, e)

    try:
        r = lmb.get_function_configuration(FunctionName=fn)
        check("Lambda GetFunctionConfiguration", r["Timeout"] == 30 and r["MemorySize"] == 256)
    except Exception as e:
        check("Lambda GetFunctionConfiguration", False, e)

    try:
        r = lmb.list_functions()
        check("Lambda ListFunctions", any(f["FunctionName"] == fn for f in r["Functions"]))
    except Exception as e:
        check("Lambda ListFunctions", False, e)

    try:
        r = lmb.invoke(FunctionName=fn, InvocationType="DryRun", Payload=b'{"key":"value"}')
        check("Lambda Invoke DryRun", r["StatusCode"] == 204)
    except Exception as e:
        check("Lambda Invoke DryRun", False, e)

    try:
        r = lmb.invoke(FunctionName=fn, InvocationType="Event", Payload=b'{"key":"async"}')
        check("Lambda Invoke Event (async)", r["StatusCode"] == 202)
    except Exception as e:
        check("Lambda Invoke Event (async)", False, e)

    try:
        r = lmb.update_function_code(FunctionName=fn, ZipFile=zip_bytes)
        check("Lambda UpdateFunctionCode", r["FunctionName"] == fn and r.get("RevisionId"))
    except Exception as e:
        check("Lambda UpdateFunctionCode", False, e)

    try:
        lmb.create_function(
            FunctionName=fn, Runtime="nodejs20.x", Role=role,
            Handler="index.handler", Code={"ZipFile": zip_bytes},
        )
        check("Lambda CreateFunction duplicate -> 409", False)
    except ClientError as e:
        check("Lambda CreateFunction duplicate -> 409",
              e.response["ResponseMetadata"]["HTTPStatusCode"] == 409)
    except Exception as e:
        check("Lambda CreateFunction duplicate -> 409", False, e)

    try:
        lmb.get_function(FunctionName="does-not-exist")
        check("Lambda GetFunction non-existent -> 404", False)
    except ClientError as e:
        check("Lambda GetFunction non-existent -> 404",
              e.response["ResponseMetadata"]["HTTPStatusCode"] == 404)
    except Exception as e:
        check("Lambda GetFunction non-existent -> 404", False, e)

    try:
        lmb.delete_function(FunctionName=fn)
        try:
            lmb.get_function(FunctionName=fn)
            check("Lambda DeleteFunction", False)
        except ClientError as e:
            check("Lambda DeleteFunction", e.response["ResponseMetadata"]["HTTPStatusCode"] == 404)
    except Exception as e:
        check("Lambda DeleteFunction", False, e)


# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------

def run_iam():
    print("--- IAM Tests ---")
    iam = client("iam")
    user = "py-sdk-test-user"
    group = "py-sdk-test-group"
    role = "py-sdk-test-role"
    policy_name = "py-sdk-test-policy"
    trust = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    policy_doc = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}'
    policy_arn = None

    try:
        r = iam.create_user(UserName=user, Path="/")
        check("IAM CreateUser", r["User"]["UserName"] == user and r["User"]["Arn"].endswith(user))
    except Exception as e:
        check("IAM CreateUser", False, e)

    try:
        r = iam.get_user(UserName=user)
        check("IAM GetUser", r["User"]["UserName"] == user)
    except Exception as e:
        check("IAM GetUser", False, e)

    try:
        r = iam.list_users()
        check("IAM ListUsers", any(u["UserName"] == user for u in r["Users"]))
    except Exception as e:
        check("IAM ListUsers", False, e)

    try:
        iam.tag_user(UserName=user, Tags=[{"Key": "env", "Value": "sdk-test"}])
        check("IAM TagUser", True)
    except Exception as e:
        check("IAM TagUser", False, e)

    try:
        r = iam.list_user_tags(UserName=user)
        check("IAM ListUserTags", any(t["Key"] == "env" for t in r["Tags"]))
    except Exception as e:
        check("IAM ListUserTags", False, e)

    try:
        iam.untag_user(UserName=user, TagKeys=["env"])
        check("IAM UntagUser", True)
    except Exception as e:
        check("IAM UntagUser", False, e)

    key_id = None
    try:
        r = iam.create_access_key(UserName=user)
        key_id = r["AccessKey"]["AccessKeyId"]
        check("IAM CreateAccessKey",
              key_id.startswith("AKIA") and r["AccessKey"]["Status"] == "Active")
    except Exception as e:
        check("IAM CreateAccessKey", False, e)

    try:
        r = iam.list_access_keys(UserName=user)
        check("IAM ListAccessKeys", len(r["AccessKeyMetadata"]) > 0)
    except Exception as e:
        check("IAM ListAccessKeys", False, e)

    if key_id:
        try:
            iam.update_access_key(UserName=user, AccessKeyId=key_id, Status="Inactive")
            check("IAM UpdateAccessKey", True)
        except Exception as e:
            check("IAM UpdateAccessKey", False, e)

        try:
            iam.delete_access_key(UserName=user, AccessKeyId=key_id)
            check("IAM DeleteAccessKey", True)
        except Exception as e:
            check("IAM DeleteAccessKey", False, e)

    try:
        r = iam.create_group(GroupName=group)
        check("IAM CreateGroup", r["Group"]["GroupName"] == group)
    except Exception as e:
        check("IAM CreateGroup", False, e)

    try:
        iam.add_user_to_group(GroupName=group, UserName=user)
        check("IAM AddUserToGroup", True)
    except Exception as e:
        check("IAM AddUserToGroup", False, e)

    try:
        r = iam.get_group(GroupName=group)
        check("IAM GetGroup", any(u["UserName"] == user for u in r["Users"]))
    except Exception as e:
        check("IAM GetGroup", False, e)

    try:
        r = iam.list_groups_for_user(UserName=user)
        check("IAM ListGroupsForUser", any(g["GroupName"] == group for g in r["Groups"]))
    except Exception as e:
        check("IAM ListGroupsForUser", False, e)

    try:
        r = iam.create_role(RoleName=role, AssumeRolePolicyDocument=trust, Description="py test role")
        check("IAM CreateRole", r["Role"]["RoleName"] == role and role in r["Role"]["Arn"])
    except Exception as e:
        check("IAM CreateRole", False, e)

    try:
        r = iam.get_role(RoleName=role)
        check("IAM GetRole", r["Role"]["RoleName"] == role)
    except Exception as e:
        check("IAM GetRole", False, e)

    try:
        r = iam.list_roles()
        check("IAM ListRoles", any(ro["RoleName"] == role for ro in r["Roles"]))
    except Exception as e:
        check("IAM ListRoles", False, e)

    try:
        r = iam.create_policy(PolicyName=policy_name, PolicyDocument=policy_doc, Description="py test policy")
        policy_arn = r["Policy"]["Arn"]
        check("IAM CreatePolicy", r["Policy"]["PolicyName"] == policy_name and policy_arn)
    except Exception as e:
        check("IAM CreatePolicy", False, e)

    if policy_arn:
        try:
            r = iam.get_policy(PolicyArn=policy_arn)
            check("IAM GetPolicy", r["Policy"]["PolicyName"] == policy_name)
        except Exception as e:
            check("IAM GetPolicy", False, e)

        try:
            iam.attach_role_policy(RoleName=role, PolicyArn=policy_arn)
            check("IAM AttachRolePolicy", True)
        except Exception as e:
            check("IAM AttachRolePolicy", False, e)

        try:
            r = iam.list_attached_role_policies(RoleName=role)
            check("IAM ListAttachedRolePolicies", any(p["PolicyArn"] == policy_arn for p in r["AttachedPolicies"]))
        except Exception as e:
            check("IAM ListAttachedRolePolicies", False, e)

        try:
            iam.attach_user_policy(UserName=user, PolicyArn=policy_arn)
            check("IAM AttachUserPolicy", True)
        except Exception as e:
            check("IAM AttachUserPolicy", False, e)

        try:
            r = iam.list_attached_user_policies(UserName=user)
            check("IAM ListAttachedUserPolicies", any(p["PolicyArn"] == policy_arn for p in r["AttachedPolicies"]))
        except Exception as e:
            check("IAM ListAttachedUserPolicies", False, e)

        try:
            iam.put_role_policy(RoleName=role, PolicyName="inline-exec", PolicyDocument='{"Version":"2012-10-17"}')
            check("IAM PutRolePolicy", True)
        except Exception as e:
            check("IAM PutRolePolicy", False, e)

        try:
            r = iam.get_role_policy(RoleName=role, PolicyName="inline-exec")
            check("IAM GetRolePolicy", r["PolicyName"] == "inline-exec")
        except Exception as e:
            check("IAM GetRolePolicy", False, e)

        try:
            r = iam.list_role_policies(RoleName=role)
            check("IAM ListRolePolicies", "inline-exec" in r["PolicyNames"])
        except Exception as e:
            check("IAM ListRolePolicies", False, e)

    try:
        iam.get_user(UserName="nonexistent-user-py-xyz")
        check("IAM GetUser NotFound", False)
    except ClientError as e:
        check("IAM GetUser NotFound", e.response["ResponseMetadata"]["HTTPStatusCode"] == 404)
    except Exception as e:
        check("IAM GetUser NotFound", False, e)

    # Cleanup
    try:
        iam.delete_role_policy(RoleName=role, PolicyName="inline-exec")
        if policy_arn:
            iam.detach_role_policy(RoleName=role, PolicyArn=policy_arn)
            iam.detach_user_policy(UserName=user, PolicyArn=policy_arn)
        iam.delete_role(RoleName=role)
        if policy_arn:
            iam.delete_policy(PolicyArn=policy_arn)
        iam.remove_user_from_group(GroupName=group, UserName=user)
        iam.delete_group(GroupName=group)
        iam.delete_user(UserName=user)
        check("IAM Cleanup", True)
    except Exception as e:
        check("IAM Cleanup", False, e)


# ---------------------------------------------------------------------------
# STS
# ---------------------------------------------------------------------------

def run_sts():
    print("--- STS Tests ---")
    sts = client("sts")

    try:
        r = sts.get_caller_identity()
        check("STS GetCallerIdentity",
              r.get("Account") and r.get("Arn") and r.get("UserId"))
    except Exception as e:
        check("STS GetCallerIdentity", False, e)

    try:
        r = sts.get_caller_identity()
        check("STS GetCallerIdentity AccountId", r["Account"] == "000000000000")
    except Exception as e:
        check("STS GetCallerIdentity AccountId", False, e)

    try:
        r = sts.assume_role(
            RoleArn="arn:aws:iam::000000000000:role/py-sdk-assumed-role",
            RoleSessionName="py-sdk-session",
            DurationSeconds=3600,
        )
        creds = r["Credentials"]
        check("STS AssumeRole",
              creds["AccessKeyId"].startswith("ASIA") and creds["SecretAccessKey"] and creds["SessionToken"])
    except Exception as e:
        check("STS AssumeRole", False, e)

    try:
        r = sts.assume_role(
            RoleArn="arn:aws:iam::000000000000:role/my-role",
            RoleSessionName="my-session",
        )
        check("STS AssumeRole AssumedRoleUser", "assumed-role/my-role/my-session" in r["AssumedRoleUser"]["Arn"])
    except Exception as e:
        check("STS AssumeRole AssumedRoleUser", False, e)

    try:
        r = sts.get_session_token(DurationSeconds=7200)
        creds = r["Credentials"]
        check("STS GetSessionToken",
              creds["AccessKeyId"].startswith("ASIA") and creds["SessionToken"])
    except Exception as e:
        check("STS GetSessionToken", False, e)

    try:
        r = sts.assume_role_with_web_identity(
            RoleArn="arn:aws:iam::000000000000:role/web-identity-role",
            RoleSessionName="web-session",
            WebIdentityToken="dummy-token",
            DurationSeconds=3600,
        )
        creds = r["Credentials"]
        check("STS AssumeRoleWithWebIdentity",
              creds["AccessKeyId"].startswith("ASIA")
              and "assumed-role/web-identity-role/web-session" in r["AssumedRoleUser"]["Arn"])
    except Exception as e:
        check("STS AssumeRoleWithWebIdentity", False, e)

    try:
        r = sts.get_federation_token(Name="py-fed-user", DurationSeconds=3600)
        creds = r["Credentials"]
        check("STS GetFederationToken",
              creds["AccessKeyId"].startswith("ASIA")
              and "federated-user/py-fed-user" in r["FederatedUser"]["Arn"])
    except Exception as e:
        check("STS GetFederationToken", False, e)

    try:
        r = sts.decode_authorization_message(EncodedMessage="test-encoded-message")
        check("STS DecodeAuthorizationMessage", bool(r.get("DecodedMessage")))
    except Exception as e:
        check("STS DecodeAuthorizationMessage", False, e)

    try:
        sts.assume_role(RoleSessionName="s")
        check("STS AssumeRole missing RoleArn", False)
    except (ClientError, ParamValidationError) as e:
        # boto3 validates RoleArn client-side; server returns 400 if it slips through
        check("STS AssumeRole missing RoleArn", True)
    except Exception as e:
        check("STS AssumeRole missing RoleArn", False, e)


# ---------------------------------------------------------------------------
# Secrets Manager
# ---------------------------------------------------------------------------

def run_secretsmanager():
    print("--- Secrets Manager Tests ---")
    sm = client("secretsmanager")
    ts = int(time.time() * 1000)
    name = f"py-sdk-test-secret-{ts}"
    value = "my-super-secret-value"
    updated_value = "my-updated-secret-value"
    created_arn = None
    original_version_id = None

    try:
        r = sm.create_secret(
            Name=name,
            SecretString=value,
            Description="Test secret",
            Tags=[{"Key": "env", "Value": "test"}],
        )
        created_arn = r["ARN"]
        original_version_id = r["VersionId"]
        check("SM CreateSecret", created_arn and name in created_arn and r["Name"] == name)
    except Exception as e:
        check("SM CreateSecret", False, e)

    try:
        r = sm.get_secret_value(SecretId=name)
        check("SM GetSecretValue by name", r["SecretString"] == value and r["Name"] == name)
    except Exception as e:
        check("SM GetSecretValue by name", False, e)

    if created_arn:
        try:
            r = sm.get_secret_value(SecretId=created_arn)
            check("SM GetSecretValue by ARN", r["SecretString"] == value)
        except Exception as e:
            check("SM GetSecretValue by ARN", False, e)

    try:
        r = sm.put_secret_value(SecretId=name, SecretString=updated_value)
        new_version_id = r["VersionId"]
        check("SM PutSecretValue", new_version_id and new_version_id != original_version_id)
    except Exception as e:
        check("SM PutSecretValue", False, e)

    try:
        r = sm.get_secret_value(SecretId=name)
        check("SM GetSecretValue after PutSecretValue", r["SecretString"] == updated_value)
    except Exception as e:
        check("SM GetSecretValue after PutSecretValue", False, e)

    try:
        r = sm.describe_secret(SecretId=name)
        has_tags = bool(r.get("Tags"))
        has_two_versions = len(r.get("VersionIdsToStages", {})) == 2
        check("SM DescribeSecret", has_tags and has_two_versions and not r.get("RotationEnabled", True))
    except Exception as e:
        check("SM DescribeSecret", False, e)

    try:
        sm.update_secret(SecretId=name, Description="Updated description")
        r = sm.describe_secret(SecretId=name)
        check("SM UpdateSecret description", r.get("Description") == "Updated description")
    except Exception as e:
        check("SM UpdateSecret description", False, e)

    try:
        r = sm.list_secrets()
        check("SM ListSecrets", any(s["Name"] == name for s in r["SecretList"]))
    except Exception as e:
        check("SM ListSecrets", False, e)

    try:
        sm.tag_resource(SecretId=name, Tags=[{"Key": "team", "Value": "backend"}])
        r = sm.describe_secret(SecretId=name)
        tags = {t["Key"]: t["Value"] for t in r.get("Tags", [])}
        check("SM TagResource", tags.get("team") == "backend")
    except Exception as e:
        check("SM TagResource", False, e)

    try:
        sm.untag_resource(SecretId=name, TagKeys=["team"])
        r = sm.describe_secret(SecretId=name)
        tags = {t["Key"] for t in r.get("Tags", [])}
        check("SM UntagResource", "team" not in tags)
    except Exception as e:
        check("SM UntagResource", False, e)

    try:
        r = sm.list_secret_version_ids(SecretId=name)
        stages = [stage for v in r["Versions"] for stage in v.get("VersionStages", [])]
        check("SM ListSecretVersionIds", "AWSCURRENT" in stages and "AWSPREVIOUS" in stages)
    except Exception as e:
        check("SM ListSecretVersionIds", False, e)

    try:
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)
        try:
            sm.get_secret_value(SecretId=name)
            check("SM DeleteSecret (force)", False)
        except ClientError as ce:
            check("SM DeleteSecret (force)", ce.response["ResponseMetadata"]["HTTPStatusCode"] == 400)
    except Exception as e:
        check("SM DeleteSecret (force)", False, e)

    dup_name = f"py-sdk-dup-secret-{ts}"
    try:
        sm.create_secret(Name=dup_name, SecretString="v1")
        try:
            sm.create_secret(Name=dup_name, SecretString="v2")
            check("SM CreateSecret duplicate", False)
        except ClientError as ce:
            check("SM CreateSecret duplicate", ce.response["ResponseMetadata"]["HTTPStatusCode"] == 400)
        sm.delete_secret(SecretId=dup_name, ForceDeleteWithoutRecovery=True)
    except Exception as e:
        check("SM CreateSecret duplicate", False, e)

    try:
        sm.get_secret_value(SecretId=f"non-existent-secret-{ts}")
        check("SM GetSecretValue non-existent", False)
    except ClientError as ce:
        check("SM GetSecretValue non-existent", ce.response["ResponseMetadata"]["HTTPStatusCode"] == 400)
    except Exception as e:
        check("SM GetSecretValue non-existent", False, e)


# ---------------------------------------------------------------------------
# KMS
# ---------------------------------------------------------------------------

def run_kms():
    print("--- KMS Tests ---")
    kms = client("kms")

    key_id = None
    try:
        r = kms.create_key(Description="py-test-key")
        key_id = r["KeyMetadata"]["KeyId"]
        check("KMS CreateKey", bool(key_id))
    except Exception as e:
        check("KMS CreateKey", False, e)
        return

    try:
        r = kms.describe_key(KeyId=key_id)
        check("KMS DescribeKey", r["KeyMetadata"]["KeyId"] == key_id)
    except Exception as e:
        check("KMS DescribeKey", False, e)

    ts = int(time.time() * 1000)
    alias = f"alias/py-test-key-{ts}"
    try:
        kms.create_alias(AliasName=alias, TargetKeyId=key_id)
        check("KMS CreateAlias", True)
    except Exception as e:
        check("KMS CreateAlias", False, e)

    try:
        r = kms.list_aliases()
        check("KMS ListAliases", any(a["AliasName"] == alias for a in r["Aliases"]))
    except Exception as e:
        check("KMS ListAliases", False, e)

    plaintext = b"secret data"
    ciphertext = None
    try:
        r = kms.encrypt(KeyId=key_id, Plaintext=plaintext)
        ciphertext = r["CiphertextBlob"]
        check("KMS Encrypt", bool(ciphertext))
    except Exception as e:
        check("KMS Encrypt", False, e)

    if ciphertext:
        try:
            r = kms.decrypt(CiphertextBlob=ciphertext)
            check("KMS Decrypt", r["Plaintext"] == plaintext)
        except Exception as e:
            check("KMS Decrypt", False, e)

    try:
        r = kms.encrypt(KeyId=alias, Plaintext=b"alias data")
        check("KMS Encrypt using Alias", bool(r.get("CiphertextBlob")))
    except Exception as e:
        check("KMS Encrypt using Alias", False, e)

    try:
        r = kms.generate_data_key(KeyId=key_id, KeySpec="AES_256")
        check("KMS GenerateDataKey", bool(r.get("Plaintext")) and bool(r.get("CiphertextBlob")))
    except Exception as e:
        check("KMS GenerateDataKey", False, e)

    try:
        kms.tag_resource(KeyId=key_id, Tags=[{"TagKey": "Project", "TagValue": "Floci"}])
        r = kms.list_resource_tags(KeyId=key_id)
        check("KMS Tagging", any(t["TagKey"] == "Project" and t["TagValue"] == "Floci" for t in r["Tags"]))
    except Exception as e:
        check("KMS Tagging", False, e)

    if ciphertext:
        try:
            key_id2 = kms.create_key(Description="key2")["KeyMetadata"]["KeyId"]
            r = kms.re_encrypt(CiphertextBlob=ciphertext, DestinationKeyId=key_id2)
            check("KMS ReEncrypt", bool(r.get("CiphertextBlob")))
            r2 = kms.decrypt(CiphertextBlob=r["CiphertextBlob"])
            check("KMS ReEncrypt verification", r2["Plaintext"] == plaintext)
        except Exception as e:
            check("KMS ReEncrypt", False, e)

    try:
        r = kms.generate_data_key_without_plaintext(KeyId=key_id, KeySpec="AES_256")
        check("KMS GenerateDataKeyWithoutPlaintext", bool(r.get("CiphertextBlob")))
    except Exception as e:
        check("KMS GenerateDataKeyWithoutPlaintext", False, e)

    try:
        r = kms.sign(KeyId=key_id, Message=b"message to sign", SigningAlgorithm="RSASSA_PSS_SHA_256")
        sig = r["Signature"]
        check("KMS Sign", bool(sig))
        r2 = kms.verify(
            KeyId=key_id, Message=b"message to sign",
            Signature=sig, SigningAlgorithm="RSASSA_PSS_SHA_256",
        )
        check("KMS Verify", r2["SignatureValid"])
    except Exception as e:
        check("KMS Sign/Verify", False, e)

    try:
        kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)
        r = kms.describe_key(KeyId=key_id)
        check("KMS ScheduleKeyDeletion", r["KeyMetadata"]["KeyState"] == "PendingDeletion")
    except Exception as e:
        check("KMS ScheduleKeyDeletion", False, e)

    try:
        kms.delete_alias(AliasName=alias)
        check("KMS DeleteAlias", True)
    except Exception as e:
        check("KMS DeleteAlias", False, e)


# ---------------------------------------------------------------------------
# Kinesis
# ---------------------------------------------------------------------------

def run_kinesis():
    print("--- Kinesis Tests ---")
    # Disable CBOR — use JSON 1.1 as the emulator supports
    os.environ["AWS_CBOR_DISABLE"] = "true"
    kinesis = client("kinesis")
    ts = int(time.time() * 1000)
    stream = f"py-sdk-test-stream-{ts}"

    try:
        kinesis.create_stream(StreamName=stream, ShardCount=1)
        check("Kinesis CreateStream", True)
    except Exception as e:
        check("Kinesis CreateStream", False, e)
        return

    try:
        r = kinesis.list_streams()
        check("Kinesis ListStreams", stream in r["StreamNames"])
    except Exception as e:
        check("Kinesis ListStreams", False, e)

    shard_id = None
    try:
        r = kinesis.describe_stream(StreamName=stream)
        shard_id = r["StreamDescription"]["Shards"][0]["ShardId"]
        check("Kinesis DescribeStream", r["StreamDescription"]["StreamName"] == stream and bool(shard_id))
    except Exception as e:
        check("Kinesis DescribeStream", False, e)

    data = b"hello kinesis boto3"
    try:
        r = kinesis.put_record(StreamName=stream, Data=data, PartitionKey="pk1")
        check("Kinesis PutRecord", bool(r.get("SequenceNumber")))
    except Exception as e:
        check("Kinesis PutRecord", False, e)

    try:
        it = kinesis.get_shard_iterator(
            StreamName=stream, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
        r = kinesis.get_records(ShardIterator=it)
        found = any(rec["Data"] == data for rec in r["Records"])
        check("Kinesis GetRecords", found)
    except Exception as e:
        check("Kinesis GetRecords", False, e)

    try:
        r = kinesis.put_records(
            StreamName=stream,
            Records=[
                {"Data": b"batch1", "PartitionKey": "pk1"},
                {"Data": b"batch2", "PartitionKey": "pk2"},
            ],
        )
        check("Kinesis PutRecords batch", r["FailedRecordCount"] == 0 and len(r["Records"]) == 2)
    except Exception as e:
        check("Kinesis PutRecords batch", False, e)

    try:
        r = kinesis.describe_stream_summary(StreamName=stream)
        check("Kinesis DescribeStreamSummary",
              r["StreamDescriptionSummary"]["StreamName"] == stream)
    except Exception as e:
        check("Kinesis DescribeStreamSummary", False, e)

    try:
        kinesis.add_tags_to_stream(StreamName=stream, Tags={"project": "floci"})
        r = kinesis.list_tags_for_stream(StreamName=stream)
        check("Kinesis Tagging", any(t["Key"] == "project" and t["Value"] == "floci" for t in r["Tags"]))
    except Exception as e:
        check("Kinesis Tagging", False, e)

    try:
        kinesis.delete_stream(StreamName=stream)
        check("Kinesis DeleteStream", True)
    except Exception as e:
        check("Kinesis DeleteStream", False, e)


# ---------------------------------------------------------------------------
# CloudWatch Metrics
# ---------------------------------------------------------------------------

def run_cloudwatch_metrics():
    print("--- CloudWatch Metrics Tests ---")
    cw = client("cloudwatch")
    ns_a = f"TestApp/PySdkMetricsTests/{int(time.time() * 1000)}"
    ns_b = "TestApp/OtherNamespace"
    now = datetime.datetime.now(datetime.timezone.utc)

    try:
        cw.put_metric_data(
            Namespace=ns_a,
            MetricData=[{"MetricName": "RequestCount", "Value": 42.0, "Unit": "Count", "Timestamp": now}],
        )
        check("CWM PutMetricData no exception", True)
    except Exception as e:
        check("CWM PutMetricData", False, e)

    try:
        cw.put_metric_data(
            Namespace=ns_a,
            MetricData=[{
                "MetricName": "Latency", "Value": 125.5, "Unit": "Milliseconds",
                "Timestamp": now,
                "Dimensions": [{"Name": "Host", "Value": "web01"}],
            }],
        )
        check("CWM PutMetricData with dimensions", True)
    except Exception as e:
        check("CWM PutMetricData with dimensions", False, e)

    try:
        r = cw.list_metrics(Namespace=ns_a)
        has_rc = any(m["MetricName"] == "RequestCount" for m in r["Metrics"])
        has_lat = any(m["MetricName"] == "Latency" for m in r["Metrics"])
        check("CWM ListMetrics contains RequestCount and Latency", has_rc and has_lat)
    except Exception as e:
        check("CWM ListMetrics", False, e)

    try:
        cw.put_metric_data(
            Namespace=ns_b,
            MetricData=[{"MetricName": "OtherMetric", "Value": 1.0, "Unit": "Count", "Timestamp": now}],
        )
        r_a = cw.list_metrics(Namespace=ns_a)
        r_b = cw.list_metrics(Namespace=ns_b)
        no_a_in_b = all(m["Namespace"] != ns_a for m in r_b["Metrics"])
        no_b_in_a = all(m["Namespace"] != ns_b for m in r_a["Metrics"])
        check("CWM ListMetrics namespace isolation", no_a_in_b and no_b_in_a)
    except Exception as e:
        check("CWM ListMetrics namespace isolation", False, e)

    try:
        data_points = [
            {"MetricName": "CPUUtil", "Value": v, "Unit": "Percent",
             "Timestamp": now - datetime.timedelta(seconds=s)}
            for v, s in [(10.0, 250), (20.0, 200), (30.0, 150), (40.0, 100), (50.0, 50)]
        ]
        cw.put_metric_data(Namespace=ns_a, MetricData=data_points)
        r = cw.get_metric_statistics(
            Namespace=ns_a, MetricName="CPUUtil",
            StartTime=now - datetime.timedelta(hours=1),
            EndTime=now + datetime.timedelta(seconds=60),
            Period=3600,
            Statistics=["Sum", "SampleCount"],
        )
        total_sum = sum(dp["Sum"] for dp in r["Datapoints"])
        total_sc = sum(dp["SampleCount"] for dp in r["Datapoints"])
        check("CWM GetMetricStatistics has datapoints", len(r["Datapoints"]) > 0)
        check("CWM GetMetricStatistics sum=150", abs(total_sum - 150.0) < 0.001)
        check("CWM GetMetricStatistics sampleCount=5", abs(total_sc - 5.0) < 0.001)
    except Exception as e:
        check("CWM GetMetricStatistics", False, e)

    alarm_name = f"py-sdk-test-alarm-{int(time.time() * 1000)}"
    try:
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            MetricName="CPUUtilization",
            Namespace="AWS/EC2",
            Statistic="Average",
            Period=60,
            Threshold=80.0,
            ComparisonOperator="GreaterThanThreshold",
            EvaluationPeriods=1,
            AlarmActions=["arn:aws:sns:us-east-1:000000000000:my-topic"],
        )
        check("CWM PutMetricAlarm", True)
    except Exception as e:
        check("CWM PutMetricAlarm", False, e)

    try:
        r = cw.describe_alarms(AlarmNames=[alarm_name])
        alarms = r["MetricAlarms"]
        found = any(a["AlarmName"] == alarm_name for a in alarms)
        check("CWM DescribeAlarms found", found)
        if found:
            alarm = next(a for a in alarms if a["AlarmName"] == alarm_name)
            check("CWM Alarm state initialized", alarm["StateValue"] == "INSUFFICIENT_DATA")
    except Exception as e:
        check("CWM DescribeAlarms", False, e)

    try:
        cw.set_alarm_state(AlarmName=alarm_name, StateValue="ALARM", StateReason="Threshold breached")
        r = cw.describe_alarms(AlarmNames=[alarm_name])
        check("CWM SetAlarmState verified", r["MetricAlarms"][0]["StateValue"] == "ALARM")
    except Exception as e:
        check("CWM SetAlarmState", False, e)

    try:
        cw.delete_alarms(AlarmNames=[alarm_name])
        r = cw.describe_alarms(AlarmNames=[alarm_name])
        check("CWM DeleteAlarms verified", len(r["MetricAlarms"]) == 0)
    except Exception as e:
        check("CWM DeleteAlarms", False, e)


# ---------------------------------------------------------------------------
# Cognito
# ---------------------------------------------------------------------------

def run_cognito():
    print("--- Cognito Tests ---")
    cognito = client("cognito-idp")

    pool_id = None
    try:
        r = cognito.create_user_pool(PoolName="py-test-pool")
        pool_id = r["UserPool"]["Id"]
        check("Cognito CreateUserPool", bool(pool_id))
    except Exception as e:
        check("Cognito CreateUserPool", False, e)
        return

    client_id = None
    try:
        r = cognito.create_user_pool_client(UserPoolId=pool_id, ClientName="py-test-client")
        client_id = r["UserPoolClient"]["ClientId"]
        check("Cognito CreateUserPoolClient", bool(client_id))
    except Exception as e:
        check("Cognito CreateUserPoolClient", False, e)

    ts = int(time.time() * 1000)
    username = f"py-test-user-{ts}"
    try:
        r = cognito.admin_create_user(
            UserPoolId=pool_id,
            Username=username,
            UserAttributes=[{"Name": "email", "Value": "pytest@example.com"}],
        )
        check("Cognito AdminCreateUser", r["User"]["Username"] == username)
    except Exception as e:
        check("Cognito AdminCreateUser", False, e)

    access_token = None
    if client_id:
        try:
            r = cognito.admin_initiate_auth(
                UserPoolId=pool_id,
                ClientId=client_id,
                AuthFlow="ADMIN_NO_SRP_AUTH",
                AuthParameters={"USERNAME": username, "PASSWORD": "any"},
            )
            access_token = r["AuthenticationResult"]["AccessToken"]
            check("Cognito AdminInitiateAuth", bool(access_token))
        except Exception as e:
            check("Cognito AdminInitiateAuth", False, e)

    if access_token:
        try:
            r = cognito.get_user(AccessToken=access_token)
            check("Cognito GetUser", r["Username"] == username)
        except Exception as e:
            check("Cognito GetUser", False, e)

    try:
        cognito.admin_delete_user(UserPoolId=pool_id, Username=username)
        check("Cognito AdminDeleteUser", True)
    except Exception as e:
        check("Cognito AdminDeleteUser", False, e)

    if client_id:
        try:
            cognito.delete_user_pool_client(UserPoolId=pool_id, ClientId=client_id)
            check("Cognito DeleteUserPoolClient", True)
        except Exception as e:
            check("Cognito DeleteUserPoolClient", False, e)

    try:
        cognito.delete_user_pool(UserPoolId=pool_id)
        check("Cognito DeleteUserPool", True)
    except Exception as e:
        check("Cognito DeleteUserPool", False, e)


# ---------------------------------------------------------------------------
# CloudFormation naming compatibility (PR #163)
# ---------------------------------------------------------------------------

def run_cloudformation_naming():
    print("--- CloudFormation Naming Tests ---")
    cfn = client("cloudformation")
    token = format(int(time.time() * 1000), "x")

    def create_stack(stack_name, template_dict):
        return cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template_dict))

    def wait_for_stack_terminal_state(stack_name, expected_success=True):
        success_states = {"CREATE_COMPLETE", "UPDATE_COMPLETE"}
        failure_states = {
            "CREATE_FAILED",
            "ROLLBACK_IN_PROGRESS",
            "ROLLBACK_FAILED",
            "ROLLBACK_COMPLETE",
            "DELETE_FAILED",
            "DELETE_COMPLETE",
        }

        for _ in range(40):
            resp = cfn.describe_stacks(StackName=stack_name)
            status = resp.get("Stacks", [{}])[0].get("StackStatus", "")
            if status in success_states:
                return expected_success, status
            if status in failure_states:
                return (not expected_success), status
            time.sleep(1)
        return False, "TIMEOUT"

    def describe_resources(stack_name):
        return cfn.describe_stack_resources(StackName=stack_name).get("StackResources", [])

    def physical_id(resources, logical_id):
        for resource in resources:
            if resource.get("LogicalResourceId") == logical_id:
                return resource.get("PhysicalResourceId")
        return None

    def delete_stack(stack_name, check_name):
        try:
            cfn.delete_stack(StackName=stack_name)
            check(check_name, True)
        except Exception as e:
            check(check_name, False, e)

    auto_stack = f"cfn-auto-naming-{token}"
    auto_template = {
        "Resources": {
            "AutoBucket": {"Type": "AWS::S3::Bucket"},
            "AutoQueue": {"Type": "AWS::SQS::Queue"},
            "AutoTopic": {"Type": "AWS::SNS::Topic"},
            "AutoParameter": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {"Type": "String", "Value": "v1"},
            },
            "CrossRefQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": {"Fn::Sub": "${AutoBucket}-cross"}},
            },
        }
    }

    try:
        create_stack(auto_stack, auto_template)
        check("CFN Naming auto CreateStack", True)
    except Exception as e:
        check("CFN Naming auto CreateStack", False, e)
        return

    try:
        ok, status = wait_for_stack_terminal_state(auto_stack, expected_success=True)
        check("CFN Naming auto terminal status", ok, status)
    except Exception as e:
        check("CFN Naming auto terminal status", False, e)
        delete_stack(auto_stack, "CFN Naming auto DeleteStack")
        return

    auto_resources = []
    try:
        auto_resources = describe_resources(auto_stack)
        check("CFN Naming auto DescribeStackResources", len(auto_resources) > 0)
    except Exception as e:
        check("CFN Naming auto DescribeStackResources", False, e)
        delete_stack(auto_stack, "CFN Naming auto DeleteStack")
        return

    auto_bucket = physical_id(auto_resources, "AutoBucket")
    auto_queue = physical_id(auto_resources, "AutoQueue")
    auto_topic = physical_id(auto_resources, "AutoTopic")
    auto_param = physical_id(auto_resources, "AutoParameter")
    cross_queue = physical_id(auto_resources, "CrossRefQueue")

    check("CFN Naming auto S3 generated", bool(auto_bucket))
    if auto_bucket:
        check(
            "CFN Naming auto S3 constraints",
            3 <= len(auto_bucket) <= 63
            and auto_bucket == auto_bucket.lower()
            and all(ch.islower() or ch.isdigit() or ch in ".-" for ch in auto_bucket),
        )

    check("CFN Naming auto SQS generated", bool(auto_queue))
    if auto_queue:
        queue_name = auto_queue.rsplit("/", 1)[-1]
        check("CFN Naming auto SQS constraints", 0 < len(queue_name) <= 80)

    check("CFN Naming auto SNS generated", bool(auto_topic))
    if auto_topic:
        topic_name = auto_topic.rsplit(":", 1)[-1]
        check("CFN Naming auto SNS constraints", 0 < len(topic_name) <= 256)

    check("CFN Naming auto SSM generated", bool(auto_param))
    if auto_param:
        check("CFN Naming auto SSM constraints", len(auto_param) <= 2048)

    check("CFN Naming cross-reference queue generated", bool(cross_queue))
    if auto_bucket and cross_queue:
        cross_queue_name = cross_queue.rsplit("/", 1)[-1]
        check("CFN Naming cross-reference queue uses AutoBucket", cross_queue_name.startswith(f"{auto_bucket}-cross"))

    delete_stack(auto_stack, "CFN Naming auto DeleteStack")

    explicit_stack = f"cfn-explicit-naming-{token}"
    explicit_bucket = f"cfn-explicit-{token}"
    explicit_queue = f"cfn-explicit-{token}"
    explicit_topic = f"cfn-explicit-{token}"
    explicit_param = f"/cfn-explicit/{token}"
    explicit_template = {
        "Resources": {
            "NamedBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": explicit_bucket},
            },
            "NamedQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": explicit_queue},
            },
            "NamedTopic": {
                "Type": "AWS::SNS::Topic",
                "Properties": {"TopicName": explicit_topic},
            },
            "NamedParameter": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {"Name": explicit_param, "Type": "String", "Value": "explicit"},
            },
        }
    }

    try:
        create_stack(explicit_stack, explicit_template)
        check("CFN Naming explicit CreateStack", True)
    except Exception as e:
        check("CFN Naming explicit CreateStack", False, e)
        return

    try:
        ok, status = wait_for_stack_terminal_state(explicit_stack, expected_success=True)
        check("CFN Naming explicit terminal status", ok, status)
    except Exception as e:
        check("CFN Naming explicit terminal status", False, e)
        delete_stack(explicit_stack, "CFN Naming explicit DeleteStack")
        return

    try:
        resources = describe_resources(explicit_stack)
        actual_bucket = physical_id(resources, "NamedBucket")
        actual_queue = physical_id(resources, "NamedQueue")
        actual_topic = physical_id(resources, "NamedTopic")
        actual_param = physical_id(resources, "NamedParameter")

        check("CFN Naming explicit S3", actual_bucket == explicit_bucket)
        check("CFN Naming explicit SQS", bool(actual_queue) and explicit_queue in actual_queue)
        check("CFN Naming explicit SNS", bool(actual_topic) and explicit_topic in actual_topic)
        check("CFN Naming explicit SSM", actual_param == explicit_param)
    except Exception as e:
        check("CFN Naming explicit names respected", False, e)

    delete_stack(explicit_stack, "CFN Naming explicit DeleteStack")


def run_s3_cors():
    import urllib.request
    import urllib.error

    print("--- S3 CORS Enforcement Tests ---")
    s3 = client("s3")
    bucket = "py-sdk-cors-test-" + str(int(time.time()))

    def raw(method, path, headers=None):
        """Makes a raw HTTP request; returns (status_code, lowercase_headers_dict)."""
        url = f"{ENDPOINT}/{bucket}{path}"
        req = urllib.request.Request(url, method=method)
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, {k.lower(): v for k, v in resp.headers.items()}
        except urllib.error.HTTPError as e:
            return e.code, {k.lower(): v for k, v in e.headers.items()}

    # ── Setup ─────────────────────────────────────────────────────────────────
    try:
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="cors-test.txt", Body=b"hello cors",
                      ContentType="text/plain")
        check("S3 CORS setup (create bucket + object)", True)
    except Exception as e:
        check("S3 CORS setup (create bucket + object)", False, e)
        return

    # ── No CORS config: preflight → 403 ──────────────────────────────────────
    try:
        status, _ = raw("OPTIONS", "/cors-test.txt", {
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
        })
        check("S3 CORS preflight without config → 403", status == 403)
    except Exception as e:
        check("S3 CORS preflight without config → 403", False, e)

    # ── Wildcard-origin CORS config ───────────────────────────────────────────
    try:
        s3.put_bucket_cors(
            Bucket=bucket,
            CORSConfiguration={
                "CORSRules": [{
                    "AllowedOrigins": ["*"],
                    "AllowedMethods": ["GET", "PUT", "POST", "DELETE", "HEAD"],
                    "AllowedHeaders": ["*"],
                    "ExposeHeaders": ["ETag"],
                    "MaxAgeSeconds": 3000,
                }]
            },
        )
        check("S3 CORS PutBucketCors (wildcard)", True)
    except Exception as e:
        check("S3 CORS PutBucketCors (wildcard)", False, e)

    try:
        status, headers = raw("OPTIONS", "/cors-test.txt", {
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
        })
        check("S3 CORS wildcard preflight → 200", status == 200)
        check("S3 CORS wildcard preflight → Allow-Origin: *",
              headers.get("access-control-allow-origin") == "*")
        check("S3 CORS wildcard preflight → Max-Age: 3000",
              headers.get("access-control-max-age") == "3000")
        check("S3 CORS wildcard preflight → Allow-Methods contains GET",
              "GET" in headers.get("access-control-allow-methods", "").upper())
    except Exception as e:
        check("S3 CORS wildcard preflight → 200", False, e)

    # Actual GET with Origin → receives CORS response headers
    try:
        status, headers = raw("GET", "/cors-test.txt", {"Origin": "http://localhost:3000"})
        check("S3 CORS actual GET → Allow-Origin: *",
              headers.get("access-control-allow-origin") == "*")
        vary = headers.get("vary", "")
        check("S3 CORS actual GET → Vary: Origin",
              any(t.strip().lower() == "origin" for t in vary.split(",")))
        check("S3 CORS actual GET → Expose-Headers contains ETag",
              "ETag" in headers.get("access-control-expose-headers", ""))
    except Exception as e:
        check("S3 CORS actual GET → Allow-Origin: *", False, e)

    # Actual GET without Origin header → no CORS headers
    try:
        _, headers = raw("GET", "/cors-test.txt")
        check("S3 CORS actual GET (no Origin) → no Allow-Origin",
              "access-control-allow-origin" not in headers)
    except Exception as e:
        check("S3 CORS actual GET (no Origin) → no Allow-Origin", False, e)

    # OPTIONS without Origin header → no CORS headers
    try:
        _, headers = raw("OPTIONS", "/cors-test.txt")
        check("S3 CORS OPTIONS without Origin → no Allow-Origin",
              "access-control-allow-origin" not in headers)
    except Exception as e:
        check("S3 CORS OPTIONS without Origin → no Allow-Origin", False, e)

    # ── Specific-origin CORS config ───────────────────────────────────────────
    try:
        s3.put_bucket_cors(
            Bucket=bucket,
            CORSConfiguration={
                "CORSRules": [{
                    "AllowedOrigins": ["https://example.com"],
                    "AllowedMethods": ["GET", "PUT"],
                    "AllowedHeaders": ["Content-Type", "Authorization"],
                    "ExposeHeaders": ["ETag", "x-amz-request-id"],
                    "MaxAgeSeconds": 600,
                }]
            },
        )
        check("S3 CORS PutBucketCors (specific origin)", True)
    except Exception as e:
        check("S3 CORS PutBucketCors (specific origin)", False, e)

    try:
        status, headers = raw("OPTIONS", "/cors-test.txt", {
            "Origin": "https://example.com",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "Content-Type",
        })
        check("S3 CORS specific origin preflight → 200", status == 200)
        check("S3 CORS specific origin preflight → echoes origin",
              headers.get("access-control-allow-origin") == "https://example.com")
        check("S3 CORS specific origin preflight → Max-Age: 600",
              headers.get("access-control-max-age") == "600")
    except Exception as e:
        check("S3 CORS specific origin preflight → 200", False, e)

    try:
        status, _ = raw("OPTIONS", "/cors-test.txt", {
            "Origin": "https://attacker.evil.com",
            "Access-Control-Request-Method": "GET",
        })
        check("S3 CORS non-matching origin → 403", status == 403)
    except Exception as e:
        check("S3 CORS non-matching origin → 403", False, e)

    try:
        status, _ = raw("OPTIONS", "/cors-test.txt", {
            "Origin": "https://example.com",
            "Access-Control-Request-Method": "DELETE",
        })
        check("S3 CORS non-matching method → 403", status == 403)
    except Exception as e:
        check("S3 CORS non-matching method → 403", False, e)

    try:
        _, headers = raw("GET", "/cors-test.txt", {"Origin": "https://example.com"})
        check("S3 CORS actual GET matching specific origin → echoes origin",
              headers.get("access-control-allow-origin") == "https://example.com")
    except Exception as e:
        check("S3 CORS actual GET matching specific origin → echoes origin", False, e)

    try:
        _, headers = raw("GET", "/cors-test.txt", {"Origin": "https://not-allowed.com"})
        check("S3 CORS actual GET non-matching origin → no Allow-Origin",
              "access-control-allow-origin" not in headers)
    except Exception as e:
        check("S3 CORS actual GET non-matching origin → no Allow-Origin", False, e)

    # ── DeleteBucketCors → preflights return 403 again ────────────────────────
    try:
        s3.delete_bucket_cors(Bucket=bucket)
        check("S3 CORS DeleteBucketCors", True)
    except Exception as e:
        check("S3 CORS DeleteBucketCors", False, e)

    try:
        status, _ = raw("OPTIONS", "/cors-test.txt", {
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
        })
        check("S3 CORS preflight after delete → 403", status == 403)
    except Exception as e:
        check("S3 CORS preflight after delete → 403", False, e)

    # ── Subdomain wildcard origin pattern ─────────────────────────────────────
    try:
        s3.put_bucket_cors(
            Bucket=bucket,
            CORSConfiguration={
                "CORSRules": [{
                    "AllowedOrigins": ["http://*.example.com"],
                    "AllowedMethods": ["GET"],
                    "AllowedHeaders": ["*"],
                    "MaxAgeSeconds": 120,
                }]
            },
        )
        check("S3 CORS PutBucketCors (subdomain wildcard)", True)
    except Exception as e:
        check("S3 CORS PutBucketCors (subdomain wildcard)", False, e)

    try:
        status, headers = raw("OPTIONS", "/cors-test.txt", {
            "Origin": "http://app.example.com",
            "Access-Control-Request-Method": "GET",
        })
        check("S3 CORS subdomain wildcard matches http://app.example.com",
              status == 200 and
              headers.get("access-control-allow-origin") == "http://app.example.com")
    except Exception as e:
        check("S3 CORS subdomain wildcard matches http://app.example.com", False, e)

    try:
        status, _ = raw("OPTIONS", "/cors-test.txt", {
            "Origin": "https://app.example.com",
            "Access-Control-Request-Method": "GET",
        })
        check("S3 CORS subdomain wildcard rejects https:// → 403", status == 403)
    except Exception as e:
        check("S3 CORS subdomain wildcard rejects https:// → 403", False, e)

    try:
        status, _ = raw("OPTIONS", "/cors-test.txt", {
            "Origin": "http://app.other.com",
            "Access-Control-Request-Method": "GET",
        })
        check("S3 CORS subdomain wildcard rejects different domain → 403", status == 403)
    except Exception as e:
        check("S3 CORS subdomain wildcard rejects different domain → 403", False, e)

    # ── Cleanup ────────────────────────────────────────────────────────────────
    try:
        s3.delete_bucket_cors(Bucket=bucket)
    except Exception:
        pass
    try:
        s3.delete_object(Bucket=bucket, Key="cors-test.txt")
        s3.delete_bucket(Bucket=bucket)
    except Exception as e:
        check("S3 CORS cleanup", False, e)


# ---------------------------------------------------------------------------
# S3 Notification Filter
# ---------------------------------------------------------------------------

def run_s3_notifications():
    print("--- S3 Notification Filter Tests ---")
    s3 = client("s3")
    sqs = client("sqs")
    sns = client("sns")

    account_id = "000000000000"
    prefix = "s3-notif-filter-"
    queue_name = prefix + "queue"
    topic_name = prefix + "topic"
    bucket_name = prefix + "bucket"

    queue_arn = f"arn:aws:sqs:us-east-1:{account_id}:{queue_name}"

    # Create SQS queue
    try:
        sqs.create_queue(QueueName=queue_name)
        check("S3 Notifications create SQS queue", True)
    except Exception as e:
        check("S3 Notifications create SQS queue", False, e)
        return

    # Create SNS topic
    try:
        r = sns.create_topic(Name=topic_name)
        topic_arn = r["TopicArn"]
        check("S3 Notifications create SNS topic", True)
    except Exception as e:
        check("S3 Notifications create SNS topic", False, e)
        try:
            sqs.delete_queue(QueueUrl=sqs.get_queue_url(QueueName=queue_name)["QueueUrl"])
        except Exception:
            pass
        return

    # Create S3 bucket
    try:
        s3.create_bucket(Bucket=bucket_name)
        check("S3 Notifications create bucket", True)
    except Exception as e:
        check("S3 Notifications create bucket", False, e)
        try:
            sns.delete_topic(TopicArn=topic_arn)
            sqs.delete_queue(QueueUrl=sqs.get_queue_url(QueueName=queue_name)["QueueUrl"])
        except Exception:
            pass
        return

    try:
        # Put notification configuration with prefix/suffix filters
        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "Id": "sqs-filtered",
                        "QueueArn": queue_arn,
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {
                            "Key": {
                                "FilterRules": [
                                    {"Name": "prefix", "Value": "incoming/"},
                                    {"Name": "suffix", "Value": ".csv"},
                                ]
                            }
                        },
                    }
                ],
                "TopicConfigurations": [
                    {
                        "Id": "sns-filtered",
                        "TopicArn": topic_arn,
                        "Events": ["s3:ObjectRemoved:*"],
                        "Filter": {
                            "Key": {
                                "FilterRules": [
                                    {"Name": "prefix", "Value": ""},
                                    {"Name": "suffix", "Value": ".txt"},
                                ]
                            }
                        },
                    }
                ],
            },
        )
        check("S3 Notifications put notification configuration", True)
    except Exception as e:
        check("S3 Notifications put notification configuration", False, e)
        try:
            s3.delete_bucket(Bucket=bucket_name)
            sns.delete_topic(TopicArn=topic_arn)
            sqs.delete_queue(QueueUrl=sqs.get_queue_url(QueueName=queue_name)["QueueUrl"])
        except Exception:
            pass
        return

    try:
        r = s3.get_bucket_notification_configuration(Bucket=bucket_name)

        queue_configs = r.get("QueueConfigurations", [])
        sqs_entry = next((c for c in queue_configs if c.get("QueueArn") == queue_arn), None)
        check("S3 Notifications SQS config present", sqs_entry is not None)
        if sqs_entry is not None:
            sqs_rules = sqs_entry.get("Filter", {}).get("Key", {}).get("FilterRules", [])
            check("S3 Notifications SQS config has 2 filter rules", len(sqs_rules) == 2, sqs_rules)

        topic_configs = r.get("TopicConfigurations", [])
        sns_entry = next((c for c in topic_configs if c.get("TopicArn") == topic_arn), None)
        check("S3 Notifications SNS config present", sns_entry is not None)
        if sns_entry is not None:
            sns_rules = sns_entry.get("Filter", {}).get("Key", {}).get("FilterRules", [])
            check("S3 Notifications SNS config has 2 filter rules", len(sns_rules) == 2, sns_rules)

    except Exception as e:
        check("S3 Notifications get notification configuration", False, e)
    finally:
        try:
            s3.delete_bucket(Bucket=bucket_name)
        except Exception:
            pass
        try:
            sns.delete_topic(TopicArn=topic_arn)
        except Exception:
            pass
        try:
            queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
            sqs.delete_queue(QueueUrl=queue_url)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

ALL_GROUPS = [
    ("ssm", run_ssm),
    ("sqs", run_sqs),
    ("sns", run_sns),
    ("s3", run_s3),
    ("s3-cors", run_s3_cors),
    ("dynamodb", run_dynamodb),
    ("dynamodb-gsi", run_dynamodb_gsi),
    ("lambda", run_lambda),
    ("iam", run_iam),
    ("sts", run_sts),
    ("secretsmanager", run_secretsmanager),
    ("kms", run_kms),
    ("kinesis", run_kinesis),
    ("cloudwatch-metrics", run_cloudwatch_metrics),
    ("cloudformation-naming", run_cloudformation_naming),
    ("cognito", run_cognito),
    ("s3-notifications", run_s3_notifications),
]


def resolve_enabled(args):
    names = set()
    if args:
        for arg in args:
            for part in arg.split(","):
                t = part.strip().lower()
                if t:
                    names.add(t)
        if names:
            return names
    env = os.environ.get("FLOCI_TESTS", "")
    if env.strip():
        for part in env.split(","):
            t = part.strip().lower()
            if t:
                names.add(t)
        if names:
            return names
    return None


if __name__ == "__main__":
    print("=== Floci SDK Test (boto3) ===\n")
    enabled = resolve_enabled(sys.argv[1:])
    if enabled:
        print(f"Running groups: {enabled}\n")

    for group_name, fn in ALL_GROUPS:
        if enabled is None or group_name in enabled:
            fn()
            print()

    print(f"=== Results: {passed} passed, {failed} failed ===")
    sys.exit(1 if failed > 0 else 0)
