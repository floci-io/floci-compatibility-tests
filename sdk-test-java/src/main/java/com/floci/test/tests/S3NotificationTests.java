package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

@FlociTestGroup
public class S3NotificationTests implements TestGroup {

    @Override
    public String name() { return "s3-notifications"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- S3 Event Notification Tests ---");

        S3Client s3 = S3Client.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .forcePathStyle(true)
                .build();

        SqsClient sqs = SqsClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build();

        SnsClient sns = SnsClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build();

        final String bucket = "s3-notif-test-bucket";
        final String createdQueue = "s3-created-queue";
        final String removedQueue = "s3-removed-queue";
        final String topicName = "s3-event-topic";
        final String accountId = "000000000000";

        String createdQueueUrl = ctx.endpoint + "/" + accountId + "/" + createdQueue;
        String removedQueueUrl = ctx.endpoint + "/" + accountId + "/" + removedQueue;
        String createdQueueArn = "arn:aws:sqs:us-east-1:" + accountId + ":" + createdQueue;
        String removedQueueArn = "arn:aws:sqs:us-east-1:" + accountId + ":" + removedQueue;

        // Setup: create queues
        try { sqs.createQueue(CreateQueueRequest.builder().queueName(createdQueue).build()); } catch (Exception ignore) {}
        try { sqs.createQueue(CreateQueueRequest.builder().queueName(removedQueue).build()); } catch (Exception ignore) {}

        // Create SNS topic
        String topicArn;
        try {
            topicArn = sns.createTopic(CreateTopicRequest.builder().name(topicName).build()).topicArn();
        } catch (Exception e) {
            ctx.check("S3 Notifications setup: CreateTopic", false, e);
            cleanup(s3, sqs, sns, bucket, createdQueueUrl, removedQueueUrl, null);
            return;
        }

        // Subscribe removedQueue to SNS topic
        try {
            sns.subscribe(SubscribeRequest.builder()
                    .topicArn(topicArn)
                    .protocol("sqs")
                    .endpoint(removedQueueUrl)
                    .build());
        } catch (Exception e) {
            ctx.check("S3 Notifications setup: SNS Subscribe", false, e);
            cleanup(s3, sqs, sns, bucket, createdQueueUrl, removedQueueUrl, topicArn);
            return;
        }

        // Create bucket
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        } catch (Exception e) {
            ctx.check("S3 Notifications setup: CreateBucket", false, e);
            cleanup(s3, sqs, sns, bucket, createdQueueUrl, removedQueueUrl, topicArn);
            return;
        }

        // 5. PutBucketNotificationConfiguration
        try {
            s3.putBucketNotificationConfiguration(PutBucketNotificationConfigurationRequest.builder()
                    .bucket(bucket)
                    .notificationConfiguration(NotificationConfiguration.builder()
                            .queueConfigurations(QueueConfiguration.builder()
                                    .id("sqs-created")
                                    .queueArn(createdQueueArn)
                                    .events(Event.S3_OBJECT_CREATED)
                                    .filter(NotificationConfigurationFilter.builder()
                                            .key(S3KeyFilter.builder()
                                                    .filterRules(FilterRule.builder()
                                                            .name(FilterRuleName.PREFIX).value("").build())
                                                    .build())
                                            .build())
                                    .build())
                            .topicConfigurations(TopicConfiguration.builder()
                                    .id("sns-removed")
                                    .topicArn(topicArn)
                                    .events(Event.S3_OBJECT_REMOVED)
                                    .filter(NotificationConfigurationFilter.builder()
                                            .key(S3KeyFilter.builder()
                                                    .filterRules(
                                                            FilterRule.builder()
                                                                    .name(FilterRuleName.PREFIX).value("").build(),
                                                            FilterRule.builder()
                                                                    .name(FilterRuleName.SUFFIX).value(".txt").build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build());
            ctx.check("S3 PutBucketNotificationConfiguration", true);
        } catch (Exception e) {
            ctx.check("S3 PutBucketNotificationConfiguration", false, e);
            cleanup(s3, sqs, sns, bucket, createdQueueUrl, removedQueueUrl, topicArn);
            return;
        }

        // 6. GetBucketNotificationConfiguration
        try {
            GetBucketNotificationConfigurationResponse nc = s3.getBucketNotificationConfiguration(
                    GetBucketNotificationConfigurationRequest.builder().bucket(bucket).build());
            boolean hasQueue = nc.queueConfigurations().stream()
                    .anyMatch(q -> q.queueArn().equals(createdQueueArn));
            boolean hasTopic = nc.topicConfigurations().stream()
                    .anyMatch(t -> t.topicArn().equals(topicArn));
            boolean queueFilterPreserved = nc.queueConfigurations().stream()
                    .anyMatch(q -> q.queueArn().equals(createdQueueArn)
                            && q.filter() != null && q.filter().key() != null
                            && q.filter().key().filterRules().size() == 1);
            boolean topicFilterPreserved = nc.topicConfigurations().stream()
                    .anyMatch(t -> t.topicArn().equals(topicArn)
                            && t.filter() != null && t.filter().key() != null
                            && t.filter().key().filterRules().size() == 2);
            ctx.check("S3 GetBucketNotificationConfiguration (queue)", hasQueue);
            ctx.check("S3 GetBucketNotificationConfiguration (topic)", hasTopic);
            ctx.check("S3 GetBucketNotificationConfiguration (queue filter round-trip)", queueFilterPreserved);
            ctx.check("S3 GetBucketNotificationConfiguration (topic filter round-trip)", topicFilterPreserved);
        } catch (Exception e) {
            ctx.check("S3 GetBucketNotificationConfiguration (queue)", false, e);
            ctx.check("S3 GetBucketNotificationConfiguration (topic)", false, e);
            ctx.check("S3 GetBucketNotificationConfiguration (queue filter round-trip)", false, e);
            ctx.check("S3 GetBucketNotificationConfiguration (topic filter round-trip)", false, e);
        }

        // 7. PutObject → fires ObjectCreated:Put to createdQueue
        try {
            s3.putObject(PutObjectRequest.builder().bucket(bucket).key("hello.txt").build(),
                    RequestBody.fromString("hello world"));
            ctx.check("S3 PutObject (triggers notification)", true);
        } catch (Exception e) {
            ctx.check("S3 PutObject (triggers notification)", false, e);
            cleanup(s3, sqs, sns, bucket, createdQueueUrl, removedQueueUrl, topicArn);
            return;
        }

        // 8. Receive message from createdQueue — verify S3 event
        try {
            Thread.sleep(200);
            ReceiveMessageResponse resp = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(createdQueueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(2)
                    .build());
            ctx.check("S3 ObjectCreated:Put event delivered to SQS", !resp.messages().isEmpty());
            if (!resp.messages().isEmpty()) {
                String body = resp.messages().get(0).body();
                ctx.check("S3 ObjectCreated:Put eventName correct", body.contains("ObjectCreated:Put"));
                ctx.check("S3 ObjectCreated:Put bucket correct", body.contains(bucket));
                ctx.check("S3 ObjectCreated:Put key correct", body.contains("hello.txt"));
                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(createdQueueUrl)
                        .receiptHandle(resp.messages().get(0).receiptHandle())
                        .build());
            }
        } catch (Exception e) {
            ctx.check("S3 ObjectCreated:Put event delivered to SQS", false, e);
        }

        // 9. DeleteObject → fires ObjectRemoved:Delete to SNS → removedQueue
        try {
            s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key("hello.txt").build());
            ctx.check("S3 DeleteObject (triggers notification)", true);
        } catch (Exception e) {
            ctx.check("S3 DeleteObject (triggers notification)", false, e);
            cleanup(s3, sqs, sns, bucket, createdQueueUrl, removedQueueUrl, topicArn);
            return;
        }

        // 10. Receive SNS-wrapped S3 event from removedQueue
        try {
            Thread.sleep(200);
            ReceiveMessageResponse resp = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(removedQueueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(2)
                    .build());
            ctx.check("S3 ObjectRemoved:Delete event delivered via SNS", !resp.messages().isEmpty());
            if (!resp.messages().isEmpty()) {
                String body = resp.messages().get(0).body();
                ctx.check("S3 ObjectRemoved:Delete eventName in SNS envelope",
                        body.contains("ObjectRemoved:Delete") || body.contains("ObjectRemoved"));
                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(removedQueueUrl)
                        .receiptHandle(resp.messages().get(0).receiptHandle())
                        .build());
            }
        } catch (Exception e) {
            ctx.check("S3 ObjectRemoved:Delete event delivered via SNS", false, e);
        }

        cleanup(s3, sqs, sns, bucket, createdQueueUrl, removedQueueUrl, topicArn);
    }

    private void cleanup(S3Client s3, SqsClient sqs, SnsClient sns,
                         String bucket, String createdQueueUrl, String removedQueueUrl, String topicArn) {
        try { s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build()); } catch (Exception ignore) {}
        try { sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(createdQueueUrl).build()); } catch (Exception ignore) {}
        try { sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(removedQueueUrl).build()); } catch (Exception ignore) {}
        if (topicArn != null) {
            try { sns.deleteTopic(DeleteTopicRequest.builder().topicArn(topicArn).build()); } catch (Exception ignore) {}
        }
        try { s3.close(); } catch (Exception ignore) {}
        try { sqs.close(); } catch (Exception ignore) {}
        try { sns.close(); } catch (Exception ignore) {}
    }
}
