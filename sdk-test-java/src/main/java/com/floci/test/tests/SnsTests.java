package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Map;

@FlociTestGroup
public class SnsTests implements TestGroup {

    @Override
    public String name() { return "sns"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- SNS Tests ---");

        try (SnsClient sns = SnsClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build();
             SqsClient sqs = SqsClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            String topicName = "sdk-test-topic-" + System.currentTimeMillis();
            String topicArn = null;

            // 1. CreateTopic
            try {
                CreateTopicResponse resp = sns.createTopic(CreateTopicRequest.builder().name(topicName).build());
                topicArn = resp.topicArn();
                ctx.check("SNS CreateTopic", topicArn != null && topicArn.contains(topicName));
            } catch (Exception e) {
                ctx.check("SNS CreateTopic", false, e);
                return;
            }

            // 2. ListTopics
            try {
                ListTopicsResponse resp = sns.listTopics();
                final String fTopicArn = topicArn;
                boolean found = resp.topics().stream().anyMatch(t -> t.topicArn().equals(fTopicArn));
                ctx.check("SNS ListTopics", found);
            } catch (Exception e) {
                ctx.check("SNS ListTopics", false, e);
            }

            // 3. GetTopicAttributes
            try {
                GetTopicAttributesResponse resp = sns.getTopicAttributes(GetTopicAttributesRequest.builder().topicArn(topicArn).build());
                ctx.check("SNS GetTopicAttributes", resp.attributes().containsKey("TopicArn"));
            } catch (Exception e) {
                ctx.check("SNS GetTopicAttributes", false, e);
            }

            // 4. Subscribe (SQS)
            String queueUrl = null;
            String queueArn = null;
            String subscriptionArn = null;
            try {
                String queueName = "sns-test-queue-" + System.currentTimeMillis();
                queueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName(queueName).build()).queueUrl();
                queueArn = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                        .queueUrl(queueUrl)
                        .attributeNames(QueueAttributeName.QUEUE_ARN)
                        .build())
                        .attributes().get(QueueAttributeName.QUEUE_ARN);

                SubscribeResponse subResp = sns.subscribe(SubscribeRequest.builder()
                        .topicArn(topicArn)
                        .protocol("sqs")
                        .endpoint(queueArn)
                        .build());
                subscriptionArn = subResp.subscriptionArn();
                ctx.check("SNS Subscribe SQS", subscriptionArn != null);
            } catch (Exception e) {
                ctx.check("SNS Subscribe SQS", false, e);
            }

            // 5. ListSubscriptionsByTopic
            try {
                ListSubscriptionsByTopicResponse resp = sns.listSubscriptionsByTopic(ListSubscriptionsByTopicRequest.builder().topicArn(topicArn).build());
                final String fSubArn = subscriptionArn;
                boolean found = resp.subscriptions().stream().anyMatch(s -> s.subscriptionArn().equals(fSubArn));
                ctx.check("SNS ListSubscriptionsByTopic", found);
            } catch (Exception e) {
                ctx.check("SNS ListSubscriptionsByTopic", false, e);
            }

            // 6. Publish
            try {
                PublishResponse resp = sns.publish(PublishRequest.builder()
                        .topicArn(topicArn)
                        .message("hello from sns")
                        .subject("test-subject")
                        .build());
                ctx.check("SNS Publish", resp.messageId() != null);
            } catch (Exception e) {
                ctx.check("SNS Publish", false, e);
            }

            // 7. Verify SQS delivery
            try {
                Thread.sleep(500); // Allow async delivery
                ReceiveMessageResponse recv = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(2)
                        .build());
                ctx.check("SNS SQS Delivery", !recv.messages().isEmpty());
                if (!recv.messages().isEmpty()) {
                    String body = recv.messages().get(0).body();
                    ctx.check("SNS Message Body contains content", body.contains("hello from sns"));
                    sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(recv.messages().get(0).receiptHandle()).build());
                }
            } catch (Exception e) {
                ctx.check("SNS SQS Delivery", false, e);
            }

            // 8. Publish with Message Attributes
            try {
                sns.publish(PublishRequest.builder()
                        .topicArn(topicArn)
                        .message("msg with attrs")
                        .messageAttributes(Map.of(
                                "my-attr", software.amazon.awssdk.services.sns.model.MessageAttributeValue.builder().dataType("String").stringValue("my-value").build()
                        ))
                        .build());
                Thread.sleep(500);
                ReceiveMessageResponse recv = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(2)
                        .build());
                ctx.check("SNS Delivery with Attrs", !recv.messages().isEmpty());
                if (!recv.messages().isEmpty()) {
                    ctx.check("SNS Attr present in body", recv.messages().get(0).body().contains("my-value"));
                    sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(recv.messages().get(0).receiptHandle()).build());
                }
            } catch (Exception e) {
                ctx.check("SNS Delivery with Attrs", false, e);
            }

            // 9. Unsubscribe
            try {
                sns.unsubscribe(UnsubscribeRequest.builder().subscriptionArn(subscriptionArn).build());
                ctx.check("SNS Unsubscribe", true);
            } catch (Exception e) {
                ctx.check("SNS Unsubscribe", false, e);
            }

            // 10. DeleteTopic
            try {
                sns.deleteTopic(DeleteTopicRequest.builder().topicArn(topicArn).build());
                ctx.check("SNS DeleteTopic", true);
            } catch (Exception e) {
                ctx.check("SNS DeleteTopic", false, e);
            }

            // Cleanup SQS
            if (queueUrl != null) {
                try {
                    sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
                } catch (Exception ignored) {}
            }

        } catch (Exception e) {
            ctx.check("SNS Client", false, e);
        }
    }
}
