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

            // 9. RawMessageDelivery - raw body without SNS envelope
            try {
                String rawQueueName = "sns-raw-delivery-" + System.currentTimeMillis();
                String rawQueueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName(rawQueueName).build()).queueUrl();
                String rawQueueArn = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                        .queueUrl(rawQueueUrl)
                        .attributeNames(QueueAttributeName.QUEUE_ARN)
                        .build())
                        .attributes().get(QueueAttributeName.QUEUE_ARN);

                String rawSubArn = sns.subscribe(SubscribeRequest.builder()
                        .topicArn(topicArn)
                        .protocol("sqs")
                        .endpoint(rawQueueArn)
                        .build()).subscriptionArn();

                sns.setSubscriptionAttributes(SetSubscriptionAttributesRequest.builder()
                        .subscriptionArn(rawSubArn)
                        .attributeName("RawMessageDelivery")
                        .attributeValue("true")
                        .build());

                sns.publish(PublishRequest.builder()
                        .topicArn(topicArn)
                        .message("raw-delivery-content")
                        .messageAttributes(Map.of(
                                "color", software.amazon.awssdk.services.sns.model.MessageAttributeValue.builder()
                                        .dataType("String").stringValue("blue").build(),
                                "count", software.amazon.awssdk.services.sns.model.MessageAttributeValue.builder()
                                        .dataType("Number").stringValue("42").build()
                        ))
                        .build());

                Thread.sleep(500);
                ReceiveMessageResponse rawRecv = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(rawQueueUrl)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(2)
                        .messageAttributeNames("All")
                        .build());

                ctx.check("SNS RawMessageDelivery received", !rawRecv.messages().isEmpty());
                if (!rawRecv.messages().isEmpty()) {
                    String body = rawRecv.messages().get(0).body();
                    ctx.check("SNS RawMessageDelivery no envelope", !body.contains("\"Type\":\"Notification\""));
                    ctx.check("SNS RawMessageDelivery raw content", body.equals("raw-delivery-content"));
                    Map<String, software.amazon.awssdk.services.sqs.model.MessageAttributeValue> msgAttrs =
                            rawRecv.messages().get(0).messageAttributes();
                    ctx.check("SNS RawMessageDelivery String attr forwarded",
                            msgAttrs.containsKey("color") && "blue".equals(msgAttrs.get("color").stringValue()));
                    ctx.check("SNS RawMessageDelivery Number attr forwarded",
                            msgAttrs.containsKey("count") && "Number".equals(msgAttrs.get("count").dataType()));
                }

                sns.unsubscribe(UnsubscribeRequest.builder().subscriptionArn(rawSubArn).build());
                sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(rawQueueUrl).build());
            } catch (Exception e) {
                ctx.check("SNS RawMessageDelivery", false, e);
            }

            // 10. Unsubscribe
            try {
                sns.unsubscribe(UnsubscribeRequest.builder().subscriptionArn(subscriptionArn).build());
                ctx.check("SNS Unsubscribe", true);
            } catch (Exception e) {
                ctx.check("SNS Unsubscribe", false, e);
            }

            // 11. DeleteTopic
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

            // 12. SNS FIFO with explicit MessageDeduplicationId
            try {
                String fifoQueueName = "sns-fifo-explicit-" + System.currentTimeMillis() + ".fifo";
                String fifoQueueUrl = sqs.createQueue(CreateQueueRequest.builder()
                        .queueName(fifoQueueName)
                        .attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true"))
                        .build()).queueUrl();
                String fifoQueueArn = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                        .queueUrl(fifoQueueUrl)
                        .attributeNames(QueueAttributeName.QUEUE_ARN)
                        .build())
                        .attributes().get(QueueAttributeName.QUEUE_ARN);

                String fifoTopicName = "sns-fifo-explicit-" + System.currentTimeMillis() + ".fifo";
                String fifoTopicArn = sns.createTopic(CreateTopicRequest.builder()
                        .name(fifoTopicName)
                        .attributes(Map.of("FifoTopic", "true"))
                        .build()).topicArn();

                String fifoSubArn = sns.subscribe(SubscribeRequest.builder()
                        .topicArn(fifoTopicArn)
                        .protocol("sqs")
                        .endpoint(fifoQueueArn)
                        .build()).subscriptionArn();

                String explicitDedupId = "dedup-" + System.currentTimeMillis();
                sns.publish(PublishRequest.builder()
                        .topicArn(fifoTopicArn)
                        .message("fifo message with explicit dedup")
                        .messageGroupId("test-group")
                        .messageDeduplicationId(explicitDedupId)
                        .build());

                Thread.sleep(500);
                ReceiveMessageResponse fifoRecv = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(fifoQueueUrl)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(2)
                        .messageSystemAttributeNames(MessageSystemAttributeName.MESSAGE_DEDUPLICATION_ID)
                        .build());

                ctx.check("SNS FIFO explicit dedup - message received", !fifoRecv.messages().isEmpty());
                if (!fifoRecv.messages().isEmpty()) {
                    var msg = fifoRecv.messages().get(0);
                    String receivedDedupId = msg.attributes().get(MessageSystemAttributeName.MESSAGE_DEDUPLICATION_ID);
                    ctx.check("SNS FIFO explicit dedup - dedup ID matches", explicitDedupId.equals(receivedDedupId));
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(fifoQueueUrl)
                            .receiptHandle(msg.receiptHandle())
                            .build());
                }

                sns.unsubscribe(UnsubscribeRequest.builder().subscriptionArn(fifoSubArn).build());
                sns.deleteTopic(DeleteTopicRequest.builder().topicArn(fifoTopicArn).build());
                sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(fifoQueueUrl).build());
            } catch (Exception e) {
                ctx.check("SNS FIFO explicit dedup", false, e);
            }

            // 13. SNS FIFO with topic-level ContentBasedDeduplication
            try {
                String cbdQueueName = "sns-fifo-cbd-" + System.currentTimeMillis() + ".fifo";
                String cbdQueueUrl = sqs.createQueue(CreateQueueRequest.builder()
                        .queueName(cbdQueueName)
                        .attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true"))
                        .build()).queueUrl();
                String cbdQueueArn = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                        .queueUrl(cbdQueueUrl)
                        .attributeNames(QueueAttributeName.QUEUE_ARN)
                        .build())
                        .attributes().get(QueueAttributeName.QUEUE_ARN);

                String cbdTopicName = "sns-fifo-cbd-" + System.currentTimeMillis() + ".fifo";
                String cbdTopicArn = sns.createTopic(CreateTopicRequest.builder()
                        .name(cbdTopicName)
                        .attributes(Map.of(
                                "FifoTopic", "true",
                                "ContentBasedDeduplication", "true"
                        ))
                        .build()).topicArn();

                String cbdSubArn = sns.subscribe(SubscribeRequest.builder()
                        .topicArn(cbdTopicArn)
                        .protocol("sqs")
                        .endpoint(cbdQueueArn)
                        .build()).subscriptionArn();

                sns.publish(PublishRequest.builder()
                        .topicArn(cbdTopicArn)
                        .message("fifo message with content-based dedup")
                        .messageGroupId("test-group")
                        // No explicit MessageDeduplicationId - should be generated from content hash
                        .build());

                Thread.sleep(500);
                ReceiveMessageResponse cbdRecv = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(cbdQueueUrl)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(2)
                        .messageSystemAttributeNames(MessageSystemAttributeName.MESSAGE_DEDUPLICATION_ID)
                        .build());

                ctx.check("SNS FIFO content-based dedup - message received", !cbdRecv.messages().isEmpty());
                if (!cbdRecv.messages().isEmpty()) {
                    var msg = cbdRecv.messages().get(0);
                    String receivedDedupId = msg.attributes().get(MessageSystemAttributeName.MESSAGE_DEDUPLICATION_ID);
                    ctx.check("SNS FIFO content-based dedup - dedup ID present", receivedDedupId != null && !receivedDedupId.isEmpty());
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(cbdQueueUrl)
                            .receiptHandle(msg.receiptHandle())
                            .build());
                }

                sns.unsubscribe(UnsubscribeRequest.builder().subscriptionArn(cbdSubArn).build());
                sns.deleteTopic(DeleteTopicRequest.builder().topicArn(cbdTopicArn).build());
                sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(cbdQueueUrl).build());
            } catch (Exception e) {
                ctx.check("SNS FIFO content-based dedup", false, e);
            }

        } catch (Exception e) {
            ctx.check("SNS Client", false, e);
        }
    }
}
