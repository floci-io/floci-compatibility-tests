package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;

@FlociTestGroup
public class SqsTests implements TestGroup {

    @Override
    public String name() { return "sqs"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- SQS Tests ---");

        try (SqsClient sqs = SqsClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            // 1. Create queue
            String queueUrl = null;
            try {
                CreateQueueResponse createResp = sqs.createQueue(CreateQueueRequest.builder()
                        .queueName("sdk-test-queue")
                        .build());
                queueUrl = createResp.queueUrl();
                ctx.check("SQS CreateQueue", queueUrl != null && queueUrl.contains("sdk-test-queue"));
            } catch (Exception e) {
                ctx.check("SQS CreateQueue", false, e);
            }

            // 1b. GetQueueUrl
            try {
                GetQueueUrlResponse getUrlResp = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName("sdk-test-queue")
                        .build());
                ctx.check("SQS GetQueueUrl", queueUrl != null && queueUrl.equals(getUrlResp.queueUrl()));
            } catch (Exception e) {
                ctx.check("SQS GetQueueUrl", false, e);
            }

            // 2. List queues
            try {
                ListQueuesResponse listResp = sqs.listQueues();
                boolean found = listResp.queueUrls().stream()
                        .anyMatch(u -> u.contains("sdk-test-queue"));
                ctx.check("SQS ListQueues", found);
            } catch (Exception e) {
                ctx.check("SQS ListQueues", false, e);
            }

            // 3. Send message
            try {
                SendMessageResponse sendResp = sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody("Hello from AWS SDK!")
                        .build());
                String messageId = sendResp.messageId();
                ctx.check("SQS SendMessage", messageId != null && !messageId.isEmpty());
            } catch (Exception e) {
                ctx.check("SQS SendMessage", false, e);
            }

            // 4. Receive message
            String receiptHandle = null;
            try {
                ReceiveMessageResponse receiveResp = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)
                        .build());
                List<Message> messages = receiveResp.messages();
                boolean ok = !messages.isEmpty()
                        && "Hello from AWS SDK!".equals(messages.get(0).body());
                receiptHandle = messages.isEmpty() ? null : messages.get(0).receiptHandle();
                ctx.check("SQS ReceiveMessage", ok);
            } catch (Exception e) {
                ctx.check("SQS ReceiveMessage", false, e);
            }

            // 5. Delete message
            try {
                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(receiptHandle)
                        .build());
                ctx.check("SQS DeleteMessage", true);
            } catch (Exception e) {
                ctx.check("SQS DeleteMessage", false, e);
            }

            // 6. Verify queue is empty
            try {
                ReceiveMessageResponse receiveResp = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)
                        .build());
                ctx.check("SQS Queue empty after delete", receiveResp.messages().isEmpty());
            } catch (Exception e) {
                ctx.check("SQS Queue empty after delete", false, e);
            }

            // 7. SendMessageBatch
            try {
                SendMessageBatchResponse batchSendResp = sqs.sendMessageBatch(SendMessageBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(
                                SendMessageBatchRequestEntry.builder().id("msg1").messageBody("Batch message 1").build(),
                                SendMessageBatchRequestEntry.builder().id("msg2").messageBody("Batch message 2").build(),
                                SendMessageBatchRequestEntry.builder().id("msg3").messageBody("Batch message 3").build()
                        )
                        .build());
                ctx.check("SQS SendMessageBatch", batchSendResp.successful().size() == 3
                        && batchSendResp.failed().isEmpty());
            } catch (Exception e) {
                ctx.check("SQS SendMessageBatch", false, e);
            }

            // 8. Receive batch messages and DeleteMessageBatch
            try {
                ReceiveMessageResponse receiveResp = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(3)
                        .build());
                List<Message> msgs = receiveResp.messages();

                List<DeleteMessageBatchRequestEntry> deleteEntries = new ArrayList<>();
                for (int i = 0; i < msgs.size(); i++) {
                    deleteEntries.add(DeleteMessageBatchRequestEntry.builder()
                            .id("del" + i)
                            .receiptHandle(msgs.get(i).receiptHandle())
                            .build());
                }

                DeleteMessageBatchResponse batchDeleteResp = sqs.deleteMessageBatch(DeleteMessageBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(deleteEntries)
                        .build());
                ctx.check("SQS DeleteMessageBatch", batchDeleteResp.successful().size() == 3);
            } catch (Exception e) {
                ctx.check("SQS DeleteMessageBatch", false, e);
            }

            // 9. Verify queue empty after batch delete
            try {
                ReceiveMessageResponse receiveResp = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)
                        .build());
                ctx.check("SQS Queue empty after batch delete", receiveResp.messages().isEmpty());
            } catch (Exception e) {
                ctx.check("SQS Queue empty after batch delete", false, e);
            }

            // 10. SetQueueAttributes
            try {
                sqs.setQueueAttributes(SetQueueAttributesRequest.builder()
                        .queueUrl(queueUrl)
                        .attributesWithStrings(java.util.Map.of("VisibilityTimeout", "60"))
                        .build());
                GetQueueAttributesResponse attrs = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                        .queueUrl(queueUrl)
                        .attributeNamesWithStrings("VisibilityTimeout")
                        .build());
                ctx.check("SQS SetQueueAttributes",
                        "60".equals(attrs.attributesAsStrings().get("VisibilityTimeout")));
            } catch (Exception e) {
                ctx.check("SQS SetQueueAttributes", false, e);
            }

            // 11. TagQueue
            try {
                sqs.tagQueue(TagQueueRequest.builder()
                        .queueUrl(queueUrl)
                        .tags(java.util.Map.of("env", "test", "team", "backend"))
                        .build());
                ctx.check("SQS TagQueue", true);
            } catch (Exception e) {
                ctx.check("SQS TagQueue", false, e);
            }

            // 12. ListQueueTags
            try {
                ListQueueTagsResponse tagsResp = sqs.listQueueTags(ListQueueTagsRequest.builder()
                        .queueUrl(queueUrl)
                        .build());
                ctx.check("SQS ListQueueTags",
                        "test".equals(tagsResp.tags().get("env"))
                        && "backend".equals(tagsResp.tags().get("team")));
            } catch (Exception e) {
                ctx.check("SQS ListQueueTags", false, e);
            }

            // 13. UntagQueue
            try {
                sqs.untagQueue(UntagQueueRequest.builder()
                        .queueUrl(queueUrl)
                        .tagKeys("team")
                        .build());
                ListQueueTagsResponse tagsResp = sqs.listQueueTags(ListQueueTagsRequest.builder()
                        .queueUrl(queueUrl)
                        .build());
                ctx.check("SQS UntagQueue",
                        "test".equals(tagsResp.tags().get("env"))
                        && !tagsResp.tags().containsKey("team"));
            } catch (Exception e) {
                ctx.check("SQS UntagQueue", false, e);
            }

            // 14. ChangeMessageVisibilityBatch
            try {
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody("vis-batch-1").build());
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody("vis-batch-2").build());
                ReceiveMessageResponse rcv = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl).maxNumberOfMessages(2).build());

                List<ChangeMessageVisibilityBatchRequestEntry> visEntries = new ArrayList<>();
                for (int i = 0; i < rcv.messages().size(); i++) {
                    visEntries.add(ChangeMessageVisibilityBatchRequestEntry.builder()
                            .id("vis" + i)
                            .receiptHandle(rcv.messages().get(i).receiptHandle())
                            .visibilityTimeout(0)
                            .build());
                }
                ChangeMessageVisibilityBatchResponse visBatchResp = sqs.changeMessageVisibilityBatch(
                        ChangeMessageVisibilityBatchRequest.builder()
                                .queueUrl(queueUrl)
                                .entries(visEntries)
                                .build());
                ctx.check("SQS ChangeMessageVisibilityBatch", visBatchResp.successful().size() == 2);

                // Clean up
                ReceiveMessageResponse cleanup = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl).maxNumberOfMessages(2).build());
                for (Message msg : cleanup.messages()) {
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(queueUrl).receiptHandle(msg.receiptHandle()).build());
                }
            } catch (Exception e) {
                ctx.check("SQS ChangeMessageVisibilityBatch", false, e);
            }

            // 15. String Message Attribute (SDK validates MD5OfMessageAttributes client-side)
            final String qUrl = queueUrl;
            try {
                sqs.sendMessage(b -> b.queueUrl(qUrl).messageBody("msg-attrs")
                        .messageAttributes(java.util.Map.of("myattr",
                                MessageAttributeValue.builder().dataType("String").stringValue("myval").build())));
                ReceiveMessageResponse rcv = sqs.receiveMessage(b -> b.queueUrl(qUrl).maxNumberOfMessages(1).messageAttributeNames("All"));
                ctx.check("SQS MessageAttributes string", !rcv.messages().isEmpty()
                        && "myval".equals(rcv.messages().get(0).messageAttributes().get("myattr").stringValue()));
                sqs.deleteMessage(b -> b.queueUrl(qUrl).receiptHandle(rcv.messages().get(0).receiptHandle()));
            } catch (Exception e) {
                ctx.check("SQS MessageAttributes string", false, e);
            }

            // 16. Binary Message Attribute (SDK validates MD5OfMessageAttributes client-side)
            try {
                byte[] binaryPayload = new byte[]{1, 2, 3, 4, 5};
                sqs.sendMessage(b -> b.queueUrl(qUrl).messageBody("binary-msg")
                        .messageAttributes(java.util.Map.of("payload",
                                MessageAttributeValue.builder()
                                        .dataType("Binary")
                                        .binaryValue(software.amazon.awssdk.core.SdkBytes.fromByteArray(binaryPayload))
                                        .build())));
                ReceiveMessageResponse rcv = sqs.receiveMessage(b -> b.queueUrl(qUrl).maxNumberOfMessages(1).messageAttributeNames("All"));
                boolean hasBinaryAttr = !rcv.messages().isEmpty()
                        && rcv.messages().get(0).messageAttributes().containsKey("payload")
                        && rcv.messages().get(0).messageAttributes().get("payload").binaryValue() != null;
                ctx.check("SQS MessageAttributes binary", hasBinaryAttr);
                if (!rcv.messages().isEmpty()) {
                    sqs.deleteMessage(b -> b.queueUrl(qUrl).receiptHandle(rcv.messages().get(0).receiptHandle()));
                }
            } catch (Exception e) {
                ctx.check("SQS MessageAttributes binary", false, e);
            }

            // 17. Long Polling
            try {
                long start = System.currentTimeMillis();
                sqs.receiveMessage(b -> b.queueUrl(qUrl).maxNumberOfMessages(1).waitTimeSeconds(2));
                long elapsed = System.currentTimeMillis() - start;
                ctx.check("SQS Long Polling", elapsed >= 1800); // Should wait ~2s
            } catch (Exception e) {
                ctx.check("SQS Long Polling", false, e);
            }

            // 18. DLQ Routing
            String dlqUrl = null;
            try {
                dlqUrl = sqs.createQueue(b -> b.queueName("sdk-test-dlq")).queueUrl();
                final String dUrl = dlqUrl;
                String dlqArn = sqs.getQueueAttributes(b -> b.queueUrl(dUrl).attributeNames(QueueAttributeName.QUEUE_ARN)).attributes().get(QueueAttributeName.QUEUE_ARN);
                
                String redrivePolicy = "{\"maxReceiveCount\":\"2\", \"deadLetterTargetArn\":\"" + dlqArn + "\"}";
                sqs.setQueueAttributes(b -> b.queueUrl(qUrl).attributes(java.util.Map.of(QueueAttributeName.REDRIVE_POLICY, redrivePolicy)));
                
                // Send a message to main queue
                sqs.sendMessage(b -> b.queueUrl(qUrl).messageBody("dlq-test"));
                
                // Receive 1 (count=1)
                Message m1 = sqs.receiveMessage(b -> b.queueUrl(qUrl).maxNumberOfMessages(1)).messages().get(0);
                sqs.changeMessageVisibility(b -> b.queueUrl(qUrl).receiptHandle(m1.receiptHandle()).visibilityTimeout(0));
                
                // Receive 2 (count=2)
                Message m2 = sqs.receiveMessage(b -> b.queueUrl(qUrl).maxNumberOfMessages(1)).messages().get(0);
                sqs.changeMessageVisibility(b -> b.queueUrl(qUrl).receiptHandle(m2.receiptHandle()).visibilityTimeout(0));
                
                // Receive 3 (count=3 -> moves to DLQ)
                ReceiveMessageResponse r3 = sqs.receiveMessage(b -> b.queueUrl(qUrl).maxNumberOfMessages(1));
                ctx.check("SQS DLQ Main Queue Empty", r3.messages().isEmpty());
                
                ReceiveMessageResponse dlqRcv = sqs.receiveMessage(b -> b.queueUrl(dUrl).maxNumberOfMessages(1));
                ctx.check("SQS DLQ Message Moved", !dlqRcv.messages().isEmpty() && "dlq-test".equals(dlqRcv.messages().get(0).body()));
                
                if (!dlqRcv.messages().isEmpty()) {
                    sqs.deleteMessage(b -> b.queueUrl(dUrl).receiptHandle(dlqRcv.messages().get(0).receiptHandle()));
                }
            } catch (Exception e) {
                ctx.check("SQS DLQ Routing", false, e);
            }

            // 19. ListDeadLetterSourceQueues
            final String finalDlqUrl = dlqUrl;
            try {
                ListDeadLetterSourceQueuesResponse resp = sqs.listDeadLetterSourceQueues(b -> b.queueUrl(finalDlqUrl));
                boolean found = resp.queueUrls().stream().anyMatch(u -> u.equals(qUrl));
                ctx.check("SQS ListDeadLetterSourceQueues", found);
            } catch (Exception e) {
                ctx.check("SQS ListDeadLetterSourceQueues", false, e);
            }

            // 20. StartMessageMoveTask
            try {
                // DLQ to Source
                StartMessageMoveTaskResponse moveResp = sqs.startMessageMoveTask(b -> b.sourceArn(sqs.getQueueAttributes(a -> a.queueUrl(finalDlqUrl).attributeNames(QueueAttributeName.QUEUE_ARN)).attributes().get(QueueAttributeName.QUEUE_ARN)));
                ctx.check("SQS StartMessageMoveTask", moveResp.taskHandle() != null);

                ListMessageMoveTasksResponse listMoves = sqs.listMessageMoveTasks(b -> b.sourceArn(sqs.getQueueAttributes(a -> a.queueUrl(finalDlqUrl).attributeNames(QueueAttributeName.QUEUE_ARN)).attributes().get(QueueAttributeName.QUEUE_ARN)));
                ctx.check("SQS ListMessageMoveTasks", listMoves.results() != null);
            } catch (Exception e) {
                ctx.check("SQS MessageMoveTask", false, e);
            }

            // Cleanup queues
            try {
                sqs.deleteQueue(DeleteQueueRequest.builder()
                        .queueUrl(qUrl)
                        .build());
                if (finalDlqUrl != null) sqs.deleteQueue(b -> b.queueUrl(finalDlqUrl));
                ctx.check("SQS DeleteQueue", true);
            } catch (Exception e) {
                ctx.check("SQS DeleteQueue", false, e);
            }
        }
    }
}
