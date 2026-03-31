package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.List;

@FlociTestGroup
public class EventBridgeTests implements TestGroup {

    @Override
    public String name() { return "eventbridge"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- EventBridge Tests ---");

        try (EventBridgeClient eb = EventBridgeClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build();
             SqsClient sqs = SqsClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            // 1. CreateEventBus
            String busArn = null;
            try {
                CreateEventBusResponse resp = eb.createEventBus(CreateEventBusRequest.builder()
                        .name("test-bus")
                        .build());
                busArn = resp.eventBusArn();
                ctx.check("EB CreateEventBus", busArn != null && busArn.contains("test-bus"));
            } catch (Exception e) {
                ctx.check("EB CreateEventBus", false, e);
            }

            // 2. DescribeEventBus
            try {
                DescribeEventBusResponse resp = eb.describeEventBus(DescribeEventBusRequest.builder()
                        .name("test-bus")
                        .build());
                ctx.check("EB DescribeEventBus", resp.arn() != null && resp.arn().contains("test-bus"));
            } catch (Exception e) {
                ctx.check("EB DescribeEventBus", false, e);
            }

            // 3. ListEventBuses — both default and test-bus present
            try {
                ListEventBusesResponse resp = eb.listEventBuses(ListEventBusesRequest.builder().build());
                boolean hasDefault = resp.eventBuses().stream().anyMatch(b -> "default".equals(b.name()));
                boolean hasTestBus = resp.eventBuses().stream().anyMatch(b -> "test-bus".equals(b.name()));
                ctx.check("EB ListEventBuses", hasDefault && hasTestBus);
            } catch (Exception e) {
                ctx.check("EB ListEventBuses", false, e);
            }

            // 4. PutRule
            String ruleArn = null;
            try {
                PutRuleResponse resp = eb.putRule(PutRuleRequest.builder()
                        .name("test-rule")
                        .eventBusName("test-bus")
                        .eventPattern("{\"source\":[\"com.myapp\"]}")
                        .state(RuleState.ENABLED)
                        .build());
                ruleArn = resp.ruleArn();
                ctx.check("EB PutRule", ruleArn != null && ruleArn.contains("test-rule"));
            } catch (Exception e) {
                ctx.check("EB PutRule", false, e);
            }

            // 5. Create SQS queue for target
            String queueUrl = null;
            String queueArn = null;
            try {
                queueUrl = sqs.createQueue(CreateQueueRequest.builder()
                        .queueName("eb-target-queue")
                        .build()).queueUrl();
                queueArn = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                        .queueUrl(queueUrl)
                        .attributeNamesWithStrings("QueueArn")
                        .build()).attributesAsStrings().get("QueueArn");
                ctx.check("EB SQS queue created", queueUrl != null && queueArn != null);
            } catch (Exception e) {
                ctx.check("EB SQS queue created", false, e);
            }

            // 6. PutTargets
            try {
                PutTargetsResponse resp = eb.putTargets(PutTargetsRequest.builder()
                        .rule("test-rule")
                        .eventBusName("test-bus")
                        .targets(software.amazon.awssdk.services.eventbridge.model.Target.builder()
                                .id("sqs-target-1")
                                .arn(queueArn)
                                .build())
                        .build());
                ctx.check("EB PutTargets", resp.failedEntryCount() == 0);
            } catch (Exception e) {
                ctx.check("EB PutTargets", false, e);
            }

            // 7. ListTargetsByRule
            try {
                ListTargetsByRuleResponse resp = eb.listTargetsByRule(ListTargetsByRuleRequest.builder()
                        .rule("test-rule")
                        .eventBusName("test-bus")
                        .build());
                ctx.check("EB ListTargetsByRule", resp.targets().size() == 1
                        && "sqs-target-1".equals(resp.targets().get(0).id()));
            } catch (Exception e) {
                ctx.check("EB ListTargetsByRule", false, e);
            }

            // 8. PutEvents — matching event
            try {
                PutEventsResponse resp = eb.putEvents(PutEventsRequest.builder()
                        .entries(PutEventsRequestEntry.builder()
                                .eventBusName("test-bus")
                                .source("com.myapp")
                                .detailType("MyEvent")
                                .detail("{\"key\":\"value\"}")
                                .build())
                        .build());
                ctx.check("EB PutEvents no failure", resp.failedEntryCount() == 0);
            } catch (Exception e) {
                ctx.check("EB PutEvents no failure", false, e);
            }

            // 9. SQS ReceiveMessage — event should arrive
            try {
                // Short sleep to let async routing complete
                Thread.sleep(500);
                ReceiveMessageResponse recv = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(2)
                        .build());
                ctx.check("EB SQS received event", !recv.messages().isEmpty());
                // Delete received messages
                final String fQueueUrl1 = queueUrl;
                for (var msg : recv.messages()) {
                    sqs.deleteMessage(b -> b.queueUrl(fQueueUrl1).receiptHandle(msg.receiptHandle()));
                }
            } catch (Exception e) {
                ctx.check("EB SQS received event", false, e);
            }

            // 10. DisableRule — PutEvents should NOT deliver
            try {
                eb.disableRule(DisableRuleRequest.builder()
                        .name("test-rule")
                        .eventBusName("test-bus")
                        .build());
                eb.putEvents(PutEventsRequest.builder()
                        .entries(PutEventsRequestEntry.builder()
                                .eventBusName("test-bus")
                                .source("com.myapp")
                                .detailType("MyEvent")
                                .detail("{\"key\":\"value\"}")
                                .build())
                        .build());
                Thread.sleep(300);
                ReceiveMessageResponse recv = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(1)
                        .build());
                ctx.check("EB DisableRule no delivery", recv.messages().isEmpty());
            } catch (Exception e) {
                ctx.check("EB DisableRule no delivery", false, e);
            }

            // 11. EnableRule — PutEvents should deliver again
            try {
                eb.enableRule(EnableRuleRequest.builder()
                        .name("test-rule")
                        .eventBusName("test-bus")
                        .build());
                eb.putEvents(PutEventsRequest.builder()
                        .entries(PutEventsRequestEntry.builder()
                                .eventBusName("test-bus")
                                .source("com.myapp")
                                .detailType("MyEvent")
                                .detail("{\"key\":\"value\"}")
                                .build())
                        .build());
                Thread.sleep(500);
                ReceiveMessageResponse recv = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(2)
                        .build());
                ctx.check("EB EnableRule delivery", !recv.messages().isEmpty());
                final String fQueueUrl2 = queueUrl;
                for (var msg : recv.messages()) {
                    sqs.deleteMessage(b -> b.queueUrl(fQueueUrl2).receiptHandle(msg.receiptHandle()));
                }
            } catch (Exception e) {
                ctx.check("EB EnableRule delivery", false, e);
            }

            // 12. RemoveTargets
            try {
                RemoveTargetsResponse resp = eb.removeTargets(RemoveTargetsRequest.builder()
                        .rule("test-rule")
                        .eventBusName("test-bus")
                        .ids("sqs-target-1")
                        .build());
                ctx.check("EB RemoveTargets", resp.failedEntryCount() == 0);
            } catch (Exception e) {
                ctx.check("EB RemoveTargets", false, e);
            }

            // 13. DeleteRule
            try {
                eb.deleteRule(DeleteRuleRequest.builder()
                        .name("test-rule")
                        .eventBusName("test-bus")
                        .build());
                ctx.check("EB DeleteRule", true);
            } catch (Exception e) {
                ctx.check("EB DeleteRule", false, e);
            }

            // 14. DeleteEventBus
            try {
                eb.deleteEventBus(DeleteEventBusRequest.builder()
                        .name("test-bus")
                        .build());
                ctx.check("EB DeleteEventBus", true);
            } catch (Exception e) {
                ctx.check("EB DeleteEventBus", false, e);
            }

            // Cleanup SQS queue
            if (queueUrl != null) {
                try {
                    sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
                } catch (Exception ignored) {}
            }
        }
    }
}
