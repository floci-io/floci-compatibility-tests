package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.*;
import software.amazon.awssdk.services.lambda.model.Runtime;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Duration;

/**
 * Tests SQS → Lambda event source mapping (ESM).
 *
 * <p>Flow:
 * <ol>
 *   <li>Create SQS queue (trigger source).
 *   <li>Create Lambda function.
 *   <li>Create ESM: queue ARN → function.
 *   <li>CRUD checks (Get, List, Update, Disable).
 *   <li>Send messages → wait for poller → verify queue drained (messages deleted on success).
 *   <li>Batch delivery: send 5 messages with batchSize=5 → single Lambda invocation drains all.
 *   <li>Disabled ESM: disable → send message → verify message stays in queue.
 *   <li>Delete ESM + cleanup.
 * </ol>
 */
@FlociTestGroup
public class SqsLambdaEsmTests implements TestGroup {

    private static final String ACCOUNT_ID = "000000000000";
    private static final int POLL_WAIT_MS = 4_000; // poll interval is 1 s; 4 s is enough headroom

    @Override
    public String name() {
        return "sqs-lambda-esm";
    }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- SQS → Lambda Event Source Mapping Tests ---");
        System.out.println("  NOTE: Lambda cold-start on first ESM trigger can take ~30s+ (Node.js bootstrap).");

        String queueName = "esm-trigger-queue";
        String functionName = "esm-test-fn";
        String queueUrl = ctx.endpoint + "/" + ACCOUNT_ID + "/" + queueName;
        String queueArn = "arn:aws:sqs:us-east-1:" + ACCOUNT_ID + ":" + queueName;

        try (SqsClient sqs = SqsClient.builder()
                .endpointOverride(ctx.endpoint).region(ctx.region)
                .credentialsProvider(ctx.credentials).build();
             LambdaClient lambda = LambdaClient.builder()
                     .endpointOverride(ctx.endpoint).region(ctx.region)
                     .credentialsProvider(ctx.credentials)
                     .overrideConfiguration(ClientOverrideConfiguration.builder()
                             .apiCallTimeout(Duration.ofMinutes(5))
                             .apiCallAttemptTimeout(Duration.ofMinutes(5))
                             .build())
                     .build()) {

            // ── Setup ───────────────────────────────────────────────────────

            // Create SQS queue
            try {
                sqs.createQueue(CreateQueueRequest.builder().queueName(queueName).build());
                ctx.check("ESM: CreateQueue", true);
            } catch (Exception e) {
                ctx.check("ESM: CreateQueue", false, e);
                return;
            }

            // Create Lambda function (minimal handler — just needs to succeed)
            try {
                lambda.createFunction(CreateFunctionRequest.builder()
                        .functionName(functionName)
                        .runtime(Runtime.NODEJS20_X)
                        .role("arn:aws:iam::000000000000:role/lambda-role")
                        .handler("index.handler").timeout(120).memorySize(256)
                        .code(FunctionCode.builder()
                                .zipFile(SdkBytes.fromByteArray(LambdaUtils.minimalZip()))
                                .build())
                        .build());
                ctx.check("ESM: CreateFunction", true);
            } catch (Exception e) {
                ctx.check("ESM: CreateFunction", false, e);
                return;
            }

            // ── CreateEventSourceMapping ────────────────────────────────────

            String esmUuid;
            try {
                CreateEventSourceMappingResponse esm = lambda.createEventSourceMapping(
                        CreateEventSourceMappingRequest.builder()
                                .functionName(functionName)
                                .eventSourceArn(queueArn)
                                .batchSize(1)
                                .build());
                esmUuid = esm.uuid();
                ctx.check("ESM: CreateEventSourceMapping",
                        esmUuid != null
                                && queueArn.equals(esm.eventSourceArn())
                                && esm.functionArn() != null && esm.functionArn().contains(functionName)
                                && "Enabled".equals(esm.state()));
            } catch (Exception e) {
                ctx.check("ESM: CreateEventSourceMapping", false, e);
                return;
            }

            // ── GetEventSourceMapping ───────────────────────────────────────

            try {
                GetEventSourceMappingResponse get = lambda.getEventSourceMapping(
                        GetEventSourceMappingRequest.builder().uuid(esmUuid).build());
                ctx.check("ESM: GetEventSourceMapping",
                        esmUuid.equals(get.uuid())
                                && queueArn.equals(get.eventSourceArn())
                                && get.batchSize() == 1);
            } catch (Exception e) {
                ctx.check("ESM: GetEventSourceMapping", false, e);
            }

            // ── ListEventSourceMappings ─────────────────────────────────────

            try {
                ListEventSourceMappingsResponse list = lambda.listEventSourceMappings(
                        ListEventSourceMappingsRequest.builder()
                                .functionName(functionName).build());
                boolean found = list.eventSourceMappings().stream()
                        .anyMatch(e -> esmUuid.equals(e.uuid()));
                ctx.check("ESM: ListEventSourceMappings", found);
            } catch (Exception e) {
                ctx.check("ESM: ListEventSourceMappings", false, e);
            }

            // ── Trigger: send 1 message, wait, verify queue is empty ────────

            try {
                System.out.println("  (cold start — sending trigger message, polling up to 120s...)");
                sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(queueUrl).messageBody("{\"test\":\"hello-esm\"}").build());

                // Use GetQueueAttributes (not ReceiveMessage) so the poller can still see the message.
                // Lambda deleted the message when both visible + in-flight counts reach 0.
                boolean consumed = false;
                long deadline = System.currentTimeMillis() + 120_000;
                while (System.currentTimeMillis() < deadline) {
                    GetQueueAttributesResponse attrs = sqs.getQueueAttributes(
                            GetQueueAttributesRequest.builder()
                                    .queueUrl(queueUrl)
                                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                            QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                                    .build());
                    int visible = Integer.parseInt(attrs.attributes()
                            .getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0"));
                    int inFlight = Integer.parseInt(attrs.attributes()
                            .getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0"));
                    if (visible == 0 && inFlight == 0) {
                        consumed = true;
                        break;
                    }
                    Thread.sleep(1_000);
                }
                ctx.check("ESM: message consumed by Lambda (queue empty)", consumed);
            } catch (Exception e) {
                ctx.check("ESM: message consumed by Lambda (queue empty)", false, e);
            }

            // ── Batch: update batchSize=5, send 5 messages, verify drained ─

            try {
                lambda.updateEventSourceMapping(UpdateEventSourceMappingRequest.builder()
                        .uuid(esmUuid).batchSize(5).build());
                ctx.check("ESM: UpdateEventSourceMapping (batchSize=5)", true);
            } catch (Exception e) {
                ctx.check("ESM: UpdateEventSourceMapping (batchSize=5)", false, e);
            }

            try {
                for (int i = 1; i <= 5; i++) {
                    sqs.sendMessage(SendMessageRequest.builder()
                            .queueUrl(queueUrl).messageBody("{\"batch\":" + i + "}").build());
                }
                System.out.println("  (batch: sent 5 messages, polling up to 120s for Lambda cold start...)");
                boolean batchEmpty = false;
                long deadline = System.currentTimeMillis() + 120_000;
                while (System.currentTimeMillis() < deadline) {
                    GetQueueAttributesResponse attrs = sqs.getQueueAttributes(
                            GetQueueAttributesRequest.builder()
                                    .queueUrl(queueUrl)
                                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                            QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                                    .build());
                    int visible = Integer.parseInt(attrs.attributes()
                            .getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0"));
                    int inFlight = Integer.parseInt(attrs.attributes()
                            .getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0"));
                    if (visible == 0 && inFlight == 0) {
                        batchEmpty = true;
                        break;
                    }
                    Thread.sleep(1_000);
                }
                ctx.check("ESM: 5 batch messages consumed by Lambda", batchEmpty);
            } catch (Exception e) {
                ctx.check("ESM: 5 batch messages consumed by Lambda", false, e);
            }

            // ── Disable ESM: message should stay in queue ───────────────────

            try {
                lambda.updateEventSourceMapping(UpdateEventSourceMappingRequest.builder()
                        .uuid(esmUuid).enabled(false).build());

                GetEventSourceMappingResponse disabled = lambda.getEventSourceMapping(
                        GetEventSourceMappingRequest.builder().uuid(esmUuid).build());
                ctx.check("ESM: Disable (state=Disabled)",
                        "Disabled".equals(disabled.state()));
            } catch (Exception e) {
                ctx.check("ESM: Disable (state=Disabled)", false, e);
            }

            try {
                sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(queueUrl).messageBody("{\"test\":\"should-stay\"}").build());

                Thread.sleep(POLL_WAIT_MS);

                ReceiveMessageResponse peek = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl).maxNumberOfMessages(1).waitTimeSeconds(1).build());
                ctx.check("ESM: disabled — message not consumed (stays in queue)",
                        !peek.messages().isEmpty());

                // clean up the leftover message
                if (!peek.messages().isEmpty()) {
                    sqs.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(peek.messages().get(0).receiptHandle())
                            .build());
                }
            } catch (Exception e) {
                ctx.check("ESM: disabled — message not consumed (stays in queue)", false, e);
            }

            // ── Error case: non-existent function ──────────────────────────

            try {
                lambda.createEventSourceMapping(CreateEventSourceMappingRequest.builder()
                        .functionName("no-such-function")
                        .eventSourceArn(queueArn)
                        .build());
                ctx.check("ESM: non-existent function → error", false);
            } catch (software.amazon.awssdk.services.lambda.model.ResourceNotFoundException e) {
                ctx.check("ESM: non-existent function → error", true);
            } catch (Exception e) {
                ctx.check("ESM: non-existent function → error", false, e);
            }

            // ── DeleteEventSourceMapping ────────────────────────────────────

            try {
                lambda.deleteEventSourceMapping(DeleteEventSourceMappingRequest.builder()
                        .uuid(esmUuid).build());
                ctx.check("ESM: DeleteEventSourceMapping", true);
            } catch (Exception e) {
                ctx.check("ESM: DeleteEventSourceMapping", false, e);
            }

            // ── Cleanup ─────────────────────────────────────────────────────

            try {
                lambda.deleteFunction(DeleteFunctionRequest.builder()
                        .functionName(functionName).build());
            } catch (Exception ignore) {
            }
            try {
                sqs.deleteQueue(DeleteQueueRequest.builder()
                        .queueUrl(queueUrl).build());
            } catch (Exception ignore) {
            }
        }
    }
}
