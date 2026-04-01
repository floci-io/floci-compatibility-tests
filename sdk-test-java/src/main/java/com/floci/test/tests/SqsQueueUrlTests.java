package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

/**
 * Tests SQS JSON 1.0 requests routed directly to the queue URL path (/{accountId}/{queueName}).
 *
 * Ruby SDK aws-sdk-sqs >= 1.71 (and similar SDKs) POST to the queue URL path instead of
 * the global POST /. Without a dedicated handler, these requests fall through to S3Controller
 * which returns a NoSuchBucket XML error.
 *
 * Issue: https://github.com/hectorvent/floci/issues/17
 */
@FlociTestGroup
public class SqsQueueUrlTests implements TestGroup {

    @Override
    public String name() { return "sqs-queue-url"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- SQS Queue URL Path Tests ---");
        System.out.println("  NOTE: Simulates Ruby SDK >= 1.71 (POST /{accountId}/{queueName} with JSON 1.0).");

        try (SqsClient sqs = SqsClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            // Setup: create queue via normal SDK
            String queueUrl;
            try {
                queueUrl = sqs.createQueue(b -> b.queueName("sdk-qurl-test")).queueUrl();
                ctx.check("SQS QueueUrl CreateQueue", queueUrl != null);
            } catch (Exception e) {
                ctx.check("SQS QueueUrl CreateQueue", false, e);
                return;
            }

            String queuePath = URI.create(queueUrl).getPath();

            // SendMessage via queue URL path
            try {
                String resp = postJson(ctx.endpoint, queuePath, "AmazonSQS.SendMessage",
                        "{\"QueueUrl\":\"" + queueUrl + "\",\"MessageBody\":\"hello-from-queue-url-path\"}");
                ctx.check("SQS QueueUrl SendMessage", resp.contains("MessageId"));
            } catch (Exception e) {
                ctx.check("SQS QueueUrl SendMessage", false, e);
            }

            // Verify message body via normal SDK
            try {
                ReceiveMessageResponse rcv = sqs.receiveMessage(b -> b
                        .queueUrl(queueUrl).maxNumberOfMessages(1));
                boolean ok = !rcv.messages().isEmpty()
                        && "hello-from-queue-url-path".equals(rcv.messages().get(0).body());
                ctx.check("SQS QueueUrl message body correct", ok);
                if (!rcv.messages().isEmpty()) {
                    sqs.deleteMessage(b -> b.queueUrl(queueUrl)
                            .receiptHandle(rcv.messages().get(0).receiptHandle()));
                }
            } catch (Exception e) {
                ctx.check("SQS QueueUrl message body correct", false, e);
            }

            // GetQueueAttributes via queue URL path
            try {
                String resp = postJson(ctx.endpoint, queuePath, "AmazonSQS.GetQueueAttributes",
                        "{\"QueueUrl\":\"" + queueUrl + "\",\"AttributeNames\":[\"All\"]}");
                ctx.check("SQS QueueUrl GetQueueAttributes", resp.contains("Attributes"));
            } catch (Exception e) {
                ctx.check("SQS QueueUrl GetQueueAttributes", false, e);
            }

            // ReceiveMessage via queue URL path (after sending a message via SDK)
            String receiptHandle = null;
            try {
                sqs.sendMessage(b -> b.queueUrl(queueUrl).messageBody("msg-for-receive-via-path"));
                String resp = postJson(ctx.endpoint, queuePath, "AmazonSQS.ReceiveMessage",
                        "{\"QueueUrl\":\"" + queueUrl + "\",\"MaxNumberOfMessages\":1}");
                boolean ok = resp.contains("msg-for-receive-via-path");
                ctx.check("SQS QueueUrl ReceiveMessage", ok);
                receiptHandle = extractField(resp, "ReceiptHandle");
            } catch (Exception e) {
                ctx.check("SQS QueueUrl ReceiveMessage", false, e);
            }

            // DeleteMessage via queue URL path
            if (receiptHandle != null) {
                final String rh = receiptHandle;
                try {
                    postJson(ctx.endpoint, queuePath, "AmazonSQS.DeleteMessage",
                            "{\"QueueUrl\":\"" + queueUrl + "\",\"ReceiptHandle\":\"" + rh + "\"}");
                    ctx.check("SQS QueueUrl DeleteMessage", true);
                } catch (Exception e) {
                    ctx.check("SQS QueueUrl DeleteMessage", false, e);
                }
            }

            // FIFO queue via queue URL path
            String fifoUrl = null;
            try {
                fifoUrl = sqs.createQueue(b -> b.queueName("sdk-qurl-fifo.fifo")
                        .attributes(Map.of(
                                QueueAttributeName.FIFO_QUEUE, "true",
                                QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "true")))
                        .queueUrl();
                ctx.check("SQS QueueUrl FIFO CreateQueue", fifoUrl != null);
            } catch (Exception e) {
                ctx.check("SQS QueueUrl FIFO CreateQueue", false, e);
            }

            if (fifoUrl != null) {
                String fifoPath = URI.create(fifoUrl).getPath();
                try {
                    String resp = postJson(ctx.endpoint, fifoPath, "AmazonSQS.SendMessage",
                            "{\"QueueUrl\":\"" + fifoUrl + "\",\"MessageBody\":\"fifo-msg\",\"MessageGroupId\":\"g1\"}");
                    ctx.check("SQS QueueUrl FIFO SendMessage", resp.contains("MessageId"));
                } catch (Exception e) {
                    ctx.check("SQS QueueUrl FIFO SendMessage", false, e);
                }

                final String fu = fifoUrl;
                try {
                    sqs.deleteQueue(b -> b.queueUrl(fu));
                } catch (Exception ignored) {}
            }

            // Cleanup
            try {
                sqs.deleteQueue(b -> b.queueUrl(queueUrl));
                ctx.check("SQS QueueUrl DeleteQueue", true);
            } catch (Exception e) {
                ctx.check("SQS QueueUrl DeleteQueue", false, e);
            }
        }
    }

    private String postJson(URI endpoint, String path, String target, String body) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(endpoint.toString() + path))
                .header("Content-Type", "application/x-amz-json-1.0")
                .header("X-Amz-Target", target)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            throw new RuntimeException("HTTP " + resp.statusCode() + ": " + resp.body());
        }
        return resp.body();
    }

    private String extractField(String json, String field) {
        String key = "\"" + field + "\":\"";
        int idx = json.indexOf(key);
        if (idx < 0) { return null; }
        int start = idx + key.length();
        int end = json.indexOf("\"", start);
        return end < 0 ? null : json.substring(start, end);
    }
}