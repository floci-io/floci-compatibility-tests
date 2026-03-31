package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.*;
import software.amazon.awssdk.services.lambda.model.Runtime;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Invokes Lambda functions directly via raw HTTP (no AWS SDK client),
 * posting to the Lambda Invoke REST endpoint:
 * <pre>POST /2015-03-31/functions/{name}/invocations</pre>
 */
@FlociTestGroup
public class LambdaHttpTests implements TestGroup {

    @Override
    public String name() { return "lambda-http"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- Lambda Direct HTTP Invocation Tests ---");
        System.out.println("  NOTE: first invocation may cold-start a Docker container.");

        String functionName = "sdk-http-invoke-fn";
        String role = "arn:aws:iam::000000000000:role/lambda-role";

        // Create the function via SDK
        try (LambdaClient lambda = LambdaClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofMinutes(5))
                        .apiCallAttemptTimeout(Duration.ofMinutes(5))
                        .build())
                .build()) {

            try {
                lambda.createFunction(CreateFunctionRequest.builder()
                        .functionName(functionName)
                        .runtime(Runtime.NODEJS20_X)
                        .role(role)
                        .handler("index.handler")
                        .timeout(30)
                        .memorySize(256)
                        .code(FunctionCode.builder()
                                .zipFile(SdkBytes.fromByteArray(LambdaUtils.handlerZip()))
                                .build())
                        .build());
                ctx.check("Lambda HTTP: CreateFunction", true);
            } catch (Exception e) {
                ctx.check("Lambda HTTP: CreateFunction", false, e);
                return;
            }

            HttpClient http = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();

            String invokeUrl = ctx.endpoint + "/2015-03-31/functions/" + functionName + "/invocations";

            // 1. RequestResponse via raw HTTP
            try {
                System.out.println("  (cold start — waiting for container...)");
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(invokeUrl))
                        .header("Content-Type", "application/json")
                        .header("X-Amz-Invocation-Type", "RequestResponse")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"HTTP\"}"))
                        .timeout(Duration.ofMinutes(5))
                        .build();

                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                System.out.println("  Response status : " + resp.statusCode());
                System.out.println("  Response body   : " + resp.body());

                ctx.check("Lambda HTTP: RequestResponse status 200", resp.statusCode() == 200);
                ctx.check("Lambda HTTP: RequestResponse body contains greeting",
                        resp.body().contains("Hello, HTTP!"));
            } catch (Exception e) {
                ctx.check("Lambda HTTP: RequestResponse status 200", false, e);
                ctx.check("Lambda HTTP: RequestResponse body contains greeting", false, e);
            }

            // 2. Second invocation (warm) — verify reuse
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(invokeUrl))
                        .header("Content-Type", "application/json")
                        .header("X-Amz-Invocation-Type", "RequestResponse")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"Warm\"}"))
                        .timeout(Duration.ofMinutes(5))
                        .build();

                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                System.out.println("  Warm body       : " + resp.body());

                ctx.check("Lambda HTTP: warm invocation", resp.statusCode() == 200
                        && resp.body().contains("Hello, Warm!"));
            } catch (Exception e) {
                ctx.check("Lambda HTTP: warm invocation", false, e);
            }

            // 3. DryRun via raw HTTP (X-Amz-Invocation-Type: DryRun → 204)
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(invokeUrl))
                        .header("Content-Type", "application/json")
                        .header("X-Amz-Invocation-Type", "DryRun")
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .timeout(Duration.ofSeconds(30))
                        .build();

                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                ctx.check("Lambda HTTP: DryRun status 204", resp.statusCode() == 204);
            } catch (Exception e) {
                ctx.check("Lambda HTTP: DryRun status 204", false, e);
            }

            // 4. Event (async) via raw HTTP (X-Amz-Invocation-Type: Event → 202)
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(invokeUrl))
                        .header("Content-Type", "application/json")
                        .header("X-Amz-Invocation-Type", "Event")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"Async\"}"))
                        .timeout(Duration.ofSeconds(30))
                        .build();

                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                ctx.check("Lambda HTTP: Event (async) status 202", resp.statusCode() == 202);
            } catch (Exception e) {
                ctx.check("Lambda HTTP: Event (async) status 202", false, e);
            }

            // 5. Non-existent function → 404
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(ctx.endpoint + "/2015-03-31/functions/no-such-fn/invocations"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .timeout(Duration.ofSeconds(10))
                        .build();

                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                ctx.check("Lambda HTTP: non-existent function → 404", resp.statusCode() == 404);
            } catch (Exception e) {
                ctx.check("Lambda HTTP: non-existent function → 404", false, e);
            }

            // Cleanup
            try {
                lambda.deleteFunction(DeleteFunctionRequest.builder()
                        .functionName(functionName).build());
                ctx.check("Lambda HTTP: DeleteFunction", true);
            } catch (Exception e) {
                ctx.check("Lambda HTTP: DeleteFunction", false, e);
            }
        }
    }
}
