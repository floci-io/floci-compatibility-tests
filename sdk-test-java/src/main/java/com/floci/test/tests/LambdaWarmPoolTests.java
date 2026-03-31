package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.*;
import software.amazon.awssdk.services.lambda.model.Runtime;

import java.time.Duration;

@FlociTestGroup
public class LambdaWarmPoolTests implements TestGroup {

    @Override
    public String name() { return "lambda-warmpool"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- Lambda Warm Pool Load Test (110 invocations) ---");
        System.out.println("  NOTE: first invocation cold-starts the container; the rest should reuse it.");

        try (LambdaClient lambda = LambdaClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofMinutes(5))
                        .apiCallAttemptTimeout(Duration.ofMinutes(5))
                        .build())
                .build()) {

            String functionName = "warm-pool-load-fn";
            String role = "arn:aws:iam::000000000000:role/lambda-role";
            int totalCalls = 110;

            // Create function
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
                ctx.check("WarmPool: CreateFunction", true);
            } catch (Exception e) {
                ctx.check("WarmPool: CreateFunction", false, e);
                return;
            }

            long[] latencies = new long[totalCalls];
            int successCount = 0;
            int failCount = 0;
            long coldStartMs = -1;

            for (int i = 0; i < totalCalls; i++) {
                boolean isFirst = i == 0;
                if (isFirst) System.out.println("  [1/" + totalCalls + "] Cold start — waiting for container...");
                else if (i % 10 == 0) System.out.println("  [" + (i + 1) + "/" + totalCalls + "] invoking...");

                long start = System.currentTimeMillis();
                try {
                    InvokeResponse resp = lambda.invoke(InvokeRequest.builder()
                            .functionName(functionName)
                            .invocationType(InvocationType.REQUEST_RESPONSE)
                            .payload(SdkBytes.fromUtf8String("{\"name\":\"call-" + i + "\"}"))
                            .build());

                    long elapsed = System.currentTimeMillis() - start;
                    latencies[i] = elapsed;

                    boolean ok = resp.statusCode() == 200
                            && resp.functionError() == null
                            && resp.payload().asUtf8String().contains("Hello, call-" + i + "!");
                    if (ok) successCount++;
                    else {
                        failCount++;
                        if (i < 3 || i == totalCalls - 1) {
                            System.out.println("    FAIL [" + i + "]: status=" + resp.statusCode()
                                    + " err=" + resp.functionError()
                                    + " payload=" + resp.payload().asUtf8String());
                        }
                    }
                    if (isFirst) coldStartMs = elapsed;
                } catch (Exception e) {
                    latencies[i] = System.currentTimeMillis() - start;
                    failCount++;
                    if (i < 3) System.out.println("    EXCEPTION [" + i + "]: " + e.getMessage());
                }
            }

            // Statistics
            long warmTotal = 0;
            long warmMin = Long.MAX_VALUE;
            long warmMax = 0;
            int warmCount = totalCalls - 1;
            for (int i = 1; i < totalCalls; i++) {
                warmTotal += latencies[i];
                if (latencies[i] < warmMin) warmMin = latencies[i];
                if (latencies[i] > warmMax) warmMax = latencies[i];
            }
            long warmAvg = warmCount > 0 ? warmTotal / warmCount : 0;

            System.out.println("\n  === Warm Pool Load Test Results ===");
            System.out.println("  Total calls    : " + totalCalls);
            System.out.println("  Succeeded      : " + successCount);
            System.out.println("  Failed         : " + failCount);
            System.out.println("  Cold start     : " + coldStartMs + " ms");
            System.out.println("  Warm avg       : " + warmAvg + " ms");
            System.out.println("  Warm min       : " + warmMin + " ms");
            System.out.println("  Warm max       : " + warmMax + " ms");

            ctx.check("WarmPool: " + totalCalls + " invocations succeeded", failCount == 0);

            // Cleanup
            try {
                lambda.deleteFunction(DeleteFunctionRequest.builder()
                        .functionName(functionName).build());
                ctx.check("WarmPool: DeleteFunction", true);
            } catch (Exception e) {
                ctx.check("WarmPool: DeleteFunction", false, e);
            }
        }
    }
}
