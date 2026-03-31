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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@FlociTestGroup
public class LambdaConcurrentTests implements TestGroup {

    @Override
    public String name() { return "lambda-concurrent"; }

    @Override
    public void run(TestContext ctx) {
        int threads = 10;
        int invocationsPerThread = 1000;
        int totalCalls = threads * invocationsPerThread;

        System.out.println("--- Lambda Concurrent Pool Test (" + totalCalls + " invocations / " + threads + " threads) ---");
        System.out.println("  Expect up to " + threads + " containers to cold-start then stay warm for the rest.");

        try (LambdaClient lambda = LambdaClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofMinutes(5))
                        .apiCallAttemptTimeout(Duration.ofMinutes(5))
                        .build())
                .build()) {

            String functionName = "concurrent-pool-fn";
            String role = "arn:aws:iam::000000000000:role/lambda-role";

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
                ctx.check("ConcurrentPool: CreateFunction", true);
            } catch (Exception e) {
                ctx.check("ConcurrentPool: CreateFunction", false, e);
                return;
            }

            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount   = new AtomicInteger(0);
            AtomicLong    totalLatency = new AtomicLong(0);
            AtomicLong    minLatency  = new AtomicLong(Long.MAX_VALUE);
            AtomicLong    maxLatency  = new AtomicLong(0);

            CountDownLatch startGate = new CountDownLatch(1);
            CountDownLatch done      = new CountDownLatch(threads);
            ExecutorService pool     = Executors.newFixedThreadPool(threads);

            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                pool.submit(() -> {
                    try {
                        startGate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int i = 0; i < invocationsPerThread; i++) {
                        long start = System.currentTimeMillis();
                        try {
                            InvokeResponse resp = lambda.invoke(InvokeRequest.builder()
                                    .functionName(functionName)
                                    .invocationType(InvocationType.REQUEST_RESPONSE)
                                    .payload(SdkBytes.fromUtf8String(
                                            "{\"name\":\"t" + threadId + "-i" + i + "\"}"))
                                    .build());
                            long elapsed = System.currentTimeMillis() - start;
                            totalLatency.addAndGet(elapsed);
                            minLatency.updateAndGet(m -> Math.min(m, elapsed));
                            maxLatency.updateAndGet(m -> Math.max(m, elapsed));

                            boolean ok = resp.statusCode() == 200 && resp.functionError() == null;
                            if (ok) successCount.incrementAndGet();
                            else    failCount.incrementAndGet();
                        } catch (Exception e) {
                            failCount.incrementAndGet();
                        }
                    }
                    done.countDown();
                });
            }

            long wallStart = System.currentTimeMillis();
            startGate.countDown();
            try {
                done.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            pool.shutdown();
            long wallMs = System.currentTimeMillis() - wallStart;

            long avg = (successCount.get() + failCount.get()) > 0
                    ? totalLatency.get() / (successCount.get() + failCount.get()) : 0;

            System.out.println("\n  === Concurrent Pool Results ===");
            System.out.println("  Threads        : " + threads);
            System.out.println("  Total calls    : " + totalCalls);
            System.out.println("  Succeeded      : " + successCount.get());
            System.out.println("  Failed         : " + failCount.get());
            System.out.println("  Wall time      : " + wallMs + " ms");
            System.out.println("  Throughput     : " + String.format("%.1f", totalCalls * 1000.0 / wallMs) + " req/s");
            System.out.println("  Latency avg    : " + avg + " ms");
            System.out.println("  Latency min    : " + minLatency.get() + " ms");
            System.out.println("  Latency max    : " + maxLatency.get() + " ms");
            System.out.println("  (check Docker Desktop for floci-" + functionName + "-* containers)");

            ctx.check("ConcurrentPool: " + totalCalls + " invocations succeeded", failCount.get() == 0);

            // Cleanup
            try {
                lambda.deleteFunction(DeleteFunctionRequest.builder()
                        .functionName(functionName).build());
                ctx.check("ConcurrentPool: DeleteFunction", true);
            } catch (Exception e) {
                ctx.check("ConcurrentPool: DeleteFunction", false, e);
            }
        }
    }
}
