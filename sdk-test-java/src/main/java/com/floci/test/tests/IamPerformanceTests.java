package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Performance and concurrency tests for the IAM service.
 *
 * <p>Measures:
 * <ul>
 *   <li>Throughput: concurrent CreateUser calls (ops/sec)</li>
 *   <li>Latency: p50/p95/p99 for GetUser</li>
 *   <li>Concurrent correctness: simultaneous writes with no data corruption</li>
 * </ul>
 *
 * <p>Run with: {@code FLOCI_TESTS=iam-perf mvn exec:java}
 */
@FlociTestGroup
public class IamPerformanceTests implements TestGroup {

    private static final int CONCURRENT_USERS = 20;
    private static final int LATENCY_SAMPLES = 100;

    @Override
    public String name() { return "iam-perf"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- IAM Performance Tests ---");

        try (IamClient iam = IamClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            runThroughputTest(ctx, iam);
            runLatencyTest(ctx, iam);
            runConcurrencyCorrectnessTest(ctx, iam);
        }
    }

    /**
     * Creates {@code CONCURRENT_USERS} users in parallel and measures throughput (ops/sec).
     */
    private void runThroughputTest(TestContext ctx, IamClient iam) {
        int count = CONCURRENT_USERS;
        List<String> userNames = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            userNames.add("perf-user-throughput-" + i);
        }

        AtomicInteger success = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(count);
        ExecutorService pool = Executors.newFixedThreadPool(10);

        long start = System.currentTimeMillis();

        for (String userName : userNames) {
            pool.submit(() -> {
                try {
                    iam.createUser(CreateUserRequest.builder().userName(userName).build());
                    success.incrementAndGet();
                } catch (Exception ignored) {
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        pool.shutdown();

        long durationMs = System.currentTimeMillis() - start;
        double opsPerSec = success.get() * 1000.0 / Math.max(1, durationMs);

        System.out.printf("  IAM Throughput: %d/%d users created in %dms (%.1f ops/sec)%n",
                success.get(), count, durationMs, opsPerSec);
        ctx.check("IAM Throughput (" + count + " concurrent creates)",
                success.get() == count);

        // Cleanup
        for (String userName : userNames) {
            try { iam.deleteUser(DeleteUserRequest.builder().userName(userName).build()); }
            catch (Exception ignored) {}
        }
    }

    /**
     * Measures GetUser latency (p50, p95, p99) over {@code LATENCY_SAMPLES} sequential calls.
     */
    private void runLatencyTest(TestContext ctx, IamClient iam) {
        String userName = "perf-user-latency";
        try {
            iam.createUser(CreateUserRequest.builder().userName(userName).build());
        } catch (Exception ignored) {}

        long[] latencies = new long[LATENCY_SAMPLES];
        int failures = 0;

        for (int i = 0; i < LATENCY_SAMPLES; i++) {
            long t = System.currentTimeMillis();
            try {
                iam.getUser(GetUserRequest.builder().userName(userName).build());
            } catch (Exception e) {
                failures++;
            }
            latencies[i] = System.currentTimeMillis() - t;
        }

        java.util.Arrays.sort(latencies);
        long p50 = latencies[LATENCY_SAMPLES / 2];
        long p95 = latencies[(int) (LATENCY_SAMPLES * 0.95)];
        long p99 = latencies[(int) (LATENCY_SAMPLES * 0.99)];

        System.out.printf("  IAM GetUser Latency: p50=%dms  p95=%dms  p99=%dms  failures=%d%n",
                p50, p95, p99, failures);
        ctx.check("IAM Latency p99 < 500ms", p99 < 500 && failures == 0);

        try { iam.deleteUser(DeleteUserRequest.builder().userName(userName).build()); }
        catch (Exception ignored) {}
    }

    /**
     * Concurrently creates users, then verifies all were created with correct data (no corruption).
     */
    private void runConcurrencyCorrectnessTest(TestContext ctx, IamClient iam) {
        int count = 50;
        List<String> userNames = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            userNames.add("perf-concurrent-" + i);
        }

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(count);
        AtomicInteger created = new AtomicInteger(0);
        ExecutorService pool = Executors.newFixedThreadPool(20);

        for (String userName : userNames) {
            pool.submit(() -> {
                try {
                    startGate.await(); // all threads start simultaneously
                    iam.createUser(CreateUserRequest.builder().userName(userName).build());
                    created.incrementAndGet();
                } catch (Exception ignored) {
                } finally {
                    done.countDown();
                }
            });
        }

        startGate.countDown(); // release all threads

        try {
            done.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        pool.shutdown();

        // Verify all created users can be retrieved with correct names
        int verified = 0;
        for (String userName : userNames) {
            try {
                GetUserResponse resp = iam.getUser(GetUserRequest.builder().userName(userName).build());
                if (userName.equals(resp.user().userName())) {
                    verified++;
                }
            } catch (Exception ignored) {}
        }

        System.out.printf("  IAM Concurrency: %d created, %d verified correct%n", created.get(), verified);
        ctx.check("IAM Concurrent correctness (50 users)", created.get() == count && verified == count);

        // Cleanup
        for (String userName : userNames) {
            try { iam.deleteUser(DeleteUserRequest.builder().userName(userName).build()); }
            catch (Exception ignored) {}
        }
    }
}
