package com.floci.test;

import com.floci.test.tests.*;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Thin test runner for Floci SDK tests.
 *
 * <p>By default all test groups run in order. To run only specific groups, pass
 * their names as comma-separated CLI args or set the {@code FLOCI_TESTS} environment
 * variable:
 *
 * <pre>
 *   # Run everything
 *   java -cp ... com.floci.test.FlociTest
 *
 *   # Run only SQS and S3
 *   java -cp ... com.floci.test.FlociTest sqs,s3
 *
 *   # Same via env var
 *   FLOCI_TESTS=sqs,s3 java -cp ... com.floci.test.FlociTest
 * </pre>
 *
 * <p>Available group names:
 * <ul>
 *   <li>{@code sqs}
 *   <li>{@code s3}
 *   <li>{@code s3-object-lock}
 *   <li>{@code ssm}
 *   <li>{@code dynamodb}
 *   <li>{@code dynamodb-advanced}
 *   <li>{@code dynamodb-streams}
 *   <li>{@code lambda}
 *   <li>{@code lambda-invoke}
 *   <li>{@code lambda-warmpool}
 *   <li>{@code lambda-concurrent}
 *   <li>{@code apigateway}
 *   <li>{@code s3-notifications}
 *   <li>{@code iam}
 *   <li>{@code sts}
 *   <li>{@code iam-perf}
 *   <li>{@code elasticache}
 *   <li>{@code elasticache-mgmt}
 *   <li>{@code elasticache-lettuce}
 *   <li>{@code rds-mgmt}
 *   <li>{@code rds-cluster}
 *   <li>{@code rds-iam}
 *   <li>{@code eventbridge}
 *   <li>{@code cloudwatch-logs}
 *   <li>{@code cloudwatch-metrics}
 *   <li>{@code secretsmanager}
 *   <li>{@code sfn-jsonata}
 *   <li>{@code s3-large-object}
 *   <li>{@code s3-virtual-host}
 *   <li>{@code ses}
 * </ul>
 */
public class FlociTest {

    public static void main(String[] args) {
        System.out.println("=== Floci SDK Test (AWS SDK v2.31.8) ===\n");

        List<TestGroup> allGroups = List.of(
                new SsmTests(),
                new SqsTests(),
                new SqsLambdaEsmTests(),
                new SnsTests(),
                new S3Tests(),
                new S3ObjectLockTests(),
                new S3AdvancedTests(),
                new S3LargeObjectTests(),
                new S3VirtualHostTests(),
                new DynamoDbTests(),
                new DynamoDbAdvancedTests(),
                new DynamoDbLsiTests(),
                new DynamoDbStreamsTests(),
                new LambdaTests(),
                new LambdaInvokeTests(),
                new LambdaHttpTests(),
                new LambdaWarmPoolTests(),
                new LambdaConcurrentTests(),
                new ApiGatewayTests(),
                new ApiGatewayExecuteTests(),
                new ApiGatewayV2Tests(),
                new S3NotificationTests(),
                new IamTests(),
                new StsTests(),
                new IamPerformanceTests(),
                new ElastiCacheTests(),
                new ElastiCacheManagementTests(),
                new ElastiCacheLettuceTests(),
                new RdsManagementTests(),
                new RdsClusterTests(),
                new RdsIamTests(),
                new EventBridgeTests(),
                new KinesisTests(),
                new CloudWatchLogsTests(),
                new CloudWatchMetricsTests(),
                new SecretsManagerTests(),
                new KmsTests(),
                new CognitoTests(),
                new StepFunctionsTests(),
                new StepFunctionsJsonataTests(),
                new StepFunctionsDynamoDbTests(),
                new SesTests(),
                new ApiGatewayAwsIntegrationTests()
        );

        Set<String> enabled = resolveEnabled(args);
//        Set<String> enabled = Set.of("apigateway");
        if (enabled != null) {
            System.out.println("Running groups: " + enabled + "\n");
        }
        TestContext ctx = new TestContext();

        for (TestGroup group : allGroups) {
            if (enabled == null || enabled.contains(group.name())) {
                group.run(ctx);
                System.out.println();
            }
        }

        System.out.println("=== Results: " + ctx.getPassed() + " passed, " + ctx.getFailed() + " failed ===");
        if (ctx.getFailed() > 0) {
            System.exit(1);
        }
    }

    /**
     * Returns the set of enabled group names, or {@code null} to run all groups.
     * CLI args take precedence over the {@code FLOCI_TESTS} environment variable.
     */
    private static Set<String> resolveEnabled(String[] args) {
        // CLI args: each arg may be a single name or a comma-separated list
        if (args.length > 0) {
            Set<String> set = new LinkedHashSet<>();
            for (String arg : args) {
                for (String part : arg.split(",")) {
                    String trimmed = part.trim().toLowerCase();
                    if (!trimmed.isEmpty()) set.add(trimmed);
                }
            }
            if (!set.isEmpty()) return set;
        }

        // FLOCI_TESTS env var
        String env = System.getenv("FLOCI_TESTS");
        if (env != null && !env.isBlank()) {
            Set<String> set = new LinkedHashSet<>();
            for (String part : env.split(",")) {
                String trimmed = part.trim().toLowerCase();
                if (!trimmed.isEmpty()) set.add(trimmed);
            }
            if (!set.isEmpty()) return set;
        }

        return null; // run all
    }
}
