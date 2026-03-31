package com.floci.test;

import org.reflections.Reflections;

import java.util.Comparator;
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
 * <p>Available group names are determined by the {@link TestGroup#name()} of every class
 * annotated with {@link FlociTestGroup}. See the {@code tests/} package for the full list.
 */
public class FlociTest {

    public static void main(String[] args) {
        System.out.println("=== Floci SDK Test (AWS SDK v2.31.8) ===\n");

        List<TestGroup> allGroups = new Reflections("com.floci.test.tests")
                .getTypesAnnotatedWith(FlociTestGroup.class)
                .stream()
                .map(c -> {
                    try {
                        return (TestGroup) c.getDeclaredConstructor().newInstance();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to instantiate " + c.getName(), e);
                    }
                })
                .sorted(Comparator
                        .comparingInt((TestGroup g) -> effectiveOrder(g.getClass().getAnnotation(FlociTestGroup.class)))
                        .thenComparing(TestGroup::name))
                .toList();

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

    private static int effectiveOrder(FlociTestGroup ann) {
        return ann.order() >= 0 ? ann.order() : ann.value();
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
