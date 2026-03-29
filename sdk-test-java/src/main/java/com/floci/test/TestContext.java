package com.floci.test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.net.URI;

/**
 * Shared test state: AWS client config and pass/fail counters.
 */
public class TestContext {

    public final URI endpoint = URI.create(
            System.getenv("FLOCI_ENDPOINT") != null
                    ? System.getenv("FLOCI_ENDPOINT")
                    : "http://localhost:4566");

    /** Returns true when running against real AWS (no endpoint override). */
    public boolean isRealAws() {
        return "aws".equalsIgnoreCase(System.getenv("FLOCI_TARGET"));
    }

    /** Hostname to use for direct TCP connections (JDBC, Redis). Derived from FLOCI_ENDPOINT. */
    public final String proxyHost = endpoint.getHost();

    public final Region region = Region.US_EAST_1;
    public final StaticCredentialsProvider credentials =
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

    private int passed = 0;
    private int failed = 0;

    public void check(String name, boolean ok) {
        check(name, ok, null);
    }

    public void check(String name, boolean ok, Exception error) {
        if (ok) {
            passed++;
            System.out.println("  PASS  " + name);
        } else {
            failed++;
            System.out.println("  FAIL  " + name);
            if (error != null) {
                System.out.println("        -> " + error.getMessage());
                Throwable cause = error.getCause();
                while (cause != null) {
                    System.out.println("         Caused by: " + cause.getClass().getSimpleName()
                            + ": " + cause.getMessage());
                    cause = cause.getCause();
                }
            } else {
                System.out.println();
            }
        }
    }

    public int getPassed() { return passed; }
    public int getFailed() { return failed; }
}
