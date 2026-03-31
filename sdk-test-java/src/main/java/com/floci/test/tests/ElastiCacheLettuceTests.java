package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.protocol.ProtocolVersion;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;

import java.time.Duration;

/**
 * End-to-end Lettuce Redis client tests for all three ElastiCache auth modes.
 * <p>
 * Management plane (cluster/user lifecycle): AWS SDK {@link ElastiCacheClient} (Query protocol / XML).
 * Data plane (Redis commands): Lettuce Redis client over TCP.
 * <p>
 * Auth modes covered:
 * <ul>
 *   <li>NO_AUTH  — cluster created without authToken and without transitEncryption</li>
 *   <li>PASSWORD — cluster created with authToken; proxy validates Redis AUTH password</li>
 *   <li>IAM      — cluster created with transitEncryptionEnabled=true; proxy validates SigV4 token</li>
 * </ul>
 */
@FlociTestGroup
public class ElastiCacheLettuceTests implements TestGroup {

    @Override
    public String name() { return "elasticache-lettuce"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- ElastiCache Lettuce Tests ---");

        try (ElastiCacheClient ec = ElastiCacheClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            testNoAuth(ctx, ec);
            testPassword(ctx, ec);
            testIam(ctx, ec);
        }
    }

    // ── NO_AUTH ───────────────────────────────────────────────────────────────

    private void testNoAuth(TestContext ctx, ElastiCacheClient ec) {
        String groupId = "lettuce-noauth";
        RedisClient redisClient = null;
        boolean groupCreated = false;
        try {
            var resp = ec.createReplicationGroup(r -> r
                    .replicationGroupId(groupId)
                    .replicationGroupDescription("Lettuce no-auth test"));
            groupCreated = true;
            int port = resp.replicationGroup().configurationEndpoint().port();

            // Allow Valkey container to finish starting
            Thread.sleep(2000);

            RedisURI uri = RedisURI.Builder.redis(ctx.proxyHost, port).build();
            redisClient = newClient(uri);

            try (var conn = redisClient.connect()) {
                var cmds = conn.sync();
                ctx.check("EC-Lettuce NoAuth PING", "PONG".equals(cmds.ping()));
                cmds.set("noauth-key", "hello");
                ctx.check("EC-Lettuce NoAuth SET/GET", "hello".equals(cmds.get("noauth-key")));
            }
        } catch (Exception e) {
            ctx.check("EC-Lettuce NoAuth PING", false, e);
            ctx.check("EC-Lettuce NoAuth SET/GET", false);
        } finally {
            if (redisClient != null) { redisClient.shutdown(); }
            if (groupCreated) {
                try { ec.deleteReplicationGroup(r -> r.replicationGroupId(groupId)); } catch (Exception ignored) {}
            }
        }
    }

    // ── PASSWORD ──────────────────────────────────────────────────────────────

    private void testPassword(TestContext ctx, ElastiCacheClient ec) {
        String groupId = "lettuce-password";
        String password = "testpassword1234";
        RedisClient goodClient = null;
        RedisClient badClient = null;
        boolean groupCreated = false;
        try {
            var resp = ec.createReplicationGroup(r -> r
                    .replicationGroupId(groupId)
                    .replicationGroupDescription("Lettuce password test")
                    .authToken(password)
                    .transitEncryptionEnabled(true));
            groupCreated = true;
            int port = resp.replicationGroup().configurationEndpoint().port();

            Thread.sleep(2000);

            // Correct password — should connect and respond to PING
            RedisURI goodUri = RedisURI.Builder.redis(ctx.proxyHost, port)
                    .withPassword(password.toCharArray())
                    .build();
            goodClient = newClient(goodUri);
            try (var conn = goodClient.connect()) {
                ctx.check("EC-Lettuce Password PING", "PONG".equals(conn.sync().ping()));
            }

            // Wrong password — connection or first command must throw
            RedisURI badUri = RedisURI.Builder.redis(ctx.proxyHost, port)
                    .withPassword("wrongpassword!!!".toCharArray())
                    .build();
            badClient = newClient(badUri);
            try {
                try (var conn = badClient.connect()) {
                    conn.sync().ping();
                }
                ctx.check("EC-Lettuce Password Reject Wrong", false);
            } catch (Exception e) {
                ctx.check("EC-Lettuce Password Reject Wrong", true);
            }
        } catch (Exception e) {
            ctx.check("EC-Lettuce Password PING", false, e);
            ctx.check("EC-Lettuce Password Reject Wrong", false);
        } finally {
            if (goodClient != null) { goodClient.shutdown(); }
            if (badClient != null) { badClient.shutdown(); }
            if (groupCreated) {
                try { ec.deleteReplicationGroup(r -> r.replicationGroupId(groupId)); } catch (Exception ignored) {}
            }
        }
    }

    // ── IAM ───────────────────────────────────────────────────────────────────

    private void testIam(TestContext ctx, ElastiCacheClient ec) {
        String groupId = "lettuce-iam";
        String userId = "lettuce-iam-user";
        RedisClient goodClient = null;
        RedisClient badClient = null;
        boolean groupCreated = false;
        try {
            // transitEncryptionEnabled=true + no authToken → IAM mode on the emulator
            var resp = ec.createReplicationGroup(r -> r
                    .replicationGroupId(groupId)
                    .replicationGroupDescription("Lettuce IAM test")
                    .transitEncryptionEnabled(true));
            groupCreated = true;
            int port = resp.replicationGroup().configurationEndpoint().port();

            Thread.sleep(2000);

            // Generate a valid SigV4 presigned token
            AwsCredentials credentials = ctx.credentials.resolveCredentials();
            String token = new ElastiCacheTests.IAMAuthTokenRequest(userId, groupId, ctx.region.id())
                    .toSignedRequestUri(credentials);

            // Valid token — AUTH userId <token>
            RedisURI goodUri = RedisURI.Builder.redis(ctx.proxyHost, port)
                    .withAuthentication(userId, token.toCharArray())
                    .build();
            goodClient = newClient(goodUri);
            try (var conn = goodClient.connect()) {
                ctx.check("EC-Lettuce IAM PING", "PONG".equals(conn.sync().ping()));
            }

            // Invalid token — should be rejected
            RedisURI badUri = RedisURI.Builder.redis(ctx.proxyHost, port)
                    .withAuthentication(userId, "not-a-valid-iam-token".toCharArray())
                    .build();
            badClient = newClient(badUri);
            try {
                try (var conn = badClient.connect()) {
                    conn.sync().ping();
                }
                ctx.check("EC-Lettuce IAM Reject Invalid Token", false);
            } catch (Exception e) {
                ctx.check("EC-Lettuce IAM Reject Invalid Token", true);
            }
        } catch (Exception e) {
            ctx.check("EC-Lettuce IAM PING", false, e);
            ctx.check("EC-Lettuce IAM Reject Invalid Token", false);
        } finally {
            if (goodClient != null) { goodClient.shutdown(); }
            if (badClient != null) { badClient.shutdown(); }
            if (groupCreated) {
                try { ec.deleteReplicationGroup(r -> r.replicationGroupId(groupId)); } catch (Exception ignored) {}
            }
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /**
     * Creates a Lettuce RedisClient with RESP2 protocol (no HELLO negotiation)
     * and a 5-second command timeout.
     */
    private static RedisClient newClient(RedisURI uri) {
        RedisClient client = RedisClient.create(uri);
        client.setOptions(ClientOptions.builder()
                .protocolVersion(ProtocolVersion.RESP2)
                .build());
        client.setDefaultTimeout(Duration.ofSeconds(5));
        return client;
    }
}
