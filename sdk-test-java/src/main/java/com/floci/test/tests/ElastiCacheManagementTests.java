package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.elasticache.model.InputAuthenticationType;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Management tests for ElastiCache replication groups and users.
 * Uses the AWS SDK ElastiCacheClient (Query protocol / XML) for all management-plane
 * calls; raw sockets only for data-plane AUTH protocol testing.
 */
@FlociTestGroup
public class ElastiCacheManagementTests implements TestGroup {

    @Override
    public String name() { return "elasticache-mgmt"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- ElastiCache Management Tests ---");

        String groupId = "mgmt-test-cluster";
        String userId  = "mgmt-test-user";
        String password = "testpassword1234";  // AWS requires >= 16 chars for authToken

        try (ElastiCacheClient ec = ElastiCacheClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            int[] proxyPort = { -1 };

            // 1. CreateReplicationGroup (PASSWORD auth via authToken)
            try {
                var resp = ec.createReplicationGroup(r -> r
                        .replicationGroupId(groupId)
                        .replicationGroupDescription("SDK mgmt test")
                        .authToken(password)
                        .transitEncryptionEnabled(true));
                var ep = resp.replicationGroup().configurationEndpoint();
                boolean ok = ep != null && ep.port() > 0
                        && groupId.equals(resp.replicationGroup().replicationGroupId());
                ctx.check("EC-Mgmt CreateReplicationGroup", ok);
                if (ep != null) {
                    proxyPort[0] = ep.port();
                }
            } catch (Exception e) {
                ctx.check("EC-Mgmt CreateReplicationGroup", false, e);
            }

            // 2. DescribeReplicationGroups (filtered)
            try {
                var resp = ec.describeReplicationGroups(r -> r.replicationGroupId(groupId));
                boolean found = resp.replicationGroups().stream()
                        .anyMatch(g -> groupId.equals(g.replicationGroupId()));
                ctx.check("EC-Mgmt DescribeReplicationGroups", found);
            } catch (Exception e) {
                ctx.check("EC-Mgmt DescribeReplicationGroups", false, e);
            }

            // 3. CreateUser (PASSWORD)
            try {
                var resp = ec.createUser(r -> r
                        .userId(userId)
                        .userName("alice")
                        .accessString("on ~* +@all")
                        .authenticationMode(a -> a
                                .type(InputAuthenticationType.PASSWORD)
                                .passwords(password))
                        .engine("redis"));
                ctx.check("EC-Mgmt CreateUser", userId.equals(resp.userId()));
            } catch (Exception e) {
                ctx.check("EC-Mgmt CreateUser", false, e);
            }

            // 4. DescribeUsers
            try {
                var resp = ec.describeUsers(r -> r.userId(userId));
                boolean found = resp.users().stream()
                        .anyMatch(u -> userId.equals(u.userId()));
                ctx.check("EC-Mgmt DescribeUsers", found);
            } catch (Exception e) {
                ctx.check("EC-Mgmt DescribeUsers", false, e);
            }

            // Allow the Valkey container a moment to finish starting
            try { Thread.sleep(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            // 5. Connect with correct authToken (group-level password)
            if (proxyPort[0] > 0) {
                try {
                    String reply = redisAuth(ctx.proxyHost, proxyPort[0], null, password);
                    ctx.check("EC-Mgmt Auth correct password", "+OK".equals(reply));
                } catch (Exception e) {
                    ctx.check("EC-Mgmt Auth correct password", false, e);
                }

                // 6. Connect with wrong password
                try {
                    String reply = redisAuth(ctx.proxyHost, proxyPort[0], null, "wrongpasswordxxx");
                    ctx.check("EC-Mgmt Auth wrong password rejected",
                            reply != null && reply.startsWith("-ERR"));
                } catch (Exception e) {
                    ctx.check("EC-Mgmt Auth wrong password rejected", false, e);
                }
            } else {
                ctx.check("EC-Mgmt Auth correct password", false);
                ctx.check("EC-Mgmt Auth wrong password rejected", false);
            }

            // 7. ModifyUser — change password
            String newPassword = "newpassword56789";
            try {
                var resp = ec.modifyUser(r -> r
                        .userId(userId)
                        .authenticationMode(a -> a
                                .type(InputAuthenticationType.PASSWORD)
                                .passwords(newPassword)));
                ctx.check("EC-Mgmt ModifyUser", userId.equals(resp.userId()));
            } catch (Exception e) {
                ctx.check("EC-Mgmt ModifyUser", false, e);
            }

            // 8. Connect with old password — should still work (group-level authToken unchanged)
            if (proxyPort[0] > 0) {
                try {
                    String reply = redisAuth(ctx.proxyHost, proxyPort[0], null, password);
                    ctx.check("EC-Mgmt Auth original password still works after ModifyUser",
                            "+OK".equals(reply));
                } catch (Exception e) {
                    ctx.check("EC-Mgmt Auth original password still works after ModifyUser", false, e);
                }
            } else {
                ctx.check("EC-Mgmt Auth original password still works after ModifyUser", false);
            }

            // ── Cleanup ───────────────────────────────────────────────────────

            // 9. DeleteUser
            try {
                ec.deleteUser(r -> r.userId(userId));
                ctx.check("EC-Mgmt DeleteUser", true);
            } catch (Exception e) {
                ctx.check("EC-Mgmt DeleteUser", false, e);
            }

            // 10. DeleteReplicationGroup
            try {
                ec.deleteReplicationGroup(r -> r.replicationGroupId(groupId));
                ctx.check("EC-Mgmt DeleteReplicationGroup", true);
            } catch (Exception e) {
                ctx.check("EC-Mgmt DeleteReplicationGroup", false, e);
            }
        }
    }

    /**
     * Sends a Redis AUTH command over a raw TCP socket and returns the first response line.
     * Raw socket is necessary here because this is a data-plane protocol test — there is
     * no AWS SDK equivalent for connecting to a Redis endpoint.
     */
    private static String redisAuth(String host, int port, String username, String password)
            throws Exception {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 5000);
            socket.setSoTimeout(5000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();
            out.write(buildAuthCommand(username, password));
            out.flush();
            return readLine(in);
        }
    }

    private static byte[] buildAuthCommand(String username, String password) {
        StringBuilder sb = new StringBuilder();
        if (username != null && !username.isEmpty()) {
            sb.append("*3\r\n$4\r\nAUTH\r\n");
            appendBulk(sb, username);
        } else {
            sb.append("*2\r\n$4\r\nAUTH\r\n");
        }
        appendBulk(sb, password);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void appendBulk(StringBuilder sb, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        sb.append("$").append(bytes.length).append("\r\n").append(value).append("\r\n");
    }

    private static String readLine(InputStream in) throws Exception {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\r') { in.read(); break; }
            sb.append((char) c);
        }
        return sb.toString();
    }
}
