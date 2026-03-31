package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for Aurora-style DB clusters (MySQL engine).
 * Creates a cluster, adds an instance to it, verifies connectivity,
 * then tears down in reverse order.
 */
@FlociTestGroup
public class RdsClusterTests implements TestGroup {

    @Override
    public String name() { return "rds-cluster"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- RDS Cluster Tests (MySQL) ---");

        String clusterId = "mgmt-test-cluster";
        String instanceId = "mgmt-test-cluster-instance";
        String password = "testpassword1234";

        try (RdsClient rds = RdsClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            int[] clusterPort = {-1};
            int[] instancePort = {-1};

            // 1. CreateDBCluster (mysql)
            try {
                var resp = rds.createDBCluster(r -> r
                        .dbClusterIdentifier(clusterId)
                        .engine("mysql")
                        .engineVersion("8.0.36")
                        .masterUsername("root")
                        .masterUserPassword(password)
                        .databaseName("testdb"));
                DBCluster cluster = resp.dbCluster();
                boolean ok = cluster != null && cluster.endpoint() != null && cluster.port() > 0;
                ctx.check("RDS-Cluster CreateDBCluster", ok);
                if (cluster != null) {
                    clusterPort[0] = cluster.port();
                }
            } catch (Exception e) {
                ctx.check("RDS-Cluster CreateDBCluster", false, e);
            }

            // 2. CreateDBInstance (mysql, in cluster)
            try {
                var resp = rds.createDBInstance(r -> r
                        .dbInstanceIdentifier(instanceId)
                        .engine("mysql")
                        .engineVersion("8.0.36")
                        .masterUsername("root")
                        .masterUserPassword(password)
                        .dbInstanceClass("db.t3.micro")
                        .allocatedStorage(20)
                        .dbClusterIdentifier(clusterId));
                DBInstance inst = resp.dbInstance();
                boolean ok = inst != null && inst.endpoint() != null && inst.endpoint().port() > 0;
                ctx.check("RDS-Cluster CreateDBInstance (cluster member)", ok);
                if (inst != null && inst.endpoint() != null) {
                    instancePort[0] = inst.endpoint().port();
                }
            } catch (Exception e) {
                ctx.check("RDS-Cluster CreateDBInstance (cluster member)", false, e);
            }

            // 3. DescribeDBClusters — verify member list
            try {
                var resp = rds.describeDBClusters(r -> r.dbClusterIdentifier(clusterId));
                boolean found = resp.dbClusters().stream()
                        .anyMatch(c -> clusterId.equals(c.dbClusterIdentifier())
                                && c.dbClusterMembers().stream()
                                        .anyMatch(m -> instanceId.equals(m.dbInstanceIdentifier())));
                ctx.check("RDS-Cluster DescribeDBClusters (member present)", found);
            } catch (Exception e) {
                ctx.check("RDS-Cluster DescribeDBClusters (member present)", false, e);
            }

            // 4. DescribeDBInstances — verify DBClusterIdentifier
            try {
                var resp = rds.describeDBInstances(r -> r.dbInstanceIdentifier(instanceId));
                boolean found = resp.dbInstances().stream()
                        .anyMatch(i -> clusterId.equals(i.dbClusterIdentifier()));
                ctx.check("RDS-Cluster DescribeDBInstances (cluster id set)", found);
            } catch (Exception e) {
                ctx.check("RDS-Cluster DescribeDBInstances (cluster id set)", false, e);
            }

            // 5. Wait for MySQL container
            System.out.println("  Waiting 8s for MySQL container...");
            try { Thread.sleep(8000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            // 6. JDBC connect to cluster endpoint + full CRUD
            if (clusterPort[0] > 0) {
                String url = "jdbc:mysql://" + ctx.proxyHost + ":" + clusterPort[0]
                        + "/testdb?useSSL=false&allowPublicKeyRetrieval=true&allowClearTextPassword=true";
                try (Connection conn = DriverManager.getConnection(url, "root", password)) {
                    ctx.check("RDS-Cluster JDBC cluster endpoint", conn != null && !conn.isClosed());
                    runCrudTests(ctx, conn, "rds_cluster_test");
                } catch (Exception e) {
                    ctx.check("RDS-Cluster JDBC cluster endpoint", false, e);
                    markCrudFailed(ctx);
                }
            } else {
                ctx.check("RDS-Cluster JDBC cluster endpoint", false);
                markCrudFailed(ctx);
            }

            // 7. JDBC connect to instance endpoint — basic connectivity check
            if (instancePort[0] > 0) {
                String url = "jdbc:mysql://" + ctx.proxyHost + ":" + instancePort[0]
                        + "/testdb?useSSL=false&allowPublicKeyRetrieval=true&allowClearTextPassword=true";
                try (Connection conn = DriverManager.getConnection(url, "root", password);
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT DATABASE()")) {
                    boolean ok = rs.next() && "testdb".equals(rs.getString(1));
                    ctx.check("RDS-Cluster JDBC instance endpoint", ok);
                } catch (Exception e) {
                    ctx.check("RDS-Cluster JDBC instance endpoint", false, e);
                }
            } else {
                ctx.check("RDS-Cluster JDBC instance endpoint", false);
            }

            // 8. DeleteDBInstance (cluster member)
            try {
                rds.deleteDBInstance(r -> r
                        .dbInstanceIdentifier(instanceId)
                        .skipFinalSnapshot(true));
                ctx.check("RDS-Cluster DeleteDBInstance", true);
            } catch (Exception e) {
                ctx.check("RDS-Cluster DeleteDBInstance", false, e);
            }

            // 9. DeleteDBCluster (now empty)
            try {
                rds.deleteDBCluster(r -> r
                        .dbClusterIdentifier(clusterId)
                        .skipFinalSnapshot(true));
                ctx.check("RDS-Cluster DeleteDBCluster", true);
            } catch (Exception e) {
                ctx.check("RDS-Cluster DeleteDBCluster", false, e);
            }
        }
    }

    // ── Real SQL through the cluster connection ────────────────────────────────

    private static void runCrudTests(TestContext ctx, Connection conn, String table) {
        try {
            conn.setAutoCommit(true);

            // CREATE TABLE
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + table);
                s.execute("""
                        CREATE TABLE %s (
                            id      INT AUTO_INCREMENT PRIMARY KEY,
                            city    VARCHAR(100) NOT NULL,
                            country VARCHAR(100) NOT NULL,
                            pop     BIGINT       NOT NULL
                        )""".formatted(table));
                ctx.check("RDS-Cluster CREATE TABLE", true);
            } catch (Exception e) {
                ctx.check("RDS-Cluster CREATE TABLE", false, e);
                return;
            }

            // INSERT 4 rows
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (city, country, pop) VALUES (?, ?, ?)")) {
                Object[][] rows = {
                    {"Tokyo",    "Japan",  13960000L},
                    {"Delhi",    "India",  32941000L},
                    {"Shanghai", "China",  24870000L},
                    {"Dhaka",    "Bangladesh", 23210000L}
                };
                for (Object[] row : rows) {
                    ps.setString(1, (String) row[0]);
                    ps.setString(2, (String) row[1]);
                    ps.setLong(3, (Long) row[2]);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                ctx.check("RDS-Cluster INSERT 4 rows", counts.length == 4);
            } catch (Exception e) {
                ctx.check("RDS-Cluster INSERT 4 rows", false, e);
            }

            // SELECT COUNT(*) = 4
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
                boolean ok = rs.next() && rs.getLong(1) == 4;
                ctx.check("RDS-Cluster SELECT COUNT(*) = 4", ok);
            } catch (Exception e) {
                ctx.check("RDS-Cluster SELECT COUNT(*) = 4", false, e);
            }

            // SELECT WHERE city = 'Tokyo'
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT country, pop FROM " + table + " WHERE city = ?")) {
                ps.setString(1, "Tokyo");
                try (ResultSet rs = ps.executeQuery()) {
                    boolean ok = rs.next()
                            && "Japan".equals(rs.getString("country"))
                            && rs.getLong("pop") == 13960000L;
                    ctx.check("RDS-Cluster SELECT WHERE city='Tokyo'", ok);
                }
            } catch (Exception e) {
                ctx.check("RDS-Cluster SELECT WHERE city='Tokyo'", false, e);
            }

            // UPDATE pop for Delhi
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE " + table + " SET pop = ? WHERE city = ?")) {
                ps.setLong(1, 33000000L);
                ps.setString(2, "Delhi");
                int updated = ps.executeUpdate();
                ctx.check("RDS-Cluster UPDATE Delhi pop=33000000", updated == 1);
            } catch (Exception e) {
                ctx.check("RDS-Cluster UPDATE Delhi pop=33000000", false, e);
            }

            // Verify update
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT pop FROM " + table + " WHERE city = ?")) {
                ps.setString(1, "Delhi");
                try (ResultSet rs = ps.executeQuery()) {
                    boolean ok = rs.next() && rs.getLong("pop") == 33000000L;
                    ctx.check("RDS-Cluster SELECT updated pop=33000000", ok);
                }
            } catch (Exception e) {
                ctx.check("RDS-Cluster SELECT updated pop=33000000", false, e);
            }

            // DELETE cities with pop < 20000000
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM " + table + " WHERE pop < ?")) {
                ps.setLong(1, 20000000L);
                int deleted = ps.executeUpdate();
                ctx.check("RDS-Cluster DELETE pop<20M (1 row)", deleted == 1);
            } catch (Exception e) {
                ctx.check("RDS-Cluster DELETE pop<20M (1 row)", false, e);
            }

            // SELECT COUNT(*) = 3 after delete
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
                boolean ok = rs.next() && rs.getLong(1) == 3;
                ctx.check("RDS-Cluster SELECT COUNT(*) after DELETE = 3", ok);
            } catch (Exception e) {
                ctx.check("RDS-Cluster SELECT COUNT(*) after DELETE = 3", false, e);
            }

            // Read remaining rows ordered by city
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT city FROM " + table + " ORDER BY city")) {
                List<String> cities = new ArrayList<>();
                while (rs.next()) {
                    cities.add(rs.getString("city"));
                }
                boolean ok = cities.equals(List.of("Delhi", "Dhaka", "Shanghai"));
                ctx.check("RDS-Cluster SELECT remaining cities (Delhi,Dhaka,Shanghai)", ok);
            } catch (Exception e) {
                ctx.check("RDS-Cluster SELECT remaining cities (Delhi,Dhaka,Shanghai)", false, e);
            }

            // Aggregate: SUM(pop) for remaining rows
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT SUM(pop) AS total FROM " + table)) {
                boolean ok = rs.next() && rs.getLong("total") > 0;
                ctx.check("RDS-Cluster SELECT SUM(pop)", ok);
            } catch (Exception e) {
                ctx.check("RDS-Cluster SELECT SUM(pop)", false, e);
            }

            // DROP TABLE
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE " + table);
                ctx.check("RDS-Cluster DROP TABLE", true);
            } catch (Exception e) {
                ctx.check("RDS-Cluster DROP TABLE", false, e);
            }

        } catch (Exception e) {
            ctx.check("RDS-Cluster CRUD unexpected error", false, e);
        }
    }

    private static void markCrudFailed(TestContext ctx) {
        List.of(
            "RDS-Cluster CREATE TABLE",
            "RDS-Cluster INSERT 4 rows",
            "RDS-Cluster SELECT COUNT(*) = 4",
            "RDS-Cluster SELECT WHERE city='Tokyo'",
            "RDS-Cluster UPDATE Delhi pop=33000000",
            "RDS-Cluster SELECT updated pop=33000000",
            "RDS-Cluster DELETE pop<20M (1 row)",
            "RDS-Cluster SELECT COUNT(*) after DELETE = 3",
            "RDS-Cluster SELECT remaining cities (Delhi,Dhaka,Shanghai)",
            "RDS-Cluster SELECT SUM(pop)",
            "RDS-Cluster DROP TABLE"
        ).forEach(t -> ctx.check(t, false));
    }
}
