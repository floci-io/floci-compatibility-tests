package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Management and data-plane tests for a standalone RDS PostgreSQL DB instance.
 *
 * <p>Covers:
 * <ul>
 *   <li>Instance lifecycle (create, describe, modify, reboot, delete)
 *   <li>Auth proxy: correct password succeeds, wrong password rejected
 *   <li>Real SQL: CREATE TABLE → INSERT → SELECT → UPDATE → DELETE → DROP
 * </ul>
 */
@FlociTestGroup
public class RdsManagementTests implements TestGroup {

    @Override
    public String name() { return "rds-mgmt"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- RDS Management Tests (PostgreSQL) ---");

        String instanceId = "mgmt-test-pg";
        String password   = "testpassword1234";
        String dbUser     = "admin";
        String dbName     = "testdb";

        try (RdsClient rds = RdsClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            int[] proxyPort = {-1};

            // 1. CreateDBInstance (postgres, no IAM)
            try {
                var resp = rds.createDBInstance(r -> r
                        .dbInstanceIdentifier(instanceId)
                        .engine("postgres")
                        .engineVersion("16.3")
                        .masterUsername(dbUser)
                        .masterUserPassword(password)
                        .dbName(dbName)
                        .dbInstanceClass("db.t3.micro")
                        .allocatedStorage(20));
                DBInstance inst = resp.dbInstance();
                boolean ok = inst != null && inst.endpoint() != null && inst.endpoint().port() > 0
                        && instanceId.equals(inst.dbInstanceIdentifier());
                ctx.check("RDS-Mgmt CreateDBInstance", ok);
                if (inst != null && inst.endpoint() != null) {
                    proxyPort[0] = inst.endpoint().port();
                }
            } catch (Exception e) {
                ctx.check("RDS-Mgmt CreateDBInstance", false, e);
            }

            // 2. DescribeDBInstances
            try {
                var resp = rds.describeDBInstances(r -> r.dbInstanceIdentifier(instanceId));
                boolean found = resp.dbInstances().stream()
                        .anyMatch(i -> instanceId.equals(i.dbInstanceIdentifier()));
                ctx.check("RDS-Mgmt DescribeDBInstances", found);
            } catch (Exception e) {
                ctx.check("RDS-Mgmt DescribeDBInstances", false, e);
            }

            // 3. Wait for PostgreSQL container to be ready
            System.out.println("  Waiting 5s for PostgreSQL container...");
            try { Thread.sleep(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            if (proxyPort[0] <= 0) {
                markAllFailed(ctx);
                return;
            }

            String url = "jdbc:postgresql://" + ctx.proxyHost + ":" + proxyPort[0]
                    + "/" + dbName + "?sslmode=disable";

            // 4. Auth: correct password
            try (Connection conn = DriverManager.getConnection(url, dbUser, password)) {
                ctx.check("RDS-Mgmt JDBC correct password", conn != null && !conn.isClosed());
            } catch (Exception e) {
                ctx.check("RDS-Mgmt JDBC correct password", false, e);
            }

            // 5. Auth: wrong password rejected
            try (Connection conn = DriverManager.getConnection(url, dbUser, "wrongpasswordxxx")) {
                ctx.check("RDS-Mgmt JDBC wrong password rejected", false);
            } catch (Exception e) {
                ctx.check("RDS-Mgmt JDBC wrong password rejected", true);
            }

            // 6–11. Real SQL: full CRUD lifecycle
            runCrudTests(ctx, url, dbUser, password);

            // 12. ModifyDBInstance — enable IAM auth
            try {
                var resp = rds.modifyDBInstance(r -> r
                        .dbInstanceIdentifier(instanceId)
                        .enableIAMDatabaseAuthentication(true)
                        .applyImmediately(true));
                boolean ok = resp.dbInstance() != null
                        && Boolean.TRUE.equals(resp.dbInstance().iamDatabaseAuthenticationEnabled());
                ctx.check("RDS-Mgmt ModifyDBInstance (enable IAM)", ok);
            } catch (Exception e) {
                ctx.check("RDS-Mgmt ModifyDBInstance (enable IAM)", false, e);
            }

            // 13. RebootDBInstance
            try {
                var resp = rds.rebootDBInstance(r -> r.dbInstanceIdentifier(instanceId));
                ctx.check("RDS-Mgmt RebootDBInstance", resp.dbInstance() != null);
            } catch (Exception e) {
                ctx.check("RDS-Mgmt RebootDBInstance", false, e);
            }

            // 14. DeleteDBInstance
            try {
                rds.deleteDBInstance(r -> r
                        .dbInstanceIdentifier(instanceId)
                        .skipFinalSnapshot(true));
                ctx.check("RDS-Mgmt DeleteDBInstance", true);
            } catch (Exception e) {
                ctx.check("RDS-Mgmt DeleteDBInstance", false, e);
            }
        }
    }

    /**
     * Opens a fresh connection and exercises CREATE TABLE → INSERT → SELECT →
     * UPDATE → DELETE → DROP, reporting each step individually.
     */
    private static void runCrudTests(TestContext ctx, String url, String user, String password) {
        String table = "rds_mgmt_test";
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            conn.setAutoCommit(true);

            // 6. CREATE TABLE
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + table);
                s.execute("""
                        CREATE TABLE %s (
                            id    SERIAL PRIMARY KEY,
                            name  TEXT    NOT NULL,
                            score INTEGER NOT NULL
                        )""".formatted(table));
                ctx.check("RDS-Mgmt CREATE TABLE", true);
            } catch (Exception e) {
                ctx.check("RDS-Mgmt CREATE TABLE", false, e);
                return;
            }

            // 7. INSERT 3 rows
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (name, score) VALUES (?, ?)")) {
                ps.setString(1, "Alice"); ps.setInt(2, 90); ps.addBatch();
                ps.setString(1, "Bob");   ps.setInt(2, 75); ps.addBatch();
                ps.setString(1, "Carol"); ps.setInt(2, 88); ps.addBatch();
                int[] counts = ps.executeBatch();
                boolean ok = counts.length == 3;
                ctx.check("RDS-Mgmt INSERT 3 rows", ok);
            } catch (Exception e) {
                ctx.check("RDS-Mgmt INSERT 3 rows", false, e);
            }

            // 8. SELECT COUNT(*) = 3
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
                boolean ok = rs.next() && rs.getLong(1) == 3;
                ctx.check("RDS-Mgmt SELECT COUNT(*) = 3", ok);
            } catch (Exception e) {
                ctx.check("RDS-Mgmt SELECT COUNT(*) = 3", false, e);
            }

            // 9. SELECT with WHERE
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT name, score FROM " + table + " WHERE name = ?")) {
                ps.setString(1, "Alice");
                try (ResultSet rs = ps.executeQuery()) {
                    boolean ok = rs.next() && "Alice".equals(rs.getString("name"))
                            && rs.getInt("score") == 90;
                    ctx.check("RDS-Mgmt SELECT WHERE name='Alice'", ok);
                }
            } catch (Exception e) {
                ctx.check("RDS-Mgmt SELECT WHERE name='Alice'", false, e);
            }

            // 10. UPDATE
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE " + table + " SET score = ? WHERE name = ?")) {
                ps.setInt(1, 95);
                ps.setString(2, "Alice");
                int updated = ps.executeUpdate();
                ctx.check("RDS-Mgmt UPDATE Alice score=95", updated == 1);
            } catch (Exception e) {
                ctx.check("RDS-Mgmt UPDATE Alice score=95", false, e);
            }

            // Verify update
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT score FROM " + table + " WHERE name = ?")) {
                ps.setString(1, "Alice");
                try (ResultSet rs = ps.executeQuery()) {
                    boolean ok = rs.next() && rs.getInt("score") == 95;
                    ctx.check("RDS-Mgmt SELECT updated score=95", ok);
                }
            } catch (Exception e) {
                ctx.check("RDS-Mgmt SELECT updated score=95", false, e);
            }

            // 11. DELETE one row
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM " + table + " WHERE name = ?")) {
                ps.setString(1, "Bob");
                int deleted = ps.executeUpdate();
                ctx.check("RDS-Mgmt DELETE Bob", deleted == 1);
            } catch (Exception e) {
                ctx.check("RDS-Mgmt DELETE Bob", false, e);
            }

            // SELECT COUNT(*) = 2
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
                boolean ok = rs.next() && rs.getLong(1) == 2;
                ctx.check("RDS-Mgmt SELECT COUNT(*) after DELETE = 2", ok);
            } catch (Exception e) {
                ctx.check("RDS-Mgmt SELECT COUNT(*) after DELETE = 2", false, e);
            }

            // Read all remaining rows
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT name, score FROM " + table + " ORDER BY name")) {
                List<String> names = new ArrayList<>();
                while (rs.next()) {
                    names.add(rs.getString("name") + ":" + rs.getInt("score"));
                }
                boolean ok = names.equals(List.of("Alice:95", "Carol:88"));
                ctx.check("RDS-Mgmt SELECT remaining rows (Alice:95, Carol:88)", ok);
            } catch (Exception e) {
                ctx.check("RDS-Mgmt SELECT remaining rows (Alice:95, Carol:88)", false, e);
            }

            // DROP TABLE
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE " + table);
                ctx.check("RDS-Mgmt DROP TABLE", true);
            } catch (Exception e) {
                ctx.check("RDS-Mgmt DROP TABLE", false, e);
            }

        } catch (Exception e) {
            ctx.check("RDS-Mgmt CRUD connection", false, e);
        }
    }

    private static void markAllFailed(TestContext ctx) {
        for (String t : List.of(
                "RDS-Mgmt JDBC correct password",
                "RDS-Mgmt JDBC wrong password rejected",
                "RDS-Mgmt CREATE TABLE",
                "RDS-Mgmt INSERT 3 rows",
                "RDS-Mgmt SELECT COUNT(*) = 3",
                "RDS-Mgmt SELECT WHERE name='Alice'",
                "RDS-Mgmt UPDATE Alice score=95",
                "RDS-Mgmt SELECT updated score=95",
                "RDS-Mgmt DELETE Bob",
                "RDS-Mgmt SELECT COUNT(*) after DELETE = 2",
                "RDS-Mgmt SELECT remaining rows (Alice:95, Carol:88)",
                "RDS-Mgmt DROP TABLE",
                "RDS-Mgmt ModifyDBInstance (enable IAM)",
                "RDS-Mgmt RebootDBInstance",
                "RDS-Mgmt DeleteDBInstance")) {
            ctx.check(t, false);
        }
    }
}
