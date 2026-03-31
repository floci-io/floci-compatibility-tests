package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.DBInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for RDS IAM database authentication with PostgreSQL.
 *
 * <p>Generates an IAM auth token using {@link RdsUtilities} and validates it
 * works as a JDBC password, then exercises real SQL (CREATE TABLE → INSERT →
 * SELECT → UPDATE → DELETE → DROP) through the IAM-authenticated connection.
 */
@FlociTestGroup
public class RdsIamTests implements TestGroup {

    @Override
    public String name() { return "rds-iam"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- RDS IAM Authentication Tests ---");

        String instanceId    = "iam-test-pg";
        String masterPassword = "testpassword1234";
        String dbUser        = "admin";
        String dbName        = "testdb";

        try (RdsClient rds = RdsClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            int[] proxyPort = {-1};

            // 1. CreateDBInstance (postgres, IAM enabled)
            try {
                var resp = rds.createDBInstance(r -> r
                        .dbInstanceIdentifier(instanceId)
                        .engine("postgres")
                        .engineVersion("16.3")
                        .masterUsername(dbUser)
                        .masterUserPassword(masterPassword)
                        .dbName(dbName)
                        .dbInstanceClass("db.t3.micro")
                        .allocatedStorage(20)
                        .enableIAMDatabaseAuthentication(true));
                DBInstance inst = resp.dbInstance();
                boolean ok = inst != null && inst.endpoint() != null && inst.endpoint().port() > 0
                        && Boolean.TRUE.equals(inst.iamDatabaseAuthenticationEnabled());
                ctx.check("RDS-IAM CreateDBInstance (IAM enabled)", ok);
                if (inst != null && inst.endpoint() != null) {
                    proxyPort[0] = inst.endpoint().port();
                }
            } catch (Exception e) {
                ctx.check("RDS-IAM CreateDBInstance (IAM enabled)", false, e);
            }

            if (proxyPort[0] <= 0) {
                markAllFailed(ctx);
                return;
            }

            // 2. Wait for PostgreSQL container
            System.out.println("  Waiting 5s for PostgreSQL container...");
            try { Thread.sleep(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            String pgUrl = "jdbc:postgresql://" + ctx.proxyHost + ":" + proxyPort[0]
                    + "/" + dbName + "?sslmode=disable";

            // 3. Generate IAM auth token via RdsUtilities
            String iamToken = null;
            try {
                RdsUtilities utils = RdsUtilities.builder()
                        .region(ctx.region)
                        .credentialsProvider(ctx.credentials)
                        .build();
                iamToken = utils.generateAuthenticationToken(b -> b
                        .hostname(ctx.proxyHost)
                        .port(proxyPort[0])
                        .username(dbUser));
                boolean ok = iamToken != null && iamToken.contains("X-Amz-Signature");
                ctx.check("RDS-IAM Generate IAM token", ok);
            } catch (Exception e) {
                ctx.check("RDS-IAM Generate IAM token", false, e);
            }

            // 4. JDBC connect with IAM token — then run full CRUD
            if (iamToken != null) {
                try (Connection conn = DriverManager.getConnection(pgUrl, dbUser, iamToken)) {
                    ctx.check("RDS-IAM JDBC connect with IAM token",
                            conn != null && !conn.isClosed());
                    runCrudTests(ctx, conn, "rds_iam_test");
                } catch (Exception e) {
                    ctx.check("RDS-IAM JDBC connect with IAM token", false, e);
                    markCrudFailed(ctx);
                }
            } else {
                ctx.check("RDS-IAM JDBC connect with IAM token", false);
                markCrudFailed(ctx);
            }

            // 5. Tampered / invalid IAM token is rejected
            String badToken = "localhost:" + proxyPort[0]
                    + "/?Action=connect&DBUser=" + dbUser + "&X-Amz-Signature=invalidsignature";
            try (Connection conn = DriverManager.getConnection(pgUrl, dbUser, badToken)) {
                ctx.check("RDS-IAM invalid token rejected", false);
            } catch (Exception e) {
                ctx.check("RDS-IAM invalid token rejected", true);
            }

            // 6. Master password still works even with IAM enabled
            try (Connection conn = DriverManager.getConnection(pgUrl, dbUser, masterPassword);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                boolean ok = rs.next() && dbName.equals(rs.getString(1));
                ctx.check("RDS-IAM master password still works", ok);
            } catch (Exception e) {
                ctx.check("RDS-IAM master password still works", false, e);
            }

            // 7. DeleteDBInstance
            try {
                rds.deleteDBInstance(r -> r
                        .dbInstanceIdentifier(instanceId)
                        .skipFinalSnapshot(true));
                ctx.check("RDS-IAM DeleteDBInstance", true);
            } catch (Exception e) {
                ctx.check("RDS-IAM DeleteDBInstance", false, e);
            }
        }
    }

    // ── Real SQL through the IAM-authenticated connection ─────────────────────

    private static void runCrudTests(TestContext ctx, Connection conn, String table) {
        try {
            conn.setAutoCommit(true);

            // CREATE TABLE
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + table);
                s.execute("""
                        CREATE TABLE %s (
                            id      SERIAL PRIMARY KEY,
                            product TEXT    NOT NULL,
                            qty     INTEGER NOT NULL,
                            price   NUMERIC(10,2) NOT NULL
                        )""".formatted(table));
                ctx.check("RDS-IAM CREATE TABLE", true);
            } catch (Exception e) {
                ctx.check("RDS-IAM CREATE TABLE", false, e);
                return;
            }

            // INSERT 4 rows
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (product, qty, price) VALUES (?, ?, ?)")) {
                Object[][] rows = {
                    {"Widget A", 10, 9.99},
                    {"Widget B", 5,  19.99},
                    {"Gadget X", 3,  49.99},
                    {"Gadget Y", 7,  29.99}
                };
                for (Object[] row : rows) {
                    ps.setString(1, (String) row[0]);
                    ps.setInt(2, (Integer) row[1]);
                    ps.setBigDecimal(3, new java.math.BigDecimal(row[2].toString()));
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                ctx.check("RDS-IAM INSERT 4 rows", counts.length == 4);
            } catch (Exception e) {
                ctx.check("RDS-IAM INSERT 4 rows", false, e);
            }

            // SELECT COUNT(*) = 4
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
                boolean ok = rs.next() && rs.getLong(1) == 4;
                ctx.check("RDS-IAM SELECT COUNT(*) = 4", ok);
            } catch (Exception e) {
                ctx.check("RDS-IAM SELECT COUNT(*) = 4", false, e);
            }

            // SELECT WHERE product = 'Gadget X'
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT qty, price FROM " + table + " WHERE product = ?")) {
                ps.setString(1, "Gadget X");
                try (ResultSet rs = ps.executeQuery()) {
                    boolean ok = rs.next()
                            && rs.getInt("qty") == 3
                            && rs.getBigDecimal("price").compareTo(new java.math.BigDecimal("49.99")) == 0;
                    ctx.check("RDS-IAM SELECT WHERE product='Gadget X'", ok);
                }
            } catch (Exception e) {
                ctx.check("RDS-IAM SELECT WHERE product='Gadget X'", false, e);
            }

            // UPDATE qty for Widget A
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE " + table + " SET qty = qty + ? WHERE product = ?")) {
                ps.setInt(1, 5);
                ps.setString(2, "Widget A");
                int updated = ps.executeUpdate();
                ctx.check("RDS-IAM UPDATE Widget A qty+=5", updated == 1);
            } catch (Exception e) {
                ctx.check("RDS-IAM UPDATE Widget A qty+=5", false, e);
            }

            // Verify update
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT qty FROM " + table + " WHERE product = ?")) {
                ps.setString(1, "Widget A");
                try (ResultSet rs = ps.executeQuery()) {
                    boolean ok = rs.next() && rs.getInt("qty") == 15;
                    ctx.check("RDS-IAM SELECT updated qty=15", ok);
                }
            } catch (Exception e) {
                ctx.check("RDS-IAM SELECT updated qty=15", false, e);
            }

            // DELETE low-stock items (qty < 5)
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM " + table + " WHERE qty < ?")) {
                ps.setInt(1, 5);
                int deleted = ps.executeUpdate();
                ctx.check("RDS-IAM DELETE qty<5 (1 row)", deleted == 1);
            } catch (Exception e) {
                ctx.check("RDS-IAM DELETE qty<5 (1 row)", false, e);
            }

            // SELECT COUNT(*) = 3 after delete
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
                boolean ok = rs.next() && rs.getLong(1) == 3;
                ctx.check("RDS-IAM SELECT COUNT(*) after DELETE = 3", ok);
            } catch (Exception e) {
                ctx.check("RDS-IAM SELECT COUNT(*) after DELETE = 3", false, e);
            }

            // Aggregate: SUM(qty * price) for remaining rows
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT SUM(qty * price) AS total FROM " + table)) {
                boolean ok = rs.next() && rs.getBigDecimal("total") != null;
                ctx.check("RDS-IAM SELECT SUM(qty*price)", ok);
            } catch (Exception e) {
                ctx.check("RDS-IAM SELECT SUM(qty*price)", false, e);
            }

            // DROP TABLE
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE " + table);
                ctx.check("RDS-IAM DROP TABLE", true);
            } catch (Exception e) {
                ctx.check("RDS-IAM DROP TABLE", false, e);
            }

        } catch (Exception e) {
            ctx.check("RDS-IAM CRUD unexpected error", false, e);
        }
    }

    private static void markAllFailed(TestContext ctx) {
        List.of(
            "RDS-IAM Generate IAM token",
            "RDS-IAM JDBC connect with IAM token"
        ).forEach(t -> ctx.check(t, false));
        markCrudFailed(ctx);
        List.of(
            "RDS-IAM invalid token rejected",
            "RDS-IAM master password still works",
            "RDS-IAM DeleteDBInstance"
        ).forEach(t -> ctx.check(t, false));
    }

    private static void markCrudFailed(TestContext ctx) {
        List.of(
            "RDS-IAM CREATE TABLE",
            "RDS-IAM INSERT 4 rows",
            "RDS-IAM SELECT COUNT(*) = 4",
            "RDS-IAM SELECT WHERE product='Gadget X'",
            "RDS-IAM UPDATE Widget A qty+=5",
            "RDS-IAM SELECT updated qty=15",
            "RDS-IAM DELETE qty<5 (1 row)",
            "RDS-IAM SELECT COUNT(*) after DELETE = 3",
            "RDS-IAM SELECT SUM(qty*price)",
            "RDS-IAM DROP TABLE"
        ).forEach(t -> ctx.check(t, false));
    }
}
