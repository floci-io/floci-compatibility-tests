package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.List;
import java.util.Map;

@FlociTestGroup
public class DynamoDbScanFilterTests implements TestGroup {

    @Override
    public String name() { return "dynamodb-scan-filter"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- DynamoDB Scan/Query FilterExpression Tests ---");

        try (DynamoDbClient ddb = DynamoDbClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            runScanContainsListTests(ctx, ddb);
            runBoolFilterTests(ctx, ddb);
            runGsiBoolFilterTests(ctx, ddb);
            runNestedAttributeExistsTests(ctx, ddb);
            runContainsStringSetTests(ctx, ddb);
        }
    }

    private void runScanContainsListTests(TestContext ctx, DynamoDbClient ddb) {
        String tableName = "scan-contains-list";
        try {
            ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                    .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .build());

            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("1"),
                    "tags", AttributeValue.fromL(List.of(AttributeValue.fromS("a"), AttributeValue.fromS("b")))
            )).build());
            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("2"),
                    "tags", AttributeValue.fromL(List.of(AttributeValue.fromS("a"), AttributeValue.fromS("c")))
            )).build());
            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("3"),
                    "tags", AttributeValue.fromL(List.of(AttributeValue.fromS("b"), AttributeValue.fromS("c")))
            )).build());

            ScanResponse response = ddb.scan(ScanRequest.builder()
                    .tableName(tableName)
                    .filterExpression("contains(tags, :v)")
                    .expressionAttributeValues(Map.of(":v", AttributeValue.fromS("a")))
                    .build());

            ctx.check("DDB Scan contains() on List returns matching items", response.count() == 2);

        } catch (Exception e) {
            ctx.check("DDB Scan contains() on List returns matching items", false, e);
        } finally {
            deleteSilently(ddb, tableName);
        }
    }

    private void runBoolFilterTests(TestContext ctx, DynamoDbClient ddb) {
        String tableName = "scan-bool-ne";
        try {
            ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                    .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .build());

            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("1"),
                    "deleted", AttributeValue.fromBool(false)
            )).build());
            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("2"),
                    "deleted", AttributeValue.fromBool(true)
            )).build());
            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("3"),
                    "deleted", AttributeValue.fromBool(false)
            )).build());

            ScanResponse response = ddb.scan(ScanRequest.builder()
                    .tableName(tableName)
                    .filterExpression("deleted <> :d")
                    .expressionAttributeValues(Map.of(":d", AttributeValue.fromBool(true)))
                    .build());

            ctx.check("DDB Scan BOOL <> filters correctly", response.count() == 2);

        } catch (Exception e) {
            ctx.check("DDB Scan BOOL <> filters correctly", false, e);
        } finally {
            deleteSilently(ddb, tableName);
        }
    }

    private void runGsiBoolFilterTests(TestContext ctx, DynamoDbClient ddb) {
        String tableName = "query-gsi-bool-ne";
        try {
            ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("grp").attributeType(ScalarAttributeType.S).build()
                    )
                    .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                            .indexName("grp-idx")
                            .keySchema(KeySchemaElement.builder().attributeName("grp").keyType(KeyType.HASH).build())
                            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                            .build())
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .build());

            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("1"), "grp", AttributeValue.fromS("g1"), "deleted", AttributeValue.fromBool(false)
            )).build());
            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("2"), "grp", AttributeValue.fromS("g1"), "deleted", AttributeValue.fromBool(true)
            )).build());
            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("3"), "grp", AttributeValue.fromS("g1"), "deleted", AttributeValue.fromBool(false)
            )).build());

            QueryResponse response = ddb.query(QueryRequest.builder()
                    .tableName(tableName)
                    .indexName("grp-idx")
                    .keyConditionExpression("grp = :g")
                    .filterExpression("deleted <> :d")
                    .expressionAttributeValues(Map.of(
                            ":g", AttributeValue.fromS("g1"),
                            ":d", AttributeValue.fromBool(true)
                    ))
                    .build());

            ctx.check("DDB Query GSI BOOL <> filters correctly", response.count() == 2);

        } catch (Exception e) {
            ctx.check("DDB Query GSI BOOL <> filters correctly", false, e);
        } finally {
            deleteSilently(ddb, tableName);
        }
    }

    private void runNestedAttributeExistsTests(TestContext ctx, DynamoDbClient ddb) {
        String tableName = "scan-nested-attr-exists";
        try {
            ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                    .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .build());

            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("1"),
                    "info", AttributeValue.fromM(Map.of("name", AttributeValue.fromS("Alice")))
            )).build());
            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("2"),
                    "info", AttributeValue.fromM(Map.of())
            )).build());
            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("3"),
                    "info", AttributeValue.fromM(Map.of("name", AttributeValue.fromS("Bob")))
            )).build());

            ScanResponse response = ddb.scan(ScanRequest.builder()
                    .tableName(tableName)
                    .filterExpression("attribute_exists(info.#n)")
                    .expressionAttributeNames(Map.of("#n", "name"))
                    .build());

            ctx.check("DDB Scan attribute_exists on nested Map path", response.count() == 2);

            ScanResponse response2 = ddb.scan(ScanRequest.builder()
                    .tableName(tableName)
                    .filterExpression("attribute_not_exists(info.#n)")
                    .expressionAttributeNames(Map.of("#n", "name"))
                    .build());

            ctx.check("DDB Scan attribute_not_exists on nested Map path", response2.count() == 1);

        } catch (Exception e) {
            ctx.check("DDB Scan attribute_exists on nested Map path", false, e);
            ctx.check("DDB Scan attribute_not_exists on nested Map path", false, e);
        } finally {
            deleteSilently(ddb, tableName);
        }
    }

    private void runContainsStringSetTests(TestContext ctx, DynamoDbClient ddb) {
        String tableName = "scan-contains-ss";
        try {
            ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                    .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .build());

            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("1"),
                    "roles", AttributeValue.fromSs(List.of("admin", "user"))
            )).build());
            ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                    "id", AttributeValue.fromS("2"),
                    "roles", AttributeValue.fromSs(List.of("user"))
            )).build());

            ScanResponse response = ddb.scan(ScanRequest.builder()
                    .tableName(tableName)
                    .filterExpression("contains(roles, :r)")
                    .expressionAttributeValues(Map.of(":r", AttributeValue.fromS("admin")))
                    .build());

            ctx.check("DDB Scan contains() on String Set", response.count() == 1);

        } catch (Exception e) {
            ctx.check("DDB Scan contains() on String Set", false, e);
        } finally {
            deleteSilently(ddb, tableName);
        }
    }

    private void deleteSilently(DynamoDbClient ddb, String tableName) {
        try {
            ddb.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
        } catch (Exception ignored) {}
    }
}
