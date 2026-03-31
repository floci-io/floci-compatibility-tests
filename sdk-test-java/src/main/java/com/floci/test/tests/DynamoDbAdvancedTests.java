package com.floci.test.tests;

import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DynamoDbAdvancedTests implements TestGroup {

    @Override
    public String name() { return "dynamodb-advanced"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- DynamoDB Advanced Tests ---");

        try (DynamoDbClient ddb = DynamoDbClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            runGsiTests(ctx, ddb);
            runPaginationTests(ctx, ddb);
            runQueryFilterPaginationTests(ctx, ddb);
            runConditionExpressionTests(ctx, ddb);
            runTransactTests(ctx, ddb);
            runTtlTests(ctx, ddb);
        }
    }

    private void runGsiTests(TestContext ctx, DynamoDbClient ddb) {
        String tableName = "adv-gsi-table";

        // Create table with GSI
        try {
            ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build()
                    )
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("gsi-pk").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("gsi-sk").attributeType(ScalarAttributeType.S).build()
                    )
                    .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                            .indexName("gsi-index")
                            .keySchema(
                                    KeySchemaElement.builder().attributeName("gsi-pk").keyType(KeyType.HASH).build(),
                                    KeySchemaElement.builder().attributeName("gsi-sk").keyType(KeyType.RANGE).build()
                            )
                            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                            .provisionedThroughput(ProvisionedThroughput.builder()
                                    .readCapacityUnits(5L).writeCapacityUnits(5L).build())
                            .build())
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits(5L).writeCapacityUnits(5L).build())
                    .build());

            // Put 3 items; 2 have gsi-pk="group-A", 1 has no gsi attributes (sparse)
            ddb.putItem(PutItemRequest.builder().tableName(tableName)
                    .item(Map.of(
                            "pk", AttributeValue.fromS("item-1"),
                            "sk", AttributeValue.fromS("rev-1"),
                            "gsi-pk", AttributeValue.fromS("group-A"),
                            "gsi-sk", AttributeValue.fromS("2024-01-01")
                    )).build());

            ddb.putItem(PutItemRequest.builder().tableName(tableName)
                    .item(Map.of(
                            "pk", AttributeValue.fromS("item-2"),
                            "sk", AttributeValue.fromS("rev-1"),
                            "gsi-pk", AttributeValue.fromS("group-A"),
                            "gsi-sk", AttributeValue.fromS("2024-01-02")
                    )).build());

            // Item 3 has no gsi-pk (sparse)
            ddb.putItem(PutItemRequest.builder().tableName(tableName)
                    .item(Map.of(
                            "pk", AttributeValue.fromS("item-3"),
                            "sk", AttributeValue.fromS("rev-1"),
                            "data", AttributeValue.fromS("no-gsi-attrs")
                    )).build());

            // Test 1: GSI query returns only items with matching gsi-pk
            QueryResponse gsiResp = ddb.query(QueryRequest.builder()
                    .tableName(tableName)
                    .indexName("gsi-index")
                    .keyConditionExpression("#gpk = :gpk")
                    .expressionAttributeNames(Map.of("#gpk", "gsi-pk"))
                    .expressionAttributeValues(Map.of(
                            ":gpk", AttributeValue.fromS("group-A")
                    ))
                    .build());
            ctx.check("DDB GSI query returns 2 items", gsiResp.count() == 2);

            // Test 2: GSI sparse — item-3 without gsi-pk is not returned
            boolean item3NotInGsi = gsiResp.items().stream()
                    .noneMatch(item -> "item-3".equals(item.get("pk") != null ? item.get("pk").s() : null));
            ctx.check("DDB GSI sparse index excludes items without GSI key", item3NotInGsi);

        } catch (Exception e) {
            ctx.check("DDB GSI query returns 2 items", false, e);
            ctx.check("DDB GSI sparse index excludes items without GSI key", false, e);
        } finally {
            deleteSilently(ddb, tableName);
        }
    }

    private void runPaginationTests(TestContext ctx, DynamoDbClient ddb) {
        String tableName = "adv-pagination-table";

        try {
            ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build()
                    )
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build()
                    )
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits(5L).writeCapacityUnits(5L).build())
                    .build());

            // Put 5 items with different PKs (for scan pagination)
            for (int i = 1; i <= 5; i++) {
                ddb.putItem(PutItemRequest.builder().tableName(tableName)
                        .item(Map.of(
                                "pk", AttributeValue.fromS("scan-pk-" + i),
                                "sk", AttributeValue.fromS("item"),
                                "idx", AttributeValue.fromN(String.valueOf(i))
                        )).build());
            }

            // Test 3: Scan pagination — scan Limit=2, follow LastEvaluatedKey until 5 total
            int totalScanned = 0;
            Map<String, AttributeValue> lastKey = null;
            do {
                ScanRequest.Builder scanBuilder = ScanRequest.builder()
                        .tableName(tableName)
                        .limit(2);
                if (lastKey != null) {
                    scanBuilder.exclusiveStartKey(lastKey);
                }
                ScanResponse scanResp = ddb.scan(scanBuilder.build());
                totalScanned += scanResp.count();
                lastKey = scanResp.hasLastEvaluatedKey() ? scanResp.lastEvaluatedKey() : null;
            } while (lastKey != null);
            ctx.check("DDB Scan pagination retrieves all 5 items", totalScanned == 5);

            // Add 5 items with same PK for query pagination
            for (int i = 1; i <= 5; i++) {
                ddb.putItem(PutItemRequest.builder().tableName(tableName)
                        .item(Map.of(
                                "pk", AttributeValue.fromS("query-pk"),
                                "sk", AttributeValue.fromS("item-" + String.format("%02d", i)),
                                "idx", AttributeValue.fromN(String.valueOf(i))
                        )).build());
            }

            // Test 4: Query pagination — query Limit=2, paginate until 5 total
            int totalQueried = 0;
            Map<String, AttributeValue> queryLastKey = null;
            do {
                QueryRequest.Builder queryBuilder = QueryRequest.builder()
                        .tableName(tableName)
                        .keyConditionExpression("pk = :pk")
                        .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS("query-pk")))
                        .limit(2);
                if (queryLastKey != null) {
                    queryBuilder.exclusiveStartKey(queryLastKey);
                }
                QueryResponse queryResp = ddb.query(queryBuilder.build());
                totalQueried += queryResp.count();
                queryLastKey = queryResp.hasLastEvaluatedKey() ? queryResp.lastEvaluatedKey() : null;
            } while (queryLastKey != null);
            ctx.check("DDB Query pagination retrieves all 5 items", totalQueried == 5);

        } catch (Exception e) {
            ctx.check("DDB Scan pagination retrieves all 5 items", false, e);
            ctx.check("DDB Query pagination retrieves all 5 items", false, e);
        } finally {
            deleteSilently(ddb, tableName);
        }
    }

    private void runQueryFilterPaginationTests(TestContext ctx, DynamoDbClient ddb) {
        String tableName = "adv-query-filter-table";

        try {
            ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build()
                    )
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build()
                    )
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits(5L).writeCapacityUnits(5L).build())
                    .build());

            ddb.putItem(PutItemRequest.builder().tableName(tableName)
                    .item(Map.of(
                            "pk", AttributeValue.fromS("user-1"),
                            "sk", AttributeValue.fromS("order-001"),
                            "status", AttributeValue.fromS("expired-1"),
                            "expiresAt", AttributeValue.fromN("90")
                    )).build());
            ddb.putItem(PutItemRequest.builder().tableName(tableName)
                    .item(Map.of(
                            "pk", AttributeValue.fromS("user-1"),
                            "sk", AttributeValue.fromS("order-002"),
                            "status", AttributeValue.fromS("alive-1"),
                            "expiresAt", AttributeValue.fromN("100")
                    )).build());
            ddb.putItem(PutItemRequest.builder().tableName(tableName)
                    .item(Map.of(
                            "pk", AttributeValue.fromS("user-1"),
                            "sk", AttributeValue.fromS("order-003"),
                            "status", AttributeValue.fromS("alive-2"),
                            "expiresAt", AttributeValue.fromN("110")
                    )).build());

            QueryRequest baseRequest = QueryRequest.builder()
                    .tableName(tableName)
                    .keyConditionExpression("pk = :pk")
                    .filterExpression("#expires >= :now")
                    .expressionAttributeNames(Map.of("#expires", "expiresAt"))
                    .expressionAttributeValues(Map.of(
                            ":pk", AttributeValue.fromS("user-1"),
                            ":now", AttributeValue.fromN("100")
                    ))
                    .limit(2)
                    .build();

            QueryResponse page1 = ddb.query(baseRequest);
            boolean firstPageMatches = page1.count() == 1
                    && page1.scannedCount() == 2
                    && page1.hasItems()
                    && "alive-1".equals(page1.items().get(0).get("status").s())
                    && page1.hasLastEvaluatedKey()
                    && "order-002".equals(page1.lastEvaluatedKey().get("sk").s());
            ctx.check("DDB Query FilterExpression limit keeps pre-filter page state", firstPageMatches);

            QueryResponse page2 = ddb.query(baseRequest.toBuilder()
                    .exclusiveStartKey(page1.lastEvaluatedKey())
                    .build());
            boolean secondPageMatches = page2.count() == 1
                    && page2.scannedCount() == 1
                    && page2.hasItems()
                    && "alive-2".equals(page2.items().get(0).get("status").s())
                    && !page2.hasLastEvaluatedKey();
            ctx.check("DDB Query FilterExpression pagination resumes from last evaluated key", secondPageMatches);

        } catch (Exception e) {
            ctx.check("DDB Query FilterExpression limit keeps pre-filter page state", false, e);
            ctx.check("DDB Query FilterExpression pagination resumes from last evaluated key", false, e);
        } finally {
            deleteSilently(ddb, tableName);
        }
    }

    private void runConditionExpressionTests(TestContext ctx, DynamoDbClient ddb) {
        String tableName = "adv-condition-table";

        try {
            ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build()
                    )
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build()
                    )
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits(5L).writeCapacityUnits(5L).build())
                    .build());

            // Test 5: attribute_not_exists — first PutItem succeeds, duplicate fails
            try {
                ddb.putItem(PutItemRequest.builder().tableName(tableName)
                        .item(Map.of(
                                "pk", AttributeValue.fromS("unique-item"),
                                "version", AttributeValue.fromN("1")
                        ))
                        .conditionExpression("attribute_not_exists(pk)")
                        .build());
                ctx.check("DDB ConditionExpression attribute_not_exists first put succeeds", true);
            } catch (Exception e) {
                ctx.check("DDB ConditionExpression attribute_not_exists first put succeeds", false, e);
            }

            try {
                ddb.putItem(PutItemRequest.builder().tableName(tableName)
                        .item(Map.of(
                                "pk", AttributeValue.fromS("unique-item"),
                                "version", AttributeValue.fromN("2")
                        ))
                        .conditionExpression("attribute_not_exists(pk)")
                        .build());
                ctx.check("DDB ConditionExpression attribute_not_exists duplicate fails with 400", false);
            } catch (ConditionalCheckFailedException e) {
                ctx.check("DDB ConditionExpression attribute_not_exists duplicate fails with 400", true);
            } catch (Exception e) {
                ctx.check("DDB ConditionExpression attribute_not_exists duplicate fails with 400", false, e);
            }

            // Test 6: attribute_exists — UpdateItem on existing item succeeds
            try {
                ddb.updateItem(UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("pk", AttributeValue.fromS("unique-item")))
                        .updateExpression("SET version = :v")
                        .expressionAttributeValues(Map.of(":v", AttributeValue.fromN("2")))
                        .conditionExpression("attribute_exists(pk)")
                        .build());
                ctx.check("DDB ConditionExpression attribute_exists update succeeds on existing item", true);
            } catch (Exception e) {
                ctx.check("DDB ConditionExpression attribute_exists update succeeds on existing item", false, e);
            }

            // Test 7: attribute_exists — UpdateItem on absent item fails
            try {
                ddb.updateItem(UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("pk", AttributeValue.fromS("nonexistent-item")))
                        .updateExpression("SET version = :v")
                        .expressionAttributeValues(Map.of(":v", AttributeValue.fromN("1")))
                        .conditionExpression("attribute_exists(pk)")
                        .build());
                ctx.check("DDB ConditionExpression attribute_exists update fails on absent item", false);
            } catch (ConditionalCheckFailedException e) {
                ctx.check("DDB ConditionExpression attribute_exists update fails on absent item", true);
            } catch (Exception e) {
                ctx.check("DDB ConditionExpression attribute_exists update fails on absent item", false, e);
            }

            // Test 8: comparison — version matches → succeeds
            try {
                ddb.updateItem(UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("pk", AttributeValue.fromS("unique-item")))
                        .updateExpression("SET version = :newV")
                        .expressionAttributeValues(Map.of(
                                ":newV", AttributeValue.fromN("3"),
                                ":expectedV", AttributeValue.fromN("2")
                        ))
                        .conditionExpression("version = :expectedV")
                        .build());
                ctx.check("DDB ConditionExpression version match succeeds", true);
            } catch (Exception e) {
                ctx.check("DDB ConditionExpression version match succeeds", false, e);
            }

            // Test 9: comparison — version mismatch → fails
            try {
                ddb.updateItem(UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("pk", AttributeValue.fromS("unique-item")))
                        .updateExpression("SET version = :newV")
                        .expressionAttributeValues(Map.of(
                                ":newV", AttributeValue.fromN("99"),
                                ":expectedV", AttributeValue.fromN("1")
                        ))
                        .conditionExpression("version = :expectedV")
                        .build());
                ctx.check("DDB ConditionExpression version mismatch fails", false);
            } catch (ConditionalCheckFailedException e) {
                ctx.check("DDB ConditionExpression version mismatch fails", true);
            } catch (Exception e) {
                ctx.check("DDB ConditionExpression version mismatch fails", false, e);
            }

        } catch (Exception e) {
            ctx.check("DDB ConditionExpression attribute_not_exists first put succeeds", false, e);
            ctx.check("DDB ConditionExpression attribute_not_exists duplicate fails with 400", false, e);
            ctx.check("DDB ConditionExpression attribute_exists update succeeds on existing item", false, e);
            ctx.check("DDB ConditionExpression attribute_exists update fails on absent item", false, e);
            ctx.check("DDB ConditionExpression version match succeeds", false, e);
            ctx.check("DDB ConditionExpression version mismatch fails", false, e);
        } finally {
            deleteSilently(ddb, tableName);
        }
    }

    private void runTransactTests(TestContext ctx, DynamoDbClient ddb) {
        String tableName = "adv-transact-table";

        try {
            ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build()
                    )
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build()
                    )
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits(5L).writeCapacityUnits(5L).build())
                    .build());

            // Seed one item to delete in the transaction
            ddb.putItem(PutItemRequest.builder().tableName(tableName)
                    .item(Map.of(
                            "pk", AttributeValue.fromS("to-delete"),
                            "data", AttributeValue.fromS("exists")
                    )).build());

            // Test 10: TransactWriteItems success — put + delete; both reflected
            try {
                ddb.transactWriteItems(TransactWriteItemsRequest.builder()
                        .transactItems(
                                TransactWriteItem.builder()
                                        .put(Put.builder()
                                                .tableName(tableName)
                                                .item(Map.of(
                                                        "pk", AttributeValue.fromS("new-item"),
                                                        "data", AttributeValue.fromS("added-by-txn")
                                                ))
                                                .build())
                                        .build(),
                                TransactWriteItem.builder()
                                        .delete(Delete.builder()
                                                .tableName(tableName)
                                                .key(Map.of("pk", AttributeValue.fromS("to-delete")))
                                                .build())
                                        .build()
                        )
                        .build());

                GetItemResponse newItemResp = ddb.getItem(GetItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("pk", AttributeValue.fromS("new-item")))
                        .build());
                GetItemResponse deletedItemResp = ddb.getItem(GetItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("pk", AttributeValue.fromS("to-delete")))
                        .build());

                boolean newItemExists = newItemResp.hasItem();
                boolean deletedItemGone = !deletedItemResp.hasItem();
                ctx.check("DDB TransactWriteItems success applies all writes", newItemExists && deletedItemGone);
            } catch (Exception e) {
                ctx.check("DDB TransactWriteItems success applies all writes", false, e);
            }

            // Seed two items for condition-failure test
            ddb.putItem(PutItemRequest.builder().tableName(tableName)
                    .item(Map.of("pk", AttributeValue.fromS("txn-item-a"), "val", AttributeValue.fromN("1")))
                    .build());
            ddb.putItem(PutItemRequest.builder().tableName(tableName)
                    .item(Map.of("pk", AttributeValue.fromS("txn-item-b"), "val", AttributeValue.fromN("1")))
                    .build());

            // Test 11: TransactWriteItems condition failure — neither write applies
            try {
                ddb.transactWriteItems(TransactWriteItemsRequest.builder()
                        .transactItems(
                                TransactWriteItem.builder()
                                        .put(Put.builder()
                                                .tableName(tableName)
                                                .item(Map.of(
                                                        "pk", AttributeValue.fromS("txn-item-a"),
                                                        "val", AttributeValue.fromN("2")
                                                ))
                                                .conditionExpression("val = :expected")
                                                .expressionAttributeValues(Map.of(
                                                        ":expected", AttributeValue.fromN("99")
                                                ))
                                                .build())
                                        .build(),
                                TransactWriteItem.builder()
                                        .put(Put.builder()
                                                .tableName(tableName)
                                                .item(Map.of(
                                                        "pk", AttributeValue.fromS("txn-item-b"),
                                                        "val", AttributeValue.fromN("2")
                                                ))
                                                .build())
                                        .build()
                        )
                        .build());
                ctx.check("DDB TransactWriteItems condition failure rolls back all writes", false);
            } catch (TransactionCanceledException e) {
                // Verify neither item was updated
                GetItemResponse aResp = ddb.getItem(GetItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("pk", AttributeValue.fromS("txn-item-a")))
                        .build());
                GetItemResponse bResp = ddb.getItem(GetItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("pk", AttributeValue.fromS("txn-item-b")))
                        .build());
                boolean aNotChanged = "1".equals(aResp.item().get("val").n());
                boolean bNotChanged = "1".equals(bResp.item().get("val").n());
                ctx.check("DDB TransactWriteItems condition failure rolls back all writes",
                        aNotChanged && bNotChanged);
            } catch (Exception e) {
                ctx.check("DDB TransactWriteItems condition failure rolls back all writes", false, e);
            }

            // Seed items for TransactGetItems
            ddb.putItem(PutItemRequest.builder().tableName(tableName)
                    .item(Map.of("pk", AttributeValue.fromS("get-a"), "data", AttributeValue.fromS("alpha")))
                    .build());
            ddb.putItem(PutItemRequest.builder().tableName(tableName)
                    .item(Map.of("pk", AttributeValue.fromS("get-b"), "data", AttributeValue.fromS("beta")))
                    .build());

            // Test 12: TransactGetItems — results in request order, null for absent
            try {
                TransactGetItemsResponse tgResp = ddb.transactGetItems(TransactGetItemsRequest.builder()
                        .transactItems(
                                TransactGetItem.builder()
                                        .get(software.amazon.awssdk.services.dynamodb.model.Get.builder()
                                                .tableName(tableName)
                                                .key(Map.of("pk", AttributeValue.fromS("get-a")))
                                                .build())
                                        .build(),
                                TransactGetItem.builder()
                                        .get(software.amazon.awssdk.services.dynamodb.model.Get.builder()
                                                .tableName(tableName)
                                                .key(Map.of("pk", AttributeValue.fromS("nonexistent")))
                                                .build())
                                        .build(),
                                TransactGetItem.builder()
                                        .get(software.amazon.awssdk.services.dynamodb.model.Get.builder()
                                                .tableName(tableName)
                                                .key(Map.of("pk", AttributeValue.fromS("get-b")))
                                                .build())
                                        .build()
                        )
                        .build());

                List<ItemResponse> responses = tgResp.responses();
                boolean firstIsA = responses.size() >= 1 && responses.get(0).hasItem()
                        && "alpha".equals(responses.get(0).item().get("data").s());
                boolean secondIsEmpty = responses.size() >= 2 && !responses.get(1).hasItem();
                boolean thirdIsB = responses.size() >= 3 && responses.get(2).hasItem()
                        && "beta".equals(responses.get(2).item().get("data").s());
                ctx.check("DDB TransactGetItems returns items in request order with null for absent",
                        firstIsA && secondIsEmpty && thirdIsB);
            } catch (Exception e) {
                ctx.check("DDB TransactGetItems returns items in request order with null for absent", false, e);
            }

        } catch (Exception e) {
            ctx.check("DDB TransactWriteItems success applies all writes", false, e);
            ctx.check("DDB TransactWriteItems condition failure rolls back all writes", false, e);
            ctx.check("DDB TransactGetItems returns items in request order with null for absent", false, e);
        } finally {
            deleteSilently(ddb, tableName);
        }
    }

    private void runTtlTests(TestContext ctx, DynamoDbClient ddb) {
        String tableName = "adv-ttl-table";

        try {
            ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build()
                    )
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build()
                    )
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits(5L).writeCapacityUnits(5L).build())
                    .build());

            // Test 13: UpdateTimeToLive enable → DescribeTimeToLive returns ENABLED + attribute name
            try {
                ddb.updateTimeToLive(UpdateTimeToLiveRequest.builder()
                        .tableName(tableName)
                        .timeToLiveSpecification(TimeToLiveSpecification.builder()
                                .attributeName("expires")
                                .enabled(true)
                                .build())
                        .build());

                DescribeTimeToLiveResponse descTtl = ddb.describeTimeToLive(
                        DescribeTimeToLiveRequest.builder().tableName(tableName).build());
                boolean isEnabled = descTtl.timeToLiveDescription().timeToLiveStatus() == TimeToLiveStatus.ENABLED;
                boolean correctAttr = "expires".equals(descTtl.timeToLiveDescription().attributeName());
                ctx.check("DDB UpdateTimeToLive enable returns ENABLED with attribute name",
                        isEnabled && correctAttr);
            } catch (Exception e) {
                ctx.check("DDB UpdateTimeToLive enable returns ENABLED with attribute name", false, e);
            }

            // Test 14: UpdateTimeToLive disable → DescribeTimeToLive returns DISABLED
            try {
                ddb.updateTimeToLive(UpdateTimeToLiveRequest.builder()
                        .tableName(tableName)
                        .timeToLiveSpecification(TimeToLiveSpecification.builder()
                                .attributeName("expires")
                                .enabled(false)
                                .build())
                        .build());

                DescribeTimeToLiveResponse descTtl = ddb.describeTimeToLive(
                        DescribeTimeToLiveRequest.builder().tableName(tableName).build());
                boolean isDisabled = descTtl.timeToLiveDescription().timeToLiveStatus() == TimeToLiveStatus.DISABLED;
                ctx.check("DDB UpdateTimeToLive disable returns DISABLED", isDisabled);
            } catch (Exception e) {
                ctx.check("DDB UpdateTimeToLive disable returns DISABLED", false, e);
            }

            // Re-enable TTL for enforcement tests
            ddb.updateTimeToLive(UpdateTimeToLiveRequest.builder()
                    .tableName(tableName)
                    .timeToLiveSpecification(TimeToLiveSpecification.builder()
                            .attributeName("expires")
                            .enabled(true)
                            .build())
                    .build());

            // Test 15: Put item with past epoch TTL → GetItem returns empty
            try {
                ddb.putItem(PutItemRequest.builder().tableName(tableName)
                        .item(Map.of(
                                "pk", AttributeValue.fromS("expired-item"),
                                "expires", AttributeValue.fromN("1")
                        )).build());

                GetItemResponse resp = ddb.getItem(GetItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("pk", AttributeValue.fromS("expired-item")))
                        .build());
                ctx.check("DDB TTL expired item not returned by GetItem", !resp.hasItem());
            } catch (Exception e) {
                ctx.check("DDB TTL expired item not returned by GetItem", false, e);
            }

            // Test 16: Expired item not visible in Scan
            try {
                ScanResponse scanResp = ddb.scan(ScanRequest.builder()
                        .tableName(tableName)
                        .build());
                boolean noneExpired = scanResp.items().stream()
                        .noneMatch(item -> "expired-item".equals(
                                item.containsKey("pk") ? item.get("pk").s() : null));
                ctx.check("DDB TTL expired item not visible in Scan", noneExpired);
            } catch (Exception e) {
                ctx.check("DDB TTL expired item not visible in Scan", false, e);
            }

            // Test 17: Item with future TTL is still returned
            try {
                long futureEpoch = System.currentTimeMillis() / 1000 + 3600;
                ddb.putItem(PutItemRequest.builder().tableName(tableName)
                        .item(Map.of(
                                "pk", AttributeValue.fromS("live-item"),
                                "expires", AttributeValue.fromN(String.valueOf(futureEpoch))
                        )).build());

                GetItemResponse resp = ddb.getItem(GetItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("pk", AttributeValue.fromS("live-item")))
                        .build());
                ctx.check("DDB TTL live item with future TTL returned by GetItem", resp.hasItem());
            } catch (Exception e) {
                ctx.check("DDB TTL live item with future TTL returned by GetItem", false, e);
            }

        } catch (Exception e) {
            ctx.check("DDB UpdateTimeToLive enable returns ENABLED with attribute name", false, e);
            ctx.check("DDB UpdateTimeToLive disable returns DISABLED", false, e);
            ctx.check("DDB TTL expired item not returned by GetItem", false, e);
            ctx.check("DDB TTL expired item not visible in Scan", false, e);
            ctx.check("DDB TTL live item with future TTL returned by GetItem", false, e);
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
