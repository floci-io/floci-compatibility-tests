package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.List;
import java.util.Map;

@FlociTestGroup
public class DynamoDbTests implements TestGroup {

    @Override
    public String name() { return "dynamodb"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- DynamoDB Tests ---");

        try (DynamoDbClient ddb = DynamoDbClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            String tableName = "sdk-test-table";
            String tableArn = null;

            // 1. CreateTable
            try {
                CreateTableResponse createResp = ddb.createTable(CreateTableRequest.builder()
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
                tableArn = createResp.tableDescription().tableArn();
                ctx.check("DDB CreateTable",
                        createResp.tableDescription().tableStatus() == TableStatus.ACTIVE);
            } catch (Exception e) {
                ctx.check("DDB CreateTable", false, e);
            }

            // 2. DescribeTable
            try {
                DescribeTableResponse descResp = ddb.describeTable(
                        DescribeTableRequest.builder().tableName(tableName).build());
                ctx.check("DDB DescribeTable", tableName.equals(descResp.table().tableName()));
            } catch (Exception e) {
                ctx.check("DDB DescribeTable", false, e);
            }

            // 3. ListTables
            try {
                ListTablesResponse listResp = ddb.listTables();
                ctx.check("DDB ListTables", listResp.tableNames().contains(tableName));
            } catch (Exception e) {
                ctx.check("DDB ListTables", false, e);
            }

            // 4. PutItem (multiple items)
            try {
                for (int i = 1; i <= 3; i++) {
                    ddb.putItem(PutItemRequest.builder()
                            .tableName(tableName)
                            .item(Map.of(
                                    "pk", AttributeValue.builder().s("user-1").build(),
                                    "sk", AttributeValue.builder().s("item-" + i).build(),
                                    "data", AttributeValue.builder().s("value-" + i).build()
                            ))
                            .build());
                }
                ddb.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(Map.of(
                                "pk", AttributeValue.builder().s("user-2").build(),
                                "sk", AttributeValue.builder().s("item-1").build(),
                                "data", AttributeValue.builder().s("other-value").build()
                        ))
                        .build());
                ctx.check("DDB PutItem", true);
            } catch (Exception e) {
                ctx.check("DDB PutItem", false, e);
            }

            // 5. GetItem
            try {
                GetItemResponse getResp = ddb.getItem(GetItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of(
                                "pk", AttributeValue.builder().s("user-1").build(),
                                "sk", AttributeValue.builder().s("item-2").build()
                        ))
                        .build());
                ctx.check("DDB GetItem", getResp.hasItem()
                        && "value-2".equals(getResp.item().get("data").s()));
            } catch (Exception e) {
                ctx.check("DDB GetItem", false, e);
            }

            // 6. UpdateItem
            try {
                UpdateItemResponse updateResp = ddb.updateItem(UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of(
                                "pk", AttributeValue.builder().s("user-1").build(),
                                "sk", AttributeValue.builder().s("item-1").build()
                        ))
                        .updateExpression("SET #d = :newVal")
                        .expressionAttributeNames(Map.of("#d", "data"))
                        .expressionAttributeValues(Map.of(
                                ":newVal", AttributeValue.builder().s("updated-value").build()
                        ))
                        .returnValues(ReturnValue.ALL_NEW)
                        .build());
                ctx.check("DDB UpdateItem", "updated-value".equals(
                        updateResp.attributes().get("data").s()));
            } catch (Exception e) {
                ctx.check("DDB UpdateItem", false, e);
            }

            // 7. Query
            try {
                QueryResponse queryResp = ddb.query(QueryRequest.builder()
                        .tableName(tableName)
                        .keyConditionExpression("pk = :pk")
                        .expressionAttributeValues(Map.of(
                                ":pk", AttributeValue.builder().s("user-1").build()
                        ))
                        .build());
                ctx.check("DDB Query", queryResp.count() == 3);
            } catch (Exception e) {
                ctx.check("DDB Query", false, e);
            }

            // 8. Scan
            try {
                ScanResponse scanResp = ddb.scan(ScanRequest.builder().tableName(tableName).build());
                ctx.check("DDB Scan", scanResp.count() == 4);
            } catch (Exception e) {
                ctx.check("DDB Scan", false, e);
            }

            // 9. BatchWriteItem
            try {
                ddb.batchWriteItem(BatchWriteItemRequest.builder()
                        .requestItems(Map.of(tableName, List.of(
                                WriteRequest.builder().putRequest(PutRequest.builder()
                                        .item(Map.of(
                                                "pk", AttributeValue.builder().s("user-3").build(),
                                                "sk", AttributeValue.builder().s("item-1").build(),
                                                "data", AttributeValue.builder().s("batch-value-1").build()
                                        )).build()).build(),
                                WriteRequest.builder().putRequest(PutRequest.builder()
                                        .item(Map.of(
                                                "pk", AttributeValue.builder().s("user-3").build(),
                                                "sk", AttributeValue.builder().s("item-2").build(),
                                                "data", AttributeValue.builder().s("batch-value-2").build()
                                        )).build()).build()
                        )))
                        .build());
                ScanResponse scanResp = ddb.scan(ScanRequest.builder().tableName(tableName).build());
                ctx.check("DDB BatchWriteItem", scanResp.count() == 6);
            } catch (Exception e) {
                ctx.check("DDB BatchWriteItem", false, e);
            }

            // 10. BatchGetItem
            try {
                BatchGetItemResponse batchGetResp = ddb.batchGetItem(BatchGetItemRequest.builder()
                        .requestItems(Map.of(tableName, KeysAndAttributes.builder()
                                .keys(List.of(
                                        Map.of(
                                                "pk", AttributeValue.builder().s("user-1").build(),
                                                "sk", AttributeValue.builder().s("item-1").build()
                                        ),
                                        Map.of(
                                                "pk", AttributeValue.builder().s("user-3").build(),
                                                "sk", AttributeValue.builder().s("item-2").build()
                                        )
                                ))
                                .build()))
                        .build());
                List<Map<String, AttributeValue>> items = batchGetResp.responses().get(tableName);
                ctx.check("DDB BatchGetItem", items != null && items.size() == 2);
            } catch (Exception e) {
                ctx.check("DDB BatchGetItem", false, e);
            }

            // 11. UpdateTable
            try {
                UpdateTableResponse updateTableResp = ddb.updateTable(UpdateTableRequest.builder()
                        .tableName(tableName)
                        .provisionedThroughput(ProvisionedThroughput.builder()
                                .readCapacityUnits(10L).writeCapacityUnits(10L).build())
                        .build());
                ctx.check("DDB UpdateTable",
                        updateTableResp.tableDescription().provisionedThroughput().readCapacityUnits() == 10L);
            } catch (Exception e) {
                ctx.check("DDB UpdateTable", false, e);
            }

            // 12. DescribeTimeToLive
            try {
                DescribeTimeToLiveResponse ttlResp = ddb.describeTimeToLive(
                        DescribeTimeToLiveRequest.builder().tableName(tableName).build());
                ctx.check("DDB DescribeTimeToLive",
                        ttlResp.timeToLiveDescription().timeToLiveStatus() == TimeToLiveStatus.DISABLED);
            } catch (Exception e) {
                ctx.check("DDB DescribeTimeToLive", false, e);
            }

            // 13. TagResource
            try {
                ddb.tagResource(TagResourceRequest.builder()
                        .resourceArn(tableArn)
                        .tags(
                                Tag.builder().key("env").value("test").build(),
                                Tag.builder().key("team").value("backend").build()
                        )
                        .build());
                ctx.check("DDB TagResource", true);
            } catch (Exception e) {
                ctx.check("DDB TagResource", false, e);
            }

            // 14. ListTagsOfResource
            try {
                ListTagsOfResourceResponse tagsResp = ddb.listTagsOfResource(
                        ListTagsOfResourceRequest.builder().resourceArn(tableArn).build());
                boolean envFound = tagsResp.tags().stream()
                        .anyMatch(t -> "env".equals(t.key()) && "test".equals(t.value()));
                boolean teamFound = tagsResp.tags().stream()
                        .anyMatch(t -> "team".equals(t.key()) && "backend".equals(t.value()));
                ctx.check("DDB ListTagsOfResource", envFound && teamFound);
            } catch (Exception e) {
                ctx.check("DDB ListTagsOfResource", false, e);
            }

            // 15. UntagResource
            try {
                ddb.untagResource(UntagResourceRequest.builder()
                        .resourceArn(tableArn).tagKeys("team").build());
                ListTagsOfResourceResponse tagsResp = ddb.listTagsOfResource(
                        ListTagsOfResourceRequest.builder().resourceArn(tableArn).build());
                boolean stillEnv = tagsResp.tags().stream()
                        .anyMatch(t -> "env".equals(t.key()) && "test".equals(t.value()));
                boolean removedTeam = tagsResp.tags().stream().noneMatch(t -> "team".equals(t.key()));
                ctx.check("DDB UntagResource", stillEnv && removedTeam);
            } catch (Exception e) {
                ctx.check("DDB UntagResource", false, e);
            }

            // 16. BatchWriteItem - delete
            try {
                ddb.batchWriteItem(BatchWriteItemRequest.builder()
                        .requestItems(Map.of(tableName, List.of(
                                WriteRequest.builder().deleteRequest(DeleteRequest.builder()
                                        .key(Map.of(
                                                "pk", AttributeValue.builder().s("user-3").build(),
                                                "sk", AttributeValue.builder().s("item-1").build()
                                        )).build()).build(),
                                WriteRequest.builder().deleteRequest(DeleteRequest.builder()
                                        .key(Map.of(
                                                "pk", AttributeValue.builder().s("user-3").build(),
                                                "sk", AttributeValue.builder().s("item-2").build()
                                        )).build()).build()
                        )))
                        .build());
                ScanResponse scanResp = ddb.scan(ScanRequest.builder().tableName(tableName).build());
                ctx.check("DDB BatchWriteItem delete", scanResp.count() == 4);
            } catch (Exception e) {
                ctx.check("DDB BatchWriteItem delete", false, e);
            }

            // 17. DeleteItem
            try {
                ddb.deleteItem(DeleteItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of(
                                "pk", AttributeValue.builder().s("user-2").build(),
                                "sk", AttributeValue.builder().s("item-1").build()
                        ))
                        .build());
                ctx.check("DDB DeleteItem", true);
            } catch (Exception e) {
                ctx.check("DDB DeleteItem", false, e);
            }

            // 18. DeleteTable
            try {
                ddb.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
                ListTablesResponse listResp = ddb.listTables();
                ctx.check("DDB DeleteTable", !listResp.tableNames().contains(tableName));
            } catch (Exception e) {
                ctx.check("DDB DeleteTable", false, e);
            }
        }
    }
}
