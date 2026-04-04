package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.Map;

@FlociTestGroup
public class DynamoDbScanConditionTests implements TestGroup {

    @Override
    public String name() { return "dynamodb-scan-condition"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- DynamoDB Scan Condition Filter Tests ---");

        try (DynamoDbClient ddb = DynamoDbClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            String tableName = "scan-condition-test";

            try {
                ddb.createTable(CreateTableRequest.builder()
                        .tableName(tableName)
                        .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
                        .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build())
                        .billingMode(BillingMode.PAY_PER_REQUEST)
                        .build());

                for (int i = 1; i <= 5; i++) {
                    ddb.putItem(PutItemRequest.builder().tableName(tableName).item(Map.of(
                            "pk", AttributeValue.fromS("item-" + i),
                            "score", AttributeValue.fromN(String.valueOf(i * 10)),
                            "name", AttributeValue.fromS("name-" + i)
                    )).build());
                }

                // EQ
                ScanResponse eqResp = ddb.scan(ScanRequest.builder().tableName(tableName)
                        .scanFilter(Map.of("score", Condition.builder()
                                .comparisonOperator(ComparisonOperator.EQ)
                                .attributeValueList(AttributeValue.fromN("30"))
                                .build()))
                        .build());
                ctx.check("DDB Scan ScanFilter EQ", eqResp.count() == 1);

                // GT
                ScanResponse gtResp = ddb.scan(ScanRequest.builder().tableName(tableName)
                        .scanFilter(Map.of("score", Condition.builder()
                                .comparisonOperator(ComparisonOperator.GT)
                                .attributeValueList(AttributeValue.fromN("30"))
                                .build()))
                        .build());
                ctx.check("DDB Scan ScanFilter GT", gtResp.count() == 2);

                // LE
                ScanResponse leResp = ddb.scan(ScanRequest.builder().tableName(tableName)
                        .scanFilter(Map.of("score", Condition.builder()
                                .comparisonOperator(ComparisonOperator.LE)
                                .attributeValueList(AttributeValue.fromN("30"))
                                .build()))
                        .build());
                ctx.check("DDB Scan ScanFilter LE", leResp.count() == 3);

                // BEGINS_WITH
                ScanResponse bwResp = ddb.scan(ScanRequest.builder().tableName(tableName)
                        .scanFilter(Map.of("name", Condition.builder()
                                .comparisonOperator(ComparisonOperator.BEGINS_WITH)
                                .attributeValueList(AttributeValue.fromS("name-"))
                                .build()))
                        .build());
                ctx.check("DDB Scan ScanFilter BEGINS_WITH", bwResp.count() == 5);

                // BETWEEN
                ScanResponse btResp = ddb.scan(ScanRequest.builder().tableName(tableName)
                        .scanFilter(Map.of("score", Condition.builder()
                                .comparisonOperator(ComparisonOperator.BETWEEN)
                                .attributeValueList(AttributeValue.fromN("20"), AttributeValue.fromN("40"))
                                .build()))
                        .build());
                ctx.check("DDB Scan ScanFilter BETWEEN", btResp.count() == 3);

                // Multiple conditions (AND)
                ScanResponse multiResp = ddb.scan(ScanRequest.builder().tableName(tableName)
                        .scanFilter(Map.of(
                                "score", Condition.builder()
                                        .comparisonOperator(ComparisonOperator.GE)
                                        .attributeValueList(AttributeValue.fromN("30"))
                                        .build(),
                                "name", Condition.builder()
                                        .comparisonOperator(ComparisonOperator.EQ)
                                        .attributeValueList(AttributeValue.fromS("name-3"))
                                        .build()))
                        .build());
                ctx.check("DDB Scan ScanFilter multiple conditions", multiResp.count() == 1);

            } catch (Exception e) {
                ctx.check("DDB Scan ScanFilter", false, e);
            } finally {
                deleteSilently(ddb, tableName);
            }
        }
    }

    private void deleteSilently(DynamoDbClient ddb, String tableName) {
        try {
            ddb.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
        } catch (Exception ignored) {}
    }
}
