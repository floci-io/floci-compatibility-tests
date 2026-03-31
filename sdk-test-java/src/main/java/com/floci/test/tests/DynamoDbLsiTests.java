package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.List;
import java.util.Map;

@FlociTestGroup
public class DynamoDbLsiTests implements TestGroup {

    @Override
    public String name() { return "dynamodb-lsi"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- DynamoDB LSI Tests ---");

        try (DynamoDbClient ddb = DynamoDbClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            String tableName = "lsi-test-table";
            String indexName = "lsi-index";

            // 1. CreateTable with LSI
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
                                AttributeDefinition.builder().attributeName("lsi-sk").attributeType(ScalarAttributeType.S).build()
                        )
                        .localSecondaryIndexes(
                                LocalSecondaryIndex.builder()
                                        .indexName(indexName)
                                        .keySchema(
                                                KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
                                                KeySchemaElement.builder().attributeName("lsi-sk").keyType(KeyType.RANGE).build()
                                        )
                                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                                        .build()
                        )
                        .provisionedThroughput(ProvisionedThroughput.builder()
                                .readCapacityUnits(5L).writeCapacityUnits(5L).build())
                        .build());
                ctx.check("DDB LSI CreateTable", true);
            } catch (Exception e) {
                ctx.check("DDB LSI CreateTable", false, e);
                return; // Can't proceed without table
            }

            // 2. Put Items
            try {
                ddb.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(Map.of(
                                "pk", AttributeValue.builder().s("user-1").build(),
                                "sk", AttributeValue.builder().s("order-1").build(),
                                "lsi-sk", AttributeValue.builder().s("2023-01-01").build(),
                                "status", AttributeValue.builder().s("shipped").build()
                        ))
                        .build());
                ddb.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(Map.of(
                                "pk", AttributeValue.builder().s("user-1").build(),
                                "sk", AttributeValue.builder().s("order-2").build(),
                                "lsi-sk", AttributeValue.builder().s("2023-01-05").build(),
                                "status", AttributeValue.builder().s("pending").build()
                        ))
                        .build());
                ddb.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(Map.of(
                                "pk", AttributeValue.builder().s("user-2").build(),
                                "sk", AttributeValue.builder().s("order-3").build(),
                                "lsi-sk", AttributeValue.builder().s("2023-01-03").build(),
                                "status", AttributeValue.builder().s("cancelled").build()
                        ))
                        .build());
                ctx.check("DDB LSI PutItem", true);
            } catch (Exception e) {
                ctx.check("DDB LSI PutItem", false, e);
            }

            // 3. Query using LSI
            try {
                QueryResponse queryResp = ddb.query(QueryRequest.builder()
                        .tableName(tableName)
                        .indexName(indexName)
                        .keyConditionExpression("pk = :pkVal AND #lsiSk > :date")
                        .expressionAttributeNames(Map.of("#lsiSk", "lsi-sk"))
                        .expressionAttributeValues(Map.of(
                                ":pkVal", AttributeValue.builder().s("user-1").build(),
                                ":date", AttributeValue.builder().s("2023-01-02").build()
                        ))
                        .build());
                
                ctx.check("DDB LSI Query count", queryResp.count() == 1);
                if (queryResp.count() == 1) {
                    ctx.check("DDB LSI Query content", 
                        "order-2".equals(queryResp.items().get(0).get("sk").s()));
                }
            } catch (Exception e) {
                ctx.check("DDB LSI Query", false, e);
            }

            // Cleanup
            try {
                ddb.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
            } catch (Exception ignored) {}

        } catch (Exception e) {
            ctx.check("DDB LSI Client", false, e);
        }
    }
}
