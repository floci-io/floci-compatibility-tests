package com.floci.test.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.SfnClientBuilder;
import software.amazon.awssdk.services.sfn.model.*;

import java.util.List;
import java.util.Map;

public class StepFunctionsDynamoDbTests implements TestGroup {

    private static final String ROLE_ARN = System.getenv("SFN_ROLE_ARN") != null
            ? System.getenv("SFN_ROLE_ARN")
            : "arn:aws:iam::000000000000:role/service-role/test-role";

    private static final boolean USE_REAL_AWS = "aws".equalsIgnoreCase(System.getenv("SFN_TARGET"));

    private static final String TABLE_NAME = "sfn-dynamo-test-" + System.currentTimeMillis();
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String name() { return "sfn-dynamodb"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- Step Functions DynamoDB Integration Tests ---");
        if (USE_REAL_AWS) System.out.println("  (targeting real AWS)");

        DynamoDbClientBuilder ddbBuilder = DynamoDbClient.builder().region(ctx.region);
        SfnClientBuilder sfnBuilder = SfnClient.builder().region(ctx.region);
        if (USE_REAL_AWS) {
            ddbBuilder.credentialsProvider(DefaultCredentialsProvider.create());
            sfnBuilder.credentialsProvider(DefaultCredentialsProvider.create());
        } else {
            ddbBuilder.endpointOverride(ctx.endpoint).credentialsProvider(ctx.credentials);
            sfnBuilder.endpointOverride(ctx.endpoint).credentialsProvider(ctx.credentials);
        }

        try (DynamoDbClient ddb = ddbBuilder.build();
             SfnClient sfn = sfnBuilder.build()) {

            // Create test table
            createTable(ddb);

            try {
                // Optimized integration tests
                testOptimizedPutItem(ctx, sfn);
                testOptimizedGetItem(ctx, sfn, ddb);
                testOptimizedUpdateItem(ctx, sfn);
                testOptimizedDeleteItem(ctx, sfn, ddb);
                testOptimizedGetItemNotFound(ctx, sfn);
                testOptimizedPutAndGetWithJsonata(ctx, sfn);

                // AWS SDK integration tests
                testAwsSdkPutItem(ctx, sfn);
                testAwsSdkGetItem(ctx, sfn, ddb);
                testAwsSdkUpdateItem(ctx, sfn);
                testAwsSdkDeleteItem(ctx, sfn, ddb);
                testAwsSdkQuery(ctx, sfn);
                testAwsSdkScan(ctx, sfn);
                testAwsSdkBatchWriteAndGet(ctx, sfn);
                testAwsSdkTransactWriteAndGet(ctx, sfn);
                testAwsSdkTableLifecycle(ctx, sfn);

                // Error path tests
                testAwsSdkErrorTableNotFound(ctx, sfn);
                testAwsSdkErrorConditionFailed(ctx, sfn);
                testOptimizedErrorTableNotFound(ctx, sfn);
            } catch (Exception e) {
                ctx.check("SFN-DynamoDB unexpected error", false, e);
            } finally {
                try { ddb.deleteTable(b -> b.tableName(TABLE_NAME)); } catch (Exception ignored) {}
            }

        } catch (Exception e) {
            ctx.check("SFN-DynamoDB setup/teardown", false, e);
        }
    }

    // ── Optimized Integration Tests ───────────────────────────────────

    private void testOptimizedPutItem(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "StartAt": "PutItem",
                  "States": {
                    "PutItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::dynamodb:putItem",
                      "Parameters": {
                        "TableName": "%s",
                        "Item": {
                          "pk": {"S": "user-1"},
                          "name": {"S": "Alice"},
                          "age": {"N": "30"}
                        }
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        try {
            String output = executeAndWait(sfn, "opt-put", definition, "{}");
            JsonNode result = mapper.readTree(output);
            // Optimized PutItem returns the Item written if ReturnValues used, 
            // but here it's empty {} by default
            ctx.check("Optimized PutItem", result != null && result.isObject());
        } catch (Exception e) {
            ctx.check("Optimized PutItem", false, e);
        }
    }

    private void testOptimizedGetItem(TestContext ctx, SfnClient sfn, DynamoDbClient ddb) {
        ddb.putItem(b -> b.tableName(TABLE_NAME).item(Map.of(
                "pk", AttributeValue.builder().s("user-2").build(),
                "name", AttributeValue.builder().s("Bob").build(),
                "age", AttributeValue.builder().n("25").build()
        )));

        String definition = """
                {
                  "StartAt": "GetItem",
                  "States": {
                    "GetItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::dynamodb:getItem",
                      "Parameters": {
                        "TableName": "%s",
                        "Key": {
                          "pk": {"S": "user-2"}
                        }
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        try {
            String output = executeAndWait(sfn, "opt-get", definition, "{}");
            JsonNode result = mapper.readTree(output);
            boolean ok = result.has("Item")
                    && "Bob".equals(result.path("Item").path("name").path("S").asText())
                    && "25".equals(result.path("Item").path("age").path("N").asText());
            ctx.check("Optimized GetItem", ok);
        } catch (Exception e) {
            ctx.check("Optimized GetItem", false, e);
        }
    }

    private void testOptimizedUpdateItem(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "StartAt": "UpdateItem",
                  "States": {
                    "UpdateItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::dynamodb:updateItem",
                      "Parameters": {
                        "TableName": "%s",
                        "Key": {
                          "pk": {"S": "user-1"}
                        },
                        "UpdateExpression": "SET age = :newAge",
                        "ExpressionAttributeValues": {
                          ":newAge": {"N": "31"}
                        },
                        "ReturnValues": "ALL_NEW"
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        try {
            String output = executeAndWait(sfn, "opt-update", definition, "{}");
            JsonNode result = mapper.readTree(output);
            boolean ok = result.has("Attributes")
                    && "31".equals(result.path("Attributes").path("age").path("N").asText())
                    && "Alice".equals(result.path("Attributes").path("name").path("S").asText());
            ctx.check("Optimized UpdateItem", ok);
        } catch (Exception e) {
            ctx.check("Optimized UpdateItem", false, e);
        }
    }

    private void testOptimizedDeleteItem(TestContext ctx, SfnClient sfn, DynamoDbClient ddb) {
        ddb.putItem(b -> b.tableName(TABLE_NAME).item(Map.of(
                "pk", AttributeValue.builder().s("user-del").build(),
                "name", AttributeValue.builder().s("ToDelete").build()
        )));

        String definition = """
                {
                  "StartAt": "DeleteItem",
                  "States": {
                    "DeleteItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::dynamodb:deleteItem",
                      "Parameters": {
                        "TableName": "%s",
                        "Key": {
                          "pk": {"S": "user-del"}
                        }
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        try {
            String output = executeAndWait(sfn, "opt-del", definition, "{}");
            JsonNode result = mapper.readTree(output);
            ctx.check("Optimized DeleteItem response", result != null && result.isObject());

            var resp = ddb.getItem(b -> b.tableName(TABLE_NAME).key(Map.of(
                    "pk", AttributeValue.builder().s("user-del").build()
            )));
            ctx.check("Optimized DeleteItem verified", !resp.hasItem() || resp.item().isEmpty());
        } catch (Exception e) {
            ctx.check("Optimized DeleteItem", false, e);
        }
    }

    private void testOptimizedGetItemNotFound(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "StartAt": "GetItem",
                  "States": {
                    "GetItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::dynamodb:getItem",
                      "Parameters": {
                        "TableName": "%s",
                        "Key": {
                          "pk": {"S": "nonexistent-key"}
                        }
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        try {
            String output = executeAndWait(sfn, "opt-get-missing", definition, "{}");
            JsonNode result = mapper.readTree(output);
            // Should succeed but with no Item field (or null Item)
            boolean ok = result != null
                    && (!result.has("Item") || result.get("Item").isNull());
            ctx.check("Optimized GetItem not found", ok);
        } catch (Exception e) {
            ctx.check("Optimized GetItem not found", false, e);
        }
    }

    private void testOptimizedPutAndGetWithJsonata(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "Put",
                  "States": {
                    "Put": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::dynamodb:putItem",
                      "Arguments": {
                        "TableName": "TABLENAME",
                        "Item": {
                          "pk": {"S": "{%% $states.input.id %%}"},
                          "name": {"S": "{%% $states.input.name %%}"},
                          "score": {"N": "{%% $string($states.input.score) %%}"}
                        }
                      },
                      "Output": "{%% $states.input %%}",
                      "Next": "Get"
                    },
                    "Get": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::dynamodb:getItem",
                      "Arguments": {
                        "TableName": "TABLENAME",
                        "Key": {
                          "pk": {"S": "{%% $states.input.id %%}"}
                        }
                      },
                      "Output": "{%% $states.result.Item %%}",
                      "End": true
                    }
                  }
                }"""
                .replace("TABLENAME", TABLE_NAME)
                .replace("{%%", "{%")
                .replace("%%}", "%}");
        String input = """
                {"id": "user-jsonata", "name": "Charlie", "score": 99}""";

        try {
            String output = executeAndWait(sfn, "opt-jsonata", definition, input);
            JsonNode result = mapper.readTree(output);
            boolean ok = result != null
                    && "Charlie".equals(result.path("name").path("S").asText())
                    && "99".equals(result.path("score").path("N").asText());
            ctx.check("Optimized PutItem+GetItem with JSONata", ok);
        } catch (Exception e) {
            ctx.check("Optimized PutItem+GetItem with JSONata", false, e);
        }
    }

    // ── AWS SDK Integration Tests ─────────────────────────────────────

    private void testAwsSdkPutItem(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "StartAt": "PutItem",
                  "States": {
                    "PutItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:putItem",
                      "Parameters": {
                        "TableName": "%s",
                        "Item": {
                          "pk": {"S": "sdk-user-1"},
                          "name": {"S": "Diana"},
                          "age": {"N": "28"}
                        }
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        try {
            String output = executeAndWait(sfn, "sdk-put", definition, "{}");
            JsonNode result = mapper.readTree(output);
            // PutItem returns empty object (no Attributes unless ReturnValues specified)
            ctx.check("SDK PutItem", result != null && result.isObject() && !result.has("Attributes"));
        } catch (Exception e) {
            ctx.check("SDK PutItem", false, e);
        }
    }

    private void testAwsSdkGetItem(TestContext ctx, SfnClient sfn, DynamoDbClient ddb) {
        ddb.putItem(b -> b.tableName(TABLE_NAME).item(Map.of(
                "pk", AttributeValue.builder().s("sdk-user-2").build(),
                "name", AttributeValue.builder().s("Eve").build(),
                "age", AttributeValue.builder().n("35").build()
        )));

        String definition = """
                {
                  "StartAt": "GetItem",
                  "States": {
                    "GetItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:getItem",
                      "Parameters": {
                        "TableName": "%s",
                        "Key": {
                          "pk": {"S": "sdk-user-2"}
                        }
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        try {
            String output = executeAndWait(sfn, "sdk-get", definition, "{}");
            JsonNode result = mapper.readTree(output);
            boolean ok = result.has("Item")
                    && "Eve".equals(result.path("Item").path("name").path("S").asText())
                    && "35".equals(result.path("Item").path("age").path("N").asText());
            ctx.check("SDK GetItem", ok);
        } catch (Exception e) {
            ctx.check("SDK GetItem", false, e);
        }
    }

    private void testAwsSdkUpdateItem(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "StartAt": "UpdateItem",
                  "States": {
                    "UpdateItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:updateItem",
                      "Parameters": {
                        "TableName": "%s",
                        "Key": {
                          "pk": {"S": "sdk-user-1"}
                        },
                        "UpdateExpression": "SET age = :newAge",
                        "ExpressionAttributeValues": {
                          ":newAge": {"N": "29"}
                        },
                        "ReturnValues": "ALL_NEW"
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        try {
            String output = executeAndWait(sfn, "sdk-update", definition, "{}");
            JsonNode result = mapper.readTree(output);
            boolean ok = result.has("Attributes")
                    && "29".equals(result.path("Attributes").path("age").path("N").asText())
                    && "Diana".equals(result.path("Attributes").path("name").path("S").asText());
            ctx.check("SDK UpdateItem", ok);
        } catch (Exception e) {
            ctx.check("SDK UpdateItem", false, e);
        }
    }

    private void testAwsSdkDeleteItem(TestContext ctx, SfnClient sfn, DynamoDbClient ddb) {
        ddb.putItem(b -> b.tableName(TABLE_NAME).item(Map.of(
                "pk", AttributeValue.builder().s("sdk-del").build(),
                "name", AttributeValue.builder().s("ToDelete").build()
        )));

        String definition = """
                {
                  "StartAt": "DeleteItem",
                  "States": {
                    "DeleteItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:deleteItem",
                      "Parameters": {
                        "TableName": "%s",
                        "Key": {
                          "pk": {"S": "sdk-del"}
                        }
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        try {
            String output = executeAndWait(sfn, "sdk-del", definition, "{}");
            JsonNode result = mapper.readTree(output);
            ctx.check("SDK DeleteItem response", result != null && result.isObject());

            var resp = ddb.getItem(b -> b.tableName(TABLE_NAME).key(Map.of(
                    "pk", AttributeValue.builder().s("sdk-del").build()
            )));
            ctx.check("SDK DeleteItem verified", !resp.hasItem() || resp.item().isEmpty());
        } catch (Exception e) {
            ctx.check("SDK DeleteItem", false, e);
        }
    }

    private void testAwsSdkQuery(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "StartAt": "Query",
                  "States": {
                    "Query": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:query",
                      "Parameters": {
                        "TableName": "%s",
                        "KeyConditionExpression": "pk = :pk",
                        "ExpressionAttributeValues": {
                          ":pk": {"S": "sdk-user-1"}
                        }
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        try {
            String output = executeAndWait(sfn, "sdk-query", definition, "{}");
            JsonNode result = mapper.readTree(output);
            boolean ok = result.has("Items")
                    && result.has("Count")
                    && result.get("Count").asInt() > 0
                    && result.get("Items").isArray();
            ctx.check("SDK Query", ok);
        } catch (Exception e) {
            ctx.check("SDK Query", false, e);
        }
    }

    private void testAwsSdkScan(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "StartAt": "Scan",
                  "States": {
                    "Scan": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:scan",
                      "Parameters": {
                        "TableName": "%s"
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        try {
            String output = executeAndWait(sfn, "sdk-scan", definition, "{}");
            JsonNode result = mapper.readTree(output);
            boolean ok = result.has("Items")
                    && result.has("Count")
                    && result.has("ScannedCount")
                    && result.get("Count").asInt() > 0
                    && result.get("Items").isArray();
            ctx.check("SDK Scan", ok);
        } catch (Exception e) {
            ctx.check("SDK Scan", false, e);
        }
    }

    private void testAwsSdkBatchWriteAndGet(TestContext ctx, SfnClient sfn) {
        String writeDef = """
                {
                  "StartAt": "BatchWrite",
                  "States": {
                    "BatchWrite": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:batchWriteItem",
                      "Parameters": {
                        "RequestItems": {
                          "%s": [
                            { "PutRequest": { "Item": { "pk": {"S": "batch-1"}, "val": {"S": "a"} } } },
                            { "PutRequest": { "Item": { "pk": {"S": "batch-2"}, "val": {"S": "b"} } } }
                          ]
                        }
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        String getDef = """
                {
                  "StartAt": "BatchGet",
                  "States": {
                    "BatchGet": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:batchGetItem",
                      "Parameters": {
                        "RequestItems": {
                          "%s": {
                            "Keys": [
                              { "pk": {"S": "batch-1"} },
                              { "pk": {"S": "batch-2"} }
                            ]
                          }
                        }
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        try {
            String writeOutput = executeAndWait(sfn, "sdk-batch-write", writeDef, "{}");
            JsonNode writeResult = mapper.readTree(writeOutput);
            ctx.check("SDK BatchWriteItem response", writeResult.has("UnprocessedItems"));

            String getOutput = executeAndWait(sfn, "sdk-batch-get", getDef, "{}");
            JsonNode getResult = mapper.readTree(getOutput);
            boolean ok = getResult.has("Responses")
                    && getResult.path("Responses").has(TABLE_NAME)
                    && getResult.path("Responses").path(TABLE_NAME).size() == 2;
            ctx.check("SDK BatchGetItem response", ok);
        } catch (Exception e) {
            ctx.check("SDK BatchWriteAndGet", false, e);
        }
    }

    private void testAwsSdkTransactWriteAndGet(TestContext ctx, SfnClient sfn) {
        String writeDef = """
                {
                  "StartAt": "TransactWrite",
                  "States": {
                    "TransactWrite": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:transactWriteItems",
                      "Parameters": {
                        "TransactItems": [
                          { "Put": { "TableName": "%s", "Item": { "pk": {"S": "tx-1"}, "val": {"S": "x"} } } },
                          { "Put": { "TableName": "%s", "Item": { "pk": {"S": "tx-2"}, "val": {"S": "y"} } } }
                        ]
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME, TABLE_NAME);

        String getDef = """
                {
                  "StartAt": "TransactGet",
                  "States": {
                    "TransactGet": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:transactGetItems",
                      "Parameters": {
                        "TransactItems": [
                          { "Get": { "TableName": "%s", "Key": { "pk": {"S": "tx-1"} } } },
                          { "Get": { "TableName": "%s", "Key": { "pk": {"S": "tx-2"} } } }
                        ]
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME, TABLE_NAME);

        try {
            String writeOutput = executeAndWait(sfn, "sdk-tx-write", writeDef, "{}");
            JsonNode writeResult = mapper.readTree(writeOutput);
            ctx.check("SDK TransactWriteItems response", writeResult != null && writeResult.isObject());

            String getOutput = executeAndWait(sfn, "sdk-tx-get", getDef, "{}");
            JsonNode getResult = mapper.readTree(getOutput);
            boolean ok = getResult.has("Responses")
                    && getResult.get("Responses").isArray()
                    && getResult.get("Responses").size() == 2
                    && getResult.get("Responses").get(0).has("Item");
            ctx.check("SDK TransactGetItems response", ok);
        } catch (Exception e) {
            ctx.check("SDK TransactWriteAndGet", false, e);
        }
    }

    private void testAwsSdkTableLifecycle(TestContext ctx, SfnClient sfn) {
        // Build a DDB client for the waiter (real AWS needs table to be ACTIVE before delete)
        DynamoDbClientBuilder waitDdbBuilder = DynamoDbClient.builder().region(ctx.region);
        if (USE_REAL_AWS) {
            waitDdbBuilder.credentialsProvider(DefaultCredentialsProvider.create());
        } else {
            waitDdbBuilder.endpointOverride(ctx.endpoint).credentialsProvider(ctx.credentials);
        }

        String tempTableName = "sfn-lifecycle-" + System.currentTimeMillis();
        String createDef = """
                {
                  "StartAt": "Create",
                  "States": {
                    "Create": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:createTable",
                      "Parameters": {
                        "TableName": "%s",
                        "KeySchema": [ { "AttributeName": "id", "KeyType": "HASH" } ],
                        "AttributeDefinitions": [ { "AttributeName": "id", "AttributeType": "S" } ],
                        "BillingMode": "PAY_PER_REQUEST"
                      },
                      "End": true
                    }
                  }
                }""".formatted(tempTableName);

        String deleteDef = """
                {
                  "StartAt": "Delete",
                  "States": {
                    "Delete": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:deleteTable",
                      "Parameters": {
                        "TableName": "%s"
                      },
                      "End": true
                    }
                  }
                }""".formatted(tempTableName);

        try {
            String createOutput = executeAndWait(sfn, "sdk-create-tbl", createDef, "{}");
            JsonNode createResult = mapper.readTree(createOutput);
            ctx.check("SDK CreateTable response", createResult.has("TableDescription"));

            // Wait for table to be ACTIVE on real AWS before deleting
            if (USE_REAL_AWS) {
                try (DynamoDbClient waitDdb = waitDdbBuilder.build()) {
                    waitDdb.waiter().waitUntilTableExists(b -> b.tableName(tempTableName));
                }
            }

            String deleteOutput = executeAndWait(sfn, "sdk-delete-tbl", deleteDef, "{}");
            JsonNode deleteResult = mapper.readTree(deleteOutput);
            ctx.check("SDK DeleteTable response", deleteResult.has("TableDescription"));
        } catch (Exception e) {
            ctx.check("SDK TableLifecycle", false, e);
        }
    }

    // ── Error Path Tests ───────────────────────────────────────────────

    private void testAwsSdkErrorTableNotFound(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "StartAt": "GetItem",
                  "States": {
                    "GetItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:getItem",
                      "Parameters": {
                        "TableName": "nonexistent-table-12345",
                        "Key": { "pk": {"S": "x"} }
                      },
                      "End": true
                    }
                  }
                }""";

        try {
            String smArn = createStateMachine(sfn, "sdk-err-notfound", definition);
            String execArn = startExecution(sfn, smArn, "{}");
            DescribeExecutionResponse resp = waitForExecution(sfn, execArn);
            sfn.deleteStateMachine(b -> b.stateMachineArn(smArn));

            boolean failed = resp.status() == ExecutionStatus.FAILED;
            String error = resp.error();
            // AWS SDK integration errors use DynamoDb. prefix (mixed case)
            boolean correctPrefix = error != null && error.startsWith("DynamoDb.");
            boolean correctError = error != null && error.contains("ResourceNotFoundException");
            ctx.check("SDK error: table not found fails", failed);
            ctx.check("SDK error: prefix is DynamoDb.", correctPrefix);
            ctx.check("SDK error: ResourceNotFoundException", correctError);
            System.out.println("        -> error=" + error + " cause=" + resp.cause());
        } catch (Exception e) {
            ctx.check("SDK error: table not found", false, e);
        }
    }

    private void testAwsSdkErrorConditionFailed(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "StartAt": "PutItem",
                  "States": {
                    "PutItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:putItem",
                      "Parameters": {
                        "TableName": "%s",
                        "Item": { "pk": {"S": "condition-test"} },
                        "ConditionExpression": "attribute_not_exists(pk)"
                      },
                      "End": true
                    }
                  }
                }""".formatted(TABLE_NAME);

        try {
            // Put the item first so the condition fails on second put
            executeAndWait(sfn, "sdk-err-cond-setup", definition, "{}");

            // Second put should fail with ConditionalCheckFailedException
            String smArn = createStateMachine(sfn, "sdk-err-cond", definition);
            String execArn = startExecution(sfn, smArn, "{}");
            DescribeExecutionResponse resp = waitForExecution(sfn, execArn);
            sfn.deleteStateMachine(b -> b.stateMachineArn(smArn));

            boolean failed = resp.status() == ExecutionStatus.FAILED;
            String error = resp.error();
            boolean correctPrefix = error != null && error.startsWith("DynamoDb.");
            boolean correctError = error != null && error.contains("ConditionalCheckFailed");
            ctx.check("SDK error: condition fails", failed);
            ctx.check("SDK error: condition prefix DynamoDb.", correctPrefix);
            ctx.check("SDK error: ConditionalCheckFailed", correctError);
            System.out.println("        -> error=" + error + " cause=" + resp.cause());
        } catch (Exception e) {
            ctx.check("SDK error: condition failed", false, e);
        }
    }

    private void testOptimizedErrorTableNotFound(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "StartAt": "GetItem",
                  "States": {
                    "GetItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::dynamodb:getItem",
                      "Parameters": {
                        "TableName": "nonexistent-table-12345",
                        "Key": { "pk": {"S": "x"} }
                      },
                      "End": true
                    }
                  }
                }""";

        try {
            String smArn = createStateMachine(sfn, "opt-err-notfound", definition);
            String execArn = startExecution(sfn, smArn, "{}");
            DescribeExecutionResponse resp = waitForExecution(sfn, execArn);
            sfn.deleteStateMachine(b -> b.stateMachineArn(smArn));

            boolean failed = resp.status() == ExecutionStatus.FAILED;
            String error = resp.error();
            // Optimized integration errors use DynamoDB. prefix (uppercase DB)
            boolean correctPrefix = error != null && error.startsWith("DynamoDB.");
            boolean correctError = error != null && error.contains("ResourceNotFoundException");
            ctx.check("Optimized error: table not found fails", failed);
            ctx.check("Optimized error: prefix is DynamoDB.", correctPrefix);
            ctx.check("Optimized error: ResourceNotFoundException", correctError);
            System.out.println("        -> error=" + error + " cause=" + resp.cause());
        } catch (Exception e) {
            ctx.check("Optimized error: table not found", false, e);
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────

    private void createTable(DynamoDbClient ddb) {
        ddb.createTable(b -> b
                .tableName(TABLE_NAME)
                .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST));

        if (USE_REAL_AWS) {
            ddb.waiter().waitUntilTableExists(b -> b.tableName(TABLE_NAME));
        }
    }

    private String executeAndWait(SfnClient sfn, String nameSuffix, String definition, String input) {
        String smArn = createStateMachine(sfn, nameSuffix, definition);
        try {
            String execArn = startExecution(sfn, smArn, input);
            DescribeExecutionResponse resp = waitForExecution(sfn, execArn);

            if (resp.status() != ExecutionStatus.SUCCEEDED) {
                throw new RuntimeException("Execution " + resp.status()
                        + " error=" + resp.error() + " cause=" + resp.cause());
            }
            return resp.output();
        } finally {
            sfn.deleteStateMachine(b -> b.stateMachineArn(smArn));
        }
    }

    private String createStateMachine(SfnClient sfn, String nameSuffix, String definition) {
        CreateStateMachineResponse resp = sfn.createStateMachine(b -> b
                .name("test-" + nameSuffix + "-" + System.currentTimeMillis())
                .definition(definition)
                .roleArn(ROLE_ARN));
        return resp.stateMachineArn();
    }

    private String startExecution(SfnClient sfn, String smArn, String input) {
        StartExecutionResponse resp = sfn.startExecution(b -> b
                .stateMachineArn(smArn)
                .input(input));
        return resp.executionArn();
    }

    private DescribeExecutionResponse waitForExecution(SfnClient sfn, String execArn) {
        for (int i = 0; i < 30; i++) {
            DescribeExecutionResponse resp = sfn.describeExecution(b -> b.executionArn(execArn));
            if (resp.status() != ExecutionStatus.RUNNING) {
                return resp;
            }
            try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        throw new RuntimeException("Execution did not complete within timeout");
    }
}
