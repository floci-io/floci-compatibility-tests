package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.services.apigateway.ApiGatewayClient;
import software.amazon.awssdk.services.apigateway.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * End-to-end compatibility tests: API Gateway → Step Functions (JSONata) → DynamoDB.
 *
 * <p>Exercises all five CRUDL operations via HTTP through a deployed API Gateway stage,
 * backed by Express Step Functions state machines using JSONata and optimised DynamoDB
 * integrations.
 *
 * <p>Run against real AWS:
 * <pre>
 *   FLOCI_TARGET=aws \
 *   SFN_ROLE_ARN=arn:aws:iam::123456789012:role/sfn-role \
 *   APIGW_ROLE_ARN=arn:aws:iam::123456789012:role/apigw-sfn-role \
 *     mvn compile exec:java -Dexec.args="apigw-sfn-jsonata-crudl"
 * </pre>
 */
@FlociTestGroup
public class ApigwSfnJsonataCrudlTests implements TestGroup {

    private static final String TABLE_BASE = "apigw-sfn-crudl";
    private static final String STAGE      = "v1";

    @Override
    public String name() { return "apigw-sfn-jsonata-crudl"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- API Gateway → SFN JSONata → DynamoDB CRUDL Tests ---");

        String sfnRoleArn = ctx.isRealAws()
                ? System.getenv("SFN_ROLE_ARN")
                : "arn:aws:iam::000000000000:role/sfn-role";
        String apigwRoleArn = ctx.isRealAws()
                ? System.getenv("APIGW_ROLE_ARN")
                : "arn:aws:iam::000000000000:role/apigw-role";

        if (ctx.isRealAws() && (sfnRoleArn == null || apigwRoleArn == null)) {
            System.out.println("  SKIP  SFN_ROLE_ARN or APIGW_ROLE_ARN not set");
            return;
        }

        String tableName = TABLE_BASE + "-" + System.currentTimeMillis();

        var ddbBuilder   = DynamoDbClient.builder().region(ctx.region);
        var sfnBuilder   = SfnClient.builder().region(ctx.region);
        var apigwBuilder = ApiGatewayClient.builder().region(ctx.region);

        if (!ctx.isRealAws()) {
            ddbBuilder.endpointOverride(ctx.endpoint).credentialsProvider(ctx.credentials);
            sfnBuilder.endpointOverride(ctx.endpoint).credentialsProvider(ctx.credentials)
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .putAdvancedOption(SdkAdvancedClientOption.DISABLE_HOST_PREFIX_INJECTION, true)
                            .build());
            apigwBuilder.endpointOverride(ctx.endpoint).credentialsProvider(ctx.credentials);
        }

        try (DynamoDbClient ddb   = ddbBuilder.build();
             SfnClient      sfn   = sfnBuilder.build();
             ApiGatewayClient apigw = apigwBuilder.build()) {

            // ── Setup (no checks) ────────────────────────────────────────────
            createTable(ddb, tableName);
            if (ctx.isRealAws()) Thread.sleep(2000);

            String createArn = createSm(sfn, sfnRoleArn, tableName, "create", smCreate(tableName));
            String readArn   = createSm(sfn, sfnRoleArn, tableName, "read",   smRead(tableName));
            String updateArn = createSm(sfn, sfnRoleArn, tableName, "update", smUpdate(tableName));
            String deleteArn = createSm(sfn, sfnRoleArn, tableName, "delete", smDelete(tableName));
            String listArn   = createSm(sfn, sfnRoleArn, tableName, "list",   smList(tableName));

            if (createArn == null || readArn == null || updateArn == null
                    || deleteArn == null || listArn == null) return;

            if (ctx.isRealAws()) Thread.sleep(2000);

            String apiId = buildApi(apigw, ctx.region.id(), apigwRoleArn,
                    createArn, readArn, updateArn, deleteArn, listArn);
            String deployId = apigw.createDeployment(b -> b.restApiId(apiId)).id();
            apigw.createStage(b -> b.restApiId(apiId).stageName(STAGE).deploymentId(deployId));

            if (ctx.isRealAws()) Thread.sleep(3000);

            String base = ctx.isRealAws()
                    ? "https://" + apiId + ".execute-api." + ctx.region.id() + ".amazonaws.com/" + STAGE
                    : ctx.endpoint + "/execute-api/" + apiId + "/" + STAGE;

            HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();

            // ── 1. Create ────────────────────────────────────────────────────
            try {
                HttpResponse<String> resp = post(http, base + "/items",
                        "{\"id\":\"item-1\",\"name\":\"Widget\",\"value\":\"blue\"}");
                ctx.check("APIGW-SFN Create", resp.statusCode() == 200 && resp.body().contains("item-1"));
            } catch (Exception e) { ctx.check("APIGW-SFN Create", false, e); }

            // ── 2. Read ──────────────────────────────────────────────────────
            try {
                HttpResponse<String> resp = get(http, base + "/items/item-1");
                ctx.check("APIGW-SFN Read", resp.statusCode() == 200
                        && resp.body().contains("Widget") && resp.body().contains("blue"));
            } catch (Exception e) { ctx.check("APIGW-SFN Read", false, e); }

            // ── 3. Update ────────────────────────────────────────────────────
            try {
                HttpResponse<String> update = put(http, base + "/items/item-1",
                        "{\"name\":\"Widget Pro\",\"value\":\"green\"}");
                HttpResponse<String> verify = get(http, base + "/items/item-1");
                ctx.check("APIGW-SFN Update", update.statusCode() == 200
                        && verify.body().contains("Widget Pro") && verify.body().contains("green"));
            } catch (Exception e) { ctx.check("APIGW-SFN Update", false, e); }

            // ── 4. List ──────────────────────────────────────────────────────
            try {
                HttpResponse<String> resp = get(http, base + "/items");
                ctx.check("APIGW-SFN List", resp.statusCode() == 200 && resp.body().contains("item-1"));
            } catch (Exception e) { ctx.check("APIGW-SFN List", false, e); }

            // ── 5. Delete ────────────────────────────────────────────────────
            try {
                HttpResponse<String> resp = delete(http, base + "/items/item-1");
                ctx.check("APIGW-SFN Delete", resp.statusCode() == 200);
            } catch (Exception e) { ctx.check("APIGW-SFN Delete", false, e); }

            // ── 6. Read after Delete ─────────────────────────────────────────
            try {
                HttpResponse<String> resp = get(http, base + "/items/item-1");
                ctx.check("APIGW-SFN Read after Delete", resp.statusCode() == 200
                        && resp.body().contains("false") && !resp.body().contains("Widget"));
            } catch (Exception e) { ctx.check("APIGW-SFN Read after Delete", false, e); }

            // ── Cleanup ──────────────────────────────────────────────────────
            for (String arn : List.of(createArn, readArn, updateArn, deleteArn, listArn)) {
                try { sfn.deleteStateMachine(b -> b.stateMachineArn(arn)); } catch (Exception ignored) {}
            }
            try { apigw.deleteRestApi(b -> b.restApiId(apiId)); } catch (Exception ignored) {}
            try { ddb.deleteTable(b -> b.tableName(tableName)); } catch (Exception ignored) {}

        } catch (Exception e) {
            ctx.check("APIGW-SFN setup", false, e);
        }
    }

    // ── State machine definitions ─────────────────────────────────────────────

    private String smCreate(String table) {
        return """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "PutItem",
                  "States": {
                    "PutItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::dynamodb:putItem",
                      "Arguments": {
                        "TableName": "TABLE",
                        "Item": {
                          "pk": {"S": "{% $states.input.id %}"},
                          "sk": {"S": "item"},
                          "name": {"S": "{% $states.input.name %}"},
                          "value": {"S": "{% $states.input.value %}"}
                        }
                      },
                      "Output": {"id": "{% $states.input.id %}", "created": true},
                      "End": true
                    }
                  }
                }""".replace("TABLE", table);
    }

    private String smRead(String table) {
        return """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "GetItem",
                  "States": {
                    "GetItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::dynamodb:getItem",
                      "Arguments": {
                        "TableName": "TABLE",
                        "Key": {
                          "pk": {"S": "{% $states.input.id %}"},
                          "sk": {"S": "item"}
                        }
                      },
                      "Output": "{% $exists($states.result.Item) ? {\\n  \\"found\\": true,\\n  \\"id\\": $states.result.Item.pk.S,\\n  \\"name\\": $states.result.Item.name.S,\\n  \\"value\\": $states.result.Item.value.S\\n} : {\\"found\\": false} %}",
                      "End": true
                    }
                  }
                }""".replace("TABLE", table);
    }

    private String smUpdate(String table) {
        return """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "UpdateItem",
                  "States": {
                    "UpdateItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::dynamodb:updateItem",
                      "Arguments": {
                        "TableName": "TABLE",
                        "Key": {
                          "pk": {"S": "{% $states.input.id %}"},
                          "sk": {"S": "item"}
                        },
                        "UpdateExpression": "SET #n = :name, #v = :value",
                        "ExpressionAttributeNames": {"#n": "name", "#v": "value"},
                        "ExpressionAttributeValues": {
                          ":name":  {"S": "{% $states.input.name %}"},
                          ":value": {"S": "{% $states.input.value %}"}
                        }
                      },
                      "Output": {"id": "{% $states.input.id %}", "updated": true},
                      "End": true
                    }
                  }
                }""".replace("TABLE", table);
    }

    private String smDelete(String table) {
        return """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "DeleteItem",
                  "States": {
                    "DeleteItem": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::dynamodb:deleteItem",
                      "Arguments": {
                        "TableName": "TABLE",
                        "Key": {
                          "pk": {"S": "{% $states.input.id %}"},
                          "sk": {"S": "item"}
                        }
                      },
                      "Output": {"id": "{% $states.input.id %}", "deleted": true},
                      "End": true
                    }
                  }
                }""".replace("TABLE", table);
    }

    private String smList(String table) {
        return """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "Scan",
                  "States": {
                    "Scan": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::aws-sdk:dynamodb:scan",
                      "Arguments": {
                        "TableName": "TABLE"
                      },
                      "Output": {
                        "count": "{% $states.result.Count %}",
                        "items": "{% [$states.result.Items.{\\"id\\": pk.S, \\"name\\": name.S, \\"value\\": value.S}] %}"
                      },
                      "End": true
                    }
                  }
                }""".replace("TABLE", table);
    }

    // ── Setup helpers ─────────────────────────────────────────────────────────

    private void createTable(DynamoDbClient ddb, String tableName) {
        try {
            ddb.createTable(b -> b
                    .tableName(tableName)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build())
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("pk")
                                    .attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("sk")
                                    .attributeType(ScalarAttributeType.S).build())
                    .billingMode(BillingMode.PAY_PER_REQUEST));
        } catch (ResourceInUseException ignored) {}
    }

    private String createSm(SfnClient sfn, String roleArn, String tableName,
                             String op, String definition) {
        try {
            String name = TABLE_BASE + "-" + op + "-" + tableName.substring(tableName.lastIndexOf('-') + 1);
            return sfn.createStateMachine(b -> b
                    .name(name)
                    .definition(definition)
                    .type(StateMachineType.EXPRESS)
                    .roleArn(roleArn)).stateMachineArn();
        } catch (Exception e) {
            return null;
        }
    }

    private String buildApi(ApiGatewayClient apigw, String region, String roleArn,
                             String createArn, String readArn, String updateArn,
                             String deleteArn, String listArn) {
        String apiId = apigw.createRestApi(b -> b.name(TABLE_BASE + "-" + System.currentTimeMillis())).id();
        String rootId = apigw.getResources(b -> b.restApiId(apiId)).items()
                .stream().filter(r -> "/".equals(r.path())).findFirst()
                .map(Resource::id).orElseThrow();

        String itemsId = apigw.createResource(b -> b.restApiId(apiId).parentId(rootId).pathPart("items")).id();
        String itemId  = apigw.createResource(b -> b.restApiId(apiId).parentId(itemsId).pathPart("{id}")).id();

        String sfnUri = "arn:aws:apigateway:" + region + ":states:action/StartSyncExecution";

        wireMethod(apigw, apiId, itemsId, "POST",   sfnUri, roleArn, createArn,
                "{\"input\": \"$util.escapeJavaScript($input.json('$'))\",\"stateMachineArn\": \"" + createArn + "\"}");
        wireMethod(apigw, apiId, itemsId, "GET",    sfnUri, roleArn, listArn,
                "{\"input\": \"{}\",\"stateMachineArn\": \"" + listArn + "\"}");
        wireMethod(apigw, apiId, itemId,  "GET",    sfnUri, roleArn, readArn,
                "{\"input\": \"{\\\"id\\\": \\\"$input.params('id')\\\"}\",\"stateMachineArn\": \"" + readArn + "\"}");
        wireMethod(apigw, apiId, itemId,  "PUT",    sfnUri, roleArn, updateArn,
                "#set($b = $input.path('$'))\n{\"input\": \"{\\\"id\\\": \\\"$input.params('id')\\\","
                + "\\\"name\\\": \\\"$b.name\\\",\\\"value\\\": \\\"$b.value\\\"}\","
                + "\"stateMachineArn\": \"" + updateArn + "\"}");
        wireMethod(apigw, apiId, itemId,  "DELETE", sfnUri, roleArn, deleteArn,
                "{\"input\": \"{\\\"id\\\": \\\"$input.params('id')\\\"}\",\"stateMachineArn\": \"" + deleteArn + "\"}");

        return apiId;
    }

    private void wireMethod(ApiGatewayClient apigw, String apiId, String resourceId,
                             String httpMethod, String uri, String roleArn,
                             String smArn, String reqTemplate) {
        apigw.putMethod(b -> b.restApiId(apiId).resourceId(resourceId)
                .httpMethod(httpMethod).authorizationType("NONE"));
        apigw.putIntegration(b -> b.restApiId(apiId).resourceId(resourceId)
                .httpMethod(httpMethod).type(IntegrationType.AWS)
                .integrationHttpMethod("POST").uri(uri).credentials(roleArn)
                .requestTemplates(Map.of("application/json", reqTemplate)));
        apigw.putMethodResponse(b -> b.restApiId(apiId).resourceId(resourceId)
                .httpMethod(httpMethod).statusCode("200"));
        apigw.putIntegrationResponse(b -> b.restApiId(apiId).resourceId(resourceId)
                .httpMethod(httpMethod).statusCode("200")
                .responseTemplates(Map.of("application/json", "$input.path('$.output')")));
    }

    // ── HTTP helpers ──────────────────────────────────────────────────────────

    private HttpResponse<String> get(HttpClient http, String url) throws Exception {
        return http.send(HttpRequest.newBuilder().uri(URI.create(url)).GET()
                .timeout(Duration.ofSeconds(20)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(HttpClient http, String url, String body) throws Exception {
        return http.send(HttpRequest.newBuilder().uri(URI.create(url))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(20)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> put(HttpClient http, String url, String body) throws Exception {
        return http.send(HttpRequest.newBuilder().uri(URI.create(url))
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(20)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> delete(HttpClient http, String url) throws Exception {
        return http.send(HttpRequest.newBuilder().uri(URI.create(url))
                .DELETE().timeout(Duration.ofSeconds(20)).build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
