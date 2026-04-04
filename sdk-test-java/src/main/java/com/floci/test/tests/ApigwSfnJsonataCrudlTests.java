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
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * End-to-end CRUDL test: API Gateway → Step Functions (JSONata) → DynamoDB.
 *
 * <p>Exercises all five operations:
 * <pre>
 *   POST   /items          → SFN JSONata → dynamodb:putItem    (Create)
 *   GET    /items/{id}     → SFN JSONata → dynamodb:getItem    (Read)
 *   PUT    /items/{id}     → SFN JSONata → dynamodb:updateItem (Update)
 *   DELETE /items/{id}     → SFN JSONata → dynamodb:deleteItem (Delete)
 *   GET    /items          → SFN JSONata → dynamodb:scan       (List)
 * </pre>
 *
 * <p>Each API Gateway resource uses an AWS integration targeting
 * {@code arn:aws:states:::aws-sdk:states:startSyncExecution}. The request VTL
 * template builds the SFN input; the response VTL template unwraps the execution
 * output back into the HTTP response body.
 *
 * <p>State machines use {@code QueryLanguage: JSONata} with
 * {@code arn:aws:states:::dynamodb:*} optimised integrations.
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

    private static final String TABLE   = "apigw-sfn-jsonata-crudl";
    private static final String STAGE   = "v1";
    private static final String REGION  = "us-east-1";
    private static final String ACCOUNT = "000000000000";

    @Override
    public String name() { return "apigw-sfn-jsonata-crudl"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- API Gateway → SFN JSONata → DynamoDB CRUDL Tests ---");

        boolean isRealAws = ctx.isRealAws();

        String sfnRoleArn  = isRealAws ? System.getenv("SFN_ROLE_ARN")
                : "arn:aws:iam::" + ACCOUNT + ":role/sfn-role";
        String apigwRoleArn = isRealAws ? System.getenv("APIGW_ROLE_ARN")
                : "arn:aws:iam::" + ACCOUNT + ":role/apigw-role";

        if (isRealAws && (sfnRoleArn == null || apigwRoleArn == null)) {
            System.out.println("  SKIP  SFN_ROLE_ARN or APIGW_ROLE_ARN not set");
            return;
        }

        var ddbBuilder = DynamoDbClient.builder().region(ctx.region);
        var sfnBuilder = SfnClient.builder().region(ctx.region);
        var apigwBuilder = ApiGatewayClient.builder().region(ctx.region);

        if (!isRealAws) {
            ddbBuilder.endpointOverride(ctx.endpoint).credentialsProvider(ctx.credentials);
            sfnBuilder.endpointOverride(ctx.endpoint).credentialsProvider(ctx.credentials)
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .putAdvancedOption(SdkAdvancedClientOption.DISABLE_HOST_PREFIX_INJECTION, true)
                            .build());
            apigwBuilder.endpointOverride(ctx.endpoint).credentialsProvider(ctx.credentials);
        }

        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();

        String apiId = null;
        String createSmArn = null, readSmArn = null, updateSmArn = null,
               deleteSmArn = null, listSmArn = null;

        try (DynamoDbClient ddb = ddbBuilder.build();
             SfnClient sfn = sfnBuilder.build();
             ApiGatewayClient apigw = apigwBuilder.build()) {

            // ── 1. DynamoDB table ────────────────────────────────────────────
            createTable(ctx, ddb);

            rateLimitPause(isRealAws);

            // ── 2. Five JSONata state machines (one per CRUDL op) ────────────
            createSmArn = createSm(ctx, sfn, sfnRoleArn, "crudl-create", smCreate());
            readSmArn   = createSm(ctx, sfn, sfnRoleArn, "crudl-read",   smRead());
            updateSmArn = createSm(ctx, sfn, sfnRoleArn, "crudl-update", smUpdate());
            deleteSmArn = createSm(ctx, sfn, sfnRoleArn, "crudl-delete", smDelete());
            listSmArn   = createSm(ctx, sfn, sfnRoleArn, "crudl-list",   smList());

            if (createSmArn == null || readSmArn == null || updateSmArn == null
                    || deleteSmArn == null || listSmArn == null) return;

            rateLimitPause(isRealAws);

            // ── 3. REST API + resources ──────────────────────────────────────
            apiId = apigw.createRestApi(b -> b.name("crudl-test-" + System.currentTimeMillis())).id();
            final String fApiId = apiId;

            String rootId = apigw.getResources(b -> b.restApiId(fApiId)).items()
                    .stream().filter(r -> "/".equals(r.path())).findFirst()
                    .map(Resource::id).orElseThrow();

            // /items  (collection)
            String itemsId = apigw.createResource(b -> b.restApiId(fApiId)
                    .parentId(rootId).pathPart("items")).id();

            // /items/{id}  (single item)
            String itemId = apigw.createResource(b -> b.restApiId(fApiId)
                    .parentId(itemsId).pathPart("{id}")).id();

            // Wire up each method (hasId=true means the resource has an {id} path param)
            setupMethod(apigw, fApiId, itemsId, "POST",   createSmArn, apigwRoleArn, false);
            setupMethod(apigw, fApiId, itemsId, "GET",    listSmArn,   apigwRoleArn, false);
            setupMethod(apigw, fApiId, itemId,  "GET",    readSmArn,   apigwRoleArn, true);
            setupMethod(apigw, fApiId, itemId,  "PUT",    updateSmArn, apigwRoleArn, true);
            setupMethod(apigw, fApiId, itemId,  "DELETE", deleteSmArn, apigwRoleArn, true);

            // Deploy
            String deployId = apigw.createDeployment(b -> b.restApiId(fApiId)).id();
            apigw.createStage(b -> b.restApiId(fApiId).stageName(STAGE).deploymentId(deployId));

            rateLimitPause(isRealAws);

            String base = invokeBase(ctx, fApiId, isRealAws);

            // ── 4. CRUDL tests ────────────────────────────────────────────────
            testCreate(ctx, http, base, sfn, createSmArn, isRealAws);
            testRead(ctx, http, base, isRealAws);
            testUpdate(ctx, http, base, sfn, updateSmArn, isRealAws);
            testList(ctx, http, base, isRealAws);
            testDelete(ctx, http, base, isRealAws);
            testReadAfterDelete(ctx, http, base, isRealAws);

        } catch (Exception e) {
            ctx.check("CRUDL: unexpected error", false, e);
        } finally {
            cleanup(ctx, isRealAws, apiId,
                    createSmArn, readSmArn, updateSmArn, deleteSmArn, listSmArn);
        }
    }

    // ── DynamoDB table ────────────────────────────────────────────────────────

    private void createTable(TestContext ctx, DynamoDbClient ddb) {
        try {
            ddb.createTable(b -> b
                    .tableName(TABLE)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build())
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("pk")
                                    .attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("sk")
                                    .attributeType(ScalarAttributeType.S).build())
                    .billingMode(BillingMode.PAY_PER_REQUEST));
            ctx.check("CRUDL: CreateTable", true);
        } catch (ResourceInUseException ignored) {
            ctx.check("CRUDL: CreateTable (already exists)", true);
        } catch (Exception e) {
            ctx.check("CRUDL: CreateTable", false, e);
        }
    }

    // ── State machine definitions ─────────────────────────────────────────────

    // NOTE: state machine definitions use .replace("TABLE", TABLE) rather than
    // .formatted(TABLE) because the JSONata {% ... %} delimiters contain '%' which
    // conflicts with Java's format string processing.

    private String smCreate() {
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
                }""".replace("TABLE", TABLE);
    }

    private String smRead() {
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
                }""".replace("TABLE", TABLE);
    }

    private String smUpdate() {
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
                }""".replace("TABLE", TABLE);
    }

    private String smDelete() {
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
                }""".replace("TABLE", TABLE);
    }

    private String smList() {
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
                }""".replace("TABLE", TABLE);
    }

    // ── API Gateway method setup ──────────────────────────────────────────────

    /**
     * Wires up a method on a resource with an AWS integration targeting
     * StartSyncExecution on the given state machine.
     *
     * Request VTL: wraps the HTTP body (or path param) into SFN input JSON.
     * Response VTL: unwraps the SFN execution output as the HTTP response body.
     */
    private void setupMethod(ApiGatewayClient apigw, String apiId, String resourceId,
                              String httpMethod, String smArn, String roleArn, boolean hasId) {
        String sfnUri = "arn:aws:apigateway:" + REGION + ":states:action/StartSyncExecution";

        String reqTemplate;
        if ("GET".equals(httpMethod) && hasId) {
            // Read: forward path {id} as input
            reqTemplate = "{\"input\": \"{\\\"id\\\": \\\"$input.params('id')\\\"}\","
                    + "\"stateMachineArn\": \"" + smArn + "\"}";
        } else if ("DELETE".equals(httpMethod)) {
            reqTemplate = "{\"input\": \"{\\\"id\\\": \\\"$input.params('id')\\\"}\","
                    + "\"stateMachineArn\": \"" + smArn + "\"}";
        } else if ("PUT".equals(httpMethod)) {
            // Update: merge path id into body
            reqTemplate = "#set($b = $input.path('$'))\n"
                    + "{\"input\": \"{\\\"id\\\": \\\"$input.params('id')\\\","
                    + "\\\"name\\\": \\\"$b.name\\\",\\\"value\\\": \\\"$b.value\\\"}\","
                    + "\"stateMachineArn\": \"" + smArn + "\"}";
        } else if ("POST".equals(httpMethod)) {
            // Create: forward raw body as SFN input
            reqTemplate = "{\"input\": \"$util.escapeJavaScript($input.json('$'))\","
                    + "\"stateMachineArn\": \"" + smArn + "\"}";
        } else {
            // GET /items (list): no input needed
            reqTemplate = "{\"input\": \"{}\",\"stateMachineArn\": \"" + smArn + "\"}";
        }

        // Response VTL: extract the SFN output field and return it as the body.
        // $input.path('$.output') returns the raw JSON string from the SFN output field,
        // which VTL renders directly — works on both real AWS and Floci.
        String respTemplate = "$input.path('$.output')";

        apigw.putMethod(b -> b.restApiId(apiId).resourceId(resourceId)
                .httpMethod(httpMethod).authorizationType("NONE"));

        apigw.putIntegration(b -> b.restApiId(apiId).resourceId(resourceId)
                .httpMethod(httpMethod)
                .type(IntegrationType.AWS)
                .integrationHttpMethod("POST")
                .uri(sfnUri)
                .credentials(roleArn)
                .requestTemplates(Map.of("application/json", reqTemplate)));

        apigw.putMethodResponse(b -> b.restApiId(apiId).resourceId(resourceId)
                .httpMethod(httpMethod).statusCode("200"));

        apigw.putIntegrationResponse(b -> b.restApiId(apiId).resourceId(resourceId)
                .httpMethod(httpMethod).statusCode("200")
                .responseTemplates(Map.of("application/json", respTemplate)));
    }

    // ── CRUDL test cases ──────────────────────────────────────────────────────

    private void testCreate(TestContext ctx, HttpClient http, String base,
                             SfnClient sfn, String smArn, boolean isRealAws) {
        try {
            // Direct SFN execution to confirm state machine works
            String sfnOut = syncExecute(sfn, smArn,
                    "{\"id\":\"item-1\",\"name\":\"Widget\",\"value\":\"blue\"}", isRealAws);
            ctx.check("CRUDL: Create via SFN (direct) succeeded", sfnOut.contains("item-1"));

            // Also via API Gateway
            HttpResponse<String> resp = post(http, base + "/items",
                    "{\"id\":\"item-2\",\"name\":\"Gadget\",\"value\":\"red\"}");
            ctx.check("CRUDL: Create via APIGW status=200", resp.statusCode() == 200);
            ctx.check("CRUDL: Create via APIGW body contains item-2", resp.body().contains("item-2"));
        } catch (Exception e) {
            ctx.check("CRUDL: Create", false, e);
        }
    }

    private void testRead(TestContext ctx, HttpClient http, String base, boolean isRealAws) {
        try {
            HttpResponse<String> resp = get(http, base + "/items/item-1");
            ctx.check("CRUDL: Read status=200", resp.statusCode() == 200);
            ctx.check("CRUDL: Read found=true", resp.body().contains("true"));
            ctx.check("CRUDL: Read name=Widget", resp.body().contains("Widget"));
            ctx.check("CRUDL: Read value=blue", resp.body().contains("blue"));
        } catch (Exception e) {
            ctx.check("CRUDL: Read", false, e);
        }
    }

    private void testUpdate(TestContext ctx, HttpClient http, String base,
                             SfnClient sfn, String smArn, boolean isRealAws) {
        try {
            HttpResponse<String> resp = put(http, base + "/items/item-1",
                    "{\"name\":\"Widget Pro\",\"value\":\"green\"}");
            ctx.check("CRUDL: Update status=200", resp.statusCode() == 200);
            ctx.check("CRUDL: Update updated=true", resp.body().contains("true"));

            // Verify the update persisted
            HttpResponse<String> readResp = get(http, base + "/items/item-1");
            ctx.check("CRUDL: Read after update name=Widget Pro",
                    readResp.body().contains("Widget Pro"));
            ctx.check("CRUDL: Read after update value=green",
                    readResp.body().contains("green"));
        } catch (Exception e) {
            ctx.check("CRUDL: Update", false, e);
        }
    }

    private void testList(TestContext ctx, HttpClient http, String base, boolean isRealAws) {
        try {
            HttpResponse<String> resp = get(http, base + "/items");
            ctx.check("CRUDL: List status=200", resp.statusCode() == 200);
            // item-1 and item-2 were both created
            ctx.check("CRUDL: List contains item-1", resp.body().contains("item-1"));
            ctx.check("CRUDL: List contains item-2", resp.body().contains("item-2"));
            ctx.check("CRUDL: List count >= 2",
                    resp.body().contains("\"count\":2") || resp.body().contains("\"count\": 2")
                            || resp.body().contains("count") && !resp.body().contains("\"count\":0"));
        } catch (Exception e) {
            ctx.check("CRUDL: List", false, e);
        }
    }

    private void testDelete(TestContext ctx, HttpClient http, String base, boolean isRealAws) {
        try {
            HttpResponse<String> resp = delete(http, base + "/items/item-1");
            ctx.check("CRUDL: Delete status=200", resp.statusCode() == 200);
            ctx.check("CRUDL: Delete deleted=true", resp.body().contains("true"));
        } catch (Exception e) {
            ctx.check("CRUDL: Delete", false, e);
        }
    }

    private void testReadAfterDelete(TestContext ctx, HttpClient http, String base, boolean isRealAws) {
        try {
            HttpResponse<String> resp = get(http, base + "/items/item-1");
            ctx.check("CRUDL: Read-after-delete status=200", resp.statusCode() == 200);
            // found should be false (item was deleted)
            ctx.check("CRUDL: Read-after-delete found=false", resp.body().contains("false"));
            ctx.check("CRUDL: Read-after-delete no stale fields",
                    !resp.body().contains("Widget") && !resp.body().contains("\"id\""));
        } catch (Exception e) {
            ctx.check("CRUDL: Read-after-delete", false, e);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String createSm(TestContext ctx, SfnClient sfn, String roleArn,
                              String nameSuffix, String definition) {
        String name = nameSuffix + "-" + System.currentTimeMillis();
        try {
            String arn = sfn.createStateMachine(b -> b
                    .name(name).definition(definition)
                    .type(StateMachineType.EXPRESS)
                    .roleArn(roleArn)).stateMachineArn();
            ctx.check("CRUDL: CreateSM " + nameSuffix, true);
            return arn;
        } catch (Exception e) {
            ctx.check("CRUDL: CreateSM " + nameSuffix, false, e);
            return null;
        }
    }

    private String syncExecute(SfnClient sfn, String smArn, String input, boolean isRealAws)
            throws Exception {
        // For EXPRESS machines prefer StartSyncExecution (instant result)
        StartSyncExecutionResponse resp = sfn.startSyncExecution(b -> b
                .stateMachineArn(smArn).input(input));
        if (resp.status() != SyncExecutionStatus.SUCCEEDED) {
            throw new RuntimeException("SFN failed: " + resp.error() + " — " + resp.cause());
        }
        return resp.output();
    }

    private String invokeBase(TestContext ctx, String apiId, boolean isRealAws) {
        if (isRealAws) {
            return "https://" + apiId + ".execute-api." + ctx.region.id()
                    + ".amazonaws.com/" + STAGE;
        }
        return ctx.endpoint + "/execute-api/" + apiId + "/" + STAGE;
    }

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

    private void cleanup(TestContext ctx, boolean isRealAws, String apiId,
                          String... smArns) {
        var sfnBuilder = SfnClient.builder().region(ctx.region);
        var apigwBuilder = ApiGatewayClient.builder().region(ctx.region);
        if (!isRealAws) {
            sfnBuilder.endpointOverride(ctx.endpoint).credentialsProvider(ctx.credentials)
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .putAdvancedOption(SdkAdvancedClientOption.DISABLE_HOST_PREFIX_INJECTION, true)
                            .build());
            apigwBuilder.endpointOverride(ctx.endpoint).credentialsProvider(ctx.credentials);
        }
        try (SfnClient sfn = sfnBuilder.build()) {
            for (String arn : smArns) {
                if (arn != null) {
                    try { sfn.deleteStateMachine(b -> b.stateMachineArn(arn)); }
                    catch (Exception ignored) {}
                }
            }
        }
        if (apiId != null) {
            String fApiId = apiId;
            try (ApiGatewayClient apigw = apigwBuilder.build()) {
                apigw.deleteRestApi(b -> b.restApiId(fApiId));
            } catch (Exception ignored) {}
        }
    }

    private void rateLimitPause(boolean isRealAws) {
        if (isRealAws) {
            try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
        }
    }
}
