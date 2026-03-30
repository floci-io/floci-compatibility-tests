package com.floci.test.tests;

import com.floci.test.TestContext;
import com.floci.test.TestGroup;
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
import java.util.Map;

/**
 * Compatibility tests for API Gateway AWS (non-proxy) integration type.
 * Tests APIGW -> SFN and APIGW -> DynamoDB via VTL mapping templates.
 */
public class ApiGatewayAwsIntegrationTests implements TestGroup {

    @Override
    public String name() { return "apigateway-aws-integration"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- API Gateway AWS Integration Tests ---");

        try (ApiGatewayClient apigw = ApiGatewayClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build();
             DynamoDbClient ddb = DynamoDbClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build();
             SfnClient sfn = SfnClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            HttpClient http = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();

            String tableName = "apigw-aws-compat-test";
            String smArn = null;
            String apiId = null;

            // ── Setup: DynamoDB table ──────────────────────────────
            try {
                ddb.createTable(CreateTableRequest.builder()
                        .tableName(tableName)
                        .keySchema(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build())
                        .attributeDefinitions(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build())
                        .billingMode(BillingMode.PAY_PER_REQUEST)
                        .build());
                ctx.check("AWS Integration: CreateTable", true);
            } catch (Exception e) {
                ctx.check("AWS Integration: CreateTable", false, e);
                return;
            }

            // ── Setup: SFN state machine (Pass type) ──────────────
            try {
                CreateStateMachineResponse smResp = sfn.createStateMachine(b -> b
                        .name("apigw-compat-sm")
                        .definition("{\"StartAt\":\"Echo\",\"States\":{\"Echo\":{\"Type\":\"Pass\",\"End\":true}}}")
                        .roleArn("arn:aws:iam::000000000000:role/test-role"));
                smArn = smResp.stateMachineArn();
                ctx.check("AWS Integration: CreateStateMachine", smArn != null);
            } catch (Exception e) {
                ctx.check("AWS Integration: CreateStateMachine", false, e);
                cleanupTable(ddb, tableName);
                return;
            }

            // ── Setup: REST API ───────────────────────────────────
            String rootId;
            try {
                apiId = apigw.createRestApi(CreateRestApiRequest.builder()
                        .name("aws-integration-compat-test").build()).id();
                rootId = apigw.getResources(GetResourcesRequest.builder().restApiId(apiId).build())
                        .items().stream()
                        .filter(r -> "/".equals(r.path()))
                        .map(Resource::id)
                        .findFirst().orElseThrow();
                ctx.check("AWS Integration: CreateRestApi", true);
            } catch (Exception e) {
                ctx.check("AWS Integration: CreateRestApi", false, e);
                cleanupAll(apigw, apiId, sfn, smArn, ddb, tableName);
                return;
            }

            // ── Test 1: APIGW -> SFN StartExecution ───────────────
            try {
                String startId = apigw.createResource(CreateResourceRequest.builder()
                        .restApiId(apiId).parentId(rootId).pathPart("start").build()).id();

                apigw.putMethod(PutMethodRequest.builder()
                        .restApiId(apiId).resourceId(startId)
                        .httpMethod("POST").authorizationType("NONE").build());

                // VTL template passes body as SFN input
                String vtlTemplate = "{\"stateMachineArn\": \"" + smArn + "\", "
                        + "\"input\": \"$util.escapeJavaScript($input.json('$'))\"}";

                apigw.putIntegration(PutIntegrationRequest.builder()
                        .restApiId(apiId).resourceId(startId)
                        .httpMethod("POST").type(IntegrationType.AWS)
                        .integrationHttpMethod("POST")
                        .uri("arn:aws:apigateway:us-east-1:states:action/StartExecution")
                        .requestTemplates(Map.of("application/json", vtlTemplate))
                        .build());

                apigw.putIntegrationResponse(PutIntegrationResponseRequest.builder()
                        .restApiId(apiId).resourceId(startId)
                        .httpMethod("POST").statusCode("200").selectionPattern("")
                        .responseTemplates(Map.of("application/json", ""))
                        .build());

                // Deploy
                String depId = apigw.createDeployment(CreateDeploymentRequest.builder()
                        .restApiId(apiId).description("v1").build()).id();
                apigw.createStage(CreateStageRequest.builder()
                        .restApiId(apiId).stageName("test").deploymentId(depId).build());

                // Invoke
                String baseUrl = ctx.endpoint + "/execute-api/" + apiId + "/test";
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/start"))
                        .POST(HttpRequest.BodyPublishers.ofString("{\"test\":\"sfn-compat\"}"))
                        .header("Content-Type", "application/json")
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

                ctx.check("AWS Integration: SFN StartExecution status 200", resp.statusCode() == 200);
                ctx.check("AWS Integration: SFN response has executionArn",
                        resp.body().contains("executionArn"));
                ctx.check("AWS Integration: SFN response has startDate",
                        resp.body().contains("startDate"));

                System.out.println("  SFN response: " + resp.body());
            } catch (Exception e) {
                ctx.check("AWS Integration: SFN StartExecution status 200", false, e);
                ctx.check("AWS Integration: SFN response has executionArn", false, e);
                ctx.check("AWS Integration: SFN response has startDate", false, e);
            }

            // ── Test 2: APIGW -> DynamoDB PutItem ─────────────────
            try {
                String itemsId = apigw.createResource(CreateResourceRequest.builder()
                        .restApiId(apiId).parentId(rootId).pathPart("items").build()).id();

                apigw.putMethod(PutMethodRequest.builder()
                        .restApiId(apiId).resourceId(itemsId)
                        .httpMethod("POST").authorizationType("NONE").build());

                // VTL template transforms body into DDB PutItem request
                String ddbTemplate = "{\"TableName\": \"" + tableName + "\", "
                        + "\"Item\": {\"id\": {\"S\": \"$input.path('$.id')\"}, "
                        + "\"message\": {\"S\": \"$input.path('$.message')\"}}}";

                apigw.putIntegration(PutIntegrationRequest.builder()
                        .restApiId(apiId).resourceId(itemsId)
                        .httpMethod("POST").type(IntegrationType.AWS)
                        .integrationHttpMethod("POST")
                        .uri("arn:aws:apigateway:us-east-1:dynamodb:action/PutItem")
                        .requestTemplates(Map.of("application/json", ddbTemplate))
                        .build());

                apigw.putIntegrationResponse(PutIntegrationResponseRequest.builder()
                        .restApiId(apiId).resourceId(itemsId)
                        .httpMethod("POST").statusCode("200").selectionPattern("")
                        .responseTemplates(Map.of("application/json", ""))
                        .build());

                // Redeploy
                String depId = apigw.createDeployment(CreateDeploymentRequest.builder()
                        .restApiId(apiId).description("v2").build()).id();
                apigw.updateStage(UpdateStageRequest.builder()
                        .restApiId(apiId).stageName("test")
                        .patchOperations(PatchOperation.builder()
                                .op(Op.REPLACE).path("/deploymentId").value(depId).build())
                        .build());

                // Invoke
                String baseUrl = ctx.endpoint + "/execute-api/" + apiId + "/test";
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/items"))
                        .POST(HttpRequest.BodyPublishers.ofString("{\"id\":\"compat-1\",\"message\":\"from apigw\"}"))
                        .header("Content-Type", "application/json")
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

                ctx.check("AWS Integration: DDB PutItem status 200", resp.statusCode() == 200);

                // Verify item in DynamoDB
                GetItemResponse getResp = ddb.getItem(GetItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("id", AttributeValue.builder().s("compat-1").build()))
                        .build());
                ctx.check("AWS Integration: DDB item written",
                        getResp.hasItem() && "from apigw".equals(getResp.item().get("message").s()));

                System.out.println("  DDB PutItem response: " + resp.body());
            } catch (Exception e) {
                ctx.check("AWS Integration: DDB PutItem status 200", false, e);
                ctx.check("AWS Integration: DDB item written", false, e);
            }

            // ── Test 3: Response header mapping (static value) ────
            try {
                String headerId = apigw.createResource(CreateResourceRequest.builder()
                        .restApiId(apiId).parentId(rootId).pathPart("headers").build()).id();

                apigw.putMethod(PutMethodRequest.builder()
                        .restApiId(apiId).resourceId(headerId)
                        .httpMethod("POST").authorizationType("NONE").build());

                apigw.putIntegration(PutIntegrationRequest.builder()
                        .restApiId(apiId).resourceId(headerId)
                        .httpMethod("POST").type(IntegrationType.AWS)
                        .integrationHttpMethod("POST")
                        .uri("arn:aws:apigateway:us-east-1:dynamodb:action/ListTables")
                        .requestTemplates(Map.of("application/json", "{}"))
                        .build());

                apigw.putMethodResponse(PutMethodResponseRequest.builder()
                        .restApiId(apiId).resourceId(headerId)
                        .httpMethod("POST").statusCode("200")
                        .responseParameters(Map.of("method.response.header.X-Custom", true))
                        .build());

                apigw.putIntegrationResponse(PutIntegrationResponseRequest.builder()
                        .restApiId(apiId).resourceId(headerId)
                        .httpMethod("POST").statusCode("200").selectionPattern("")
                        .responseParameters(Map.of(
                                "method.response.header.X-Custom", "'compat-value'"))
                        .responseTemplates(Map.of("application/json", ""))
                        .build());

                // Redeploy
                String depId = apigw.createDeployment(CreateDeploymentRequest.builder()
                        .restApiId(apiId).description("v3").build()).id();
                apigw.updateStage(UpdateStageRequest.builder()
                        .restApiId(apiId).stageName("test")
                        .patchOperations(PatchOperation.builder()
                                .op(Op.REPLACE).path("/deploymentId").value(depId).build())
                        .build());

                String baseUrl = ctx.endpoint + "/execute-api/" + apiId + "/test";
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/headers"))
                        .POST(HttpRequest.BodyPublishers.ofString("{}"))
                        .header("Content-Type", "application/json")
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

                ctx.check("AWS Integration: Response header mapping status 200", resp.statusCode() == 200);
                String customHeader = resp.headers().firstValue("X-Custom").orElse(null);
                ctx.check("AWS Integration: X-Custom header = 'compat-value'",
                        "compat-value".equals(customHeader));

                System.out.println("  Header response: " + resp.statusCode() + ", X-Custom=" + customHeader);
            } catch (Exception e) {
                ctx.check("AWS Integration: Response header mapping status 200", false, e);
                ctx.check("AWS Integration: X-Custom header = 'compat-value'", false, e);
            }

            // ── Test 4: Error path with selectionPattern ──────────
            try {
                String errId = apigw.createResource(CreateResourceRequest.builder()
                        .restApiId(apiId).parentId(rootId).pathPart("error-test").build()).id();

                apigw.putMethod(PutMethodRequest.builder()
                        .restApiId(apiId).resourceId(errId)
                        .httpMethod("POST").authorizationType("NONE").build());

                // No request template — passthrough to DDB PutItem
                apigw.putIntegration(PutIntegrationRequest.builder()
                        .restApiId(apiId).resourceId(errId)
                        .httpMethod("POST").type(IntegrationType.AWS)
                        .integrationHttpMethod("POST")
                        .uri("arn:aws:apigateway:us-east-1:dynamodb:action/PutItem")
                        .build());

                // 200 default + 400 for ResourceNotFoundException
                apigw.putIntegrationResponse(PutIntegrationResponseRequest.builder()
                        .restApiId(apiId).resourceId(errId)
                        .httpMethod("POST").statusCode("200").selectionPattern("")
                        .responseTemplates(Map.of("application/json", ""))
                        .build());
                apigw.putIntegrationResponse(PutIntegrationResponseRequest.builder()
                        .restApiId(apiId).resourceId(errId)
                        .httpMethod("POST").statusCode("400")
                        .selectionPattern(".*ResourceNotFoundException.*")
                        .responseTemplates(Map.of("application/json", ""))
                        .build());

                // Redeploy
                String depId = apigw.createDeployment(CreateDeploymentRequest.builder()
                        .restApiId(apiId).description("v4").build()).id();
                apigw.updateStage(UpdateStageRequest.builder()
                        .restApiId(apiId).stageName("test")
                        .patchOperations(PatchOperation.builder()
                                .op(Op.REPLACE).path("/deploymentId").value(depId).build())
                        .build());

                // Call with non-existent table → should match 400 pattern
                String baseUrl = ctx.endpoint + "/execute-api/" + apiId + "/test";
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/error-test"))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"TableName\":\"nonexistent-table\",\"Item\":{\"id\":{\"S\":\"x\"}}}"))
                        .header("Content-Type", "application/json")
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

                ctx.check("AWS Integration: Error selectionPattern → 400", resp.statusCode() == 400);
                System.out.println("  Error response: " + resp.statusCode() + " " + resp.body());
            } catch (Exception e) {
                ctx.check("AWS Integration: Error selectionPattern → 400", false, e);
            }

            // ── Cleanup ───────────────────────────────────────────
            cleanupAll(apigw, apiId, sfn, smArn, ddb, tableName);
        }
    }

    private void cleanupAll(ApiGatewayClient apigw, String apiId,
                            SfnClient sfn, String smArn,
                            DynamoDbClient ddb, String tableName) {
        if (apiId != null) {
            try {
                apigw.deleteStage(DeleteStageRequest.builder().restApiId(apiId).stageName("test").build());
            } catch (Exception ignore) {}
            try {
                apigw.deleteRestApi(DeleteRestApiRequest.builder().restApiId(apiId).build());
            } catch (Exception ignore) {}
        }
        if (smArn != null) {
            try {
                sfn.deleteStateMachine(DeleteStateMachineRequest.builder().stateMachineArn(smArn).build());
            } catch (Exception ignore) {}
        }
        cleanupTable(ddb, tableName);
    }

    private void cleanupTable(DynamoDbClient ddb, String tableName) {
        try {
            ddb.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
        } catch (Exception ignore) {}
    }
}
