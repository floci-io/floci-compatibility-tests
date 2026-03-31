package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.apigateway.ApiGatewayClient;
import software.amazon.awssdk.services.apigateway.model.*;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.*;
import software.amazon.awssdk.services.lambda.model.Runtime;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Tests Lambda invocation routed through the API Gateway execute endpoint:
 * {@code GET /execute-api/{apiId}/{stageName}/{path}}
 *
 * <p>This exercises the full API Gateway dispatch chain:
 * HTTP client → execute-api → resource/method/integration lookup → Lambda → response.
 */
@FlociTestGroup
public class ApiGatewayExecuteTests implements TestGroup {

    @Override
    public String name() { return "apigateway-execute"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- API Gateway Execute (stage invocation) Tests ---");
        System.out.println("  NOTE: Lambda cold-start may take a moment.");

        try (ApiGatewayClient apigw = ApiGatewayClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build();
             LambdaClient lambda = LambdaClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofMinutes(5))
                        .apiCallAttemptTimeout(Duration.ofMinutes(5))
                        .build())
                .build()) {

            // ── Step 1: Create Lambda function ──────────────────────────────

            String fnName = "apigw-execute-fn";
            String fnArn;
            try {
                CreateFunctionResponse fnResp = lambda.createFunction(CreateFunctionRequest.builder()
                        .functionName(fnName)
                        .runtime(Runtime.NODEJS20_X)
                        .role("arn:aws:iam::000000000000:role/lambda-role")
                        .handler("index.handler")
                        .timeout(30).memorySize(256)
                        .code(FunctionCode.builder()
                                .zipFile(SdkBytes.fromByteArray(LambdaUtils.handlerZip()))
                                .build())
                        .build());
                fnArn = fnResp.functionArn();
                ctx.check("APIGW Execute: CreateFunction", fnArn != null);
            } catch (Exception e) {
                ctx.check("APIGW Execute: CreateFunction", false, e);
                return;
            }

            // ── Step 2: Create REST API + resource + method + integration ───

            String apiId;
            try {
                apiId = apigw.createRestApi(CreateRestApiRequest.builder()
                        .name("execute-test-api").build()).id();
                ctx.check("APIGW Execute: CreateRestApi", apiId != null);
            } catch (Exception e) {
                ctx.check("APIGW Execute: CreateRestApi", false, e);
                cleanupLambda(lambda, fnName);
                return;
            }

            // Find root resource
            String rootId;
            try {
                rootId = apigw.getResources(GetResourcesRequest.builder().restApiId(apiId).build())
                        .items().stream()
                        .filter(r -> "/".equals(r.path()))
                        .map(Resource::id)
                        .findFirst().orElseThrow();
                ctx.check("APIGW Execute: GetRootResource", true);
            } catch (Exception e) {
                ctx.check("APIGW Execute: GetRootResource", false, e);
                cleanupApi(apigw, apiId);
                cleanupLambda(lambda, fnName);
                return;
            }

            // Create /hello resource
            String resourceId;
            try {
                resourceId = apigw.createResource(CreateResourceRequest.builder()
                        .restApiId(apiId).parentId(rootId).pathPart("hello").build()).id();
                ctx.check("APIGW Execute: CreateResource /hello", true);
            } catch (Exception e) {
                ctx.check("APIGW Execute: CreateResource /hello", false, e);
                cleanupApi(apigw, apiId);
                cleanupLambda(lambda, fnName);
                return;
            }

            // PUT method GET + AWS_PROXY integration
            try {
                apigw.putMethod(PutMethodRequest.builder()
                        .restApiId(apiId).resourceId(resourceId)
                        .httpMethod("GET").authorizationType("NONE").build());

                apigw.putIntegration(PutIntegrationRequest.builder()
                        .restApiId(apiId).resourceId(resourceId)
                        .httpMethod("GET").type(IntegrationType.AWS_PROXY)
                        .integrationHttpMethod("POST")
                        .uri("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"
                                + fnArn + "/invocations")
                        .build());
                ctx.check("APIGW Execute: PutMethod + PutIntegration", true);
            } catch (Exception e) {
                ctx.check("APIGW Execute: PutMethod + PutIntegration", false, e);
                cleanupApi(apigw, apiId);
                cleanupLambda(lambda, fnName);
                return;
            }

            // Create deployment + stage "v1"
            String deploymentId;
            try {
                deploymentId = apigw.createDeployment(CreateDeploymentRequest.builder()
                        .restApiId(apiId).description("execute test").build()).id();
                apigw.createStage(CreateStageRequest.builder()
                        .restApiId(apiId).stageName("v1").deploymentId(deploymentId).build());
                ctx.check("APIGW Execute: CreateDeployment + Stage", true);
            } catch (Exception e) {
                ctx.check("APIGW Execute: CreateDeployment + Stage", false, e);
                cleanupApi(apigw, apiId);
                cleanupLambda(lambda, fnName);
                return;
            }

            // ── Step 3: Invoke through execute-api stage URL ────────────────

            HttpClient http = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();

            String baseUrl = ctx.endpoint + "/execute-api/" + apiId + "/v1";

            // 1. GET /hello — cold start
            try {
                System.out.println("  (cold start — waiting for container...)");
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/hello"))
                        .GET()
                        .timeout(Duration.ofMinutes(5))
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                System.out.println("  Response status : " + resp.statusCode());
                System.out.println("  Response body   : " + resp.body());

                ctx.check("APIGW Execute: GET /hello status 200", resp.statusCode() == 200);
                ctx.check("APIGW Execute: GET /hello body contains greeting",
                        resp.body().contains("Hello"));
            } catch (Exception e) {
                ctx.check("APIGW Execute: GET /hello status 200", false, e);
                ctx.check("APIGW Execute: GET /hello body contains greeting", false, e);
            }

            // 2. Second request (warm)
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/hello"))
                        .GET()
                        .timeout(Duration.ofMinutes(5))
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                System.out.println("  Warm body       : " + resp.body());
                ctx.check("APIGW Execute: GET /hello warm", resp.statusCode() == 200
                        && resp.body().contains("Hello"));
            } catch (Exception e) {
                ctx.check("APIGW Execute: GET /hello warm", false, e);
            }

            // 3. requestContext.stage in proxy event equals stage name "v1"
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/hello"))
                        .GET()
                        .timeout(Duration.ofMinutes(5))
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                // The handler echoes the full event via the "input" field
                ctx.check("APIGW Execute: requestContext.stage == v1",
                        resp.body().contains("\"stage\":\"v1\""));
            } catch (Exception e) {
                ctx.check("APIGW Execute: requestContext.stage == v1", false, e);
            }

            // 4. Unknown path → 404
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/no-such-path"))
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                ctx.check("APIGW Execute: unknown path → 404", resp.statusCode() == 404);
            } catch (Exception e) {
                ctx.check("APIGW Execute: unknown path → 404", false, e);
            }

            // 5. Unknown stage → 404
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(ctx.endpoint + "/execute-api/" + apiId + "/nosuchstage/hello"))
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                ctx.check("APIGW Execute: unknown stage → 404", resp.statusCode() == 404);
            } catch (Exception e) {
                ctx.check("APIGW Execute: unknown stage → 404", false, e);
            }

            // ── Step 4: MOCK integration test ──────────────────────────────

            // Create /ping resource with MOCK integration
            try {
                String pingId = apigw.createResource(CreateResourceRequest.builder()
                        .restApiId(apiId).parentId(rootId).pathPart("ping").build()).id();

                apigw.putMethod(PutMethodRequest.builder()
                        .restApiId(apiId).resourceId(pingId)
                        .httpMethod("GET").authorizationType("NONE").build());

                apigw.putIntegration(PutIntegrationRequest.builder()
                        .restApiId(apiId).resourceId(pingId)
                        .httpMethod("GET").type(IntegrationType.MOCK)
                        .requestTemplates(Map.of("application/json", "{\"statusCode\": 200}"))
                        .build());

                apigw.putIntegrationResponse(PutIntegrationResponseRequest.builder()
                        .restApiId(apiId).resourceId(pingId)
                        .httpMethod("GET").statusCode("200").selectionPattern("")
                        .responseTemplates(Map.of("application/json", "{\"pong\":true}"))
                        .build());

                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/ping"))
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                System.out.println("  MOCK body       : " + resp.body());
                ctx.check("APIGW Execute: MOCK GET /ping status 200", resp.statusCode() == 200);
                ctx.check("APIGW Execute: MOCK GET /ping body", resp.body().contains("pong"));
            } catch (Exception e) {
                ctx.check("APIGW Execute: MOCK GET /ping status 200", false, e);
                ctx.check("APIGW Execute: MOCK GET /ping body", false, e);
            }

            // ── Cleanup ─────────────────────────────────────────────────────
            cleanupApi(apigw, apiId);
            cleanupLambda(lambda, fnName);
        }
    }

    private void cleanupLambda(LambdaClient lambda, String fnName) {
        try {
            lambda.deleteFunction(DeleteFunctionRequest.builder().functionName(fnName).build());
        } catch (Exception ignore) {}
    }

    private void cleanupApi(ApiGatewayClient apigw, String apiId) {
        try {
            apigw.deleteRestApi(DeleteRestApiRequest.builder().restApiId(apiId).build());
        } catch (Exception ignore) {}
    }
}
