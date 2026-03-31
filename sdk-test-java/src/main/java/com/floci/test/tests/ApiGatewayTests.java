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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

@FlociTestGroup
public class ApiGatewayTests implements TestGroup {

    @Override
    public String name() { return "apigateway"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- API Gateway Tests ---");

        try (ApiGatewayClient apigw = ApiGatewayClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            // ── Part A: MOCK integration CRUD ──────────────────────────────

            // 1. CreateRestApi
            String apiId;
            try {
                CreateRestApiResponse createResp = apigw.createRestApi(CreateRestApiRequest.builder()
                        .name("sdk-test-api")
                        .description("SDK test REST API")
                        .build());
                apiId = createResp.id();
                ctx.check("API Gateway CreateRestApi", apiId != null && !apiId.isBlank());
            } catch (Exception e) {
                ctx.check("API Gateway CreateRestApi", false, e);
                return;
            }

            // 2. GetRestApi
            try {
                GetRestApiResponse getResp = apigw.getRestApi(
                        GetRestApiRequest.builder().restApiId(apiId).build());
                ctx.check("API Gateway GetRestApi", "sdk-test-api".equals(getResp.name()));
            } catch (Exception e) { ctx.check("API Gateway GetRestApi", false, e); }

            // 3. GetRestApis
            try {
                GetRestApisResponse listResp = apigw.getRestApis(GetRestApisRequest.builder().build());
                boolean found = listResp.items().stream().anyMatch(a -> apiId.equals(a.id()));
                ctx.check("API Gateway GetRestApis", found);
            } catch (Exception e) { ctx.check("API Gateway GetRestApis", false, e); }

            // 4. GetResources → find root "/"
            String rootId;
            try {
                GetResourcesResponse resResp = apigw.getResources(
                        GetResourcesRequest.builder().restApiId(apiId).build());
                rootId = resResp.items().stream()
                        .filter(r -> "/".equals(r.path()))
                        .map(Resource::id)
                        .findFirst().orElse(null);
                ctx.check("API Gateway GetResources (root)", rootId != null);
            } catch (Exception e) {
                ctx.check("API Gateway GetResources (root)", false, e);
                return;
            }

            // 5. CreateResource /users
            String resourceId;
            try {
                CreateResourceResponse resCreate = apigw.createResource(CreateResourceRequest.builder()
                        .restApiId(apiId).parentId(rootId).pathPart("users").build());
                resourceId = resCreate.id();
                ctx.check("API Gateway CreateResource", "/users".equals(resCreate.path()));
            } catch (Exception e) {
                ctx.check("API Gateway CreateResource", false, e);
                return;
            }

            // 6. GetResource
            try {
                GetResourceResponse getRes = apigw.getResource(
                        GetResourceRequest.builder().restApiId(apiId).resourceId(resourceId).build());
                ctx.check("API Gateway GetResource", "/users".equals(getRes.path()));
            } catch (Exception e) { ctx.check("API Gateway GetResource", false, e); }

            // 7. PutMethod GET
            try {
                apigw.putMethod(PutMethodRequest.builder()
                        .restApiId(apiId).resourceId(resourceId)
                        .httpMethod("GET").authorizationType("NONE").build());
                ctx.check("API Gateway PutMethod", true);
            } catch (Exception e) { ctx.check("API Gateway PutMethod", false, e); }

            // 8. PutMethodResponse 200
            try {
                apigw.putMethodResponse(PutMethodResponseRequest.builder()
                        .restApiId(apiId).resourceId(resourceId)
                        .httpMethod("GET").statusCode("200").build());
                ctx.check("API Gateway PutMethodResponse", true);
            } catch (Exception e) { ctx.check("API Gateway PutMethodResponse", false, e); }

            // 9. PutIntegration MOCK
            try {
                apigw.putIntegration(PutIntegrationRequest.builder()
                        .restApiId(apiId).resourceId(resourceId)
                        .httpMethod("GET").type(IntegrationType.MOCK)
                        .requestTemplates(Map.of("application/json", "{\"statusCode\": 200}"))
                        .build());
                ctx.check("API Gateway PutIntegration (MOCK)", true);
            } catch (Exception e) { ctx.check("API Gateway PutIntegration (MOCK)", false, e); }

            // 10. GetIntegration
            try {
                GetIntegrationResponse intResp = apigw.getIntegration(GetIntegrationRequest.builder()
                        .restApiId(apiId).resourceId(resourceId).httpMethod("GET").build());
                ctx.check("API Gateway GetIntegration", IntegrationType.MOCK.equals(intResp.type()));
            } catch (Exception e) { ctx.check("API Gateway GetIntegration", false, e); }

            // 11. PutIntegrationResponse 200
            try {
                apigw.putIntegrationResponse(PutIntegrationResponseRequest.builder()
                        .restApiId(apiId).resourceId(resourceId)
                        .httpMethod("GET").statusCode("200").selectionPattern("")
                        .responseTemplates(Map.of("application/json", ""))
                        .build());
                ctx.check("API Gateway PutIntegrationResponse", true);
            } catch (Exception e) { ctx.check("API Gateway PutIntegrationResponse", false, e); }

            // 12. GetIntegrationResponse
            try {
                GetIntegrationResponseResponse irResp = apigw.getIntegrationResponse(
                        GetIntegrationResponseRequest.builder()
                                .restApiId(apiId).resourceId(resourceId)
                                .httpMethod("GET").statusCode("200").build());
                ctx.check("API Gateway GetIntegrationResponse", "200".equals(irResp.statusCode()));
            } catch (Exception e) { ctx.check("API Gateway GetIntegrationResponse", false, e); }

            // 13. GetMethod
            try {
                GetMethodResponse methResp = apigw.getMethod(GetMethodRequest.builder()
                        .restApiId(apiId).resourceId(resourceId).httpMethod("GET").build());
                ctx.check("API Gateway GetMethod (has integration)",
                        methResp.methodIntegration() != null
                        && IntegrationType.MOCK.equals(methResp.methodIntegration().type()));
            } catch (Exception e) { ctx.check("API Gateway GetMethod (has integration)", false, e); }

            // 14. CreateDeployment
            String deploymentId;
            try {
                CreateDeploymentResponse depResp = apigw.createDeployment(CreateDeploymentRequest.builder()
                        .restApiId(apiId).description("v1").build());
                deploymentId = depResp.id();
                ctx.check("API Gateway CreateDeployment", deploymentId != null);
            } catch (Exception e) {
                ctx.check("API Gateway CreateDeployment", false, e);
                deploymentId = "unknown";
            }

            // 15. CreateStage
            try {
                apigw.createStage(CreateStageRequest.builder()
                        .restApiId(apiId).stageName("prod").deploymentId(deploymentId).build());
                ctx.check("API Gateway CreateStage", true);
            } catch (Exception e) { ctx.check("API Gateway CreateStage", false, e); }

            // 16. GetStage
            try {
                GetStageResponse stageResp = apigw.getStage(
                        GetStageRequest.builder().restApiId(apiId).stageName("prod").build());
                ctx.check("API Gateway GetStage",
                        "prod".equals(stageResp.stageName()) && deploymentId.equals(stageResp.deploymentId()));
            } catch (Exception e) { ctx.check("API Gateway GetStage", false, e); }

            // 17. GetStages
            try {
                GetStagesResponse stagesResp = apigw.getStages(
                        GetStagesRequest.builder().restApiId(apiId).build());
                boolean found = stagesResp.item().stream().anyMatch(s -> "prod".equals(s.stageName()));
                ctx.check("API Gateway GetStages", found);
            } catch (Exception e) { ctx.check("API Gateway GetStages", false, e); }

            // 18. UpdateStage
            try {
                apigw.updateStage(UpdateStageRequest.builder()
                        .restApiId(apiId).stageName("prod")
                        .patchOperations(PatchOperation.builder()
                                .op(Op.REPLACE).path("/description").value("Production").build())
                        .build());
                GetStageResponse updated = apigw.getStage(
                        GetStageRequest.builder().restApiId(apiId).stageName("prod").build());
                ctx.check("API Gateway UpdateStage", "Production".equals(updated.description()));
            } catch (Exception e) { ctx.check("API Gateway UpdateStage", false, e); }

            // 19. TagResource
            String apiArn = "arn:aws:apigateway:us-east-1::/restapis/" + apiId;
            try {
                apigw.tagResource(software.amazon.awssdk.services.apigateway.model.TagResourceRequest.builder()
                        .resourceArn(apiArn).tags(Map.of("env", "sdk-test")).build());
                ctx.check("API Gateway TagResource", true);
            } catch (Exception e) { ctx.check("API Gateway TagResource", false, e); }

            // 20. GetTags
            try {
                GetTagsResponse tagsResp = apigw.getTags(
                        GetTagsRequest.builder().resourceArn(apiArn).build());
                ctx.check("API Gateway GetTags", "sdk-test".equals(tagsResp.tags().get("env")));
            } catch (Exception e) { ctx.check("API Gateway GetTags", false, e); }

            // 21. UntagResource
            try {
                apigw.untagResource(software.amazon.awssdk.services.apigateway.model.UntagResourceRequest.builder()
                        .resourceArn(apiArn).tagKeys("env").build());
                GetTagsResponse after = apigw.getTags(GetTagsRequest.builder().resourceArn(apiArn).build());
                ctx.check("API Gateway UntagResource", !after.tags().containsKey("env"));
            } catch (Exception e) { ctx.check("API Gateway UntagResource", false, e); }

            // 22. GetRestApi non-existent → NotFoundException
            try {
                apigw.getRestApi(GetRestApiRequest.builder().restApiId("doesnotexist").build());
                ctx.check("API Gateway GetRestApi non-existent → 404", false);
            } catch (NotFoundException e) {
                ctx.check("API Gateway GetRestApi non-existent → 404", true);
            } catch (Exception e) { ctx.check("API Gateway GetRestApi non-existent → 404", false, e); }

            // ── Part B: AWS_PROXY Lambda integration ────────────────────────

            try (LambdaClient lambda = LambdaClient.builder()
                    .endpointOverride(ctx.endpoint).region(ctx.region).credentialsProvider(ctx.credentials)
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .apiCallTimeout(Duration.ofMinutes(5))
                            .apiCallAttemptTimeout(Duration.ofMinutes(5))
                            .build())
                    .build()) {

                String fnName = "apigw-proxy-fn";
                String fnArn;

                try {
                    CreateFunctionResponse fnResp = lambda.createFunction(
                            software.amazon.awssdk.services.lambda.model.CreateFunctionRequest.builder()
                                    .functionName(fnName)
                                    .runtime(software.amazon.awssdk.services.lambda.model.Runtime.NODEJS20_X)
                                    .role("arn:aws:iam::000000000000:role/lambda-role")
                                    .handler("index.handler").timeout(30).memorySize(256)
                                    .code(software.amazon.awssdk.services.lambda.model.FunctionCode.builder()
                                            .zipFile(SdkBytes.fromByteArray(LambdaUtils.handlerZip())).build())
                                    .build());
                    fnArn = fnResp.functionArn();
                    ctx.check("API Gateway AWS_PROXY: CreateFunction", fnArn != null);
                } catch (Exception e) {
                    ctx.check("API Gateway AWS_PROXY: CreateFunction", false, e);
                    fnArn = null;
                }

                if (fnArn != null) {
                    // Create /proxy resource
                    String proxyResourceId = null;
                    try {
                        CreateResourceResponse proxyRes = apigw.createResource(CreateResourceRequest.builder()
                                .restApiId(apiId).parentId(rootId).pathPart("proxy").build());
                        proxyResourceId = proxyRes.id();
                        ctx.check("API Gateway AWS_PROXY: CreateResource /proxy", true);
                    } catch (Exception e) {
                        ctx.check("API Gateway AWS_PROXY: CreateResource /proxy", false, e);
                    }

                    if (proxyResourceId != null) {
                        // PutMethod
                        try {
                            apigw.putMethod(PutMethodRequest.builder()
                                    .restApiId(apiId).resourceId(proxyResourceId)
                                    .httpMethod("GET").authorizationType("NONE").build());
                            ctx.check("API Gateway AWS_PROXY: PutMethod", true);
                        } catch (Exception e) { ctx.check("API Gateway AWS_PROXY: PutMethod", false, e); }

                        // PutIntegration AWS_PROXY
                        try {
                            apigw.putIntegration(PutIntegrationRequest.builder()
                                    .restApiId(apiId).resourceId(proxyResourceId)
                                    .httpMethod("GET")
                                    .type(IntegrationType.AWS_PROXY)
                                    .integrationHttpMethod("POST")
                                    .uri("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"
                                            + fnArn + "/invocations")
                                    .build());
                            ctx.check("API Gateway AWS_PROXY: PutIntegration", true);
                        } catch (Exception e) { ctx.check("API Gateway AWS_PROXY: PutIntegration", false, e); }

                        // GetIntegration
                        try {
                            GetIntegrationResponse ir = apigw.getIntegration(GetIntegrationRequest.builder()
                                    .restApiId(apiId).resourceId(proxyResourceId).httpMethod("GET").build());
                            ctx.check("API Gateway AWS_PROXY: GetIntegration",
                                    IntegrationType.AWS_PROXY.equals(ir.type())
                                    && ir.uri() != null && ir.uri().contains(fnName));
                        } catch (Exception e) { ctx.check("API Gateway AWS_PROXY: GetIntegration", false, e); }

                        // Invoke Lambda via /_api proxy
                        try {
                            System.out.println("  (AWS_PROXY: cold-starting Lambda via /_api proxy...)");
                            HttpClient http = HttpClient.newHttpClient();
                            HttpRequest httpReq = HttpRequest.newBuilder()
                                    .uri(URI.create(ctx.endpoint + "/_api/" + fnName + "/"))
                                    .GET().build();
                            HttpResponse<String> resp = http.send(httpReq,
                                    HttpResponse.BodyHandlers.ofString());
                            System.out.println("  /_api response: " + resp.body());
                            ctx.check("API Gateway AWS_PROXY: invoke via /_api",
                                    resp.statusCode() == 200 && resp.body().contains("Hello"));
                        } catch (Exception e) { ctx.check("API Gateway AWS_PROXY: invoke via /_api", false, e); }
                    }

                    // Delete Lambda function
                    try {
                        lambda.deleteFunction(software.amazon.awssdk.services.lambda.model.DeleteFunctionRequest.builder()
                                .functionName(fnName).build());
                        ctx.check("API Gateway AWS_PROXY: DeleteFunction", true);
                    } catch (Exception e) { ctx.check("API Gateway AWS_PROXY: DeleteFunction", false, e); }
                }
            }

            // ── Part C: Authorizers, API Keys, Usage Plans ─────────────────

            // 1. CreateAuthorizer
            String authorizerId;
            try {
                CreateAuthorizerResponse authResp = apigw.createAuthorizer(CreateAuthorizerRequest.builder()
                        .restApiId(apiId)
                        .name("my-lambda-auth")
                        .type(AuthorizerType.TOKEN)
                        .authorizerUri("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:my-auth/invocations")
                        .identitySource("method.request.header.Authorization")
                        .build());
                authorizerId = authResp.id();
                ctx.check("API Gateway CreateAuthorizer", authorizerId != null);
            } catch (Exception e) {
                ctx.check("API Gateway CreateAuthorizer", false, e);
                authorizerId = null;
            }

            // 2. GetAuthorizers
            try {
                String finalAuthId = authorizerId;
                GetAuthorizersResponse auths = apigw.getAuthorizers(GetAuthorizersRequest.builder().restApiId(apiId).build());
                boolean found = auths.items().stream().anyMatch(a -> a.id().equals(finalAuthId));
                ctx.check("API Gateway GetAuthorizers", found);
            } catch (Exception e) { ctx.check("API Gateway GetAuthorizers", false, e); }

            // 3. CreateApiKey
            String apiKeyId;
            try {
                CreateApiKeyResponse keyResp = apigw.createApiKey(CreateApiKeyRequest.builder()
                        .name("test-key")
                        .enabled(true)
                        .build());
                apiKeyId = keyResp.id();
                ctx.check("API Gateway CreateApiKey", apiKeyId != null);
            } catch (Exception e) {
                ctx.check("API Gateway CreateApiKey", false, e);
                apiKeyId = null;
            }

            // 4. CreateUsagePlan
            String usagePlanId;
            try {
                CreateUsagePlanResponse planResp = apigw.createUsagePlan(CreateUsagePlanRequest.builder()
                        .name("test-plan")
                        .apiStages(ApiStage.builder().apiId(apiId).stage("prod").build())
                        .build());
                usagePlanId = planResp.id();
                ctx.check("API Gateway CreateUsagePlan", usagePlanId != null);
            } catch (Exception e) {
                ctx.check("API Gateway CreateUsagePlan", false, e);
                usagePlanId = null;
            }

            // 5. CreateUsagePlanKey
            if (usagePlanId != null && apiKeyId != null) {
                try {
                    CreateUsagePlanKeyResponse upkResp = apigw.createUsagePlanKey(CreateUsagePlanKeyRequest.builder()
                            .usagePlanId(usagePlanId)
                            .keyId(apiKeyId)
                            .keyType("API_KEY")
                            .build());
                    ctx.check("API Gateway CreateUsagePlanKey", upkResp.id().equals(apiKeyId));
                } catch (Exception e) { ctx.check("API Gateway CreateUsagePlanKey", false, e); }
            }

            // ── Part D: Request Validators & Updates ─────────────────────

            // 1. CreateRequestValidator
            String validatorId;
            try {
                CreateRequestValidatorResponse vResp = apigw.createRequestValidator(CreateRequestValidatorRequest.builder()
                        .restApiId(apiId)
                        .name("my-validator")
                        .validateRequestBody(true)
                        .validateRequestParameters(true)
                        .build());
                validatorId = vResp.id();
                ctx.check("API Gateway CreateRequestValidator", validatorId != null);
            } catch (Exception e) {
                ctx.check("API Gateway CreateRequestValidator", false, e);
                validatorId = null;
            }

            // 2. GetRequestValidators
            try {
                final String fValidatorId = validatorId;
                GetRequestValidatorsResponse vsResp = apigw.getRequestValidators(GetRequestValidatorsRequest.builder().restApiId(apiId).build());
                boolean found = vsResp.items().stream().anyMatch(v -> v.id().equals(fValidatorId));
                ctx.check("API Gateway GetRequestValidators", found);
            } catch (Exception e) { ctx.check("API Gateway GetRequestValidators", false, e); }

            // 3. UpdateRestApi
            try {
                apigw.updateRestApi(UpdateRestApiRequest.builder()
                        .restApiId(apiId)
                        .patchOperations(PatchOperation.builder().op(Op.REPLACE).path("/name").value("updated-api").build())
                        .build());
                GetRestApiResponse updatedApi = apigw.getRestApi(GetRestApiRequest.builder().restApiId(apiId).build());
                ctx.check("API Gateway UpdateRestApi", "updated-api".equals(updatedApi.name()));
            } catch (Exception e) { ctx.check("API Gateway UpdateRestApi", false, e); }

            // 4. UpdateMethod
            try {
                apigw.updateMethod(UpdateMethodRequest.builder()
                        .restApiId(apiId).resourceId(resourceId).httpMethod("GET")
                        .patchOperations(PatchOperation.builder().op(Op.REPLACE).path("/authorizationType").value("AWS_IAM").build())
                        .build());
                GetMethodResponse updatedMethod = apigw.getMethod(GetMethodRequest.builder()
                        .restApiId(apiId).resourceId(resourceId).httpMethod("GET").build());
                ctx.check("API Gateway UpdateMethod", "AWS_IAM".equals(updatedMethod.authorizationType()));
            } catch (Exception e) { ctx.check("API Gateway UpdateMethod", false, e); }

            // ── Part E: Custom Domains ────────────────────────────────────

            String domainName = "api.example.com";
            try {
                apigw.createDomainName(CreateDomainNameRequest.builder()
                        .domainName(domainName)
                        .certificateName("my-cert")
                        .build());
                GetDomainNamesResponse domains = apigw.getDomainNames(GetDomainNamesRequest.builder().build());
                ctx.check("API Gateway CreateDomainName", domains.items().stream().anyMatch(d -> d.domainName().equals(domainName)));
            } catch (Exception e) { ctx.check("API Gateway CreateDomainName", false, e); }

            try {
                apigw.createBasePathMapping(CreateBasePathMappingRequest.builder()
                        .domainName(domainName)
                        .restApiId(apiId)
                        .stage("prod")
                        .basePath("v1")
                        .build());
                GetBasePathMappingsResponse mappings = apigw.getBasePathMappings(GetBasePathMappingsRequest.builder().domainName(domainName).build());
                ctx.check("API Gateway CreateBasePathMapping", mappings.items().stream().anyMatch(m -> m.basePath().equals("v1")));
            } catch (Exception e) { ctx.check("API Gateway CreateBasePathMapping", false, e); }

            // ── Part F: Cleanup ───────────────────────────────────────────

            try {
                apigw.deleteDomainName(DeleteDomainNameRequest.builder().domainName(domainName).build());
                ctx.check("API Gateway DeleteDomainName", true);
            } catch (Exception e) { ctx.check("API Gateway DeleteDomainName", false, e); }

            try {
                if (usagePlanId != null) apigw.deleteUsagePlan(DeleteUsagePlanRequest.builder().usagePlanId(usagePlanId).build());
                ctx.check("API Gateway DeleteUsagePlan", true);
            } catch (Exception e) { ctx.check("API Gateway DeleteUsagePlan", false, e); }

            try {
                apigw.deleteStage(DeleteStageRequest.builder().restApiId(apiId).stageName("prod").build());
                ctx.check("API Gateway DeleteStage", true);
            } catch (Exception e) { ctx.check("API Gateway DeleteStage", false, e); }

            try {
                apigw.deleteResource(DeleteResourceRequest.builder()
                        .restApiId(apiId).resourceId(resourceId).build());
                ctx.check("API Gateway DeleteResource", true);
            } catch (Exception e) { ctx.check("API Gateway DeleteResource", false, e); }

            try {
                apigw.deleteRestApi(DeleteRestApiRequest.builder().restApiId(apiId).build());
                ctx.check("API Gateway DeleteRestApi", true);
            } catch (Exception e) { ctx.check("API Gateway DeleteRestApi", false, e); }

            try {
                apigw.getRestApi(GetRestApiRequest.builder().restApiId(apiId).build());
                ctx.check("API Gateway GetRestApi after delete → 404", false);
            } catch (NotFoundException e) {
                ctx.check("API Gateway GetRestApi after delete → 404", true);
            } catch (Exception e) { ctx.check("API Gateway GetRestApi after delete → 404", false, e); }
        }
    }
}
