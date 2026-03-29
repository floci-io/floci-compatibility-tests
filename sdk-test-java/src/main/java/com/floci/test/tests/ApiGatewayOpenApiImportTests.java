package com.floci.test.tests;

import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.apigateway.ApiGatewayClient;
import software.amazon.awssdk.services.apigateway.model.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Compatibility tests for API Gateway OpenAPI/Swagger import.
 * Tests ImportRestApi and PutRestApi with x-amazon-apigateway-integration extensions.
 */
public class ApiGatewayOpenApiImportTests implements TestGroup {

    @Override
    public String name() { return "apigateway-openapi-import"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- API Gateway OpenAPI Import Tests ---");

        boolean isRealAws = ctx.isRealAws();

        var builder = ApiGatewayClient.builder()
                .region(ctx.region);
        if (!isRealAws) {
            builder.endpointOverride(ctx.endpoint)
                   .credentialsProvider(ctx.credentials);
        }

        try (ApiGatewayClient apigw = builder.build()) {

            HttpClient http = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();

            List<String> cleanup = new ArrayList<>();

            try {
                // ── 1. ImportRestApi with MOCK integration ──
                String mockSpec = """
                        {
                          "openapi": "3.0.1",
                          "info": { "title": "ImportTest", "description": "OpenAPI import test", "version": "1.0" },
                          "paths": {
                            "/health": {
                              "get": {
                                "x-amazon-apigateway-integration": {
                                  "type": "MOCK",
                                  "requestTemplates": { "application/json": "{\\"statusCode\\": 200}" },
                                  "responses": {
                                    "default": {
                                      "statusCode": "200",
                                      "responseTemplates": { "application/json": "{\\"status\\": \\"ok\\"}" }
                                    }
                                  }
                                }
                              }
                            },
                            "/items": {
                              "get": {
                                "x-amazon-apigateway-integration": { "type": "MOCK",
                                  "requestTemplates": { "application/json": "{\\"statusCode\\": 200}" },
                                  "responses": { "default": { "statusCode": "200" } } }
                              },
                              "post": {
                                "x-amazon-apigateway-integration": { "type": "MOCK",
                                  "requestTemplates": { "application/json": "{\\"statusCode\\": 200}" },
                                  "responses": { "default": { "statusCode": "200" } } }
                              }
                            }
                          }
                        }
                        """;

                ImportRestApiResponse importResp = apigw.importRestApi(b -> b
                        .body(SdkBytes.fromUtf8String(mockSpec)));
                String apiId = importResp.id();
                cleanup.add(apiId);
                ctx.check("ImportRestApi creates API", apiId != null && !apiId.isEmpty());
                ctx.check("ImportRestApi name from spec", "ImportTest".equals(importResp.name()));
                ctx.check("ImportRestApi description from spec", "OpenAPI import test".equals(importResp.description()));

                // ── 2. Verify resources created ──
                GetResourcesResponse resources = apigw.getResources(b -> b.restApiId(apiId));
                List<Resource> items = resources.items();
                // Should have: /, /health, /items
                ctx.check("ImportRestApi creates resources", items.size() == 3);

                boolean hasHealth = items.stream().anyMatch(r -> "/health".equals(r.path()));
                boolean hasItems = items.stream().anyMatch(r -> "/items".equals(r.path()));
                ctx.check("ImportRestApi /health resource", hasHealth);
                ctx.check("ImportRestApi /items resource", hasItems);

                // ── 3. Verify methods on /items ──
                Resource itemsResource = items.stream()
                        .filter(r -> "/items".equals(r.path())).findFirst().orElse(null);
                if (itemsResource != null) {
                    // Verify GET method exists by fetching it directly
                    try {
                        apigw.getMethod(b -> b.restApiId(apiId)
                                .resourceId(itemsResource.id()).httpMethod("GET"));
                        ctx.check("ImportRestApi GET method on /items", true);
                    } catch (Exception e) {
                        ctx.check("ImportRestApi GET method on /items", false, e);
                    }
                    try {
                        apigw.getMethod(b -> b.restApiId(apiId)
                                .resourceId(itemsResource.id()).httpMethod("POST"));
                        ctx.check("ImportRestApi POST method on /items", true);
                    } catch (Exception e) {
                        ctx.check("ImportRestApi POST method on /items", false, e);
                    }
                } else {
                    ctx.check("ImportRestApi GET method on /items", false);
                    ctx.check("ImportRestApi POST method on /items", false);
                }

                // ── 4. Verify integration type ──
                Resource healthResource = items.stream()
                        .filter(r -> "/health".equals(r.path())).findFirst().orElse(null);
                if (healthResource != null) {
                    GetIntegrationResponse integ = apigw.getIntegration(b -> b
                            .restApiId(apiId)
                            .resourceId(healthResource.id())
                            .httpMethod("GET"));
                    ctx.check("ImportRestApi MOCK integration type",
                            IntegrationType.MOCK.equals(integ.type()));
                } else {
                    ctx.check("ImportRestApi MOCK integration type", false);
                }

                // ── 5. Deploy and invoke MOCK endpoint (Floci only — uses local execute-api URL) ──
                if (!isRealAws) {
                    CreateDeploymentResponse deploy = apigw.createDeployment(b -> b.restApiId(apiId));
                    String deployId = deploy.id();
                    apigw.createStage(b -> b.restApiId(apiId).stageName("test").deploymentId(deployId));

                    String executeUrl = ctx.endpoint + "/execute-api/" + apiId + "/test/health";
                    HttpRequest req = HttpRequest.newBuilder()
                            .uri(URI.create(executeUrl))
                            .GET()
                            .timeout(Duration.ofSeconds(10))
                            .build();
                    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                    ctx.check("ImportRestApi invoke MOCK returns 200", resp.statusCode() == 200);
                    ctx.check("ImportRestApi invoke MOCK response body",
                            resp.body().contains("\"status\"") && resp.body().contains("ok"));
                }

                // ── 6. ImportRestApi with nested paths ──
                rateLimitPause(isRealAws);
                String nestedSpec = """
                        {
                          "openapi": "3.0.1",
                          "info": { "title": "NestedTest", "version": "1.0" },
                          "paths": {
                            "/orders": {
                              "get": { "x-amazon-apigateway-integration": { "type": "MOCK",
                                "requestTemplates": {"application/json": "{\\"statusCode\\": 200}"},
                                "responses": {"default": {"statusCode": "200"}} } }
                            },
                            "/orders/{orderId}": {
                              "get": { "x-amazon-apigateway-integration": { "type": "MOCK",
                                "requestTemplates": {"application/json": "{\\"statusCode\\": 200}"},
                                "responses": {"default": {"statusCode": "200"}} } }
                            },
                            "/orders/{orderId}/items": {
                              "get": { "x-amazon-apigateway-integration": { "type": "MOCK",
                                "requestTemplates": {"application/json": "{\\"statusCode\\": 200}"},
                                "responses": {"default": {"statusCode": "200"}} } }
                            }
                          }
                        }
                        """;

                ImportRestApiResponse nestedResp = apigw.importRestApi(b -> b
                        .body(SdkBytes.fromUtf8String(nestedSpec)));
                String nestedApiId = nestedResp.id();
                cleanup.add(nestedApiId);
                GetResourcesResponse nestedResources = apigw.getResources(b -> b.restApiId(nestedApiId));
                // Should have: /, /orders, /orders/{orderId}, /orders/{orderId}/items
                ctx.check("ImportRestApi nested paths count", nestedResources.items().size() == 4);

                boolean hasOrderId = nestedResources.items().stream()
                        .anyMatch(r -> "/orders/{orderId}".equals(r.path()));
                boolean hasOrderItems = nestedResources.items().stream()
                        .anyMatch(r -> "/orders/{orderId}/items".equals(r.path()));
                ctx.check("ImportRestApi nested /orders/{orderId}", hasOrderId);
                ctx.check("ImportRestApi nested /orders/{orderId}/items", hasOrderItems);

                // ── 7. PutRestApi (overwrite) ──
                rateLimitPause(isRealAws);
                String overwriteApiId = apigw.createRestApi(b -> b.name("OverwriteTarget")).id();
                cleanup.add(overwriteApiId);
                String overwriteSpec = """
                        {
                          "openapi": "3.0.1",
                          "info": { "title": "Overwritten", "version": "2.0" },
                          "paths": {
                            "/users": {
                              "get": { "x-amazon-apigateway-integration": { "type": "MOCK",
                                "requestTemplates": {"application/json": "{\\"statusCode\\": 200}"},
                                "responses": {"default": {"statusCode": "200"}} } }
                            }
                          }
                        }
                        """;

                PutRestApiResponse putResp = apigw.putRestApi(b -> b
                        .restApiId(overwriteApiId)
                        .mode(PutMode.OVERWRITE)
                        .body(SdkBytes.fromUtf8String(overwriteSpec)));
                ctx.check("PutRestApi updates name", "Overwritten".equals(putResp.name()));

                GetResourcesResponse overwriteResources = apigw.getResources(b -> b.restApiId(overwriteApiId));
                // Should have: /, /users
                ctx.check("PutRestApi replaces resources", overwriteResources.items().size() == 2);
                boolean hasUsers = overwriteResources.items().stream()
                        .anyMatch(r -> "/users".equals(r.path()));
                ctx.check("PutRestApi /users resource exists", hasUsers);

                // ── 7b. PutRestApi mode=merge is accepted ──
                PutRestApiResponse mergeResp = apigw.putRestApi(b -> b
                        .restApiId(overwriteApiId)
                        .mode(PutMode.MERGE)
                        .body(SdkBytes.fromUtf8String(overwriteSpec)));
                ctx.check("PutRestApi mode=merge accepted", mergeResp.name() != null);

                // ── 8. Schemas imported as Models ──
                rateLimitPause(isRealAws);
                String modelsSpec = """
                        {
                          "openapi": "3.0.1",
                          "info": { "title": "ModelsAPI", "version": "1.0" },
                          "paths": {
                            "/orders": {
                              "post": {
                                "requestBody": {
                                  "content": {
                                    "application/json": {
                                      "schema": { "$ref": "#/components/schemas/OrderInput" }
                                    }
                                  }
                                },
                                "x-amazon-apigateway-integration": { "type": "MOCK",
                                  "requestTemplates": {"application/json": "{\\"statusCode\\": 200}"},
                                  "responses": {"default": {"statusCode": "200"}} }
                              }
                            }
                          },
                          "components": {
                            "schemas": {
                              "OrderInput": {
                                "type": "object",
                                "required": ["itemId", "quantity"],
                                "properties": {
                                  "itemId": { "type": "string" },
                                  "quantity": { "type": "integer" }
                                }
                              },
                              "OrderOutput": {
                                "type": "object",
                                "properties": {
                                  "orderId": { "type": "string" },
                                  "status": { "type": "string" }
                                }
                              }
                            }
                          }
                        }
                        """;

                ImportRestApiResponse modelsResp = apigw.importRestApi(b -> b
                        .body(SdkBytes.fromUtf8String(modelsSpec)));
                String modelsApiId = modelsResp.id();
                cleanup.add(modelsApiId);

                // Verify models were created
                GetModelsResponse models = apigw.getModels(b -> b.restApiId(modelsApiId));
                ctx.check("ImportRestApi creates models from schemas", models.items().size() == 2);

                // Verify individual model
                GetModelResponse orderModel = apigw.getModel(b -> b
                        .restApiId(modelsApiId).modelName("OrderInput"));
                ctx.check("ImportRestApi model name", "OrderInput".equals(orderModel.name()));
                ctx.check("ImportRestApi model contentType", "application/json".equals(orderModel.contentType()));
                ctx.check("ImportRestApi model schema contains field", orderModel.schema().contains("itemId"));

                // Verify requestModels on method
                GetResourcesResponse modelsResources = apigw.getResources(b -> b.restApiId(modelsApiId));
                Resource ordersResource = modelsResources.items().stream()
                        .filter(r -> "/orders".equals(r.path())).findFirst().orElse(null);
                if (ordersResource != null) {
                    GetMethodResponse method = apigw.getMethod(b -> b.restApiId(modelsApiId)
                            .resourceId(ordersResource.id()).httpMethod("POST"));
                    ctx.check("ImportRestApi requestModels on method",
                            "OrderInput".equals(method.requestModels().get("application/json")));
                } else {
                    ctx.check("ImportRestApi requestModels on method", false);
                }

                // ── 9. Request validators import ──
                rateLimitPause(isRealAws);
                String validatorSpec = """
                        {
                          "openapi": "3.0.1",
                          "info": { "title": "ValidatorAPI", "version": "1.0" },
                          "x-amazon-apigateway-request-validators": {
                            "full": {
                              "validateRequestBody": true,
                              "validateRequestParameters": true
                            },
                            "params-only": {
                              "validateRequestBody": false,
                              "validateRequestParameters": true
                            }
                          },
                          "x-amazon-apigateway-request-validator": "full",
                          "paths": {
                            "/validated": {
                              "post": {
                                "x-amazon-apigateway-integration": { "type": "MOCK",
                                  "requestTemplates": {"application/json": "{\\"statusCode\\": 200}"},
                                  "responses": {"default": {"statusCode": "200"}} }
                              }
                            },
                            "/params-checked": {
                              "get": {
                                "x-amazon-apigateway-request-validator": "params-only",
                                "x-amazon-apigateway-integration": { "type": "MOCK",
                                  "requestTemplates": {"application/json": "{\\"statusCode\\": 200}"},
                                  "responses": {"default": {"statusCode": "200"}} }
                              }
                            }
                          }
                        }
                        """;

                ImportRestApiResponse validatorResp = apigw.importRestApi(b -> b
                        .body(SdkBytes.fromUtf8String(validatorSpec)));
                String validatorApiId = validatorResp.id();
                cleanup.add(validatorApiId);

                // Verify validators created
                GetRequestValidatorsResponse validators = apigw.getRequestValidators(b -> b.restApiId(validatorApiId));
                ctx.check("ImportRestApi creates request validators", validators.items().size() == 2);

                RequestValidator fullValidator = validators.items().stream()
                        .filter(v -> "full".equals(v.name())).findFirst().orElse(null);
                RequestValidator paramsValidator = validators.items().stream()
                        .filter(v -> "params-only".equals(v.name())).findFirst().orElse(null);
                ctx.check("ImportRestApi 'full' validator exists", fullValidator != null);
                ctx.check("ImportRestApi 'params-only' validator exists", paramsValidator != null);

                if (fullValidator != null) {
                    ctx.check("ImportRestApi full validator validates body", fullValidator.validateRequestBody());
                    ctx.check("ImportRestApi full validator validates params", fullValidator.validateRequestParameters());
                }

                // Verify default validator applied to /validated method
                GetResourcesResponse valResources = apigw.getResources(b -> b.restApiId(validatorApiId));
                Resource validatedResource = valResources.items().stream()
                        .filter(r -> "/validated".equals(r.path())).findFirst().orElse(null);
                Resource paramsCheckedResource = valResources.items().stream()
                        .filter(r -> "/params-checked".equals(r.path())).findFirst().orElse(null);

                if (validatedResource != null && fullValidator != null) {
                    GetMethodResponse valMethod = apigw.getMethod(b -> b.restApiId(validatorApiId)
                            .resourceId(validatedResource.id()).httpMethod("POST"));
                    ctx.check("ImportRestApi default validator on method",
                            fullValidator.id().equals(valMethod.requestValidatorId()));
                } else {
                    ctx.check("ImportRestApi default validator on method", false);
                }

                if (paramsCheckedResource != null && paramsValidator != null) {
                    GetMethodResponse pcMethod = apigw.getMethod(b -> b.restApiId(validatorApiId)
                            .resourceId(paramsCheckedResource.id()).httpMethod("GET"));
                    ctx.check("ImportRestApi operation-level validator override",
                            paramsValidator.id().equals(pcMethod.requestValidatorId()));
                } else {
                    ctx.check("ImportRestApi operation-level validator override", false);
                }

                // ── 9b. Validator precedence (operation > API default) ──
                rateLimitPause(isRealAws);
                String precSpec = """
                        {
                          "openapi": "3.0.1",
                          "info": { "title": "ValidatorPrecedenceAPI", "version": "1.0" },
                          "x-amazon-apigateway-request-validators": {
                            "full": { "validateRequestBody": true, "validateRequestParameters": true },
                            "body-only": { "validateRequestBody": true, "validateRequestParameters": false }
                          },
                          "x-amazon-apigateway-request-validator": "full",
                          "paths": {
                            "/default-validated": {
                              "get": {
                                "x-amazon-apigateway-integration": { "type": "MOCK",
                                  "requestTemplates": {"application/json": "{\\"statusCode\\": 200}"},
                                  "responses": {"default": {"statusCode": "200"}} }
                              }
                            },
                            "/op-override": {
                              "get": {
                                "x-amazon-apigateway-integration": { "type": "MOCK",
                                  "requestTemplates": {"application/json": "{\\"statusCode\\": 200}"},
                                  "responses": {"default": {"statusCode": "200"}} }
                              },
                              "post": {
                                "x-amazon-apigateway-request-validator": "body-only",
                                "x-amazon-apigateway-integration": { "type": "MOCK",
                                  "requestTemplates": {"application/json": "{\\"statusCode\\": 200}"},
                                  "responses": {"default": {"statusCode": "200"}} }
                              }
                            }
                          }
                        }
                        """;

                ImportRestApiResponse precImport = apigw.importRestApi(b -> b
                        .body(SdkBytes.fromUtf8String(precSpec)));
                String precApiId = precImport.id();
                cleanup.add(precApiId);

                GetRequestValidatorsResponse precVals = apigw.getRequestValidators(b -> b.restApiId(precApiId));
                RequestValidator fullVal = precVals.items().stream()
                        .filter(v -> "full".equals(v.name())).findFirst().orElse(null);
                RequestValidator bodyOnlyVal = precVals.items().stream()
                        .filter(v -> "body-only".equals(v.name())).findFirst().orElse(null);

                GetResourcesResponse precResources = apigw.getResources(b -> b.restApiId(precApiId));
                Resource defaultRes = precResources.items().stream()
                        .filter(r -> "/default-validated".equals(r.path())).findFirst().orElse(null);
                Resource opOverrideRes = precResources.items().stream()
                        .filter(r -> "/op-override".equals(r.path())).findFirst().orElse(null);

                if (defaultRes != null && fullVal != null) {
                    GetMethodResponse defMethod = apigw.getMethod(b -> b.restApiId(precApiId)
                            .resourceId(defaultRes.id()).httpMethod("GET"));
                    ctx.check("Validator precedence: API default applied",
                            fullVal.id().equals(defMethod.requestValidatorId()));
                } else {
                    ctx.check("Validator precedence: API default applied", false);
                }

                if (opOverrideRes != null && fullVal != null) {
                    GetMethodResponse getMethod = apigw.getMethod(b -> b.restApiId(precApiId)
                            .resourceId(opOverrideRes.id()).httpMethod("GET"));
                    ctx.check("Validator precedence: no-op default falls through to API default",
                            fullVal.id().equals(getMethod.requestValidatorId()));
                } else {
                    ctx.check("Validator precedence: no-op default falls through to API default", false);
                }

                if (opOverrideRes != null && bodyOnlyVal != null) {
                    GetMethodResponse postMethod = apigw.getMethod(b -> b.restApiId(precApiId)
                            .resourceId(opOverrideRes.id()).httpMethod("POST"));
                    ctx.check("Validator precedence: operation overrides API default",
                            bodyOnlyVal.id().equals(postMethod.requestValidatorId()));
                } else {
                    ctx.check("Validator precedence: operation overrides API default", false);
                }

                // ── 10-11. Validation enforcement (Floci only — uses local execute-api URL) ──
                if (!isRealAws) {
                String bodyValSpec = """
                        {
                          "openapi": "3.0.1",
                          "info": { "title": "BodyValAPI", "version": "1.0" },
                          "x-amazon-apigateway-request-validators": {
                            "body-only": {
                              "validateRequestBody": true,
                              "validateRequestParameters": false
                            }
                          },
                          "x-amazon-apigateway-request-validator": "body-only",
                          "paths": {
                            "/items": {
                              "post": {
                                "requestBody": {
                                  "content": {
                                    "application/json": {
                                      "schema": { "$ref": "#/components/schemas/ItemInput" }
                                    }
                                  }
                                },
                                "x-amazon-apigateway-integration": {
                                  "type": "MOCK",
                                  "requestTemplates": {"application/json": "{\\"statusCode\\": 200}"},
                                  "responses": {"default": {"statusCode": "200",
                                    "responseTemplates": {"application/json": "{\\"ok\\": true}"} }}
                                }
                              }
                            }
                          },
                          "components": {
                            "schemas": {
                              "ItemInput": {
                                "type": "object",
                                "required": ["name", "price"],
                                "properties": {
                                  "name": { "type": "string" },
                                  "price": { "type": "number" }
                                }
                              }
                            }
                          }
                        }
                        """;

                ImportRestApiResponse bodyValResp = apigw.importRestApi(b -> b
                        .body(SdkBytes.fromUtf8String(bodyValSpec)));
                String bodyValApiId = bodyValResp.id();
                cleanup.add(bodyValApiId);

                // Deploy
                CreateDeploymentResponse bodyValDeploy = apigw.createDeployment(b -> b.restApiId(bodyValApiId));
                apigw.createStage(b -> b.restApiId(bodyValApiId)
                        .stageName("test").deploymentId(bodyValDeploy.id()));

                String bodyValUrl = ctx.endpoint + "/execute-api/" + bodyValApiId + "/test/items";

                // Valid body — should pass
                HttpRequest validBodyReq = HttpRequest.newBuilder()
                        .uri(URI.create(bodyValUrl))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"name\": \"Widget\", \"price\": 9.99}"))
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> validBodyResp = http.send(validBodyReq, HttpResponse.BodyHandlers.ofString());
                ctx.check("Body validation accepts valid body", validBodyResp.statusCode() == 200);

                // Missing required field — should fail with 400
                HttpRequest missingFieldReq = HttpRequest.newBuilder()
                        .uri(URI.create(bodyValUrl))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"name\": \"Widget\"}"))
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> missingFieldResp = http.send(missingFieldReq, HttpResponse.BodyHandlers.ofString());
                ctx.check("Body validation rejects missing required field", missingFieldResp.statusCode() == 400);

                // Wrong type — should fail with 400
                HttpRequest wrongTypeReq = HttpRequest.newBuilder()
                        .uri(URI.create(bodyValUrl))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"name\": \"Widget\", \"price\": \"not-a-number\"}"))
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> wrongTypeResp = http.send(wrongTypeReq, HttpResponse.BodyHandlers.ofString());
                ctx.check("Body validation rejects wrong type", wrongTypeResp.statusCode() == 400);

                // ── 11. Parameter validation enforcement ──
                // Use the same validatorApiId API but create a new one for params
                String paramValSpec = """
                        {
                          "openapi": "3.0.1",
                          "info": { "title": "ParamValAPI", "version": "1.0" },
                          "x-amazon-apigateway-request-validators": {
                            "params-only": {
                              "validateRequestBody": false,
                              "validateRequestParameters": true
                            }
                          },
                          "x-amazon-apigateway-request-validator": "params-only",
                          "paths": {
                            "/search": {
                              "get": {
                                "parameters": [
                                  { "name": "q", "in": "query", "required": true, "schema": {"type": "string"} },
                                  { "name": "X-Api-Key", "in": "header", "required": true, "schema": {"type": "string"} }
                                ],
                                "x-amazon-apigateway-integration": {
                                  "type": "MOCK",
                                  "requestTemplates": {"application/json": "{\\"statusCode\\": 200}"},
                                  "responses": {"default": {"statusCode": "200",
                                    "responseTemplates": {"application/json": "{\\"results\\": []}"} }}
                                }
                              }
                            }
                          }
                        }
                        """;

                ImportRestApiResponse paramValResp = apigw.importRestApi(b -> b
                        .body(SdkBytes.fromUtf8String(paramValSpec)));
                String paramValApiId = paramValResp.id();
                cleanup.add(paramValApiId);

                // Deploy
                CreateDeploymentResponse paramValDeploy = apigw.createDeployment(b -> b.restApiId(paramValApiId));
                apigw.createStage(b -> b.restApiId(paramValApiId)
                        .stageName("test").deploymentId(paramValDeploy.id()));

                String searchUrl = ctx.endpoint + "/execute-api/" + paramValApiId + "/test/search";

                // Missing query param — should fail
                HttpRequest missingQReq = HttpRequest.newBuilder()
                        .uri(URI.create(searchUrl))
                        .header("X-Api-Key", "test-key")
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> missingQResp = http.send(missingQReq, HttpResponse.BodyHandlers.ofString());
                ctx.check("Param validation rejects missing query param", missingQResp.statusCode() == 400);

                // Missing header — should fail
                HttpRequest missingHeaderReq = HttpRequest.newBuilder()
                        .uri(URI.create(searchUrl + "?q=test"))
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> missingHeaderResp = http.send(missingHeaderReq, HttpResponse.BodyHandlers.ofString());
                ctx.check("Param validation rejects missing header", missingHeaderResp.statusCode() == 400);

                // Both present — should pass
                HttpRequest allParamsReq = HttpRequest.newBuilder()
                        .uri(URI.create(searchUrl + "?q=test"))
                        .header("X-Api-Key", "test-key")
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> allParamsResp = http.send(allParamsReq, HttpResponse.BodyHandlers.ofString());
                ctx.check("Param validation accepts all params present", allParamsResp.statusCode() == 200);
                } // end if (!isRealAws)

            } catch (Exception e) {
                ctx.check("ApiGateway OpenAPI Import", false, e);
            } finally {
                cleanupAll(apigw, cleanup);
            }
        }
    }

    /** Pause between API creations to avoid AWS rate limiting (429). */
    private void rateLimitPause(boolean isRealAws) {
        if (isRealAws) {
            try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
        }
    }

    private void cleanupAll(ApiGatewayClient apigw, List<String> apiIds) {
        for (String id : apiIds) {
            try { apigw.deleteRestApi(b -> b.restApiId(id)); } catch (Exception ignored) {}
        }
    }
}
