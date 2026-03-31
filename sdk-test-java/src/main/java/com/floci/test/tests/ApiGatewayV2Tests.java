package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.apigatewayv2.ApiGatewayV2Client;
import software.amazon.awssdk.services.apigatewayv2.model.*;

@FlociTestGroup
public class ApiGatewayV2Tests implements TestGroup {

    @Override
    public String name() { return "apigatewayv2"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- API Gateway v2 (HTTP API) Tests ---");

        try (ApiGatewayV2Client client = ApiGatewayV2Client.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            String apiName = "sdk-test-http-api-" + System.currentTimeMillis();

            // 1. CreateApi
            String apiId;
            try {
                CreateApiResponse resp = client.createApi(CreateApiRequest.builder()
                        .name(apiName)
                        .protocolType(ProtocolType.HTTP)
                        .build());
                apiId = resp.apiId();
                ctx.check("ApiV2 CreateApi", apiId != null && resp.name().equals(apiName));
            } catch (Exception e) {
                ctx.check("ApiV2 CreateApi", false, e);
                return;
            }

            // 2. CreateIntegration (Mock/Lambda placeholder)
            String integrationId;
            try {
                CreateIntegrationResponse resp = client.createIntegration(CreateIntegrationRequest.builder()
                        .apiId(apiId)
                        .integrationType(IntegrationType.AWS_PROXY)
                        .integrationUri("arn:aws:lambda:us-east-1:000000000000:function:my-fn")
                        .payloadFormatVersion("2.0")
                        .build());
                integrationId = resp.integrationId();
                ctx.check("ApiV2 CreateIntegration", integrationId != null);
            } catch (Exception e) {
                ctx.check("ApiV2 CreateIntegration", false, e);
                return;
            }

            // 3. CreateRoute
            try {
                CreateRouteResponse resp = client.createRoute(CreateRouteRequest.builder()
                        .apiId(apiId)
                        .routeKey("GET /hello")
                        .target("integrations/" + integrationId)
                        .build());
                ctx.check("ApiV2 CreateRoute", resp.routeId() != null && resp.routeKey().equals("GET /hello"));
            } catch (Exception e) {
                ctx.check("ApiV2 CreateRoute", false, e);
            }

            // 4. GetApis
            try {
                GetApisResponse resp = client.getApis();
                boolean found = resp.items().stream().anyMatch(a -> a.apiId().equals(apiId));
                ctx.check("ApiV2 GetApis", found);
            } catch (Exception e) {
                ctx.check("ApiV2 GetApis", false, e);
            }

            // Cleanup
            try {
                client.deleteApi(DeleteApiRequest.builder().apiId(apiId).build());
                ctx.check("ApiV2 DeleteApi", true);
            } catch (Exception e) {
                ctx.check("ApiV2 DeleteApi", false, e);
            }

        } catch (Exception e) {
            ctx.check("ApiV2 Client", false, e);
        }
    }
}
