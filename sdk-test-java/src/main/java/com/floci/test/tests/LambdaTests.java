package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.*;
import software.amazon.awssdk.services.lambda.model.Runtime;

@FlociTestGroup
public class LambdaTests implements TestGroup {

    @Override
    public String name() { return "lambda"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- Lambda Tests ---");

        try (LambdaClient lambda = LambdaClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            String functionName = "sdk-test-fn";
            String role = "arn:aws:iam::000000000000:role/lambda-role";

            // 1. CreateFunction
            String functionArn;
            try {
                CreateFunctionResponse createResp = lambda.createFunction(CreateFunctionRequest.builder()
                        .functionName(functionName)
                        .runtime(Runtime.NODEJS20_X)
                        .role(role)
                        .handler("index.handler")
                        .timeout(30)
                        .memorySize(256)
                        .code(FunctionCode.builder()
                                .zipFile(SdkBytes.fromByteArray(LambdaUtils.minimalZip()))
                                .build())
                        .build());
                functionArn = createResp.functionArn();
                ctx.check("Lambda CreateFunction",
                        functionName.equals(createResp.functionName())
                        && functionArn != null && functionArn.contains(functionName)
                        && "Active".equals(createResp.stateAsString())
                        && "$LATEST".equals(createResp.version()));
            } catch (Exception e) {
                ctx.check("Lambda CreateFunction", false, e);
                return;
            }

            // 2. GetFunction
            try {
                GetFunctionResponse getResp = lambda.getFunction(
                        GetFunctionRequest.builder().functionName(functionName).build());
                ctx.check("Lambda GetFunction",
                        functionName.equals(getResp.configuration().functionName())
                        && role.equals(getResp.configuration().role()));
            } catch (Exception e) {
                ctx.check("Lambda GetFunction", false, e);
            }

            // 3. GetFunctionConfiguration
            try {
                GetFunctionConfigurationResponse cfgResp = lambda.getFunctionConfiguration(
                        GetFunctionConfigurationRequest.builder().functionName(functionName).build());
                ctx.check("Lambda GetFunctionConfiguration",
                        30 == cfgResp.timeout() && 256 == cfgResp.memorySize());
            } catch (Exception e) {
                ctx.check("Lambda GetFunctionConfiguration", false, e);
            }

            // 4. ListFunctions
            try {
                ListFunctionsResponse listResp = lambda.listFunctions();
                boolean found = listResp.functions().stream()
                        .anyMatch(f -> functionName.equals(f.functionName()));
                ctx.check("Lambda ListFunctions", found);
            } catch (Exception e) {
                ctx.check("Lambda ListFunctions", false, e);
            }

            // 5. Invoke - DryRun
            try {
                InvokeResponse invokeResp = lambda.invoke(InvokeRequest.builder()
                        .functionName(functionName)
                        .invocationType(InvocationType.DRY_RUN)
                        .payload(SdkBytes.fromUtf8String("{\"key\":\"value\"}"))
                        .build());
                ctx.check("Lambda Invoke DryRun", invokeResp.statusCode() == 204);
            } catch (Exception e) {
                ctx.check("Lambda Invoke DryRun", false, e);
            }

            // 6. Invoke - Event (async)
            try {
                InvokeResponse asyncResp = lambda.invoke(InvokeRequest.builder()
                        .functionName(functionName)
                        .invocationType(InvocationType.EVENT)
                        .payload(SdkBytes.fromUtf8String("{\"key\":\"async\"}"))
                        .build());
                ctx.check("Lambda Invoke Event (async)", asyncResp.statusCode() == 202);
            } catch (Exception e) {
                ctx.check("Lambda Invoke Event (async)", false, e);
            }

            // 7. UpdateFunctionCode
            try {
                UpdateFunctionCodeResponse updateResp = lambda.updateFunctionCode(
                        UpdateFunctionCodeRequest.builder()
                                .functionName(functionName)
                                .zipFile(SdkBytes.fromByteArray(LambdaUtils.minimalZip()))
                                .build());
                ctx.check("Lambda UpdateFunctionCode",
                        functionName.equals(updateResp.functionName())
                        && updateResp.revisionId() != null);
            } catch (Exception e) {
                ctx.check("Lambda UpdateFunctionCode", false, e);
            }

            // 8. PublishVersion & ListVersionsByFunction
            try {
                PublishVersionResponse v1 = lambda.publishVersion(PublishVersionRequest.builder()
                        .functionName(functionName)
                        .description("v1")
                        .build());
                ctx.check("Lambda PublishVersion (1)", "1".equals(v1.version()) && "v1".equals(v1.description()));

                PublishVersionResponse v2 = lambda.publishVersion(PublishVersionRequest.builder()
                        .functionName(functionName)
                        .description("v2")
                        .build());
                ctx.check("Lambda PublishVersion (2)", "2".equals(v2.version()) && "v2".equals(v2.description()));

                ListVersionsByFunctionResponse listVersions = lambda.listVersionsByFunction(ListVersionsByFunctionRequest.builder()
                        .functionName(functionName)
                        .build());
                boolean hasLatest = listVersions.versions().stream().anyMatch(v -> "$LATEST".equals(v.version()));
                boolean hasV1 = listVersions.versions().stream().anyMatch(v -> "1".equals(v.version()));
                boolean hasV2 = listVersions.versions().stream().anyMatch(v -> "2".equals(v.version()));
                ctx.check("Lambda ListVersionsByFunction", hasLatest && hasV1 && hasV2 && listVersions.versions().size() >= 3);
            } catch (Exception e) {
                ctx.check("Lambda ListVersionsByFunction", false, e);
            }

            // 9. CreateFunction duplicate → ResourceConflictException
            try {
                lambda.createFunction(CreateFunctionRequest.builder()
                        .functionName(functionName)
                        .runtime(Runtime.NODEJS20_X)
                        .role(role)
                        .handler("index.handler")
                        .code(FunctionCode.builder()
                                .zipFile(SdkBytes.fromByteArray(LambdaUtils.minimalZip()))
                                .build())
                        .build());
                ctx.check("Lambda CreateFunction duplicate → 409", false);
            } catch (ResourceConflictException e) {
                ctx.check("Lambda CreateFunction duplicate → 409", true);
            } catch (Exception e) {
                ctx.check("Lambda CreateFunction duplicate → 409", false, e);
            }

            // 10. GetFunction non-existent → ResourceNotFoundException
            try {
                lambda.getFunction(GetFunctionRequest.builder()
                        .functionName("does-not-exist").build());
                ctx.check("Lambda GetFunction non-existent → 404", false);
            } catch (ResourceNotFoundException e) {
                ctx.check("Lambda GetFunction non-existent → 404", true);
            } catch (Exception e) {
                ctx.check("Lambda GetFunction non-existent → 404", false, e);
            }

            // 11. DeleteFunction
            try {
                lambda.deleteFunction(DeleteFunctionRequest.builder()
                        .functionName(functionName).build());
                try {
                    lambda.getFunction(GetFunctionRequest.builder()
                            .functionName(functionName).build());
                    ctx.check("Lambda DeleteFunction", false);
                } catch (ResourceNotFoundException e) {
                    ctx.check("Lambda DeleteFunction", true);
                }

                // Verify versions are also gone
                try {
                    lambda.listVersionsByFunction(ListVersionsByFunctionRequest.builder()
                            .functionName(functionName).build());
                    ctx.check("Lambda DeleteFunction (versions deleted)", false);
                } catch (ResourceNotFoundException e) {
                    ctx.check("Lambda DeleteFunction (versions deleted)", true);
                }
            } catch (Exception e) {
                ctx.check("Lambda DeleteFunction", false, e);
            }

            // 12. Ruby runtime support
            String rubyFn = "sdk-test-ruby-fn";
            try {
                CreateFunctionResponse rubyResp = lambda.createFunction(CreateFunctionRequest.builder()
                        .functionName(rubyFn)
                        .runtime(Runtime.RUBY3_3)
                        .role(role)
                        .handler("lambda_function.lambda_handler")
                        .code(FunctionCode.builder()
                                .zipFile(SdkBytes.fromByteArray(LambdaUtils.rubyZip()))
                                .build())
                        .build());
                ctx.check("Lambda Ruby runtime (ruby3.3) CreateFunction",
                        rubyFn.equals(rubyResp.functionName())
                        && Runtime.RUBY3_3.equals(rubyResp.runtime()));
            } catch (Exception e) {
                ctx.check("Lambda Ruby runtime (ruby3.3) CreateFunction", false, e);
            }

            try {
                lambda.deleteFunction(DeleteFunctionRequest.builder()
                        .functionName(rubyFn).build());
            } catch (Exception ignored) {}
        }
    }
}
