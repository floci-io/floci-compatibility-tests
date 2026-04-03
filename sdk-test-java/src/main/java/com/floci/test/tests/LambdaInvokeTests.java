package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.*;
import software.amazon.awssdk.services.lambda.model.Runtime;

import java.time.Duration;

@FlociTestGroup
public class LambdaInvokeTests implements TestGroup {

    @Override
    public String name() { return "lambda-invoke"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- Lambda Invoke (RequestResponse) Tests ---");
        System.out.println("  NOTE: first invocation may pull Docker image — this can take a minute.");

        try (LambdaClient lambda = LambdaClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofMinutes(5))
                        .apiCallAttemptTimeout(Duration.ofMinutes(5))
                        .build())
                .build()) {

            String functionName = "sdk-invoke-fn";
            String role = "arn:aws:iam::000000000000:role/lambda-role";

            // Create function
            try {
                lambda.createFunction(CreateFunctionRequest.builder()
                        .functionName(functionName)
                        .runtime(Runtime.NODEJS20_X)
                        .role(role)
                        .handler("index.handler")
                        .timeout(30)
                        .memorySize(256)
                        .code(FunctionCode.builder()
                                .zipFile(SdkBytes.fromByteArray(LambdaUtils.handlerZip()))
                                .build())
                        .build());
                ctx.check("Lambda Invoke: CreateFunction", true);
            } catch (Exception e) {
                ctx.check("Lambda Invoke: CreateFunction", false, e);
                return;
            }

            // 1. RequestResponse — echo event back
            try {
                System.out.println("  (cold start — waiting for container...)");
                InvokeResponse resp = lambda.invoke(InvokeRequest.builder()
                        .functionName(functionName)
                        .invocationType(InvocationType.REQUEST_RESPONSE)
                        .payload(SdkBytes.fromUtf8String("{\"name\":\"Floci\"}"))
                        .build());

                String payload = resp.payload().asUtf8String();
                System.out.println("  Response payload: " + payload);

                boolean ok = resp.statusCode() == 200
                        && resp.functionError() == null
                        && payload.contains("Hello, Floci!");
                ctx.check("Lambda Invoke: RequestResponse", ok);
            } catch (Exception e) {
                ctx.check("Lambda Invoke: RequestResponse", false, e);
            }

            // 2. Second invocation (warm container)
            try {
                InvokeResponse resp = lambda.invoke(InvokeRequest.builder()
                        .functionName(functionName)
                        .invocationType(InvocationType.REQUEST_RESPONSE)
                        .payload(SdkBytes.fromUtf8String("{\"name\":\"World\"}"))
                        .build());

                String payload = resp.payload().asUtf8String();
                System.out.println("  Warm response payload: " + payload);

                boolean ok = resp.statusCode() == 200
                        && resp.functionError() == null
                        && payload.contains("Hello, World!");
                ctx.check("Lambda Invoke: RequestResponse (warm)", ok);
            } catch (Exception e) {
                ctx.check("Lambda Invoke: RequestResponse (warm)", false, e);
            }

            // 3. Cleanup
            try {
                lambda.deleteFunction(DeleteFunctionRequest.builder()
                        .functionName(functionName).build());
                ctx.check("Lambda Invoke: DeleteFunction", true);
            } catch (Exception e) {
                ctx.check("Lambda Invoke: DeleteFunction", false, e);
            }

            // Ruby runtime invocation
            String rubyFn = "sdk-invoke-ruby-fn";
            try {
                lambda.createFunction(CreateFunctionRequest.builder()
                        .functionName(rubyFn)
                        .runtime(Runtime.RUBY3_3)
                        .role(role)
                        .handler("lambda_function.lambda_handler")
                        .timeout(30)
                        .memorySize(256)
                        .code(FunctionCode.builder()
                                .zipFile(SdkBytes.fromByteArray(LambdaUtils.rubyZip()))
                                .build())
                        .build());
                ctx.check("Lambda Ruby Invoke: CreateFunction", true);
            } catch (Exception e) {
                ctx.check("Lambda Ruby Invoke: CreateFunction", false, e);
                return;
            }

            try {
                System.out.println("  (Ruby cold start — waiting for container...)");
                InvokeResponse resp = lambda.invoke(InvokeRequest.builder()
                        .functionName(rubyFn)
                        .invocationType(InvocationType.REQUEST_RESPONSE)
                        .payload(SdkBytes.fromUtf8String("{\"name\":\"Floci\"}"))
                        .build());

                String payload = resp.payload().asUtf8String();
                System.out.println("  Ruby response payload: " + payload);

                boolean ok = resp.statusCode() == 200
                        && resp.functionError() == null
                        && payload.contains("Hello, Floci!");
                ctx.check("Lambda Ruby Invoke: RequestResponse", ok);
            } catch (Exception e) {
                ctx.check("Lambda Ruby Invoke: RequestResponse", false, e);
            }

            try {
                lambda.deleteFunction(DeleteFunctionRequest.builder()
                        .functionName(rubyFn).build());
            } catch (Exception ignored) {}

            // provided.al2023 custom runtime (bootstrap shell script)
            String providedFn = "sdk-invoke-provided-fn";
            try {
                lambda.createFunction(CreateFunctionRequest.builder()
                        .functionName(providedFn)
                        .runtime(Runtime.PROVIDED_AL2023)
                        .role(role)
                        .handler("bootstrap")
                        .timeout(30)
                        .memorySize(256)
                        .code(FunctionCode.builder()
                                .zipFile(SdkBytes.fromByteArray(LambdaUtils.providedRuntimeZip()))
                                .build())
                        .build());
                ctx.check("Lambda provided.al2023 Invoke: CreateFunction", true);
            } catch (Exception e) {
                ctx.check("Lambda provided.al2023 Invoke: CreateFunction", false, e);
                return;
            }

            try {
                System.out.println("  (provided.al2023 cold start — waiting for container...)");
                InvokeResponse resp = lambda.invoke(InvokeRequest.builder()
                        .functionName(providedFn)
                        .invocationType(InvocationType.REQUEST_RESPONSE)
                        .payload(SdkBytes.fromUtf8String("{}"))
                        .build());

                String payload = resp.payload().asUtf8String();
                System.out.println("  provided.al2023 response payload: " + payload);

                boolean ok = resp.statusCode() == 200
                        && resp.functionError() == null
                        && payload.contains("hello from provided runtime");
                ctx.check("Lambda provided.al2023 Invoke: RequestResponse", ok);
            } catch (Exception e) {
                ctx.check("Lambda provided.al2023 Invoke: RequestResponse", false, e);
            }

            try {
                lambda.deleteFunction(DeleteFunctionRequest.builder()
                        .functionName(providedFn).build());
            } catch (Exception ignored) {}
        }
    }
}
