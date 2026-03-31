package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

@FlociTestGroup
public class ElastiCacheTests implements TestGroup {

    @Override
    public String name() { return "elasticache"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- ElastiCache IAM Auth Token Tests ---");

        AwsCredentials credentials = ctx.credentials.resolveCredentials();
        String region = ctx.region.id();

        // 1. Generate IAM auth token for Redis
        String token = null;
        try {
            IAMAuthTokenRequest request = new IAMAuthTokenRequest("iam-redis-user", "my-redis-cluster", region);
            token = request.toSignedRequestUri(credentials);
            boolean valid = token != null
                    && token.contains("Action=connect")
                    && token.contains("X-Amz-Signature")
                    && token.contains("X-Amz-Credential")
                    && token.contains("my-redis-cluster");
            System.out.println("  Token (first 120 chars): " + token.substring(0, Math.min(token.length(), 120)) + "...");
            ctx.check("ElastiCache GenerateIamAuthToken", valid);
        } catch (Exception e) {
            ctx.check("ElastiCache GenerateIamAuthToken", false, e);
        }

        // 2. Token contains the correct user
        try {
            IAMAuthTokenRequest request = new IAMAuthTokenRequest("iam-redis-user", "my-redis-cluster", region);
            String t = request.toSignedRequestUri(credentials);
            ctx.check("ElastiCache Token Contains User", t.contains("User=iam-redis-user"));
        } catch (Exception e) {
            ctx.check("ElastiCache Token Contains User", false, e);
        }

        // 3. Token uses correct algorithm
        try {
            IAMAuthTokenRequest request = new IAMAuthTokenRequest("iam-redis-user", "my-redis-cluster", region);
            String t = request.toSignedRequestUri(credentials);
            ctx.check("ElastiCache Token Algorithm", t.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        } catch (Exception e) {
            ctx.check("ElastiCache Token Algorithm", false, e);
        }

        // 4. Validate token against floci emulator
        if (token != null) {
            try {
                String body = "Action=ValidateIamAuthToken&Token=" + URLEncoder.encode(token, StandardCharsets.UTF_8);
                HttpClient client = HttpClient.newHttpClient();
                HttpRequest httpRequest = HttpRequest.newBuilder()
                        .uri(ctx.endpoint)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
                boolean valid = response.statusCode() == 200
                        && response.body().contains("<Valid>true</Valid>")
                        && response.body().contains("my-redis-cluster");
                ctx.check("ElastiCache ValidateIamAuthToken", valid);
            } catch (Exception e) {
                ctx.check("ElastiCache ValidateIamAuthToken", false, e);
            }

            // 5. Expired / tampered token returns error
            try {
                String tampered = token.replaceAll("X-Amz-Signature=[^&]+", "X-Amz-Signature=deadbeef");
                String body = "Action=ValidateIamAuthToken&Token=" + URLEncoder.encode(tampered, StandardCharsets.UTF_8);
                HttpClient client = HttpClient.newHttpClient();
                HttpRequest httpRequest = HttpRequest.newBuilder()
                        .uri(ctx.endpoint)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
                ctx.check("ElastiCache ValidateIamAuthToken Tampered", response.statusCode() == 403);
            } catch (Exception e) {
                ctx.check("ElastiCache ValidateIamAuthToken Tampered", false, e);
            }
        }
    }

    /**
     * Generates ElastiCache Redis IAM auth tokens using AWS SigV4 presigning.
     * Mirrors the approach from aws-samples/elasticache-iam-auth-demo-app.
     */
    static class IAMAuthTokenRequest {
        private static final SdkHttpMethod REQUEST_METHOD = SdkHttpMethod.GET;
        private static final String REQUEST_PROTOCOL = "http://";
        private static final String SERVICE_NAME = "elasticache";
        private static final Duration TOKEN_EXPIRY = Duration.ofSeconds(900);

        private final String userId;
        private final String replicationGroupId;
        private final String region;

        IAMAuthTokenRequest(String userId, String replicationGroupId, String region) {
            this.userId = userId;
            this.replicationGroupId = replicationGroupId;
            this.region = region;
        }

        String toSignedRequestUri(AwsCredentials credentials) {
            SdkHttpFullRequest request = SdkHttpFullRequest.builder()
                    .method(REQUEST_METHOD)
                    .uri(URI.create(REQUEST_PROTOCOL + replicationGroupId + "/"))
                    .appendRawQueryParameter("Action", "connect")
                    .appendRawQueryParameter("User", userId)
                    .build();

            AwsV4HttpSigner signer = AwsV4HttpSigner.create();
            SignedRequest signed = signer.sign(r -> r
                    .identity(credentials)
                    .request(request)
                    .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, SERVICE_NAME)
                    .putProperty(AwsV4HttpSigner.REGION_NAME, region)
                    .putProperty(AwsV4HttpSigner.AUTH_LOCATION, AwsV4HttpSigner.AuthLocation.QUERY_STRING)
                    .putProperty(AwsV4HttpSigner.EXPIRATION_DURATION, TOKEN_EXPIRY)
                    .build());

            return ((SdkHttpFullRequest) signed.request()).getUri().toString()
                    .replace(REQUEST_PROTOCOL, "");
        }
    }
}
