package com.floci.test.tests;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.CognitoIdentityProviderException;
import software.amazon.awssdk.services.cognitoidentityprovider.model.CreateResourceServerResponse;
import software.amazon.awssdk.services.cognitoidentityprovider.model.CreateUserPoolClientResponse;
import software.amazon.awssdk.services.cognitoidentityprovider.model.CreateUserPoolResponse;
import software.amazon.awssdk.services.cognitoidentityprovider.model.DeleteResourceServerRequest;
import software.amazon.awssdk.services.cognitoidentityprovider.model.DeleteUserPoolClientRequest;
import software.amazon.awssdk.services.cognitoidentityprovider.model.DeleteUserPoolRequest;
import software.amazon.awssdk.services.cognitoidentityprovider.model.DescribeResourceServerRequest;
import software.amazon.awssdk.services.cognitoidentityprovider.model.DescribeResourceServerResponse;
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListResourceServersResponse;
import software.amazon.awssdk.services.cognitoidentityprovider.model.OAuthFlowType;
import software.amazon.awssdk.services.cognitoidentityprovider.model.ResourceServerScopeType;
import software.amazon.awssdk.services.cognitoidentityprovider.model.ResourceServerType;
import software.amazon.awssdk.services.cognitoidentityprovider.model.UpdateResourceServerRequest;

import java.math.BigInteger;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.Signature;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.RSAPublicKeySpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@FlociTestGroup
public class CognitoOAuthTests implements TestGroup {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> JSON_MAP = new TypeReference<>() { };

    @Override
    public String name() { return "cognito-oauth"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- Cognito OAuth Tests ---");

        HttpClient http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        try (CognitoIdentityProviderClient cognito = CognitoIdentityProviderClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            String suffix = Long.toString(System.currentTimeMillis());
            String poolName = "oauth-test-pool-" + suffix;
            String resourceServerId = "https://compat.floci.test/resource/" + suffix;
            String readScope = resourceServerId + "/read";
            String adminScope = resourceServerId + "/admin";

            String poolId = null;
            String confidentialClientId = null;
            String confidentialClientSecret = null;
            String publicClientId = null;
            String accessToken = null;

            try {
                CreateUserPoolResponse resp = cognito.createUserPool(b -> b.poolName(poolName));
                poolId = resp.userPool().id();
                ctx.check("Cognito OAuth CreateUserPool", poolId != null);
            } catch (Exception e) {
                ctx.check("Cognito OAuth CreateUserPool", false, e);
                return;
            }

            try {
                CreateResourceServerResponse resp = cognito.createResourceServer(
                        software.amazon.awssdk.services.cognitoidentityprovider.model.CreateResourceServerRequest.builder()
                                .userPoolId(poolId)
                                .identifier(resourceServerId)
                                .name("compat-resource-server")
                                .scopes(
                                ResourceServerScopeType.builder()
                                        .scopeName("read")
                                        .scopeDescription("Read access")
                                        .build(),
                                ResourceServerScopeType.builder()
                                        .scopeName("write")
                                        .scopeDescription("Write access")
                                        .build()
                                )
                                .build()
                );
                ResourceServerType server = resp.resourceServer();
                ctx.check("Cognito OAuth CreateResourceServer",
                        server != null
                                && resourceServerId.equals(server.identifier())
                                && hasScopes(server.scopes(), Set.of("read", "write")));
            } catch (Exception e) {
                ctx.check("Cognito OAuth CreateResourceServer", false, e);
                cleanupUserPool(cognito, poolId, ctx);
                return;
            }

            try {
                DescribeResourceServerResponse resp = cognito.describeResourceServer(DescribeResourceServerRequest.builder()
                        .userPoolId(poolId)
                        .identifier(resourceServerId)
                        .build());
                ResourceServerType server = resp.resourceServer();
                ctx.check("Cognito OAuth DescribeResourceServer",
                        server != null
                                && "compat-resource-server".equals(server.name())
                                && hasScopes(server.scopes(), Set.of("read", "write")));
            } catch (Exception e) {
                ctx.check("Cognito OAuth DescribeResourceServer", false, e);
            }

            try {
                ListResourceServersResponse resp = cognito.listResourceServers(
                        software.amazon.awssdk.services.cognitoidentityprovider.model.ListResourceServersRequest.builder()
                                .userPoolId(poolId)
                                .maxResults(60)
                                .build()
                );
                boolean found = false;
                for (ResourceServerType server : resp.resourceServers()) {
                    if (resourceServerId.equals(server.identifier())) {
                        found = true;
                        break;
                    }
                }
                ctx.check("Cognito OAuth ListResourceServers", found);
            } catch (Exception e) {
                ctx.check("Cognito OAuth ListResourceServers", false, e);
            }

            try {
                cognito.updateResourceServer(UpdateResourceServerRequest.builder()
                        .userPoolId(poolId)
                        .identifier(resourceServerId)
                        .name("compat-resource-server-updated")
                        .scopes(
                                ResourceServerScopeType.builder()
                                        .scopeName("read")
                                        .scopeDescription("Read access updated")
                                        .build(),
                                ResourceServerScopeType.builder()
                                        .scopeName("admin")
                                        .scopeDescription("Admin access")
                                        .build()
                        )
                        .build());

                DescribeResourceServerResponse resp = cognito.describeResourceServer(DescribeResourceServerRequest.builder()
                        .userPoolId(poolId)
                        .identifier(resourceServerId)
                        .build());
                ResourceServerType server = resp.resourceServer();
                ctx.check("Cognito OAuth UpdateResourceServer",
                        server != null
                                && "compat-resource-server-updated".equals(server.name())
                                && hasScopes(server.scopes(), Set.of("read", "admin")));
            } catch (Exception e) {
                ctx.check("Cognito OAuth UpdateResourceServer", false, e);
            }

            try {
                CreateUserPoolClientResponse resp = cognito.createUserPoolClient(
                        software.amazon.awssdk.services.cognitoidentityprovider.model.CreateUserPoolClientRequest.builder()
                                .userPoolId(poolId)
                                .clientName("compat-confidential-client-" + suffix)
                                .generateSecret(true)
                                .allowedOAuthFlowsUserPoolClient(true)
                                .allowedOAuthFlows(OAuthFlowType.CLIENT_CREDENTIALS)
                                .allowedOAuthScopes(readScope, adminScope)
                                .build()
                );
                confidentialClientId = resp.userPoolClient().clientId();
                confidentialClientSecret = resp.userPoolClient().clientSecret();
                ctx.check("Cognito OAuth Create confidential client",
                        confidentialClientId != null
                                && confidentialClientSecret != null
                                && !confidentialClientSecret.isBlank());
            } catch (Exception e) {
                ctx.check("Cognito OAuth Create confidential client", false, e);
            }

            boolean publicClientRejectedAtCreate = false;
            try {
                CreateUserPoolClientResponse resp = cognito.createUserPoolClient(
                        software.amazon.awssdk.services.cognitoidentityprovider.model.CreateUserPoolClientRequest.builder()
                                .userPoolId(poolId)
                                .clientName("compat-public-client-" + suffix)
                                .allowedOAuthFlowsUserPoolClient(true)
                                .allowedOAuthFlows(OAuthFlowType.CLIENT_CREDENTIALS)
                                .allowedOAuthScopes(readScope, adminScope)
                                .build()
                );
                publicClientId = resp.userPoolClient().clientId();
                ctx.check("Cognito OAuth Create public client",
                        publicClientId != null && resp.userPoolClient().clientSecret() == null);
            } catch (Exception e) {
                publicClientRejectedAtCreate = isPublicClientRejection(e);
                ctx.check("Cognito OAuth Public client rejected", publicClientRejectedAtCreate, publicClientRejectedAtCreate ? null : e);
            }

            DiscoveryDocument discovery = null;
            try {
                discovery = discoverOpenIdConfiguration(http, ctx.endpoint, poolId);
                ctx.check("Cognito OAuth OIDC discovery", true);
                ctx.check("Cognito OAuth OIDC token endpoint",
                        discovery.tokenEndpoint().getPath() != null
                                && discovery.tokenEndpoint().getPath().endsWith("/oauth2/token"));
                ctx.check("Cognito OAuth OIDC JWKS URI",
                        discovery.jwksUri().getPath() != null
                                && discovery.jwksUri().getPath().endsWith("/.well-known/jwks.json"));
                ctx.check("Cognito OAuth OIDC issuer", discovery.issuer() != null && !discovery.issuer().isBlank());
            } catch (Exception e) {
                ctx.check("Cognito OAuth OIDC discovery", false, e);
            }

            if (discovery != null && confidentialClientId != null && confidentialClientSecret != null) {
                try {
                    JsonHttpResponse tokenResp = requestClientCredentialsToken(
                            http,
                            discovery.tokenEndpoint(),
                            confidentialClientId,
                            confidentialClientSecret,
                            readScope
                    );
                    ctx.check("Cognito OAuth /oauth2/token happy path", tokenResp.statusCode() == 200);

                    accessToken = stringValue(tokenResp.json(), "access_token");
                    String tokenType = stringValue(tokenResp.json(), "token_type");
                    Number expiresIn = numberValue(tokenResp.json(), "expires_in");
                    String grantedScope = stringValue(tokenResp.json(), "scope");

                    ctx.check("Cognito OAuth access token returned", accessToken != null && !accessToken.isBlank());
                    ctx.check("Cognito OAuth token type Bearer", "Bearer".equalsIgnoreCase(tokenType));
                    ctx.check("Cognito OAuth expires_in present", expiresIn != null && expiresIn.longValue() > 0);
                    ctx.check("Cognito OAuth granted scope",
                            grantedScope == null || scopeContains(grantedScope, readScope));
                } catch (Exception e) {
                    ctx.check("Cognito OAuth /oauth2/token happy path", false, e);
                }
            }

            if (accessToken != null && discovery != null) {
                try {
                    Map<String, Object> header = decodeJwtPart(accessToken, 0);
                    Map<String, Object> payload = decodeJwtPart(accessToken, 1);

                    ctx.check("Cognito OAuth JWT alg is RS256", "RS256".equals(header.get("alg")));
                    ctx.check("Cognito OAuth JWT kid present",
                            header.get("kid") instanceof String && !((String) header.get("kid")).isBlank());
                    ctx.check("Cognito OAuth JWT issuer matches discovery",
                            discovery.issuer().equals(payload.get("iss")));
                    ctx.check("Cognito OAuth JWT client_id matches app client",
                            confidentialClientId.equals(payload.get("client_id")));
                    ctx.check("Cognito OAuth JWT scope claim",
                            scopeContains(stringValue(payload, "scope"), readScope));
                } catch (Exception e) {
                    ctx.check("Cognito OAuth JWT inspection", false, e);
                }

                try {
                    String kid = (String) decodeJwtPart(accessToken, 0).get("kid");
                    Map<String, Object> jwk = fetchJwk(http, discovery.jwksUri(), kid);
                    verifyRs256(accessToken, jwk);
                    ctx.check("Cognito OAuth RS256 JWT verification against JWKS", true);
                } catch (Exception e) {
                    ctx.check("Cognito OAuth RS256 JWT verification against JWKS", false, e);
                }
            }

            if (!publicClientRejectedAtCreate && discovery != null && publicClientId != null) {
                try {
                    JsonHttpResponse errorResp = requestPublicClientToken(
                            http,
                            discovery.tokenEndpoint(),
                            publicClientId,
                            readScope
                    );
                    String error = stringValue(errorResp.json(), "error");
                    ctx.check("Cognito OAuth Public client rejected", errorResp.statusCode() >= 400 && errorResp.statusCode() < 500);
                    ctx.check("Cognito OAuth Public client rejection reason",
                            "invalid_client".equals(error) || "unauthorized_client".equals(error));
                } catch (Exception e) {
                    ctx.check("Cognito OAuth Public client rejected", false, e);
                }
            }

            if (discovery != null && confidentialClientId != null && confidentialClientSecret != null) {
                try {
                    JsonHttpResponse errorResp = requestClientCredentialsToken(
                            http,
                            discovery.tokenEndpoint(),
                            confidentialClientId,
                            confidentialClientSecret,
                            resourceServerId + "/unknown"
                    );
                    String error = stringValue(errorResp.json(), "error");
                    ctx.check("Cognito OAuth unknown scope rejected", errorResp.statusCode() == 400);
                    ctx.check("Cognito OAuth unknown scope error", "invalid_scope".equals(error));
                } catch (Exception e) {
                    ctx.check("Cognito OAuth unknown scope rejected", false, e);
                }
            }

            if (confidentialClientId != null) {
                try {
                    cognito.deleteUserPoolClient(DeleteUserPoolClientRequest.builder()
                            .userPoolId(poolId)
                            .clientId(confidentialClientId)
                            .build());
                    ctx.check("Cognito OAuth Delete confidential client", true);
                    confidentialClientId = null;
                } catch (Exception e) {
                    ctx.check("Cognito OAuth Delete confidential client", false, e);
                }
            }

            if (publicClientId != null) {
                try {
                    cognito.deleteUserPoolClient(DeleteUserPoolClientRequest.builder()
                            .userPoolId(poolId)
                            .clientId(publicClientId)
                            .build());
                    ctx.check("Cognito OAuth Delete public client", true);
                    publicClientId = null;
                } catch (Exception e) {
                    ctx.check("Cognito OAuth Delete public client", false, e);
                }
            }

            try {
                cognito.deleteResourceServer(DeleteResourceServerRequest.builder()
                        .userPoolId(poolId)
                        .identifier(resourceServerId)
                        .build());

                boolean deleted = false;
                try {
                    cognito.describeResourceServer(DescribeResourceServerRequest.builder()
                            .userPoolId(poolId)
                            .identifier(resourceServerId)
                            .build());
                } catch (CognitoIdentityProviderException e) {
                    deleted = isNotFound(e);
                }
                ctx.check("Cognito OAuth DeleteResourceServer", deleted);
                resourceServerId = null;
            } catch (Exception e) {
                ctx.check("Cognito OAuth DeleteResourceServer", false, e);
            }

            cleanupUserPool(cognito, poolId, ctx);

        } catch (Exception e) {
            ctx.check("Cognito OAuth Client", false, e);
        }
    }

    private static DiscoveryDocument discoverOpenIdConfiguration(HttpClient http, URI endpoint, String poolId) throws Exception {
        List<URI> candidates = List.of(
                endpoint.resolve("/" + poolId + "/.well-known/openid-configuration"),
                endpoint.resolve("/.well-known/openid-configuration")
        );

        List<String> failures = new ArrayList<>();
        for (URI candidate : candidates) {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(candidate)
                    .GET()
                    .timeout(Duration.ofSeconds(10))
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                failures.add(candidate + " -> HTTP " + resp.statusCode());
                continue;
            }

            Map<String, Object> json = JSON.readValue(resp.body(), JSON_MAP);
            String issuer = stringValue(json, "issuer");
            String tokenEndpoint = stringValue(json, "token_endpoint");
            String jwksUri = stringValue(json, "jwks_uri");
            if (issuer == null || tokenEndpoint == null || jwksUri == null) {
                failures.add(candidate + " -> missing issuer/token_endpoint/jwks_uri");
                continue;
            }

            return new DiscoveryDocument(
                    candidate,
                    issuer,
                    resolveUri(candidate, tokenEndpoint),
                    resolveUri(candidate, jwksUri)
            );
        }

        throw new IllegalStateException("No valid OIDC discovery document found: " + failures);
    }

    private static JsonHttpResponse requestClientCredentialsToken(
            HttpClient http,
            URI tokenEndpoint,
            String clientId,
            String clientSecret,
            String scope
    ) throws Exception {
        String basicAuth = Base64.getEncoder().encodeToString((clientId + ":" + clientSecret)
                .getBytes(StandardCharsets.UTF_8));
        return postForm(http, tokenEndpoint, Map.of(
                "grant_type", "client_credentials",
                "scope", scope
        ), Map.of(
                "Authorization", "Basic " + basicAuth
        ));
    }

    private static JsonHttpResponse requestPublicClientToken(
            HttpClient http,
            URI tokenEndpoint,
            String clientId,
            String scope
    ) throws Exception {
        return postForm(http, tokenEndpoint, Map.of(
                "grant_type", "client_credentials",
                "client_id", clientId,
                "scope", scope
        ), Map.of());
    }

    private static JsonHttpResponse postForm(
            HttpClient http,
            URI uri,
            Map<String, String> form,
            Map<String, String> headers
    ) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(formEncode(form)));

        for (Map.Entry<String, String> header : headers.entrySet()) {
            builder.header(header.getKey(), header.getValue());
        }

        HttpResponse<String> resp = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        Map<String, Object> json;
        if (resp.body() == null || resp.body().isBlank()) {
            json = Map.of();
        } else {
            try {
                json = JSON.readValue(resp.body(), JSON_MAP);
            } catch (Exception ignored) {
                json = Map.of();
            }
        }
        return new JsonHttpResponse(resp.statusCode(), json, resp.body());
    }

    private static String formEncode(Map<String, String> form) {
        return form.entrySet().stream()
                .map(entry -> URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)
                        + "="
                        + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));
    }

    private static URI resolveUri(URI base, String maybeRelative) {
        URI uri = URI.create(maybeRelative);
        return uri.isAbsolute() ? uri : base.resolve(uri);
    }

    private static Map<String, Object> decodeJwtPart(String jwt, int index) throws Exception {
        String[] parts = jwt.split("\\.");
        if (parts.length != 3) {
            throw new IllegalStateException("JWT must have exactly 3 parts");
        }
        return JSON.readValue(Base64.getUrlDecoder().decode(parts[index]), JSON_MAP);
    }

    private static Map<String, Object> fetchJwk(HttpClient http, URI jwksUri, String kid) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(jwksUri)
                .GET()
                .timeout(Duration.ofSeconds(10))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("JWKS request failed with HTTP " + resp.statusCode());
        }

        Map<String, Object> json = JSON.readValue(resp.body(), JSON_MAP);
        Object keysObject = json.get("keys");
        if (!(keysObject instanceof List<?> keys)) {
            throw new IllegalStateException("JWKS keys array missing");
        }

        for (Object keyObject : keys) {
            if (!(keyObject instanceof Map<?, ?> rawKey)) {
                continue;
            }

            Map<String, Object> key = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : rawKey.entrySet()) {
                key.put(String.valueOf(entry.getKey()), entry.getValue());
            }

            if (kid.equals(key.get("kid"))) {
                return key;
            }
        }

        throw new IllegalStateException("No JWK found for kid " + kid);
    }

    private static void verifyRs256(String jwt, Map<String, Object> jwk) throws Exception {
        if (!"RSA".equals(jwk.get("kty"))) {
            throw new IllegalStateException("Expected RSA JWK but got " + jwk.get("kty"));
        }

        String[] parts = jwt.split("\\.");
        String signedContent = parts[0] + "." + parts[1];
        byte[] signatureBytes = Base64.getUrlDecoder().decode(parts[2]);

        BigInteger modulus = new BigInteger(1, Base64.getUrlDecoder().decode((String) jwk.get("n")));
        BigInteger exponent = new BigInteger(1, Base64.getUrlDecoder().decode((String) jwk.get("e")));

        RSAPublicKey publicKey = (RSAPublicKey) KeyFactory.getInstance("RSA")
                .generatePublic(new RSAPublicKeySpec(modulus, exponent));

        Signature verifier = Signature.getInstance("SHA256withRSA");
        verifier.initVerify(publicKey);
        verifier.update(signedContent.getBytes(StandardCharsets.UTF_8));
        if (!verifier.verify(signatureBytes)) {
            throw new IllegalStateException("JWT signature verification failed");
        }
    }

    private static boolean hasScopes(List<ResourceServerScopeType> scopes, Set<String> expectedScopeNames) {
        if (scopes == null) {
            return false;
        }
        Set<String> actual = scopes.stream()
                .map(ResourceServerScopeType::scopeName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return actual.equals(expectedScopeNames);
    }

    private static boolean scopeContains(String scopeClaim, String expectedScope) {
        if (scopeClaim == null || scopeClaim.isBlank()) {
            return false;
        }
        for (String scope : scopeClaim.split("\\s+")) {
            if (expectedScope.equals(scope)) {
                return true;
            }
        }
        return false;
    }

    private static String stringValue(Map<String, Object> json, String key) {
        Object value = json.get(key);
        return value instanceof String ? (String) value : null;
    }

    private static Number numberValue(Map<String, Object> json, String key) {
        Object value = json.get(key);
        return value instanceof Number ? (Number) value : null;
    }

    private static boolean isPublicClientRejection(Exception error) {
        String message = error.getMessage();
        if (message == null) {
            return false;
        }
        String normalized = message.toLowerCase();
        return normalized.contains("secret")
                || normalized.contains("client_credentials")
                || normalized.contains("public client")
                || normalized.contains("confidential client");
    }

    private static boolean isNotFound(CognitoIdentityProviderException error) {
        if (error.awsErrorDetails() != null && error.awsErrorDetails().errorCode() != null) {
            String code = error.awsErrorDetails().errorCode().toLowerCase();
            if (code.contains("notfound")) {
                return true;
            }
        }

        String message = error.getMessage();
        return message != null && message.toLowerCase().contains("not found");
    }

    private static void cleanupUserPool(CognitoIdentityProviderClient cognito, String poolId, TestContext ctx) {
        if (poolId == null) {
            return;
        }
        try {
            cognito.deleteUserPool(DeleteUserPoolRequest.builder().userPoolId(poolId).build());
            ctx.check("Cognito OAuth DeleteUserPool", true);
        } catch (Exception e) {
            ctx.check("Cognito OAuth DeleteUserPool", false, e);
        }
    }

    private record DiscoveryDocument(URI discoveryUri, String issuer, URI tokenEndpoint, URI jwksUri) { }

    private record JsonHttpResponse(int statusCode, Map<String, Object> json, String body) { }
}
