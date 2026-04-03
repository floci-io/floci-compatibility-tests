package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CORSConfiguration;
import software.amazon.awssdk.services.s3.model.CORSRule;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * S3 CORS enforcement compatibility tests.
 *
 * <p>This test group validates the behaviour introduced in PR #131 of floci: that CORS
 * rules stored via {@code PutBucketCors} are actually <em>evaluated</em> against incoming
 * HTTP requests. Earlier versions of the emulator would persist and return the configuration
 * but would not act on it, so a browser-issued OPTIONS preflight or a real cross-origin GET
 * would never receive (or be denied) the appropriate CORS response headers.
 *
 * <p>All HTTP interactions that a <em>browser</em> would issue (OPTIONS preflight, plain GET
 * with an {@code Origin} header) intentionally bypass the AWS SDK and use
 * {@link java.net.http.HttpClient} instead. This ensures the tests exercise the raw wire
 * protocol rather than any SDK abstraction layer. CORS <em>configuration</em> round-trips
 * (PutBucketCors / GetBucketCors) are already covered by {@code S3AdvancedTests}; this
 * class focuses exclusively on enforcement.
 */
@FlociTestGroup
public class S3CorsEnforcementTests implements TestGroup {

    @Override
    public String name() { return "s3-cors"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- S3 CORS Enforcement Tests ---");

        String bucket    = "compat-cors-" + System.currentTimeMillis();
        String objectKey = "cors-test.txt";
        String objectUrl = ctx.endpoint + "/" + bucket + "/" + objectKey;

        try (S3Client s3 = S3Client.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .forcePathStyle(true)
                .build()) {

            HttpClient http = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();

            // ----------------------------------------------------------------
            // 1. Setup: create bucket + upload test object
            // ----------------------------------------------------------------
            try {
                s3.createBucket(b -> b.bucket(bucket));
                s3.putObject(
                        b -> b.bucket(bucket).key(objectKey),
                        RequestBody.fromString("hello cors"));
                ctx.check("S3 CORS: setup", true);
            } catch (Exception e) {
                ctx.check("S3 CORS: setup", false, e);
                return; // cannot proceed without the bucket and object
            }

            // ----------------------------------------------------------------
            // 2. No CORS config: preflight → 403
            // ----------------------------------------------------------------
            try {
                int status = optionsStatus(http, objectUrl, "http://localhost:3000", "GET");
                ctx.check("S3 CORS: no config preflight → 403", status == 403);
            } catch (Exception e) {
                ctx.check("S3 CORS: no config preflight → 403", false, e);
            }

            // ----------------------------------------------------------------
            // 3. PutBucketCors – wildcard rule
            // ----------------------------------------------------------------
            try {
                CORSConfiguration cors = CORSConfiguration.builder()
                        .corsRules(CORSRule.builder()
                                .allowedOrigins("*")
                                .allowedMethods("GET", "PUT", "POST", "DELETE", "HEAD")
                                .allowedHeaders("*")
                                .exposeHeaders("ETag")
                                .maxAgeSeconds(3000)
                                .build())
                        .build();
                s3.putBucketCors(b -> b.bucket(bucket).corsConfiguration(cors));
                ctx.check("S3 CORS: PutBucketCors wildcard", true);
            } catch (Exception e) {
                ctx.check("S3 CORS: PutBucketCors wildcard", false, e);
            }

            // ----------------------------------------------------------------
            // 4. Wildcard preflight → 200 with correct CORS response headers
            // ----------------------------------------------------------------
            try {
                HttpResponse<String> resp = optionsResp(http, objectUrl, "http://localhost:3000", "GET");
                boolean status200     = resp.statusCode() == 200;
                boolean allowOrigin   = "*".equals(
                        resp.headers().firstValue("access-control-allow-origin").orElse(""));
                boolean maxAge3000    = "3000".equals(
                        resp.headers().firstValue("access-control-max-age").orElse(""));
                boolean allowsMethods = resp.headers()
                        .firstValue("access-control-allow-methods")
                        .map(v -> v.toUpperCase().contains("GET"))
                        .orElse(false);
                ctx.check("S3 CORS: wildcard preflight → 200",                          status200);
                ctx.check("S3 CORS: wildcard preflight Access-Control-Allow-Origin: *", allowOrigin);
                ctx.check("S3 CORS: wildcard preflight Access-Control-Max-Age: 3000",   maxAge3000);
                ctx.check("S3 CORS: wildcard preflight Allow-Methods contains GET",     allowsMethods);
            } catch (Exception e) {
                ctx.check("S3 CORS: wildcard preflight → 200",                          false, e);
                ctx.check("S3 CORS: wildcard preflight Access-Control-Allow-Origin: *", false, e);
                ctx.check("S3 CORS: wildcard preflight Access-Control-Max-Age: 3000",   false, e);
                ctx.check("S3 CORS: wildcard preflight Allow-Methods contains GET",     false, e);
            }

            // ----------------------------------------------------------------
            // 5. Actual GET with Origin → Access-Control-Allow-Origin + Vary + Expose-Headers
            // ----------------------------------------------------------------
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(objectUrl))
                        .header("Origin", "http://localhost:3000")
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> resp = send(http, req);
                boolean status200       = resp.statusCode() == 200;
                boolean allowOriginStar = "*".equals(
                        resp.headers().firstValue("access-control-allow-origin").orElse(""));
                boolean varyOrigin      = resp.headers().firstValue("vary")
                        .map(v -> v.toLowerCase().contains("origin"))
                        .orElse(false);
                boolean exposeEtag      = resp.headers().firstValue("access-control-expose-headers")
                        .map(v -> v.toLowerCase().contains("etag"))
                        .orElse(false);
                ctx.check("S3 CORS: GET with Origin → 200",                                         status200);
                ctx.check("S3 CORS: GET with Origin → Access-Control-Allow-Origin: *",              allowOriginStar);
                ctx.check("S3 CORS: GET with Origin → Vary: Origin",                                varyOrigin);
                ctx.check("S3 CORS: GET with Origin → Access-Control-Expose-Headers contains ETag", exposeEtag);
            } catch (Exception e) {
                ctx.check("S3 CORS: GET with Origin → 200",                                         false, e);
                ctx.check("S3 CORS: GET with Origin → Access-Control-Allow-Origin: *",              false, e);
                ctx.check("S3 CORS: GET with Origin → Vary: Origin",                                false, e);
                ctx.check("S3 CORS: GET with Origin → Access-Control-Expose-Headers contains ETag", false, e);
            }

            // ----------------------------------------------------------------
            // 6. Actual GET without Origin → no Access-Control-Allow-Origin header
            // ----------------------------------------------------------------
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(objectUrl))
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> resp = send(http, req);
                boolean noAllowOrigin = resp.headers().firstValue("access-control-allow-origin").isEmpty();
                ctx.check("S3 CORS: GET without Origin → no Access-Control-Allow-Origin", noAllowOrigin);
            } catch (Exception e) {
                ctx.check("S3 CORS: GET without Origin → no Access-Control-Allow-Origin", false, e);
            }

            // ----------------------------------------------------------------
            // 7. OPTIONS without Origin header → no Access-Control-Allow-Origin header
            // ----------------------------------------------------------------
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(objectUrl))
                        .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                        .header("Access-Control-Request-Method", "GET")
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> resp = send(http, req);
                boolean noAllowOrigin = resp.headers().firstValue("access-control-allow-origin").isEmpty();
                ctx.check("S3 CORS: OPTIONS without Origin → no Access-Control-Allow-Origin", noAllowOrigin);
            } catch (Exception e) {
                ctx.check("S3 CORS: OPTIONS without Origin → no Access-Control-Allow-Origin", false, e);
            }

            // ----------------------------------------------------------------
            // 8. PutBucketCors – specific origin rule
            // ----------------------------------------------------------------
            try {
                CORSConfiguration cors = CORSConfiguration.builder()
                        .corsRules(CORSRule.builder()
                                .allowedOrigins("https://example.com")
                                .allowedMethods("GET", "PUT")
                                .allowedHeaders("Content-Type", "Authorization")
                                .exposeHeaders("ETag", "x-amz-request-id")
                                .maxAgeSeconds(600)
                                .build())
                        .build();
                s3.putBucketCors(b -> b.bucket(bucket).corsConfiguration(cors));
                ctx.check("S3 CORS: PutBucketCors specific origin", true);
            } catch (Exception e) {
                ctx.check("S3 CORS: PutBucketCors specific origin", false, e);
            }

            // ----------------------------------------------------------------
            // 9. Specific origin preflight matching → 200, echoes exact origin
            // ----------------------------------------------------------------
            try {
                HttpResponse<String> resp = optionsRespWithHeaders(
                        http, objectUrl, "https://example.com", "GET", "Content-Type");
                boolean status200    = resp.statusCode() == 200;
                boolean echoesOrigin = "https://example.com".equals(
                        resp.headers().firstValue("access-control-allow-origin").orElse(""));
                ctx.check("S3 CORS: specific origin preflight → 200",              status200);
                ctx.check("S3 CORS: specific origin preflight echoes exact origin", echoesOrigin);
            } catch (Exception e) {
                ctx.check("S3 CORS: specific origin preflight → 200",              false, e);
                ctx.check("S3 CORS: specific origin preflight echoes exact origin", false, e);
            }

            // ----------------------------------------------------------------
            // 10. Non-matching origin → 403
            // ----------------------------------------------------------------
            try {
                int status = optionsStatus(http, objectUrl, "https://attacker.evil.com", "GET");
                ctx.check("S3 CORS: non-matching origin preflight → 403", status == 403);
            } catch (Exception e) {
                ctx.check("S3 CORS: non-matching origin preflight → 403", false, e);
            }

            // ----------------------------------------------------------------
            // 11. Non-matching method → 403
            // ----------------------------------------------------------------
            try {
                int status = optionsStatus(http, objectUrl, "https://example.com", "DELETE");
                ctx.check("S3 CORS: non-matching method preflight → 403", status == 403);
            } catch (Exception e) {
                ctx.check("S3 CORS: non-matching method preflight → 403", false, e);
            }

            // ----------------------------------------------------------------
            // 12. Actual GET with matching specific origin → echoes origin in Allow-Origin
            // ----------------------------------------------------------------
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(objectUrl))
                        .header("Origin", "https://example.com")
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> resp = send(http, req);
                boolean status200    = resp.statusCode() == 200;
                boolean echoesOrigin = "https://example.com".equals(
                        resp.headers().firstValue("access-control-allow-origin").orElse(""));
                ctx.check("S3 CORS: GET matching specific origin → 200",           status200);
                ctx.check("S3 CORS: GET matching specific origin → echoes origin", echoesOrigin);
            } catch (Exception e) {
                ctx.check("S3 CORS: GET matching specific origin → 200",           false, e);
                ctx.check("S3 CORS: GET matching specific origin → echoes origin", false, e);
            }

            // ----------------------------------------------------------------
            // 13. Actual GET with non-matching origin → no CORS response headers
            // ----------------------------------------------------------------
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(objectUrl))
                        .header("Origin", "https://attacker.evil.com")
                        .GET()
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> resp = send(http, req);
                boolean noAllowOrigin = resp.headers().firstValue("access-control-allow-origin").isEmpty();
                ctx.check("S3 CORS: GET non-matching origin → no CORS headers", noAllowOrigin);
            } catch (Exception e) {
                ctx.check("S3 CORS: GET non-matching origin → no CORS headers", false, e);
            }

            // ----------------------------------------------------------------
            // 14. DeleteBucketCors → preflight returns 403 again
            // ----------------------------------------------------------------
            try {
                s3.deleteBucketCors(b -> b.bucket(bucket));
                int status = optionsStatus(http, objectUrl, "https://example.com", "GET");
                ctx.check("S3 CORS: after DeleteBucketCors preflight → 403", status == 403);
            } catch (Exception e) {
                ctx.check("S3 CORS: after DeleteBucketCors preflight → 403", false, e);
            }

            // ----------------------------------------------------------------
            // 15. PutBucketCors – subdomain wildcard
            // ----------------------------------------------------------------
            try {
                CORSConfiguration cors = CORSConfiguration.builder()
                        .corsRules(CORSRule.builder()
                                .allowedOrigins("http://*.example.com")
                                .allowedMethods("GET")
                                .allowedHeaders("*")
                                .build())
                        .build();
                s3.putBucketCors(b -> b.bucket(bucket).corsConfiguration(cors));
                ctx.check("S3 CORS: PutBucketCors subdomain wildcard", true);
            } catch (Exception e) {
                ctx.check("S3 CORS: PutBucketCors subdomain wildcard", false, e);
            }

            // ----------------------------------------------------------------
            // 16. Subdomain wildcard matches http://app.example.com → 200, echoes origin
            // ----------------------------------------------------------------
            try {
                HttpResponse<String> resp = optionsResp(http, objectUrl, "http://app.example.com", "GET");
                boolean status200    = resp.statusCode() == 200;
                boolean echoesOrigin = "http://app.example.com".equals(
                        resp.headers().firstValue("access-control-allow-origin").orElse(""));
                ctx.check("S3 CORS: subdomain wildcard matches http://app.example.com → 200",  status200);
                ctx.check("S3 CORS: subdomain wildcard echoes http://app.example.com",         echoesOrigin);
            } catch (Exception e) {
                ctx.check("S3 CORS: subdomain wildcard matches http://app.example.com → 200",  false, e);
                ctx.check("S3 CORS: subdomain wildcard echoes http://app.example.com",         false, e);
            }

            // ----------------------------------------------------------------
            // 17. Subdomain wildcard rejects https://app.example.com (scheme mismatch) → 403
            // ----------------------------------------------------------------
            try {
                int status = optionsStatus(http, objectUrl, "https://app.example.com", "GET");
                ctx.check("S3 CORS: subdomain wildcard rejects https://app.example.com → 403", status == 403);
            } catch (Exception e) {
                ctx.check("S3 CORS: subdomain wildcard rejects https://app.example.com → 403", false, e);
            }

            // ----------------------------------------------------------------
            // 18. Subdomain wildcard rejects http://app.other.com (domain mismatch) → 403
            // ----------------------------------------------------------------
            try {
                int status = optionsStatus(http, objectUrl, "http://app.other.com", "GET");
                ctx.check("S3 CORS: subdomain wildcard rejects http://app.other.com → 403", status == 403);
            } catch (Exception e) {
                ctx.check("S3 CORS: subdomain wildcard rejects http://app.other.com → 403", false, e);
            }

            // ----------------------------------------------------------------
            // 19. Cleanup
            // ----------------------------------------------------------------
            try { s3.deleteBucketCors(b -> b.bucket(bucket)); } catch (Exception ignored) {}
            try { s3.deleteObject(b -> b.bucket(bucket).key(objectKey)); } catch (Exception ignored) {}
            try {
                s3.deleteBucket(b -> b.bucket(bucket));
                ctx.check("S3 CORS: cleanup", true);
            } catch (Exception e) {
                ctx.check("S3 CORS: cleanup", false, e);
            }

        } catch (Exception e) {
            ctx.check("S3 CORS: S3Client init", false, e);
        }
    }

    // -----------------------------------------------------------------------
    // Private raw-HTTP helpers
    // -----------------------------------------------------------------------

    /**
     * Issues an OPTIONS preflight request and returns the HTTP status code.
     *
     * <p>The Java {@link HttpClient} does not restrict HTTP methods the way a browser
     * does, so {@code OPTIONS} can be sent freely. The client also never throws for
     * 4xx/5xx responses — it returns the response normally with the appropriate status.
     *
     * @param http   shared {@link HttpClient} instance
     * @param url    full URL of the target resource
     * @param origin value for the {@code Origin} request header
     * @param method value for the {@code Access-Control-Request-Method} request header
     * @return the HTTP status code of the preflight response
     */
    private int optionsStatus(HttpClient http, String url,
                              String origin, String method) throws Exception {
        return optionsResp(http, url, origin, method).statusCode();
    }

    /**
     * Issues an OPTIONS preflight request and returns the full response.
     *
     * @param http   shared {@link HttpClient} instance
     * @param url    full URL of the target resource
     * @param origin value for the {@code Origin} request header
     * @param method value for the {@code Access-Control-Request-Method} request header
     * @return the full HTTP response, including headers and body
     */
    private HttpResponse<String> optionsResp(HttpClient http, String url,
                                             String origin, String method) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .header("Origin", origin)
                .header("Access-Control-Request-Method", method)
                .timeout(Duration.ofSeconds(10))
                .build();
        return send(http, req);
    }

    /**
     * Issues an OPTIONS preflight request that also carries an
     * {@code Access-Control-Request-Headers} field, and returns the full response.
     *
     * <p>This variant is used when the browser (or test) needs to declare which
     * request headers will be present on the actual cross-origin request, so that the
     * server can confirm they are covered by the configured {@code AllowedHeaders}.
     *
     * @param http           shared {@link HttpClient} instance
     * @param url            full URL of the target resource
     * @param origin         value for the {@code Origin} request header
     * @param method         value for the {@code Access-Control-Request-Method} request header
     * @param requestHeaders value for the {@code Access-Control-Request-Headers} request header
     * @return the full HTTP response, including headers and body
     */
    private HttpResponse<String> optionsRespWithHeaders(HttpClient http, String url,
                                                        String origin, String method,
                                                        String requestHeaders) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                .header("Origin", origin)
                .header("Access-Control-Request-Method", method)
                .header("Access-Control-Request-Headers", requestHeaders)
                .timeout(Duration.ofSeconds(10))
                .build();
        return send(http, req);
    }

    /**
     * Thin wrapper around {@link HttpClient#send} with a string body handler.
     *
     * <p>The Java {@link HttpClient} does not throw for 4xx/5xx responses — it returns
     * the response normally with the corresponding status code. Only genuine I/O or
     * protocol errors propagate as exceptions.
     *
     * @param http shared {@link HttpClient} instance
     * @param req  the pre-built {@link HttpRequest}
     * @return the full HTTP response
     */
    private HttpResponse<String> send(HttpClient http, HttpRequest req) throws Exception {
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }
}