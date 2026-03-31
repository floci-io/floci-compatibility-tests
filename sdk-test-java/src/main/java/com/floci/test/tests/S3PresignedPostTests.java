package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.UUID;

/**
 * S3 presigned POST compatibility tests.
 *
 * <p>Presigned POST allows browser-based uploads via {@code multipart/form-data} POST
 * to {@code /{bucket}}. The request includes a Base64-encoded policy with conditions
 * (e.g. content-length-range) and signing fields. This group verifies the emulator
 * handles these requests correctly.
 */
@FlociTestGroup
public class S3PresignedPostTests implements TestGroup {

    @Override
    public String name() { return "s3-presigned-post"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- S3 Presigned POST Tests ---");

        try (S3Client s3 = S3Client.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .forcePathStyle(true)
                .build()) {

            HttpClient http = HttpClient.newHttpClient();
            String bucket = "sdk-test-presigned-post-" + System.currentTimeMillis();
            s3.createBucket(b -> b.bucket(bucket));

            // 1. Basic presigned POST upload
            try {
                String key = "upload-" + UUID.randomUUID() + ".txt";
                String body = "Hello from presigned POST!";
                String policy = buildPolicy(bucket, key, 0, 1024);

                int status = postMultipart(http, ctx.endpoint, bucket, key, policy,
                        "text/plain", body.getBytes(StandardCharsets.UTF_8));
                ctx.check("S3 Presigned POST basic upload", status == 204 || status == 200);

                // Verify the object was stored
                var obj = s3.getObjectAsBytes(b -> b.bucket(bucket).key(key));
                ctx.check("S3 Presigned POST object content",
                        body.equals(obj.asUtf8String()));
            } catch (Exception e) { ctx.check("S3 Presigned POST basic upload", false, e); }

            // 2. Binary data upload
            try {
                String key = "binary-" + UUID.randomUUID() + ".bin";
                byte[] data = new byte[256];
                for (int i = 0; i < data.length; i++) data[i] = (byte) i;
                String policy = buildPolicy(bucket, key, 0, 512);

                int status = postMultipart(http, ctx.endpoint, bucket, key, policy,
                        "application/octet-stream", data);
                ctx.check("S3 Presigned POST binary upload", status == 204 || status == 200);

                var obj = s3.getObjectAsBytes(b -> b.bucket(bucket).key(key));
                byte[] downloaded = obj.asByteArray();
                boolean match = downloaded.length == data.length;
                for (int i = 0; match && i < data.length; i++) match = downloaded[i] == data[i];
                ctx.check("S3 Presigned POST binary content", match);
            } catch (Exception e) { ctx.check("S3 Presigned POST binary upload", false, e); }

            // 3. Content-length-range enforcement — reject oversized file
            try {
                String key = "oversized-" + UUID.randomUUID() + ".txt";
                byte[] data = new byte[200];
                String policy = buildPolicy(bucket, key, 0, 100); // max 100 bytes

                int status = postMultipart(http, ctx.endpoint, bucket, key, policy,
                        "text/plain", data);
                ctx.check("S3 Presigned POST reject oversized", status == 400 || status == 403);
            } catch (Exception e) { ctx.check("S3 Presigned POST reject oversized", false, e); }

            // 4. Upload without policy skips validation
            try {
                String key = "no-policy-" + UUID.randomUUID() + ".txt";
                String body = "no policy upload";

                int status = postMultipartNoPolicy(http, ctx.endpoint, bucket, key,
                        "text/plain", body.getBytes(StandardCharsets.UTF_8));
                ctx.check("S3 Presigned POST without policy", status == 204 || status == 200);

                var obj = s3.getObjectAsBytes(b -> b.bucket(bucket).key(key));
                ctx.check("S3 Presigned POST no-policy content",
                        body.equals(obj.asUtf8String()));
            } catch (Exception e) { ctx.check("S3 Presigned POST without policy", false, e); }

            // 5. Missing key field returns error
            try {
                String boundary = "----FormBoundary" + UUID.randomUUID();
                StringBuilder sb = new StringBuilder();
                sb.append("--").append(boundary).append("\r\n");
                sb.append("Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"\r\n");
                sb.append("Content-Type: text/plain\r\n\r\n");
                sb.append("data\r\n");
                sb.append("--").append(boundary).append("--\r\n");

                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(ctx.endpoint + "/" + bucket))
                        .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                        .POST(HttpRequest.BodyPublishers.ofString(sb.toString()))
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                ctx.check("S3 Presigned POST missing key field", resp.statusCode() == 400);
            } catch (Exception e) { ctx.check("S3 Presigned POST missing key field", false, e); }

            // 6. Missing file field returns error
            try {
                String boundary = "----FormBoundary" + UUID.randomUUID();
                StringBuilder sb = new StringBuilder();
                sb.append("--").append(boundary).append("\r\n");
                sb.append("Content-Disposition: form-data; name=\"key\"\r\n\r\n");
                sb.append("missing-file.txt\r\n");
                sb.append("--").append(boundary).append("--\r\n");

                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(ctx.endpoint + "/" + bucket))
                        .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                        .POST(HttpRequest.BodyPublishers.ofString(sb.toString()))
                        .build();
                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                ctx.check("S3 Presigned POST missing file field", resp.statusCode() == 400);
            } catch (Exception e) { ctx.check("S3 Presigned POST missing file field", false, e); }

            // 7. Content-Type from form field takes precedence
            try {
                String key = "ct-override-" + UUID.randomUUID() + ".json";
                String body = "{\"test\": true}";
                String policy = buildPolicy(bucket, key, 0, 1024);

                // Post with Content-Type form field set to application/json,
                // but file part Content-Type is text/plain
                int status = postMultipartWithContentTypeField(http, ctx.endpoint, bucket, key,
                        policy, "application/json", "text/plain",
                        body.getBytes(StandardCharsets.UTF_8));
                ctx.check("S3 Presigned POST Content-Type override upload",
                        status == 204 || status == 200);

                HeadObjectResponse head = s3.headObject(b -> b.bucket(bucket).key(key));
                ctx.check("S3 Presigned POST Content-Type from form field",
                        "application/json".equals(head.contentType()));
            } catch (Exception e) { ctx.check("S3 Presigned POST Content-Type override", false, e); }

            // 8. POST to non-existent bucket returns error
            try {
                String key = "fail-" + UUID.randomUUID() + ".txt";
                String policy = buildPolicy("no-such-bucket-xyz", key, 0, 1024);
                int status = postMultipart(http, ctx.endpoint, "no-such-bucket-xyz", key,
                        policy, "text/plain", "data".getBytes(StandardCharsets.UTF_8));
                ctx.check("S3 Presigned POST non-existent bucket",
                        status == 404 || status == 400 || status == 403);
            } catch (Exception e) { ctx.check("S3 Presigned POST non-existent bucket", false, e); }

            // Cleanup
            try {
                ListObjectsV2Response list = s3.listObjectsV2(b -> b.bucket(bucket));
                for (var obj : list.contents()) s3.deleteObject(b -> b.bucket(bucket).key(obj.key()));
                s3.deleteBucket(b -> b.bucket(bucket));
            } catch (Exception ignored) {}

        } catch (Exception e) {
            ctx.check("S3 Presigned POST Client", false, e);
        }
    }

    // ---- helpers ----

    private String buildPolicy(String bucket, String key, int minLength, int maxLength) {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        String expiration = now.plusHours(1).format(DateTimeFormatter.ISO_INSTANT);
        String date = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String amzDate = now.format(DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'"));
        String credential = "test/" + date + "/us-east-1/s3/aws4_request";

        String json = "{\"expiration\":\"" + expiration + "\",\"conditions\":["
                + "{\"bucket\":\"" + bucket + "\"},"
                + "[\"eq\",\"$key\",\"" + key + "\"],"
                + "{\"x-amz-credential\":\"" + credential + "\"},"
                + "{\"x-amz-algorithm\":\"AWS4-HMAC-SHA256\"},"
                + "{\"x-amz-date\":\"" + amzDate + "\"},"
                + "[\"content-length-range\"," + minLength + "," + maxLength + "]"
                + "]}";
        return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }

    private int postMultipart(HttpClient http, URI endpoint, String bucket,
                              String key, String policy, String contentType,
                              byte[] fileData) throws Exception {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        String date = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String amzDate = now.format(DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'"));
        String credential = "test/" + date + "/us-east-1/s3/aws4_request";

        String boundary = "----FormBoundary" + UUID.randomUUID();
        byte[] body = buildMultipartBody(boundary, key, policy, credential,
                amzDate, contentType, null, fileData);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/" + bucket))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    private int postMultipartNoPolicy(HttpClient http, URI endpoint, String bucket,
                                      String key, String contentType,
                                      byte[] fileData) throws Exception {
        String boundary = "----FormBoundary" + UUID.randomUUID();
        StringBuilder sb = new StringBuilder();
        // key field
        sb.append("--").append(boundary).append("\r\n");
        sb.append("Content-Disposition: form-data; name=\"key\"\r\n\r\n");
        sb.append(key).append("\r\n");
        // file field
        sb.append("--").append(boundary).append("\r\n");
        sb.append("Content-Disposition: form-data; name=\"file\"; filename=\"upload\"\r\n");
        sb.append("Content-Type: ").append(contentType).append("\r\n\r\n");

        byte[] prefix = sb.toString().getBytes(StandardCharsets.UTF_8);
        byte[] suffix = ("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8);
        byte[] body = new byte[prefix.length + fileData.length + suffix.length];
        System.arraycopy(prefix, 0, body, 0, prefix.length);
        System.arraycopy(fileData, 0, body, prefix.length, fileData.length);
        System.arraycopy(suffix, 0, body, prefix.length + fileData.length, suffix.length);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/" + bucket))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    private int postMultipartWithContentTypeField(HttpClient http, URI endpoint,
                                                  String bucket, String key, String policy,
                                                  String formContentType, String fileContentType,
                                                  byte[] fileData) throws Exception {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        String date = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String amzDate = now.format(DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'"));
        String credential = "test/" + date + "/us-east-1/s3/aws4_request";

        String boundary = "----FormBoundary" + UUID.randomUUID();
        byte[] body = buildMultipartBody(boundary, key, policy, credential,
                amzDate, fileContentType, formContentType, fileData);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/" + bucket))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    /**
     * Builds a multipart/form-data body with signing fields, optional Content-Type
     * form field, and the file part.
     */
    private byte[] buildMultipartBody(String boundary, String key, String policy,
                                      String credential, String amzDate,
                                      String fileContentType, String formContentType,
                                      byte[] fileData) {
        StringBuilder sb = new StringBuilder();
        // key
        sb.append("--").append(boundary).append("\r\n");
        sb.append("Content-Disposition: form-data; name=\"key\"\r\n\r\n");
        sb.append(key).append("\r\n");
        // policy
        sb.append("--").append(boundary).append("\r\n");
        sb.append("Content-Disposition: form-data; name=\"policy\"\r\n\r\n");
        sb.append(policy).append("\r\n");
        // x-amz-credential
        sb.append("--").append(boundary).append("\r\n");
        sb.append("Content-Disposition: form-data; name=\"x-amz-credential\"\r\n\r\n");
        sb.append(credential).append("\r\n");
        // x-amz-algorithm
        sb.append("--").append(boundary).append("\r\n");
        sb.append("Content-Disposition: form-data; name=\"x-amz-algorithm\"\r\n\r\n");
        sb.append("AWS4-HMAC-SHA256\r\n");
        // x-amz-date
        sb.append("--").append(boundary).append("\r\n");
        sb.append("Content-Disposition: form-data; name=\"x-amz-date\"\r\n\r\n");
        sb.append(amzDate).append("\r\n");
        // x-amz-signature (dummy — emulator doesn't validate signatures)
        sb.append("--").append(boundary).append("\r\n");
        sb.append("Content-Disposition: form-data; name=\"x-amz-signature\"\r\n\r\n");
        sb.append("0000000000000000000000000000000000000000000000000000000000000000\r\n");
        // Optional Content-Type form field (takes precedence over file part)
        if (formContentType != null) {
            sb.append("--").append(boundary).append("\r\n");
            sb.append("Content-Disposition: form-data; name=\"Content-Type\"\r\n\r\n");
            sb.append(formContentType).append("\r\n");
        }
        // file (must be last)
        sb.append("--").append(boundary).append("\r\n");
        sb.append("Content-Disposition: form-data; name=\"file\"; filename=\"upload\"\r\n");
        sb.append("Content-Type: ").append(fileContentType).append("\r\n\r\n");

        byte[] prefix = sb.toString().getBytes(StandardCharsets.UTF_8);
        byte[] suffix = ("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8);
        byte[] body = new byte[prefix.length + fileData.length + suffix.length];
        System.arraycopy(prefix, 0, body, 0, prefix.length);
        System.arraycopy(fileData, 0, body, prefix.length, fileData.length);
        System.arraycopy(suffix, 0, body, prefix.length + fileData.length, suffix.length);
        return body;
    }
}
