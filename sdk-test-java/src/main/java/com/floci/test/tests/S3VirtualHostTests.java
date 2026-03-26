package com.floci.test.tests;

import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * Tests S3 virtual-hosted-style addressing.
 *
 * <p>The AWS SDK is configured with {@code forcePathStyle(false)}, causing it
 * to send requests with the bucket name in the {@code Host} header
 * (e.g., {@code Host: bucket.localhost:4566}) instead of the URL path.
 */
public class S3VirtualHostTests implements TestGroup {

    @Override
    public String name() { return "s3-virtual-host"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- S3 Virtual Host Tests ---");

        String bucket = "sdk-vhost-bucket";

        // Path-style client for bucket creation (virtual-hosted needs the bucket to exist
        // before the SDK can resolve the host)
        try (S3Client pathStyle = S3Client.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .forcePathStyle(true)
                .build();
             S3Client vhost = S3Client.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .forcePathStyle(false)
                .build()) {

            // Create bucket via path-style (must exist before virtual-host requests)
            pathStyle.createBucket(b -> b.bucket(bucket));

            // 1. HeadBucket via virtual host
            try {
                HeadBucketResponse head = vhost.headBucket(b -> b.bucket(bucket));
                ctx.check("S3 VHost HeadBucket", head.sdkHttpResponse().isSuccessful());
            } catch (Exception e) { ctx.check("S3 VHost HeadBucket", false, e); }

            // 2. PutObject via virtual host
            try {
                PutObjectResponse put = vhost.putObject(
                        PutObjectRequest.builder()
                                .bucket(bucket).key("vhost-hello.txt")
                                .contentType("text/plain")
                                .metadata(java.util.Map.of("source", "virtual-host"))
                                .build(),
                        RequestBody.fromString("Hello from virtual host!"));
                ctx.check("S3 VHost PutObject", put.eTag() != null);
            } catch (Exception e) { ctx.check("S3 VHost PutObject", false, e); }

            // 3. GetObject via virtual host
            try {
                var get = vhost.getObjectAsBytes(b -> b.bucket(bucket).key("vhost-hello.txt"));
                ctx.check("S3 VHost GetObject",
                        "Hello from virtual host!".equals(get.asUtf8String()));
            } catch (Exception e) { ctx.check("S3 VHost GetObject", false, e); }

            // 4. HeadObject via virtual host
            try {
                HeadObjectResponse head = vhost.headObject(b -> b.bucket(bucket).key("vhost-hello.txt"));
                ctx.check("S3 VHost HeadObject",
                        head.eTag() != null
                        && "virtual-host".equals(head.metadata().get("source")));
            } catch (Exception e) { ctx.check("S3 VHost HeadObject", false, e); }

            // 5. PutObject with nested key via virtual host
            try {
                vhost.putObject(
                        PutObjectRequest.builder()
                                .bucket(bucket).key("path/to/nested.json")
                                .contentType("application/json").build(),
                        RequestBody.fromString("{\"nested\":true}"));
                ctx.check("S3 VHost PutObject nested key", true);
            } catch (Exception e) { ctx.check("S3 VHost PutObject nested key", false, e); }

            // 6. ListObjectsV2 via virtual host
            try {
                ListObjectsV2Response list = vhost.listObjectsV2(b -> b.bucket(bucket));
                boolean hasHello = list.contents().stream().anyMatch(o -> o.key().equals("vhost-hello.txt"));
                boolean hasNested = list.contents().stream().anyMatch(o -> o.key().equals("path/to/nested.json"));
                ctx.check("S3 VHost ListObjectsV2", hasHello && hasNested);
            } catch (Exception e) { ctx.check("S3 VHost ListObjectsV2", false, e); }

            // 7. ListObjectsV2 with prefix via virtual host
            try {
                ListObjectsV2Response list = vhost.listObjectsV2(b -> b.bucket(bucket).prefix("path/"));
                ctx.check("S3 VHost ListObjectsV2 prefix",
                        list.contents().size() == 1
                        && list.contents().get(0).key().equals("path/to/nested.json"));
            } catch (Exception e) { ctx.check("S3 VHost ListObjectsV2 prefix", false, e); }

            // 8. CopyObject via virtual host
            try {
                vhost.copyObject(CopyObjectRequest.builder()
                        .sourceBucket(bucket).sourceKey("vhost-hello.txt")
                        .destinationBucket(bucket).destinationKey("vhost-copy.txt")
                        .build());
                var copy = vhost.getObjectAsBytes(b -> b.bucket(bucket).key("vhost-copy.txt"));
                ctx.check("S3 VHost CopyObject",
                        "Hello from virtual host!".equals(copy.asUtf8String()));
            } catch (Exception e) { ctx.check("S3 VHost CopyObject", false, e); }

            // 9. DeleteObject via virtual host
            try {
                vhost.deleteObject(b -> b.bucket(bucket).key("vhost-copy.txt"));
                try {
                    vhost.headObject(b -> b.bucket(bucket).key("vhost-copy.txt"));
                    ctx.check("S3 VHost DeleteObject", false);
                } catch (NoSuchKeyException e2) {
                    ctx.check("S3 VHost DeleteObject", true);
                }
            } catch (Exception e) { ctx.check("S3 VHost DeleteObject", false, e); }

            // 10. Cross-style: object created via virtual host visible via path-style
            try {
                var get = pathStyle.getObjectAsBytes(b -> b.bucket(bucket).key("vhost-hello.txt"));
                ctx.check("S3 VHost cross-style read",
                        "Hello from virtual host!".equals(get.asUtf8String()));
            } catch (Exception e) { ctx.check("S3 VHost cross-style read", false, e); }

            // Cleanup
            try {
                ListObjectsV2Response list = pathStyle.listObjectsV2(b -> b.bucket(bucket));
                for (var obj : list.contents()) pathStyle.deleteObject(b -> b.bucket(bucket).key(obj.key()));
                pathStyle.deleteBucket(b -> b.bucket(bucket));
            } catch (Exception ignored) {}

        } catch (Exception e) {
            ctx.check("S3 Virtual Host Client", false, e);
        }
    }
}
