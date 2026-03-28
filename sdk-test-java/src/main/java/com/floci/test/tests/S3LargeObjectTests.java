package com.floci.test.tests;

import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * Validates large S3 object uploads (25 MB).
 * Protects against regressions of the upload-size limit fix.
 */
public class S3LargeObjectTests implements TestGroup {

    private static final int SIZE_MB = 25;
    private static final int SIZE_BYTES = SIZE_MB * 1024 * 1024;

    @Override
    public String name() { return "s3-large-object"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- S3 Large Object Tests ---");

        try (S3Client s3 = S3Client.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .forcePathStyle(true)
                .build()) {

            String bucket = "sdk-test-large-bucket-" + System.currentTimeMillis();
            String key = "large-object-25mb.bin";

            // Create dedicated bucket
            s3.createBucket(b -> b.bucket(bucket));

            // Upload 25 MB object — validates fix for upload size limit
            try {
                s3.putObject(
                        b -> b.bucket(bucket).key(key)
                                .contentLength((long) SIZE_BYTES)
                                .contentType("application/octet-stream"),
                        RequestBody.fromBytes(new byte[SIZE_BYTES]));
                ctx.check("S3 PutObject 25 MB", true);
            } catch (Exception e) {
                ctx.check("S3 PutObject 25 MB", false, e);
            }

            // Verify content-length via HeadObject
            try {
                HeadObjectResponse head = s3.headObject(b -> b.bucket(bucket).key(key));
                ctx.check("S3 HeadObject 25 MB content-length",
                        head.contentLength() != null && head.contentLength() == SIZE_BYTES);
            } catch (Exception e) {
                ctx.check("S3 HeadObject 25 MB content-length", false, e);
            }

            // Cleanup
            try {
                s3.deleteObject(b -> b.bucket(bucket).key(key));
                s3.deleteBucket(b -> b.bucket(bucket));
            } catch (Exception ignored) {}

        } catch (Exception e) {
            ctx.check("S3 Large Object Client", false, e);
        }
    }
}
