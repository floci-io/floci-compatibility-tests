package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@FlociTestGroup
public class S3RangeTests implements TestGroup {

    @Override
    public String name() { return "s3-range"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- S3 Range Tests ---");

        try (S3Client s3 = S3Client.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .forcePathStyle(true)
                .build()) {

            String bucket = "sdk-range-test-bucket-" + System.currentTimeMillis();
            String key = "range-test.txt";
            // "Hello, Range World!!" = 20 bytes (indices 0-19)
            String content = "Hello, Range World!!";

            s3.createBucket(b -> b.bucket(bucket));
            s3.putObject(PutObjectRequest.builder()
                    .bucket(bucket).key(key).contentType("text/plain").build(),
                    RequestBody.fromString(content));

            // 1. Accept-Ranges header on normal GetObject
            try {
                ResponseBytes<GetObjectResponse> resp = s3.getObjectAsBytes(
                        GetObjectRequest.builder().bucket(bucket).key(key).build());
                ctx.check("S3 GetObject Accept-Ranges header",
                        "bytes".equals(resp.response().acceptRanges()));
            } catch (Exception e) { ctx.check("S3 GetObject Accept-Ranges header", false, e); }

            // 2. Accept-Ranges header on HeadObject
            try {
                HeadObjectResponse resp = s3.headObject(b -> b.bucket(bucket).key(key));
                ctx.check("S3 HeadObject Accept-Ranges header",
                        "bytes".equals(resp.acceptRanges()));
            } catch (Exception e) { ctx.check("S3 HeadObject Accept-Ranges header", false, e); }

            // 3. Full range bytes=0-4 → "Hello" (5 bytes)
            try {
                ResponseBytes<GetObjectResponse> resp = s3.getObjectAsBytes(
                        GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=0-4").build());
                ctx.check("S3 GetObject Range bytes=0-4",
                        "Hello".equals(resp.asUtf8String())
                        && "bytes 0-4/20".equals(resp.response().contentRange()));
            } catch (Exception e) { ctx.check("S3 GetObject Range bytes=0-4", false, e); }

            // 4. Open-ended range bytes=15- → "rld!!" (bytes 15-19)
            try {
                ResponseBytes<GetObjectResponse> resp = s3.getObjectAsBytes(
                        GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=15-").build());
                ctx.check("S3 GetObject Range bytes=15-",
                        "rld!!".equals(resp.asUtf8String())
                        && "bytes 15-19/20".equals(resp.response().contentRange()));
            } catch (Exception e) { ctx.check("S3 GetObject Range bytes=15-", false, e); }

            // 5. Suffix range bytes=-5 → last 5 bytes "rld!!" (bytes 15-19)
            try {
                ResponseBytes<GetObjectResponse> resp = s3.getObjectAsBytes(
                        GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=-5").build());
                ctx.check("S3 GetObject Range bytes=-5",
                        "rld!!".equals(resp.asUtf8String())
                        && "bytes 15-19/20".equals(resp.response().contentRange()));
            } catch (Exception e) { ctx.check("S3 GetObject Range bytes=-5", false, e); }

            // 6. Single-byte range at end bytes=19-19 → "!"
            try {
                ResponseBytes<GetObjectResponse> resp = s3.getObjectAsBytes(
                        GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=19-19").build());
                ctx.check("S3 GetObject Range bytes=19-19 (last byte)",
                        "!".equals(resp.asUtf8String())
                        && "bytes 19-19/20".equals(resp.response().contentRange()));
            } catch (Exception e) { ctx.check("S3 GetObject Range bytes=19-19 (last byte)", false, e); }

            // 7. Out-of-bounds range bytes=50-100 → 416 InvalidRange
            try {
                s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=50-100").build());
                ctx.check("S3 GetObject Range out-of-bounds returns 416", false);
            } catch (S3Exception e) {
                ctx.check("S3 GetObject Range out-of-bounds returns 416",
                        e.statusCode() == 416 && "InvalidRange".equals(e.awsErrorDetails().errorCode()));
            } catch (Exception e) { ctx.check("S3 GetObject Range out-of-bounds returns 416", false, e); }

            // Cleanup
            s3.deleteObject(b -> b.bucket(bucket).key(key));
            s3.deleteBucket(b -> b.bucket(bucket));
        }
    }
}
