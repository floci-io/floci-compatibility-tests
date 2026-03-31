package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.charset.StandardCharsets;

@FlociTestGroup
public class S3Tests implements TestGroup {

    @Override
    public String name() { return "s3"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- S3 Tests ---");

        try (S3Client s3 = S3Client.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .forcePathStyle(true)
                .build()) {

            String bucket = "sdk-test-bucket";
            String key = "test-file.txt";
            String content = "Hello from AWS SDK v2!";

            // 1. Create bucket
            try {
                s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
                ctx.check("S3 CreateBucket", true);
            } catch (Exception e) {
                ctx.check("S3 CreateBucket", false, e);
            }

            // 1b. CreateBucket with LocationConstraint (regression: issue #11)
            String euBucket = "sdk-test-bucket-eu";
            try {
                s3.createBucket(CreateBucketRequest.builder()
                        .bucket(euBucket)
                        .createBucketConfiguration(CreateBucketConfiguration.builder()
                                .locationConstraint(BucketLocationConstraint.EU_CENTRAL_1)
                                .build())
                        .build());
                ctx.check("S3 CreateBucket with LocationConstraint", true);
            } catch (Exception e) {
                ctx.check("S3 CreateBucket with LocationConstraint", false, e);
            }

            // 1c. GetBucketLocation
            try {
                GetBucketLocationResponse locResp = s3.getBucketLocation(
                        GetBucketLocationRequest.builder().bucket(euBucket).build());
                ctx.check("S3 GetBucketLocation (eu-central-1)",
                        BucketLocationConstraint.EU_CENTRAL_1.equals(locResp.locationConstraint()));
            } catch (Exception e) {
                ctx.check("S3 GetBucketLocation (eu-central-1)", false, e);
            }

            // 2. List buckets
            try {
                ListBucketsResponse listResp = s3.listBuckets();
                boolean found = listResp.buckets().stream().anyMatch(b -> bucket.equals(b.name()));
                ctx.check("S3 ListBuckets", found);
            } catch (Exception e) {
                ctx.check("S3 ListBuckets", false, e);
            }

            // 3. Put object
            try {
                s3.putObject(PutObjectRequest.builder()
                                .bucket(bucket).key(key).contentType("text/plain").build(),
                        RequestBody.fromString(content));
                ctx.check("S3 PutObject", true);
            } catch (Exception e) {
                ctx.check("S3 PutObject", false, e);
            }

            // 4. List objects
            try {
                ListObjectsV2Response listResp = s3.listObjectsV2(
                        ListObjectsV2Request.builder().bucket(bucket).build());
                boolean found = listResp.contents().stream().anyMatch(o -> key.equals(o.key()));
                ctx.check("S3 ListObjects", found);
            } catch (Exception e) {
                ctx.check("S3 ListObjects", false, e);
            }

            // 5. Get object
            try {
                var resp = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
                byte[] data = resp.readAllBytes();
                String downloaded = new String(data, StandardCharsets.UTF_8);
                ctx.check("S3 GetObject", content.equals(downloaded));
            } catch (Exception e) {
                ctx.check("S3 GetObject", false, e);
            }

            // 6. Head object
            try {
                HeadObjectResponse headResp = s3.headObject(HeadObjectRequest.builder()
                        .bucket(bucket).key(key).build());
                ctx.check("S3 HeadObject", headResp.contentLength() == content.length());
                ctx.check("S3 HeadObject LastModified second precision",
                        headResp.lastModified() != null && headResp.lastModified().getNano() == 0);
            } catch (Exception e) {
                ctx.check("S3 HeadObject", false, e);
            }

            // 7. HeadBucket
            try {
                HeadBucketResponse headBucketResp = s3.headBucket(HeadBucketRequest.builder()
                        .bucket(bucket).build());
                ctx.check("S3 HeadBucket", headBucketResp.sdkHttpResponse().isSuccessful());
            } catch (Exception e) {
                ctx.check("S3 HeadBucket", false, e);
            }

            // 8. HeadBucket - non-existent
            try {
                s3.headBucket(HeadBucketRequest.builder().bucket("non-existent-bucket-xyz").build());
                ctx.check("S3 HeadBucket non-existent", false);
            } catch (NoSuchBucketException e) {
                ctx.check("S3 HeadBucket non-existent", true);
            } catch (S3Exception e) {
                ctx.check("S3 HeadBucket non-existent", e.statusCode() == 404);
            } catch (Exception e) {
                ctx.check("S3 HeadBucket non-existent", false, e);
            }

            // 9. GetBucketLocation
            try {
                GetBucketLocationResponse locResp = s3.getBucketLocation(
                        GetBucketLocationRequest.builder().bucket(bucket).build());
                ctx.check("S3 GetBucketLocation", locResp.locationConstraint() != null
                        || locResp.locationConstraintAsString() != null);
            } catch (Exception e) {
                ctx.check("S3 GetBucketLocation", false, e);
            }

            // 10. PutObjectTagging
            try {
                s3.putObjectTagging(PutObjectTaggingRequest.builder()
                        .bucket(bucket).key(key)
                        .tagging(Tagging.builder()
                                .tagSet(
                                        Tag.builder().key("env").value("test").build(),
                                        Tag.builder().key("project").value("floci").build()
                                )
                                .build())
                        .build());
                ctx.check("S3 PutObjectTagging", true);
            } catch (Exception e) {
                ctx.check("S3 PutObjectTagging", false, e);
            }

            // 11. GetObjectTagging
            try {
                GetObjectTaggingResponse tagResp = s3.getObjectTagging(
                        GetObjectTaggingRequest.builder().bucket(bucket).key(key).build());
                boolean envFound = tagResp.tagSet().stream()
                        .anyMatch(t -> "env".equals(t.key()) && "test".equals(t.value()));
                boolean projFound = tagResp.tagSet().stream()
                        .anyMatch(t -> "project".equals(t.key()) && "floci".equals(t.value()));
                ctx.check("S3 GetObjectTagging", envFound && projFound && tagResp.tagSet().size() == 2);
            } catch (Exception e) {
                ctx.check("S3 GetObjectTagging", false, e);
            }

            // 12. DeleteObjectTagging
            try {
                s3.deleteObjectTagging(DeleteObjectTaggingRequest.builder()
                        .bucket(bucket).key(key).build());
                GetObjectTaggingResponse tagResp = s3.getObjectTagging(
                        GetObjectTaggingRequest.builder().bucket(bucket).key(key).build());
                ctx.check("S3 DeleteObjectTagging", tagResp.tagSet().isEmpty());
            } catch (Exception e) {
                ctx.check("S3 DeleteObjectTagging", false, e);
            }

            // 13. PutBucketTagging
            try {
                s3.putBucketTagging(PutBucketTaggingRequest.builder()
                        .bucket(bucket)
                        .tagging(Tagging.builder()
                                .tagSet(
                                        Tag.builder().key("team").value("backend").build(),
                                        Tag.builder().key("cost-center").value("123").build()
                                )
                                .build())
                        .build());
                ctx.check("S3 PutBucketTagging", true);
            } catch (Exception e) {
                ctx.check("S3 PutBucketTagging", false, e);
            }

            // 14. GetBucketTagging
            try {
                GetBucketTaggingResponse tagResp = s3.getBucketTagging(
                        GetBucketTaggingRequest.builder().bucket(bucket).build());
                boolean teamFound = tagResp.tagSet().stream()
                        .anyMatch(t -> "team".equals(t.key()) && "backend".equals(t.value()));
                boolean costFound = tagResp.tagSet().stream()
                        .anyMatch(t -> "cost-center".equals(t.key()) && "123".equals(t.value()));
                ctx.check("S3 GetBucketTagging", teamFound && costFound && tagResp.tagSet().size() == 2);
            } catch (Exception e) {
                ctx.check("S3 GetBucketTagging", false, e);
            }

            // 15. DeleteBucketTagging
            try {
                s3.deleteBucketTagging(DeleteBucketTaggingRequest.builder().bucket(bucket).build());
                GetBucketTaggingResponse tagResp = s3.getBucketTagging(
                        GetBucketTaggingRequest.builder().bucket(bucket).build());
                ctx.check("S3 DeleteBucketTagging", tagResp.tagSet().isEmpty());
            } catch (Exception e) {
                ctx.check("S3 DeleteBucketTagging", false, e);
            }

            // 16. CopyObject to another bucket
            String destBucket = "sdk-test-bucket-copy";
            String destKey = "copied-file.txt";
            try {
                s3.createBucket(CreateBucketRequest.builder().bucket(destBucket).build());
                CopyObjectResponse copyResp = s3.copyObject(CopyObjectRequest.builder()
                        .sourceBucket(bucket).sourceKey(key)
                        .destinationBucket(destBucket).destinationKey(destKey)
                        .build());
                ctx.check("S3 CopyObject cross-bucket", copyResp.copyObjectResult().eTag() != null);
            } catch (Exception e) {
                ctx.check("S3 CopyObject cross-bucket", false, e);
            }

            // 17. Verify copied object content
            try {
                var resp = s3.getObject(GetObjectRequest.builder()
                        .bucket(destBucket).key(destKey).build());
                byte[] data = resp.readAllBytes();
                String downloaded = new String(data, StandardCharsets.UTF_8);
                ctx.check("S3 GetObject from copy", content.equals(downloaded));
            } catch (Exception e) {
                ctx.check("S3 GetObject from copy", false, e);
            }

            // 18. Clean up dest bucket
            try {
                s3.deleteObject(DeleteObjectRequest.builder().bucket(destBucket).key(destKey).build());
                s3.deleteBucket(DeleteBucketRequest.builder().bucket(destBucket).build());
                ctx.check("S3 Delete copy bucket", true);
            } catch (Exception e) {
                ctx.check("S3 Delete copy bucket", false, e);
            }

            // 19. DeleteObjects (batch delete)
            try {
                for (int i = 1; i <= 3; i++) {
                    s3.putObject(PutObjectRequest.builder()
                                    .bucket(bucket).key("batch-" + i + ".txt").build(),
                            RequestBody.fromString("batch content " + i));
                }
                DeleteObjectsResponse deleteResp = s3.deleteObjects(DeleteObjectsRequest.builder()
                        .bucket(bucket)
                        .delete(software.amazon.awssdk.services.s3.model.Delete.builder()
                                .objects(
                                        ObjectIdentifier.builder().key("batch-1.txt").build(),
                                        ObjectIdentifier.builder().key("batch-2.txt").build(),
                                        ObjectIdentifier.builder().key("batch-3.txt").build()
                                )
                                .build())
                        .build());
                ctx.check("S3 DeleteObjects batch", deleteResp.deleted().size() == 3);
            } catch (Exception e) {
                ctx.check("S3 DeleteObjects batch", false, e);
            }

            // 20. Verify batch delete — bucket should only have the original object
            try {
                ListObjectsV2Response listResp = s3.listObjectsV2(
                        ListObjectsV2Request.builder().bucket(bucket).build());
                boolean onlyOriginal = listResp.contents().size() == 1
                        && key.equals(listResp.contents().get(0).key());
                ctx.check("S3 Verify batch delete", onlyOriginal);
            } catch (Exception e) {
                ctx.check("S3 Verify batch delete", false, e);
            }

            // 21. Delete object
            try {
                s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
                ctx.check("S3 DeleteObject", true);
            } catch (Exception e) {
                ctx.check("S3 DeleteObject", false, e);
            }

            // 22. Verify object deleted
            try {
                ListObjectsV2Response listResp = s3.listObjectsV2(
                        ListObjectsV2Request.builder().bucket(bucket).build());
                ctx.check("S3 Object deleted", listResp.contents().isEmpty());
            } catch (Exception e) {
                ctx.check("S3 Object deleted", false, e);
            }

            // cleanup eu bucket
            try { s3.deleteBucket(DeleteBucketRequest.builder().bucket(euBucket).build()); } catch (Exception ignored) {}

            // 23. Delete bucket
            try {
                s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
                ctx.check("S3 DeleteBucket", true);
            } catch (Exception e) {
                ctx.check("S3 DeleteBucket", false, e);
            }
        }
    }
}
