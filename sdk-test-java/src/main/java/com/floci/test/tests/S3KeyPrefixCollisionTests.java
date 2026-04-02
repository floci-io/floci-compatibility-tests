package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.charset.StandardCharsets;

/**
 * Validates that S3 correctly handles keys that overlap with prefixes.
 * In S3's flat namespace, "output.parquet" and "output.parquet/part-0001.parquet"
 * are independent objects that must coexist without conflict.
 */
@FlociTestGroup
public class S3KeyPrefixCollisionTests implements TestGroup {

    @Override
    public String name() { return "s3-key-prefix-collision"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- S3 Key/Prefix Collision Tests ---");

        try (S3Client s3 = S3Client.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .forcePathStyle(true)
                .build()) {

            String bucket = "sdk-test-collision-" + System.currentTimeMillis();
            String markerKey = "output.parquet";
            String childKey = "output.parquet/part-0001.parquet";
            String markerContent = "marker-data";
            String childContent = "child-partition-data";

            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

            // --- Scenario 1: child first, then marker ---

            try {
                s3.putObject(PutObjectRequest.builder().bucket(bucket).key(childKey).build(), RequestBody.fromString(childContent));
                ctx.check("S3 PutObject child-first (child key)", true);
            } catch (Exception e) {
                ctx.check("S3 PutObject child-first (child key)", false, e);
            }

            try {
                s3.putObject(PutObjectRequest.builder().bucket(bucket).key(markerKey).build(), RequestBody.fromString(markerContent));
                ctx.check("S3 PutObject child-first (marker key)", true);
            } catch (Exception e) {
                ctx.check("S3 PutObject child-first (marker key)", false, e);
            }

            try {
                var response = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(childKey).build());
                String data = new String(response.readAllBytes(), StandardCharsets.UTF_8);
                ctx.check("S3 GetObject child-first (child readable)", childContent.equals(data));
            } catch (Exception e) {
                ctx.check("S3 GetObject child-first (child readable)", false, e);
            }

            try {
                var response = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(markerKey).build());
                String data = new String(response.readAllBytes(), StandardCharsets.UTF_8);
                ctx.check("S3 GetObject child-first (marker readable)", markerContent.equals(data));
            } catch (Exception e) {
                ctx.check("S3 GetObject child-first (marker readable)", false, e);
            }

            // clean up for scenario 2
            try {
                s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(childKey).build());
                s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(markerKey).build());
            } catch (Exception ignored) {}

            // --- Scenario 2: marker first, then child ---

            try {
                s3.putObject(PutObjectRequest.builder().bucket(bucket).key(markerKey).build(), RequestBody.fromString(markerContent));
                ctx.check("S3 PutObject marker-first (marker key)", true);
            } catch (Exception e) {
                ctx.check("S3 PutObject marker-first (marker key)", false, e);
            }

            try {
                s3.putObject(PutObjectRequest.builder().bucket(bucket).key(childKey).build(), RequestBody.fromString(childContent));
                ctx.check("S3 PutObject marker-first (child key)", true);
            } catch (Exception e) {
                ctx.check("S3 PutObject marker-first (child key)", false, e);
            }

            try {
                var response = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(markerKey).build());
                String data = new String(response.readAllBytes(), StandardCharsets.UTF_8);
                ctx.check("S3 GetObject marker-first (marker readable)", markerContent.equals(data));
            } catch (Exception e) {
                ctx.check("S3 GetObject marker-first (marker readable)", false, e);
            }

            try {
                var response = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(childKey).build());
                String data = new String(response.readAllBytes(), StandardCharsets.UTF_8);
                ctx.check("S3 GetObject marker-first (child readable)", childContent.equals(data));
            } catch (Exception e) {
                ctx.check("S3 GetObject marker-first (child readable)", false, e);
            }

            // --- Scenario 3: flat listing ---

            try {
                ListObjectsV2Response listResp = s3.listObjectsV2(
                        ListObjectsV2Request.builder().bucket(bucket).build());
                boolean markerFound = listResp.contents().stream().anyMatch(o -> markerKey.equals(o.key()));
                boolean childFound = listResp.contents().stream().anyMatch(o -> childKey.equals(o.key()));
                ctx.check("S3 ListObjectsV2 both keys present", markerFound && childFound);
            } catch (Exception e) {
                ctx.check("S3 ListObjectsV2 both keys present", false, e);
            }

            // --- Scenario 4: delimiter listing ---

            try {
                ListObjectsV2Response listResp = s3.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).delimiter("/").build());
                boolean markerInContents = listResp
                        .contents()
                        .stream()
                        .anyMatch(o -> markerKey.equals(o.key()));
                boolean childInPrefixes = listResp
                        .commonPrefixes()
                        .stream()
                        .anyMatch(cp -> "output.parquet/".equals(cp.prefix()));
                ctx.check("S3 ListObjectsV2 delimiter marker in Contents", markerInContents);
                ctx.check("S3 ListObjectsV2 delimiter child in CommonPrefixes", childInPrefixes);
            } catch (Exception e) {
                ctx.check("S3 ListObjectsV2 delimiter marker in Contents", false, e);
                ctx.check("S3 ListObjectsV2 delimiter child in CommonPrefixes", false, e);
            }

            // --- Scenario 5: delete marker, child survives ---

            try {
                s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(markerKey).build());
                ctx.check("S3 DeleteObject marker without affecting child", true);
            } catch (Exception e) {
                ctx.check("S3 DeleteObject marker without affecting child", false, e);
            }

            try {
                var response = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(childKey).build());
                String data = new String(response.readAllBytes(), StandardCharsets.UTF_8);
                ctx.check("S3 GetObject child survives marker deletion", childContent.equals(data));
            } catch (Exception e) {
                ctx.check("S3 GetObject child survives marker deletion", false, e);
            }

            try {
                s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(markerKey).build());
                ctx.check("S3 HeadObject marker gone after deletion", false);
            } catch (NoSuchKeyException e) {
                ctx.check("S3 HeadObject marker gone after deletion", true);
            } catch (S3Exception e) {
                ctx.check("S3 HeadObject marker gone after deletion", e.statusCode() == 404);
            } catch (Exception e) {
                ctx.check("S3 HeadObject marker gone after deletion", false, e);
            }

            // Cleanup
            try {
                ListObjectsV2Response list = s3.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).build());
                for (var obj : list.contents()) {
                    s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(obj.key()).build());
                }
                s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
            } catch (Exception ignored) {}
        }
    }
}
