package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.List;

@FlociTestGroup
public class S3MultipartTests implements TestGroup {

    @Override
    public String name() { return "s3-multipart"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- S3 Multipart Tests ---");

        try (S3Client s3 = S3Client.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .forcePathStyle(true)
                .build()) {

            String bucket = "sdk-multipart-test";
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

            try {
                runMultipartTests(ctx, s3, bucket);
            } finally {
                cleanup(s3, bucket);
            }
        }
    }

    private void runMultipartTests(TestContext ctx, S3Client s3, String bucket) {
        String key = "multipart-object.bin";
        byte[] part1 = "Part-1-Data-Hello-World-".repeat(1000).getBytes();
        byte[] part2 = "Part-2-Data-Foo-Bar-Baz-".repeat(1000).getBytes();

        // 1. CreateMultipartUpload
        String uploadId;
        try {
            CreateMultipartUploadResponse initResp = s3.createMultipartUpload(
                    CreateMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType("application/octet-stream")
                            .build());
            uploadId = initResp.uploadId();
            ctx.check("S3 Multipart CreateMultipartUpload", uploadId != null && !uploadId.isBlank());
        } catch (Exception e) {
            ctx.check("S3 Multipart CreateMultipartUpload", false, e);
            return;
        }

        // 2. UploadPart — part 1
        String etag1;
        try {
            UploadPartResponse p1 = s3.uploadPart(
                    UploadPartRequest.builder()
                            .bucket(bucket).key(key)
                            .uploadId(uploadId).partNumber(1)
                            .build(),
                    RequestBody.fromBytes(part1));
            etag1 = p1.eTag();
            ctx.check("S3 Multipart UploadPart 1", etag1 != null && !etag1.isBlank());
        } catch (Exception e) {
            ctx.check("S3 Multipart UploadPart 1", false, e);
            return;
        }

        // 3. UploadPart — part 2
        String etag2;
        try {
            UploadPartResponse p2 = s3.uploadPart(
                    UploadPartRequest.builder()
                            .bucket(bucket).key(key)
                            .uploadId(uploadId).partNumber(2)
                            .build(),
                    RequestBody.fromBytes(part2));
            etag2 = p2.eTag();
            ctx.check("S3 Multipart UploadPart 2", etag2 != null && !etag2.isBlank());
        } catch (Exception e) {
            ctx.check("S3 Multipart UploadPart 2", false, e);
            return;
        }

        // 4. ListMultipartUploads — upload must appear
        try {
            ListMultipartUploadsResponse listUploads = s3.listMultipartUploads(
                    ListMultipartUploadsRequest.builder().bucket(bucket).build());
            boolean found = listUploads.uploads().stream()
                    .anyMatch(u -> uploadId.equals(u.uploadId()) && key.equals(u.key()));
            ctx.check("S3 Multipart ListMultipartUploads", found);
        } catch (Exception e) {
            ctx.check("S3 Multipart ListMultipartUploads", false, e);
        }

        // 5. ListParts — both parts must be returned with correct numbers
        try {
            ListPartsResponse listParts = s3.listParts(
                    ListPartsRequest.builder()
                            .bucket(bucket).key(key).uploadId(uploadId)
                            .build());
            List<Part> parts = listParts.parts();
            boolean hasPart1 = parts.stream().anyMatch(p -> p.partNumber() == 1);
            boolean hasPart2 = parts.stream().anyMatch(p -> p.partNumber() == 2);
            ctx.check("S3 Multipart ListParts count", parts.size() == 2);
            ctx.check("S3 Multipart ListParts part 1 present", hasPart1);
            ctx.check("S3 Multipart ListParts part 2 present", hasPart2);
            ctx.check("S3 Multipart ListParts not truncated", Boolean.FALSE.equals(listParts.isTruncated()));
        } catch (Exception e) {
            ctx.check("S3 Multipart ListParts count", false, e);
            ctx.check("S3 Multipart ListParts part 1 present", false);
            ctx.check("S3 Multipart ListParts part 2 present", false);
            ctx.check("S3 Multipart ListParts not truncated", false);
        }

        // 6. ListParts with max-parts pagination
        try {
            ListPartsResponse page1 = s3.listParts(
                    ListPartsRequest.builder()
                            .bucket(bucket).key(key).uploadId(uploadId)
                            .maxParts(1)
                            .build());
            ctx.check("S3 Multipart ListParts paginated page 1 size", page1.parts().size() == 1);
            ctx.check("S3 Multipart ListParts paginated page 1 truncated", Boolean.TRUE.equals(page1.isTruncated()));
            ctx.check("S3 Multipart ListParts paginated page 1 part number", page1.parts().get(0).partNumber() == 1);

            ListPartsResponse page2 = s3.listParts(
                    ListPartsRequest.builder()
                            .bucket(bucket).key(key).uploadId(uploadId)
                            .maxParts(1)
                            .partNumberMarker(page1.nextPartNumberMarker())
                            .build());
            ctx.check("S3 Multipart ListParts paginated page 2 size", page2.parts().size() == 1);
            ctx.check("S3 Multipart ListParts paginated page 2 not truncated", Boolean.FALSE.equals(page2.isTruncated()));
            ctx.check("S3 Multipart ListParts paginated page 2 part number", page2.parts().get(0).partNumber() == 2);
        } catch (Exception e) {
            ctx.check("S3 Multipart ListParts paginated page 1 size", false, e);
            ctx.check("S3 Multipart ListParts paginated page 1 truncated", false);
            ctx.check("S3 Multipart ListParts paginated page 1 part number", false);
            ctx.check("S3 Multipart ListParts paginated page 2 size", false);
            ctx.check("S3 Multipart ListParts paginated page 2 not truncated", false);
            ctx.check("S3 Multipart ListParts paginated page 2 part number", false);
        }

        // 7. ListParts — unknown uploadId must throw NoSuchUploadException
        try {
            s3.listParts(ListPartsRequest.builder()
                    .bucket(bucket).key(key).uploadId("non-existent-upload-id")
                    .build());
            ctx.check("S3 Multipart ListParts unknown uploadId", false);
        } catch (NoSuchUploadException e) {
            ctx.check("S3 Multipart ListParts unknown uploadId", true);
        } catch (S3Exception e) {
            ctx.check("S3 Multipart ListParts unknown uploadId", e.statusCode() == 404);
        } catch (Exception e) {
            ctx.check("S3 Multipart ListParts unknown uploadId", false, e);
        }

        // 8. CompleteMultipartUpload
        try {
            CompleteMultipartUploadResponse completeResp = s3.completeMultipartUpload(
                    CompleteMultipartUploadRequest.builder()
                            .bucket(bucket).key(key).uploadId(uploadId)
                            .multipartUpload(CompletedMultipartUpload.builder()
                                    .parts(
                                            CompletedPart.builder().partNumber(1).eTag(etag1).build(),
                                            CompletedPart.builder().partNumber(2).eTag(etag2).build()
                                    )
                                    .build())
                            .build());
            ctx.check("S3 Multipart CompleteMultipartUpload", completeResp.eTag() != null);
        } catch (Exception e) {
            ctx.check("S3 Multipart CompleteMultipartUpload", false, e);
            return;
        }

        // 9. GetObject — verify combined content
        try {
            byte[] body = s3.getObject(GetObjectRequest.builder()
                    .bucket(bucket).key(key).build()).readAllBytes();
            int expectedLen = part1.length + part2.length;
            ctx.check("S3 Multipart GetObject content length", body.length == expectedLen);
        } catch (Exception e) {
            ctx.check("S3 Multipart GetObject content length", false, e);
        }

        // 10. ListMultipartUploads — completed upload must no longer appear
        try {
            ListMultipartUploadsResponse listAfter = s3.listMultipartUploads(
                    ListMultipartUploadsRequest.builder().bucket(bucket).build());
            boolean gone = listAfter.uploads().stream()
                    .noneMatch(u -> uploadId.equals(u.uploadId()));
            ctx.check("S3 Multipart upload gone after complete", gone);
        } catch (Exception e) {
            ctx.check("S3 Multipart upload gone after complete", false, e);
        }

        // 11. AbortMultipartUpload
        String abortKey = "abort-test.bin";
        try {
            String abortUploadId = s3.createMultipartUpload(
                    CreateMultipartUploadRequest.builder().bucket(bucket).key(abortKey).build())
                    .uploadId();

            s3.uploadPart(UploadPartRequest.builder()
                            .bucket(bucket).key(abortKey)
                            .uploadId(abortUploadId).partNumber(1).build(),
                    RequestBody.fromString("some data"));

            s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(bucket).key(abortKey).uploadId(abortUploadId).build());

            boolean aborted = s3.listMultipartUploads(
                    ListMultipartUploadsRequest.builder().bucket(bucket).build())
                    .uploads().stream()
                    .noneMatch(u -> abortUploadId.equals(u.uploadId()));
            ctx.check("S3 Multipart AbortMultipartUpload", aborted);
        } catch (Exception e) {
            ctx.check("S3 Multipart AbortMultipartUpload", false, e);
        }
    }

    private void cleanup(S3Client s3, String bucket) {
        try {
            s3.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).build())
                    .contents()
                    .forEach(o -> s3.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucket).key(o.key()).build()));
            s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
        } catch (Exception ignored) {}
    }
}