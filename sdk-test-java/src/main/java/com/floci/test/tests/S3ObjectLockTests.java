package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@FlociTestGroup
public class S3ObjectLockTests implements TestGroup {

    @Override
    public String name() { return "s3-object-lock"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- S3 Object Lock Tests ---");

        try (S3Client s3 = S3Client.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .forcePathStyle(true)
                .build()) {

            testBucketCreationWithLock(ctx, s3);
            testComplianceLock(ctx, s3);
            testGovernanceLock(ctx, s3);
            testLegalHold(ctx, s3);
            testBucketLockConfiguration(ctx, s3);
            testPutObjectRetention(ctx, s3);
        }
    }

    // --- 1. Bucket created with Object Lock enabled via header ---

    private void testBucketCreationWithLock(TestContext ctx, S3Client s3) {
        String bucket = "lock-bucket-creation";

        // 1. Create bucket with object lock enabled
        try {
            s3.createBucket(CreateBucketRequest.builder()
                    .bucket(bucket)
                    .objectLockEnabledForBucket(true)
                    .build());
            ctx.check("S3 ObjectLock CreateBucket with lock enabled", true);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock CreateBucket with lock enabled", false, e);
            return;
        }

        // 2. Versioning must have been auto-enabled — verify via GetBucketVersioning
        try {
            GetBucketVersioningResponse versioning = s3.getBucketVersioning(
                    GetBucketVersioningRequest.builder().bucket(bucket).build());
            ctx.check("S3 ObjectLock lock-enabled bucket has versioning",
                    BucketVersioningStatus.ENABLED.equals(versioning.status()));
        } catch (Exception e) {
            ctx.check("S3 ObjectLock lock-enabled bucket has versioning", false, e);
        }

        // cleanup — bucket is empty, just delete it
        try { s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build()); } catch (Exception ignore) {}
    }

    // --- 2. COMPLIANCE retention ---

    private void testComplianceLock(TestContext ctx, S3Client s3) {
        String bucket = "lock-compliance-" + System.currentTimeMillis();
        String key = "compliance-object.txt";
        Instant farFuture = Instant.now().plus(365 * 10, ChronoUnit.DAYS);

        // Setup: lock-enabled bucket
        try {
            s3.createBucket(CreateBucketRequest.builder()
                    .bucket(bucket)
                    .objectLockEnabledForBucket(true)
                    .build());
        } catch (Exception e) {
            ctx.check("S3 ObjectLock COMPLIANCE setup", false, e);
            return;
        }

        // 3. PutObject with COMPLIANCE mode
        try {
            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucket).key(key)
                            .objectLockMode(ObjectLockMode.COMPLIANCE)
                            .objectLockRetainUntilDate(farFuture)
                            .build(),
                    RequestBody.fromString("protected content"));
            ctx.check("S3 ObjectLock PutObject with COMPLIANCE mode", true);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock PutObject with COMPLIANCE mode", false, e);
            return;
        }

        // 4. HeadObject returns lock headers
        try {
            HeadObjectResponse head = s3.headObject(HeadObjectRequest.builder()
                    .bucket(bucket).key(key).build());
            boolean hasMode = ObjectLockMode.COMPLIANCE.equals(head.objectLockMode());
            boolean hasDate = head.objectLockRetainUntilDate() != null;
            ctx.check("S3 ObjectLock HeadObject returns COMPLIANCE mode", hasMode);
            ctx.check("S3 ObjectLock HeadObject returns retainUntilDate", hasDate);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock HeadObject returns COMPLIANCE mode", false, e);
            ctx.check("S3 ObjectLock HeadObject returns retainUntilDate", false, e);
        }

        // 5. Delete COMPLIANCE object — must be denied
        try {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket).key(key).build());
            ctx.check("S3 ObjectLock COMPLIANCE delete blocked", false);
        } catch (S3Exception e) {
            ctx.check("S3 ObjectLock COMPLIANCE delete blocked", e.statusCode() == 403);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock COMPLIANCE delete blocked", false, e);
        }

        // 6. Delete COMPLIANCE object with bypass — still denied (COMPLIANCE ignores bypass)
        try {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket).key(key)
                    .bypassGovernanceRetention(true)
                    .build());
            ctx.check("S3 ObjectLock COMPLIANCE bypass still blocked", false);
        } catch (S3Exception e) {
            ctx.check("S3 ObjectLock COMPLIANCE bypass still blocked", e.statusCode() == 403);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock COMPLIANCE bypass still blocked", false, e);
        }

        // 7. Overwrite COMPLIANCE object — must be denied
        try {
            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucket).key(key).build(),
                    RequestBody.fromString("overwrite attempt"));
            ctx.check("S3 ObjectLock COMPLIANCE overwrite blocked", false);
        } catch (S3Exception e) {
            ctx.check("S3 ObjectLock COMPLIANCE overwrite blocked", e.statusCode() == 403);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock COMPLIANCE overwrite blocked", false, e);
        }

        // No cleanup: COMPLIANCE-locked object cannot be deleted — left in the local emulator
    }

    // --- 3. GOVERNANCE retention ---

    private void testGovernanceLock(TestContext ctx, S3Client s3) {
        String bucket = "lock-governance-bucket";
        String key = "governance-object.txt";
        Instant farFuture = Instant.now().plus(365 * 10, ChronoUnit.DAYS);

        // Setup
        try {
            s3.createBucket(CreateBucketRequest.builder()
                    .bucket(bucket)
                    .objectLockEnabledForBucket(true)
                    .build());
        } catch (Exception e) {
            ctx.check("S3 ObjectLock GOVERNANCE setup", false, e);
            return;
        }

        // 8. PutObject with GOVERNANCE mode
        try {
            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucket).key(key)
                            .objectLockMode(ObjectLockMode.GOVERNANCE)
                            .objectLockRetainUntilDate(farFuture)
                            .build(),
                    RequestBody.fromString("governance content"));
            ctx.check("S3 ObjectLock PutObject with GOVERNANCE mode", true);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock PutObject with GOVERNANCE mode", false, e);
            return;
        }

        // 9. Delete GOVERNANCE without bypass — blocked
        try {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket).key(key).build());
            ctx.check("S3 ObjectLock GOVERNANCE delete blocked (no bypass)", false);
        } catch (S3Exception e) {
            ctx.check("S3 ObjectLock GOVERNANCE delete blocked (no bypass)", e.statusCode() == 403);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock GOVERNANCE delete blocked (no bypass)", false, e);
        }

        // 10. Delete GOVERNANCE with bypass — succeeds
        try {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket).key(key)
                    .bypassGovernanceRetention(true)
                    .build());
            ctx.check("S3 ObjectLock GOVERNANCE delete with bypass succeeds", true);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock GOVERNANCE delete with bypass succeeds", false, e);
        }

        // cleanup
        try { s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build()); } catch (Exception ignore) {}
    }

    // --- 4. Legal hold ---

    private void testLegalHold(TestContext ctx, S3Client s3) {
        String bucket = "lock-legal-hold-bucket";
        String key = "hold-object.txt";

        // Setup
        try {
            s3.createBucket(CreateBucketRequest.builder()
                    .bucket(bucket)
                    .objectLockEnabledForBucket(true)
                    .build());
            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucket).key(key).build(),
                    RequestBody.fromString("legal hold content"));
        } catch (Exception e) {
            ctx.check("S3 ObjectLock legal hold setup", false, e);
            return;
        }

        // 11. Put legal hold ON
        try {
            s3.putObjectLegalHold(PutObjectLegalHoldRequest.builder()
                    .bucket(bucket).key(key)
                    .legalHold(ObjectLockLegalHold.builder()
                            .status(ObjectLockLegalHoldStatus.ON)
                            .build())
                    .build());
            ctx.check("S3 ObjectLock PutObjectLegalHold ON", true);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock PutObjectLegalHold ON", false, e);
            return;
        }

        // 12. GetObjectLegalHold — verify status is ON
        try {
            GetObjectLegalHoldResponse resp = s3.getObjectLegalHold(
                    GetObjectLegalHoldRequest.builder().bucket(bucket).key(key).build());
            ctx.check("S3 ObjectLock GetObjectLegalHold returns ON",
                    ObjectLockLegalHoldStatus.ON.equals(resp.legalHold().status()));
        } catch (Exception e) {
            ctx.check("S3 ObjectLock GetObjectLegalHold returns ON", false, e);
        }

        // 13. Delete with legal hold ON — blocked (even with bypass)
        try {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket).key(key)
                    .bypassGovernanceRetention(true)
                    .build());
            ctx.check("S3 ObjectLock legal hold blocks delete", false);
        } catch (S3Exception e) {
            ctx.check("S3 ObjectLock legal hold blocks delete", e.statusCode() == 403);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock legal hold blocks delete", false, e);
        }

        // 14. Release legal hold (set to OFF)
        try {
            s3.putObjectLegalHold(PutObjectLegalHoldRequest.builder()
                    .bucket(bucket).key(key)
                    .legalHold(ObjectLockLegalHold.builder()
                            .status(ObjectLockLegalHoldStatus.OFF)
                            .build())
                    .build());
            ctx.check("S3 ObjectLock PutObjectLegalHold OFF", true);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock PutObjectLegalHold OFF", false, e);
            return;
        }

        // 15. GetObjectLegalHold — verify status is OFF
        try {
            GetObjectLegalHoldResponse resp = s3.getObjectLegalHold(
                    GetObjectLegalHoldRequest.builder().bucket(bucket).key(key).build());
            ctx.check("S3 ObjectLock GetObjectLegalHold returns OFF",
                    ObjectLockLegalHoldStatus.OFF.equals(resp.legalHold().status()));
        } catch (Exception e) {
            ctx.check("S3 ObjectLock GetObjectLegalHold returns OFF", false, e);
        }

        // 16. Delete after legal hold released — succeeds
        try {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket).key(key)
                    .bypassGovernanceRetention(true)
                    .build());
            ctx.check("S3 ObjectLock delete after legal hold released", true);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock delete after legal hold released", false, e);
        }

        // cleanup
        try { s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build()); } catch (Exception ignore) {}
    }

    // --- 5. Bucket-level lock configuration (default retention) ---

    private void testBucketLockConfiguration(TestContext ctx, S3Client s3) {
        String bucket = "lock-config-bucket";
        String key = "auto-locked.txt";

        // Setup
        try {
            s3.createBucket(CreateBucketRequest.builder()
                    .bucket(bucket)
                    .objectLockEnabledForBucket(true)
                    .build());
        } catch (Exception e) {
            ctx.check("S3 ObjectLock config setup", false, e);
            return;
        }

        // 17. PutObjectLockConfiguration with default GOVERNANCE / 30 days
        try {
            s3.putObjectLockConfiguration(PutObjectLockConfigurationRequest.builder()
                    .bucket(bucket)
                    .objectLockConfiguration(ObjectLockConfiguration.builder()
                            .objectLockEnabled(ObjectLockEnabled.ENABLED)
                            .rule(ObjectLockRule.builder()
                                    .defaultRetention(DefaultRetention.builder()
                                            .mode(ObjectLockRetentionMode.GOVERNANCE)
                                            .days(30)
                                            .build())
                                    .build())
                            .build())
                    .build());
            ctx.check("S3 ObjectLock PutObjectLockConfiguration", true);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock PutObjectLockConfiguration", false, e);
            return;
        }

        // 18. GetObjectLockConfiguration — verify mode and days
        try {
            GetObjectLockConfigurationResponse resp = s3.getObjectLockConfiguration(
                    GetObjectLockConfigurationRequest.builder().bucket(bucket).build());
            ObjectLockConfiguration conf = resp.objectLockConfiguration();
            boolean enabled = ObjectLockEnabled.ENABLED.equals(conf.objectLockEnabled());
            boolean rightMode = conf.rule() != null
                    && ObjectLockRetentionMode.GOVERNANCE.equals(conf.rule().defaultRetention().mode());
            boolean rightDays = conf.rule() != null
                    && Integer.valueOf(30).equals(conf.rule().defaultRetention().days());
            ctx.check("S3 ObjectLock GetObjectLockConfiguration enabled", enabled);
            ctx.check("S3 ObjectLock GetObjectLockConfiguration mode=GOVERNANCE", rightMode);
            ctx.check("S3 ObjectLock GetObjectLockConfiguration days=30", rightDays);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock GetObjectLockConfiguration enabled", false, e);
            ctx.check("S3 ObjectLock GetObjectLockConfiguration mode=GOVERNANCE", false, e);
            ctx.check("S3 ObjectLock GetObjectLockConfiguration days=30", false, e);
        }

        // 19. PutObject without explicit lock headers — default retention auto-applied
        try {
            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucket).key(key).build(),
                    RequestBody.fromString("default retention content"));
            ctx.check("S3 ObjectLock PutObject (no explicit lock headers)", true);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock PutObject (no explicit lock headers)", false, e);
            return;
        }

        // 20. GetObjectRetention — verify default retention was applied
        try {
            GetObjectRetentionResponse ret = s3.getObjectRetention(
                    GetObjectRetentionRequest.builder().bucket(bucket).key(key).build());
            boolean hasMode = ObjectLockRetentionMode.GOVERNANCE.equals(ret.retention().mode());
            boolean hasDate = ret.retention().retainUntilDate() != null;
            ctx.check("S3 ObjectLock default retention mode applied", hasMode);
            ctx.check("S3 ObjectLock default retention date applied", hasDate);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock default retention mode applied", false, e);
            ctx.check("S3 ObjectLock default retention date applied", false, e);
        }

        // 21. Delete auto-locked object with bypass — succeeds (GOVERNANCE)
        try {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket).key(key)
                    .bypassGovernanceRetention(true)
                    .build());
            ctx.check("S3 ObjectLock delete default-locked object with bypass", true);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock delete default-locked object with bypass", false, e);
        }

        // cleanup
        try { s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build()); } catch (Exception ignore) {}
    }

    // --- 6. PutObjectRetention on an existing object ---

    private void testPutObjectRetention(TestContext ctx, S3Client s3) {
        String bucket = "lock-retention-bucket";
        String key = "retention-object.txt";
        Instant retainUntil = Instant.now().plus(365, ChronoUnit.DAYS);

        // Setup
        try {
            s3.createBucket(CreateBucketRequest.builder()
                    .bucket(bucket)
                    .objectLockEnabledForBucket(true)
                    .build());
            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucket).key(key).build(),
                    RequestBody.fromString("retention target"));
        } catch (Exception e) {
            ctx.check("S3 ObjectLock PutObjectRetention setup", false, e);
            return;
        }

        // 22. PutObjectRetention — set GOVERNANCE mode
        try {
            s3.putObjectRetention(PutObjectRetentionRequest.builder()
                    .bucket(bucket).key(key)
                    .retention(ObjectLockRetention.builder()
                            .mode(ObjectLockRetentionMode.GOVERNANCE)
                            .retainUntilDate(retainUntil)
                            .build())
                    .build());
            ctx.check("S3 ObjectLock PutObjectRetention", true);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock PutObjectRetention", false, e);
            return;
        }

        // 23. GetObjectRetention — verify mode and date
        try {
            GetObjectRetentionResponse resp = s3.getObjectRetention(
                    GetObjectRetentionRequest.builder().bucket(bucket).key(key).build());
            boolean hasMode = ObjectLockRetentionMode.GOVERNANCE.equals(resp.retention().mode());
            boolean dateClose = resp.retention().retainUntilDate() != null
                    && Math.abs(resp.retention().retainUntilDate().getEpochSecond()
                            - retainUntil.getEpochSecond()) < 10;
            ctx.check("S3 ObjectLock GetObjectRetention mode", hasMode);
            ctx.check("S3 ObjectLock GetObjectRetention date", dateClose);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock GetObjectRetention mode", false, e);
            ctx.check("S3 ObjectLock GetObjectRetention date", false, e);
        }

        // 24. Extend retention (allowed for GOVERNANCE) — bypass needed for same-or-lower date
        Instant longerRetain = Instant.now().plus(365 * 2, ChronoUnit.DAYS);
        try {
            s3.putObjectRetention(PutObjectRetentionRequest.builder()
                    .bucket(bucket).key(key)
                    .bypassGovernanceRetention(true)
                    .retention(ObjectLockRetention.builder()
                            .mode(ObjectLockRetentionMode.GOVERNANCE)
                            .retainUntilDate(longerRetain)
                            .build())
                    .build());
            ctx.check("S3 ObjectLock extend GOVERNANCE retention", true);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock extend GOVERNANCE retention", false, e);
        }

        // 25. Delete with bypass — object is GOVERNANCE, so bypass removes it
        try {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket).key(key)
                    .bypassGovernanceRetention(true)
                    .build());
            ctx.check("S3 ObjectLock delete retained object with bypass", true);
        } catch (Exception e) {
            ctx.check("S3 ObjectLock delete retained object with bypass", false, e);
        }

        // cleanup
        try { s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build()); } catch (Exception ignore) {}
    }
}
