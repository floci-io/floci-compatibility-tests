package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@FlociTestGroup
public class SecretsManagerTests implements TestGroup {

    @Override
    public String name() { return "secretsmanager"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- Secrets Manager Tests ---");

        try (SecretsManagerClient sm = SecretsManagerClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            String secretName = "sdk-test-secret-" + System.currentTimeMillis();
            String secretValue = "my-super-secret-value";
            String updatedValue = "my-updated-secret-value";
            String[] createdArn = {null};
            String[] originalVersionId = {null};

            // 1. CreateSecret — ARN contains name, VersionId non-null
            try {
                CreateSecretResponse resp = sm.createSecret(CreateSecretRequest.builder()
                        .name(secretName)
                        .secretString(secretValue)
                        .description("Test secret")
                        .tags(Tag.builder().key("env").value("test").build())
                        .build());
                createdArn[0] = resp.arn();
                originalVersionId[0] = resp.versionId();
                ctx.check("SM CreateSecret",
                        resp.arn() != null
                        && resp.arn().contains(secretName)
                        && resp.versionId() != null
                        && resp.name().equals(secretName));
            } catch (Exception e) {
                ctx.check("SM CreateSecret", false, e);
            }

            // 2. GetSecretValue by name — SecretString matches
            try {
                GetSecretValueResponse resp = sm.getSecretValue(GetSecretValueRequest.builder()
                        .secretId(secretName)
                        .build());
                ctx.check("SM GetSecretValue by name",
                        secretValue.equals(resp.secretString())
                        && resp.name().equals(secretName));
            } catch (Exception e) {
                ctx.check("SM GetSecretValue by name", false, e);
            }

            // 3. GetSecretValue by ARN — resolves correctly
            try {
                if (createdArn[0] != null) {
                    GetSecretValueResponse resp = sm.getSecretValue(GetSecretValueRequest.builder()
                            .secretId(createdArn[0])
                            .build());
                    ctx.check("SM GetSecretValue by ARN",
                            secretValue.equals(resp.secretString()));
                } else {
                    ctx.check("SM GetSecretValue by ARN", false);
                }
            } catch (Exception e) {
                ctx.check("SM GetSecretValue by ARN", false, e);
            }

            // 4. PutSecretValue — new VersionId != original
            String[] newVersionId = {null};
            try {
                PutSecretValueResponse resp = sm.putSecretValue(PutSecretValueRequest.builder()
                        .secretId(secretName)
                        .secretString(updatedValue)
                        .build());
                newVersionId[0] = resp.versionId();
                ctx.check("SM PutSecretValue",
                        resp.versionId() != null
                        && !resp.versionId().equals(originalVersionId[0]));
            } catch (Exception e) {
                ctx.check("SM PutSecretValue", false, e);
            }

            // 5. GetSecretValue after put — returns updated value
            try {
                GetSecretValueResponse resp = sm.getSecretValue(GetSecretValueRequest.builder()
                        .secretId(secretName)
                        .build());
                ctx.check("SM GetSecretValue after PutSecretValue",
                        updatedValue.equals(resp.secretString()));
            } catch (Exception e) {
                ctx.check("SM GetSecretValue after PutSecretValue", false, e);
            }

            // 6. DescribeSecret — Tags present, VersionIdsToStages has 2 entries, RotationEnabled=false
            try {
                DescribeSecretResponse resp = sm.describeSecret(DescribeSecretRequest.builder()
                        .secretId(secretName)
                        .build());
                boolean hasTags = resp.tags() != null && !resp.tags().isEmpty();
                boolean hasTwoVersions = resp.versionIdsToStages() != null
                        && resp.versionIdsToStages().size() == 2;
                ctx.check("SM DescribeSecret",
                        hasTags
                        && hasTwoVersions
                        && !resp.rotationEnabled());
            } catch (Exception e) {
                ctx.check("SM DescribeSecret", false, e);
            }

            // 7. UpdateSecret description — reflected in DescribeSecret
            try {
                sm.updateSecret(UpdateSecretRequest.builder()
                        .secretId(secretName)
                        .description("Updated description")
                        .build());
                DescribeSecretResponse resp = sm.describeSecret(DescribeSecretRequest.builder()
                        .secretId(secretName)
                        .build());
                ctx.check("SM UpdateSecret description",
                        "Updated description".equals(resp.description()));
            } catch (Exception e) {
                ctx.check("SM UpdateSecret description", false, e);
            }

            // 8. ListSecrets — secret appears
            try {
                ListSecretsResponse resp = sm.listSecrets(ListSecretsRequest.builder().build());
                boolean found = resp.secretList().stream()
                        .anyMatch(s -> secretName.equals(s.name()));
                ctx.check("SM ListSecrets", found);
            } catch (Exception e) {
                ctx.check("SM ListSecrets", false, e);
            }

            // 9. TagResource — tag visible in DescribeSecret
            try {
                sm.tagResource(TagResourceRequest.builder()
                        .secretId(secretName)
                        .tags(Tag.builder().key("team").value("backend").build())
                        .build());
                DescribeSecretResponse resp = sm.describeSecret(DescribeSecretRequest.builder()
                        .secretId(secretName)
                        .build());
                boolean hasTag = resp.tags().stream()
                        .anyMatch(t -> "team".equals(t.key()) && "backend".equals(t.value()));
                ctx.check("SM TagResource", hasTag);
            } catch (Exception e) {
                ctx.check("SM TagResource", false, e);
            }

            // 10. UntagResource — tag removed
            try {
                sm.untagResource(UntagResourceRequest.builder()
                        .secretId(secretName)
                        .tagKeys("team")
                        .build());
                DescribeSecretResponse resp = sm.describeSecret(DescribeSecretRequest.builder()
                        .secretId(secretName)
                        .build());
                boolean tagRemoved = resp.tags().stream()
                        .noneMatch(t -> "team".equals(t.key()));
                ctx.check("SM UntagResource", tagRemoved);
            } catch (Exception e) {
                ctx.check("SM UntagResource", false, e);
            }

            // 11. ListSecretVersionIds — two versions with AWSCURRENT + AWSPREVIOUS stages
            try {
                ListSecretVersionIdsResponse resp = sm.listSecretVersionIds(
                        ListSecretVersionIdsRequest.builder()
                                .secretId(secretName)
                                .build());
                Map<String, List<String>> versionMap = resp.versions().stream()
                        .collect(Collectors.toMap(
                                SecretVersionsListEntry::versionId,
                                SecretVersionsListEntry::versionStages));
                boolean hasAwsCurrent = versionMap.values().stream()
                        .anyMatch(stages -> stages.contains("AWSCURRENT"));
                boolean hasAwsPrevious = versionMap.values().stream()
                        .anyMatch(stages -> stages.contains("AWSPREVIOUS"));
                ctx.check("SM ListSecretVersionIds",
                        versionMap.size() == 2 && hasAwsCurrent && hasAwsPrevious);
            } catch (Exception e) {
                ctx.check("SM ListSecretVersionIds", false, e);
            }

            // 12. RotateSecret stub — DescribeSecret shows rotationEnabled=true
            try {
                RotateSecretResponse rotateResp = sm.rotateSecret(RotateSecretRequest.builder()
                        .secretId(secretName)
                        .rotationRules(RotationRulesType.builder().automaticallyAfterDays(30L).build())
                        .build());
                DescribeSecretResponse describeResp = sm.describeSecret(DescribeSecretRequest.builder()
                        .secretId(secretName)
                        .build());
                ctx.check("SM RotateSecret stub",
                        rotateResp.arn().equals(createdArn[0])
                        && describeResp.rotationEnabled());
            } catch (Exception e) {
                ctx.check("SM RotateSecret stub", false, e);
            }

            // 13. DeleteSecret (force) -> subsequent GetSecretValue throws SecretsManagerException 400
            try {
                sm.deleteSecret(DeleteSecretRequest.builder()
                        .secretId(secretName)
                        .forceDeleteWithoutRecovery(true)
                        .build());
                try {
                    sm.getSecretValue(GetSecretValueRequest.builder()
                            .secretId(secretName)
                            .build());
                    ctx.check("SM DeleteSecret (force)", false);
                } catch (SecretsManagerException e) {
                    ctx.check("SM DeleteSecret (force)", e.statusCode() == 400);
                }
            } catch (Exception e) {
                ctx.check("SM DeleteSecret (force)", false, e);
            }

            // 14. CreateSecret duplicate — SecretsManagerException 400
            String dupName = "sdk-test-dup-secret-" + System.currentTimeMillis();
            try {
                sm.createSecret(CreateSecretRequest.builder()
                        .name(dupName)
                        .secretString("value1")
                        .build());
                try {
                    sm.createSecret(CreateSecretRequest.builder()
                            .name(dupName)
                            .secretString("value2")
                            .build());
                    ctx.check("SM CreateSecret duplicate", false);
                } catch (SecretsManagerException e) {
                    ctx.check("SM CreateSecret duplicate", e.statusCode() == 400);
                }
                // Cleanup
                sm.deleteSecret(DeleteSecretRequest.builder()
                        .secretId(dupName)
                        .forceDeleteWithoutRecovery(true)
                        .build());
            } catch (Exception e) {
                ctx.check("SM CreateSecret duplicate", false, e);
            }

            // 15. GetRandomPassword — returns non-empty password with requested length
            try {
                GetRandomPasswordResponse resp = sm.getRandomPassword(GetRandomPasswordRequest.builder()
                        .passwordLength(32L)
                        .excludePunctuation(true)
                        .build());
                ctx.check("SM GetRandomPassword",
                        resp.randomPassword() != null
                        && resp.randomPassword().length() == 32);
            } catch (Exception e) {
                ctx.check("SM GetRandomPassword", false, e);
            }

            // 16. GetSecretValue non-existent — SecretsManagerException 400
            try {
                sm.getSecretValue(GetSecretValueRequest.builder()
                        .secretId("non-existent-secret-" + System.currentTimeMillis())
                        .build());
                ctx.check("SM GetSecretValue non-existent", false);
            } catch (SecretsManagerException e) {
                ctx.check("SM GetSecretValue non-existent", e.statusCode() == 400);
            } catch (Exception e) {
                ctx.check("SM GetSecretValue non-existent", false, e);
            }
        }
    }
}
