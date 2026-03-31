package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.*;

import java.nio.charset.StandardCharsets;

@FlociTestGroup
public class KmsTests implements TestGroup {

    @Override
    public String name() { return "kms"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- KMS Tests ---");

        try (KmsClient kms = KmsClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            // 1. CreateKey
            String keyId;
            try {
                CreateKeyResponse resp = kms.createKey(b -> b.description("test-key"));
                keyId = resp.keyMetadata().keyId();
                ctx.check("KMS CreateKey", keyId != null);
            } catch (Exception e) {
                ctx.check("KMS CreateKey", false, e);
                return;
            }

            // 2. DescribeKey
            try {
                DescribeKeyResponse resp = kms.describeKey(b -> b.keyId(keyId));
                ctx.check("KMS DescribeKey", resp.keyMetadata().keyId().equals(keyId));
            } catch (Exception e) {
                ctx.check("KMS DescribeKey", false, e);
            }

            // 3. CreateAlias
            String aliasName = "alias/test-key-" + System.currentTimeMillis();
            try {
                kms.createAlias(b -> b.aliasName(aliasName).targetKeyId(keyId));
                ctx.check("KMS CreateAlias", true);
            } catch (Exception e) {
                ctx.check("KMS CreateAlias", false, e);
            }

            // 4. ListAliases
            try {
                ListAliasesResponse resp = kms.listAliases();
                boolean found = resp.aliases().stream().anyMatch(a -> a.aliasName().equals(aliasName));
                ctx.check("KMS ListAliases", found);
            } catch (Exception e) {
                ctx.check("KMS ListAliases", false, e);
            }

            // 5. Encrypt (using keyId)
            SdkBytes ciphertext;
            String plaintext = "secret data";
            try {
                EncryptResponse resp = kms.encrypt(b -> b
                        .keyId(keyId)
                        .plaintext(SdkBytes.fromString(plaintext, StandardCharsets.UTF_8)));
                ciphertext = resp.ciphertextBlob();
                ctx.check("KMS Encrypt", ciphertext != null);
            } catch (Exception e) {
                ctx.check("KMS Encrypt", false, e);
                ciphertext = null;
            }

            // 6. Decrypt
            if (ciphertext != null) {
                try {
                    final String fPlaintext = plaintext;
                    final SdkBytes fCiphertext = ciphertext;
                    DecryptResponse resp = kms.decrypt(b -> b.ciphertextBlob(fCiphertext));
                    ctx.check("KMS Decrypt", resp.plaintext().asUtf8String().equals(fPlaintext));
                } catch (Exception e) {
                    ctx.check("KMS Decrypt", false, e);
                }
            }

            // 7. Encrypt (using alias)
            try {
                EncryptResponse resp = kms.encrypt(b -> b
                        .keyId(aliasName)
                        .plaintext(SdkBytes.fromString("alias data", StandardCharsets.UTF_8)));
                ctx.check("KMS Encrypt using Alias", resp.ciphertextBlob() != null);
            } catch (Exception e) {
                ctx.check("KMS Encrypt using Alias", false, e);
            }

            // 8. GenerateDataKey
            try {
                GenerateDataKeyResponse resp = kms.generateDataKey(b -> b.keyId(keyId).keySpec(DataKeySpec.AES_256));
                ctx.check("KMS GenerateDataKey", resp.plaintext() != null && resp.ciphertextBlob() != null);
            } catch (Exception e) {
                ctx.check("KMS GenerateDataKey", false, e);
            }

            // 9. TagResource
            try {
                kms.tagResource(b -> b.keyId(keyId).tags(Tag.builder().tagKey("Project").tagValue("Floci").build()));
                ListResourceTagsResponse tagsResp = kms.listResourceTags(b -> b.keyId(keyId));
                boolean found = tagsResp.tags().stream().anyMatch(t -> t.tagKey().equals("Project") && t.tagValue().equals("Floci"));
                ctx.check("KMS Tagging", found);
            } catch (Exception e) {
                ctx.check("KMS Tagging", false, e);
            }

            // 10. ReEncrypt
            if (ciphertext != null) {
                try {
                    String keyId2 = kms.createKey(b -> b.description("key2")).keyMetadata().keyId();
                    final SdkBytes fCiphertext = ciphertext;
                    ReEncryptResponse reResp = kms.reEncrypt(b -> b.ciphertextBlob(fCiphertext).destinationKeyId(keyId2));
                    ctx.check("KMS ReEncrypt", reResp.ciphertextBlob() != null);
                    
                    DecryptResponse decResp = kms.decrypt(b -> b.ciphertextBlob(reResp.ciphertextBlob()));
                    ctx.check("KMS ReEncrypt verification", decResp.plaintext().asUtf8String().equals(plaintext));
                } catch (Exception e) {
                    ctx.check("KMS ReEncrypt", false, e);
                }
            }

            // 11. GenerateDataKeyWithoutPlaintext
            try {
                GenerateDataKeyWithoutPlaintextResponse resp = kms.generateDataKeyWithoutPlaintext(b -> b.keyId(keyId).keySpec(DataKeySpec.AES_256));
                ctx.check("KMS GenerateDataKeyWithoutPlaintext", resp.ciphertextBlob() != null);
            } catch (Exception e) {
                ctx.check("KMS GenerateDataKeyWithoutPlaintext", false, e);
            }

            // 12. Sign & Verify
            try {
                SdkBytes msg = SdkBytes.fromString("message to sign", StandardCharsets.UTF_8);
                SignResponse signResp = kms.sign(b -> b.keyId(keyId).message(msg).signingAlgorithm(SigningAlgorithmSpec.RSASSA_PSS_SHA_256));
                ctx.check("KMS Sign", signResp.signature() != null);

                VerifyResponse verifyResp = kms.verify(b -> b.keyId(keyId).message(msg).signature(signResp.signature()).signingAlgorithm(SigningAlgorithmSpec.RSASSA_PSS_SHA_256));
                ctx.check("KMS Verify", verifyResp.signatureValid());
            } catch (Exception e) {
                ctx.check("KMS Sign/Verify", false, e);
            }

            // 13. ScheduleKeyDeletion
            try {
                ScheduleKeyDeletionResponse resp = kms.scheduleKeyDeletion(b -> b.keyId(keyId).pendingWindowInDays(7));
                DescribeKeyResponse desc = kms.describeKey(b -> b.keyId(keyId));
                ctx.check("KMS ScheduleKeyDeletion", desc.keyMetadata().keyState() == KeyState.PENDING_DELETION);
            } catch (Exception e) {
                ctx.check("KMS ScheduleKeyDeletion", false, e);
            }

            // 14. DeleteAlias
            try {
                kms.deleteAlias(b -> b.aliasName(aliasName));
                ctx.check("KMS DeleteAlias", true);
            } catch (Exception e) {
                ctx.check("KMS DeleteAlias", false, e);
            }

        } catch (Exception e) {
            ctx.check("KMS Client", false, e);
        }
    }
}
