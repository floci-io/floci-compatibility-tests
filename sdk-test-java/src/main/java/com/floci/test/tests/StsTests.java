package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithSamlRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithSamlResponse;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;
import software.amazon.awssdk.services.sts.model.DecodeAuthorizationMessageRequest;
import software.amazon.awssdk.services.sts.model.DecodeAuthorizationMessageResponse;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;
import software.amazon.awssdk.services.sts.model.GetFederationTokenRequest;
import software.amazon.awssdk.services.sts.model.GetFederationTokenResponse;
import software.amazon.awssdk.services.sts.model.GetSessionTokenRequest;
import software.amazon.awssdk.services.sts.model.GetSessionTokenResponse;
import software.amazon.awssdk.services.sts.model.StsException;

@FlociTestGroup
public class StsTests implements TestGroup {

    @Override
    public String name() { return "sts"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- STS Tests ---");

        try (StsClient sts = StsClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            // 1. GetCallerIdentity
            try {
                GetCallerIdentityResponse resp = sts.getCallerIdentity(
                        GetCallerIdentityRequest.builder().build());
                ctx.check("STS GetCallerIdentity",
                        resp.account() != null
                        && resp.arn() != null
                        && resp.userId() != null);
            } catch (Exception e) {
                ctx.check("STS GetCallerIdentity", false, e);
            }

            // 2. GetCallerIdentity returns correct account ID
            try {
                GetCallerIdentityResponse resp = sts.getCallerIdentity(
                        GetCallerIdentityRequest.builder().build());
                ctx.check("STS GetCallerIdentity AccountId",
                        "000000000000".equals(resp.account()));
            } catch (Exception e) {
                ctx.check("STS GetCallerIdentity AccountId", false, e);
            }

            // 3. AssumeRole
            try {
                AssumeRoleResponse resp = sts.assumeRole(AssumeRoleRequest.builder()
                        .roleArn("arn:aws:iam::000000000000:role/sdk-test-assumed-role")
                        .roleSessionName("sdk-test-session")
                        .durationSeconds(3600)
                        .build());
                boolean hasCredentials = resp.credentials() != null
                        && resp.credentials().accessKeyId() != null
                        && resp.credentials().accessKeyId().startsWith("ASIA")
                        && resp.credentials().secretAccessKey() != null
                        && resp.credentials().sessionToken() != null
                        && resp.credentials().expiration() != null;
                ctx.check("STS AssumeRole", hasCredentials);
            } catch (Exception e) {
                ctx.check("STS AssumeRole", false, e);
            }

            // 4. AssumeRole returns assumed role user ARN
            try {
                AssumeRoleResponse resp = sts.assumeRole(AssumeRoleRequest.builder()
                        .roleArn("arn:aws:iam::000000000000:role/my-role")
                        .roleSessionName("my-session")
                        .build());
                boolean hasAssumedRoleUser = resp.assumedRoleUser() != null
                        && resp.assumedRoleUser().arn().contains("assumed-role/my-role/my-session");
                ctx.check("STS AssumeRole AssumedRoleUser", hasAssumedRoleUser);
            } catch (Exception e) {
                ctx.check("STS AssumeRole AssumedRoleUser", false, e);
            }

            // 5. AssumeRole with custom duration
            try {
                AssumeRoleResponse resp = sts.assumeRole(AssumeRoleRequest.builder()
                        .roleArn("arn:aws:iam::000000000000:role/short-lived-role")
                        .roleSessionName("short-session")
                        .durationSeconds(900)
                        .build());
                boolean expiresSoon = resp.credentials().expiration()
                        .isBefore(java.time.Instant.now().plusSeconds(901));
                ctx.check("STS AssumeRole CustomDuration", expiresSoon && resp.credentials() != null);
            } catch (Exception e) {
                ctx.check("STS AssumeRole CustomDuration", false, e);
            }

            // 6. GetSessionToken
            try {
                GetSessionTokenResponse gst = sts.getSessionToken(
                        GetSessionTokenRequest.builder().durationSeconds(7200).build());
                ctx.check("STS GetSessionToken",
                        gst.credentials() != null
                        && gst.credentials().accessKeyId().startsWith("ASIA")
                        && gst.credentials().sessionToken() != null
                        && gst.credentials().expiration().isAfter(java.time.Instant.now()));
            } catch (Exception e) {
                ctx.check("STS GetSessionToken", false, e);
            }

            // 7. AssumeRoleWithWebIdentity
            try {
                AssumeRoleWithWebIdentityResponse arwi = sts.assumeRoleWithWebIdentity(
                        AssumeRoleWithWebIdentityRequest.builder()
                                .roleArn("arn:aws:iam::000000000000:role/web-identity-role")
                                .roleSessionName("web-session")
                                .webIdentityToken("eyJhbGciOiJSUzI1NiJ9.test-token")
                                .durationSeconds(3600)
                                .build());
                ctx.check("STS AssumeRoleWithWebIdentity",
                        arwi.credentials() != null
                        && arwi.credentials().accessKeyId().startsWith("ASIA")
                        && arwi.assumedRoleUser().arn().contains("assumed-role/web-identity-role/web-session"));
            } catch (Exception e) {
                ctx.check("STS AssumeRoleWithWebIdentity", false, e);
            }

            // 8. GetFederationToken
            try {
                GetFederationTokenResponse gft = sts.getFederationToken(
                        GetFederationTokenRequest.builder()
                                .name("sdk-test-feduser")
                                .durationSeconds(3600)
                                .build());
                ctx.check("STS GetFederationToken",
                        gft.credentials() != null
                        && gft.credentials().accessKeyId().startsWith("ASIA")
                        && gft.federatedUser().arn().contains("federated-user/sdk-test-feduser"));
            } catch (Exception e) {
                ctx.check("STS GetFederationToken", false, e);
            }

            // 9. DecodeAuthorizationMessage
            try {
                DecodeAuthorizationMessageResponse dam = sts.decodeAuthorizationMessage(
                        DecodeAuthorizationMessageRequest.builder()
                                .encodedMessage("test-encoded-message")
                                .build());
                ctx.check("STS DecodeAuthorizationMessage",
                        dam.decodedMessage() != null && !dam.decodedMessage().isEmpty());
            } catch (Exception e) {
                ctx.check("STS DecodeAuthorizationMessage", false, e);
            }

            // 10. AssumeRole missing RoleArn → exception
            try {
                sts.assumeRole(AssumeRoleRequest.builder().roleSessionName("s").build());
                ctx.check("STS AssumeRole missing RoleArn", false);
            } catch (StsException e) {
                ctx.check("STS AssumeRole missing RoleArn", e.statusCode() == 400);
            } catch (Exception e) {
                ctx.check("STS AssumeRole missing RoleArn", false, e);
            }

            // 11. AssumeRoleWithSaml - credentials and assumed role ARN
            try {
                AssumeRoleWithSamlResponse saml = sts.assumeRoleWithSAML(
                        AssumeRoleWithSamlRequest.builder()
                                .roleArn("arn:aws:iam::000000000000:role/saml-role")
                                .principalArn("arn:aws:iam::000000000000:saml-provider/MySAML")
                                .samlAssertion("base64-encoded-saml-assertion")
                                .durationSeconds(3600)
                                .build());
                ctx.check("STS AssumeRoleWithSAML credentials",
                        saml.credentials() != null
                        && saml.credentials().accessKeyId().startsWith("ASIA")
                        && saml.credentials().secretAccessKey() != null
                        && saml.credentials().sessionToken() != null
                        && saml.credentials().expiration().isAfter(java.time.Instant.now()));
            } catch (Exception e) {
                ctx.check("STS AssumeRoleWithSAML credentials", false, e);
            }

            // 12. AssumeRoleWithSaml - assumed role user ARN shape
            try {
                AssumeRoleWithSamlResponse saml = sts.assumeRoleWithSAML(
                        AssumeRoleWithSamlRequest.builder()
                                .roleArn("arn:aws:iam::000000000000:role/my-saml-role")
                                .principalArn("arn:aws:iam::000000000000:saml-provider/Corp")
                                .samlAssertion("assertion")
                                .build());
                ctx.check("STS AssumeRoleWithSAML AssumedRoleUser",
                        saml.assumedRoleUser() != null
                        && saml.assumedRoleUser().arn().contains("assumed-role/my-saml-role/"));
            } catch (Exception e) {
                ctx.check("STS AssumeRoleWithSAML AssumedRoleUser", false, e);
            }

            // 13. AssumeRoleWithWebIdentity missing WebIdentityToken → 400
            try {
                sts.assumeRoleWithWebIdentity(AssumeRoleWithWebIdentityRequest.builder()
                        .roleArn("arn:aws:iam::000000000000:role/r")
                        .roleSessionName("s")
                        .build());
                ctx.check("STS AssumeRoleWithWebIdentity missing token", false);
            } catch (StsException e) {
                ctx.check("STS AssumeRoleWithWebIdentity missing token", e.statusCode() == 400);
            } catch (Exception e) {
                ctx.check("STS AssumeRoleWithWebIdentity missing token", false, e);
            }

            // 14. GetFederationToken - FederatedUserId format is accountId:name
            try {
                GetFederationTokenResponse gft = sts.getFederationToken(
                        GetFederationTokenRequest.builder()
                                .name("myuser")
                                .build());
                ctx.check("STS GetFederationToken FederatedUserId format",
                        gft.federatedUser() != null
                        && gft.federatedUser().federatedUserId().equals("000000000000:myuser"));
            } catch (Exception e) {
                ctx.check("STS GetFederationToken FederatedUserId format", false, e);
            }

            // 15. GetFederationToken missing Name → 400
            try {
                sts.getFederationToken(GetFederationTokenRequest.builder().build());
                ctx.check("STS GetFederationToken missing Name", false);
            } catch (StsException e) {
                ctx.check("STS GetFederationToken missing Name", e.statusCode() == 400);
            } catch (Exception e) {
                ctx.check("STS GetFederationToken missing Name", false, e);
            }

            // 16. GetSessionToken default duration (no DurationSeconds)
            try {
                GetSessionTokenResponse gst = sts.getSessionToken(
                        GetSessionTokenRequest.builder().build());
                ctx.check("STS GetSessionToken default duration",
                        gst.credentials() != null
                        && gst.credentials().expiration().isAfter(java.time.Instant.now().plusSeconds(3600)));
            } catch (Exception e) {
                ctx.check("STS GetSessionToken default duration", false, e);
            }

            // 17. DecodeAuthorizationMessage echoes message unchanged
            try {
                String msg = "exact-message-to-echo-back";
                DecodeAuthorizationMessageResponse dam = sts.decodeAuthorizationMessage(
                        DecodeAuthorizationMessageRequest.builder()
                                .encodedMessage(msg)
                                .build());
                ctx.check("STS DecodeAuthorizationMessage echo",
                        msg.equals(dam.decodedMessage()));
            } catch (Exception e) {
                ctx.check("STS DecodeAuthorizationMessage echo", false, e);
            }

            // 18. DecodeAuthorizationMessage missing EncodedMessage → 400
            try {
                sts.decodeAuthorizationMessage(DecodeAuthorizationMessageRequest.builder().build());
                ctx.check("STS DecodeAuthorizationMessage missing message", false);
            } catch (StsException e) {
                ctx.check("STS DecodeAuthorizationMessage missing message", e.statusCode() == 400);
            } catch (Exception e) {
                ctx.check("STS DecodeAuthorizationMessage missing message", false, e);
            }
        }
    }
}
