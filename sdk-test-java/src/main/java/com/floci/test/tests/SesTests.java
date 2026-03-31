package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.ses.SesClient;
import software.amazon.awssdk.services.ses.model.*;

import java.util.List;

@FlociTestGroup
public class SesTests implements TestGroup {

    @Override
    public String name() { return "ses"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- SES Tests ---");

        try (SesClient ses = SesClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            String testEmail = "test-" + System.currentTimeMillis() + "@example.com";
            String testDomain = "test-" + System.currentTimeMillis() + ".example.com";

            // 1. VerifyEmailIdentity
            try {
                ses.verifyEmailIdentity(VerifyEmailIdentityRequest.builder()
                        .emailAddress(testEmail).build());
                ctx.check("SES VerifyEmailIdentity", true);
            } catch (Exception e) {
                ctx.check("SES VerifyEmailIdentity", false, e);
                return;
            }

            // 2. VerifyDomainIdentity
            try {
                VerifyDomainIdentityResponse resp = ses.verifyDomainIdentity(
                        VerifyDomainIdentityRequest.builder().domain(testDomain).build());
                ctx.check("SES VerifyDomainIdentity", resp.verificationToken() != null
                        && !resp.verificationToken().isEmpty());
            } catch (Exception e) {
                ctx.check("SES VerifyDomainIdentity", false, e);
            }

            // 3. ListIdentities (all)
            try {
                ListIdentitiesResponse resp = ses.listIdentities(
                        ListIdentitiesRequest.builder().build());
                boolean hasEmail = resp.identities().contains(testEmail);
                boolean hasDomain = resp.identities().contains(testDomain);
                ctx.check("SES ListIdentities", hasEmail && hasDomain);
            } catch (Exception e) {
                ctx.check("SES ListIdentities", false, e);
            }

            // 4. ListIdentities filtered by type
            try {
                ListIdentitiesResponse resp = ses.listIdentities(
                        ListIdentitiesRequest.builder().identityType(IdentityType.EMAIL_ADDRESS).build());
                boolean hasEmail = resp.identities().contains(testEmail);
                boolean hasDomain = resp.identities().contains(testDomain);
                ctx.check("SES ListIdentities by type", hasEmail && !hasDomain);
            } catch (Exception e) {
                ctx.check("SES ListIdentities by type", false, e);
            }

            // 5. GetIdentityVerificationAttributes
            try {
                GetIdentityVerificationAttributesResponse resp = ses.getIdentityVerificationAttributes(
                        GetIdentityVerificationAttributesRequest.builder()
                                .identities(testEmail).build());
                var attrs = resp.verificationAttributes().get(testEmail);
                ctx.check("SES GetIdentityVerificationAttributes",
                        attrs != null && attrs.verificationStatus() == VerificationStatus.SUCCESS);
            } catch (Exception e) {
                ctx.check("SES GetIdentityVerificationAttributes", false, e);
            }

            // 6. ListVerifiedEmailAddresses
            try {
                ListVerifiedEmailAddressesResponse resp = ses.listVerifiedEmailAddresses();
                ctx.check("SES ListVerifiedEmailAddresses",
                        resp.verifiedEmailAddresses().contains(testEmail));
            } catch (Exception e) {
                ctx.check("SES ListVerifiedEmailAddresses", false, e);
            }

            // 7. SendEmail
            String messageId = null;
            try {
                SendEmailResponse resp = ses.sendEmail(SendEmailRequest.builder()
                        .source(testEmail)
                        .destination(Destination.builder()
                                .toAddresses("recipient@example.com").build())
                        .message(Message.builder()
                                .subject(Content.builder().data("Test Subject").build())
                                .body(Body.builder()
                                        .text(Content.builder().data("Hello from SES test").build())
                                        .build())
                                .build())
                        .build());
                messageId = resp.messageId();
                ctx.check("SES SendEmail", messageId != null && !messageId.isEmpty());
            } catch (Exception e) {
                ctx.check("SES SendEmail", false, e);
            }

            // 8. SendRawEmail
            try {
                SendRawEmailResponse resp = ses.sendRawEmail(SendRawEmailRequest.builder()
                        .source(testEmail)
                        .destinations("recipient@example.com")
                        .rawMessage(RawMessage.builder()
                                .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String(
                                        "From: " + testEmail + "\r\nTo: recipient@example.com\r\n"
                                        + "Subject: Raw Test\r\n\r\nRaw body"))
                                .build())
                        .build());
                ctx.check("SES SendRawEmail", resp.messageId() != null && !resp.messageId().isEmpty());
            } catch (Exception e) {
                ctx.check("SES SendRawEmail", false, e);
            }

            // 9. GetSendQuota
            try {
                GetSendQuotaResponse resp = ses.getSendQuota();
                ctx.check("SES GetSendQuota",
                        resp.max24HourSend() > 0 && resp.sentLast24Hours() >= 2.0);
            } catch (Exception e) {
                ctx.check("SES GetSendQuota", false, e);
            }

            // 10. GetSendStatistics
            try {
                GetSendStatisticsResponse resp = ses.getSendStatistics();
                ctx.check("SES GetSendStatistics", resp.sendDataPoints() != null);
            } catch (Exception e) {
                ctx.check("SES GetSendStatistics", false, e);
            }

            // 11. GetAccountSendingEnabled
            try {
                GetAccountSendingEnabledResponse resp = ses.getAccountSendingEnabled();
                ctx.check("SES GetAccountSendingEnabled", resp.enabled());
            } catch (Exception e) {
                ctx.check("SES GetAccountSendingEnabled", false, e);
            }

            // 12. GetIdentityDkimAttributes
            try {
                GetIdentityDkimAttributesResponse resp = ses.getIdentityDkimAttributes(
                        GetIdentityDkimAttributesRequest.builder()
                                .identities(testDomain).build());
                var attrs = resp.dkimAttributes().get(testDomain);
                ctx.check("SES GetIdentityDkimAttributes", attrs != null);
            } catch (Exception e) {
                ctx.check("SES GetIdentityDkimAttributes", false, e);
            }

            // 13. SetIdentityNotificationTopic
            try {
                ses.setIdentityNotificationTopic(SetIdentityNotificationTopicRequest.builder()
                        .identity(testEmail)
                        .notificationType("Bounce")
                        .snsTopic("arn:aws:sns:us-east-1:000000000000:bounce-topic")
                        .build());
                ctx.check("SES SetIdentityNotificationTopic", true);
            } catch (Exception e) {
                ctx.check("SES SetIdentityNotificationTopic", false, e);
            }

            // 14. GetIdentityNotificationAttributes
            try {
                GetIdentityNotificationAttributesResponse resp = ses.getIdentityNotificationAttributes(
                        GetIdentityNotificationAttributesRequest.builder()
                                .identities(testEmail).build());
                var attrs = resp.notificationAttributes().get(testEmail);
                ctx.check("SES GetIdentityNotificationAttributes",
                        attrs != null && attrs.bounceTopic().contains("bounce-topic"));
            } catch (Exception e) {
                ctx.check("SES GetIdentityNotificationAttributes", false, e);
            }

            // 15. DeleteIdentity
            try {
                ses.deleteIdentity(DeleteIdentityRequest.builder()
                        .identity(testEmail).build());
                ses.deleteIdentity(DeleteIdentityRequest.builder()
                        .identity(testDomain).build());
                // Verify deletion
                ListIdentitiesResponse resp = ses.listIdentities(
                        ListIdentitiesRequest.builder().build());
                boolean emailGone = !resp.identities().contains(testEmail);
                boolean domainGone = !resp.identities().contains(testDomain);
                ctx.check("SES DeleteIdentity", emailGone && domainGone);
            } catch (Exception e) {
                ctx.check("SES DeleteIdentity", false, e);
            }

        } catch (Exception e) {
            ctx.check("SES Client", false, e);
        }
    }
}
