package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

@FlociTestGroup
public class CognitoTests implements TestGroup {

    @Override
    public String name() { return "cognito"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- Cognito Tests ---");

        try (CognitoIdentityProviderClient cognito = CognitoIdentityProviderClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            // 1. CreateUserPool
            String poolId;
            try {
                CreateUserPoolResponse resp = cognito.createUserPool(b -> b.poolName("test-pool"));
                poolId = resp.userPool().id();
                ctx.check("Cognito CreateUserPool", poolId != null);
            } catch (Exception e) {
                ctx.check("Cognito CreateUserPool", false, e);
                return;
            }

            // 2. CreateUserPoolClient
            String clientId;
            try {
                CreateUserPoolClientResponse resp = cognito.createUserPoolClient(b -> b
                        .userPoolId(poolId)
                        .clientName("test-client"));
                clientId = resp.userPoolClient().clientId();
                ctx.check("Cognito CreateUserPoolClient", clientId != null);
            } catch (Exception e) {
                ctx.check("Cognito CreateUserPoolClient", false, e);
                clientId = null;
            }

            // 3. AdminCreateUser
            String username = "test-user-" + System.currentTimeMillis();
            final String fUsername = username;
            final String fPoolId = poolId;
            try {
                AdminCreateUserResponse resp = cognito.adminCreateUser(b -> b
                        .userPoolId(fPoolId)
                        .username(fUsername)
                        .userAttributes(AttributeType.builder().name("email").value("test@example.com").build()));
                ctx.check("Cognito AdminCreateUser", resp.user().username().equals(fUsername));
            } catch (Exception e) {
                ctx.check("Cognito AdminCreateUser", false, e);
            }

            // 4. AdminInitiateAuth
            String accessToken = null;
            if (clientId != null) {
                final String fClientId = clientId;
                try {
                    AdminInitiateAuthResponse resp = cognito.adminInitiateAuth(b -> b
                            .userPoolId(fPoolId)
                            .clientId(fClientId)
                            .authFlow(AuthFlowType.ADMIN_NO_SRP_AUTH)
                            .authParameters(Map.of("USERNAME", fUsername, "PASSWORD", "any")));
                    accessToken = resp.authenticationResult().accessToken();
                    ctx.check("Cognito AdminInitiateAuth", accessToken != null);
                } catch (Exception e) {
                    ctx.check("Cognito AdminInitiateAuth", false, e);
                }
            }

            // 5. GetUser
            if (accessToken != null) {
                try {
                    final String token = accessToken;
                    GetUserResponse resp = cognito.getUser(b -> b.accessToken(token));
                    ctx.check("Cognito GetUser", resp.username().equals(fUsername));
                } catch (Exception e) {
                    ctx.check("Cognito GetUser", false, e);
                }
            }

            // ── Groups ────────────────────────────────────────────────────

            // CreateGroup
            try {
                CreateGroupResponse groupResp = cognito.createGroup(b -> b
                        .userPoolId(fPoolId)
                        .groupName("test-group")
                        .description("Test group")
                        .precedence(1));
                ctx.check("Cognito CreateGroup",
                        "test-group".equals(groupResp.group().groupName()));
            } catch (Exception e) {
                ctx.check("Cognito CreateGroup", false, e);
            }

            // GetGroup
            try {
                GetGroupResponse ggResp = cognito.getGroup(b -> b
                        .userPoolId(fPoolId).groupName("test-group"));
                ctx.check("Cognito GetGroup",
                        "test-group".equals(ggResp.group().groupName())
                        && "Test group".equals(ggResp.group().description())
                        && ggResp.group().precedence() == 1);
            } catch (Exception e) {
                ctx.check("Cognito GetGroup", false, e);
            }

            // CreateGroup duplicate
            try {
                cognito.createGroup(b -> b.userPoolId(fPoolId).groupName("test-group"));
                ctx.check("Cognito CreateGroup duplicate rejected", false);
            } catch (GroupExistsException e) {
                ctx.check("Cognito CreateGroup duplicate rejected", true);
            } catch (Exception e) {
                ctx.check("Cognito CreateGroup duplicate rejected", false, e);
            }

            // ListGroups
            try {
                ListGroupsResponse lgResp = cognito.listGroups(b -> b.userPoolId(fPoolId));
                ctx.check("Cognito ListGroups",
                        lgResp.groups().stream().anyMatch(g -> "test-group".equals(g.groupName())));
            } catch (Exception e) {
                ctx.check("Cognito ListGroups", false, e);
            }

            // AdminAddUserToGroup
            try {
                cognito.adminAddUserToGroup(b -> b
                        .userPoolId(fPoolId).groupName("test-group").username(fUsername));
                ctx.check("Cognito AdminAddUserToGroup", true);
            } catch (Exception e) {
                ctx.check("Cognito AdminAddUserToGroup", false, e);
            }

            // AdminListGroupsForUser
            try {
                AdminListGroupsForUserResponse algResp = cognito.adminListGroupsForUser(b -> b
                        .userPoolId(fPoolId).username(fUsername));
                ctx.check("Cognito AdminListGroupsForUser",
                        algResp.groups().stream().anyMatch(g -> "test-group".equals(g.groupName())));
            } catch (Exception e) {
                ctx.check("Cognito AdminListGroupsForUser", false, e);
            }

            // Authenticate and verify cognito:groups in JWT
            if (clientId != null) {
                final String fClientId = clientId;
                try {
                    AdminInitiateAuthResponse authResp = cognito.adminInitiateAuth(b -> b
                            .userPoolId(fPoolId).clientId(fClientId)
                            .authFlow(AuthFlowType.ADMIN_NO_SRP_AUTH)
                            .authParameters(Map.of("USERNAME", fUsername, "PASSWORD", "any")));
                    String token = authResp.authenticationResult().accessToken();
                    String[] parts = token.split("\\.");
                    String payload = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
                    ctx.check("Cognito JWT cognito:groups claim",
                            payload.contains("\"cognito:groups\"") && payload.contains("\"test-group\""));
                } catch (Exception e) {
                    ctx.check("Cognito JWT cognito:groups claim", false, e);
                }
            }

            // AdminRemoveUserFromGroup
            try {
                cognito.adminRemoveUserFromGroup(b -> b
                        .userPoolId(fPoolId).groupName("test-group").username(fUsername));
                ctx.check("Cognito AdminRemoveUserFromGroup", true);
            } catch (Exception e) {
                ctx.check("Cognito AdminRemoveUserFromGroup", false, e);
            }

            // AdminListGroupsForUser — empty after removal
            try {
                AdminListGroupsForUserResponse algResp2 = cognito.adminListGroupsForUser(b -> b
                        .userPoolId(fPoolId).username(fUsername));
                ctx.check("Cognito AdminListGroupsForUser empty", algResp2.groups().isEmpty());
            } catch (Exception e) {
                ctx.check("Cognito AdminListGroupsForUser empty", false, e);
            }

            // DeleteGroup
            try {
                cognito.deleteGroup(b -> b.userPoolId(fPoolId).groupName("test-group"));
                ctx.check("Cognito DeleteGroup", true);
            } catch (Exception e) {
                ctx.check("Cognito DeleteGroup", false, e);
            }

            // GetGroup after delete — expect not found
            try {
                cognito.getGroup(b -> b.userPoolId(fPoolId).groupName("test-group"));
                ctx.check("Cognito GetGroup not found", false);
            } catch (ResourceNotFoundException e) {
                ctx.check("Cognito GetGroup not found", true);
            } catch (Exception e) {
                ctx.check("Cognito GetGroup not found", false, e);
            }

            // ── Cleanup ───────────────────────────────────────────────────

            try {
                cognito.adminDeleteUser(b -> b.userPoolId(fPoolId).username(fUsername));
                ctx.check("Cognito AdminDeleteUser", true);
            } catch (Exception e) { ctx.check("Cognito AdminDeleteUser", false, e); }

            if (clientId != null) {
                final String fClientId = clientId;
                try {
                    cognito.deleteUserPoolClient(b -> b.userPoolId(fPoolId).clientId(fClientId));
                    ctx.check("Cognito DeleteUserPoolClient", true);
                } catch (Exception e) { ctx.check("Cognito DeleteUserPoolClient", false, e); }
            }

            try {
                cognito.deleteUserPool(b -> b.userPoolId(fPoolId));
                ctx.check("Cognito DeleteUserPool", true);
            } catch (Exception e) { ctx.check("Cognito DeleteUserPool", false, e); }

        } catch (Exception e) {
            ctx.check("Cognito Client", false, e);
        }
    }
}
