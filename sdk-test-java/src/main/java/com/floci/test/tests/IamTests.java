package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.*;

import java.util.List;

@FlociTestGroup
public class IamTests implements TestGroup {

    @Override
    public String name() { return "iam"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- IAM Tests ---");

        try (IamClient iam = IamClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            String userName = "sdk-test-user";
            String groupName = "sdk-test-group";
            String roleName = "sdk-test-role";
            String policyName = "sdk-test-policy";
            String instanceProfileName = "sdk-test-profile";
            String trustPolicy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\","
                    + "\"Principal\":{\"Service\":\"lambda.amazonaws.com\"},\"Action\":\"sts:AssumeRole\"}]}";
            String policyDocument = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\","
                    + "\"Action\":\"s3:GetObject\",\"Resource\":\"*\"}]}";

            // ── Users ──────────────────────────────────────────────────────────

            // 1. CreateUser
            String createdUserId;
            try {
                CreateUserResponse resp = iam.createUser(CreateUserRequest.builder()
                        .userName(userName).path("/").build());
                createdUserId = resp.user().userId();
                ctx.check("IAM CreateUser",
                        userName.equals(resp.user().userName())
                        && resp.user().userId() != null
                        && resp.user().arn().contains(userName));
            } catch (Exception e) {
                ctx.check("IAM CreateUser", false, e);
                createdUserId = null;
            }

            // 2. GetUser
            try {
                GetUserResponse resp = iam.getUser(GetUserRequest.builder().userName(userName).build());
                ctx.check("IAM GetUser", userName.equals(resp.user().userName()));
            } catch (Exception e) {
                ctx.check("IAM GetUser", false, e);
            }

            // 3. ListUsers
            try {
                ListUsersResponse resp = iam.listUsers();
                boolean found = resp.users().stream().anyMatch(u -> userName.equals(u.userName()));
                ctx.check("IAM ListUsers", found);
            } catch (Exception e) {
                ctx.check("IAM ListUsers", false, e);
            }

            // 4. TagUser
            try {
                iam.tagUser(TagUserRequest.builder()
                        .userName(userName)
                        .tags(Tag.builder().key("env").value("sdk-test").build())
                        .build());
                ctx.check("IAM TagUser", true);
            } catch (Exception e) {
                ctx.check("IAM TagUser", false, e);
            }

            // 5. ListUserTags
            try {
                ListUserTagsResponse resp = iam.listUserTags(
                        ListUserTagsRequest.builder().userName(userName).build());
                boolean found = resp.tags().stream().anyMatch(t -> "env".equals(t.key()));
                ctx.check("IAM ListUserTags", found);
            } catch (Exception e) {
                ctx.check("IAM ListUserTags", false, e);
            }

            // 6. UntagUser
            try {
                iam.untagUser(UntagUserRequest.builder()
                        .userName(userName).tagKeys("env").build());
                ctx.check("IAM UntagUser", true);
            } catch (Exception e) {
                ctx.check("IAM UntagUser", false, e);
            }

            // ── Access Keys ────────────────────────────────────────────────────

            // 7. CreateAccessKey
            String createdKeyId;
            try {
                CreateAccessKeyResponse resp = iam.createAccessKey(
                        CreateAccessKeyRequest.builder().userName(userName).build());
                createdKeyId = resp.accessKey().accessKeyId();
                ctx.check("IAM CreateAccessKey",
                        createdKeyId != null && createdKeyId.startsWith("AKIA")
                        && resp.accessKey().secretAccessKey() != null
                        && StatusType.ACTIVE.equals(resp.accessKey().status()));
            } catch (Exception e) {
                ctx.check("IAM CreateAccessKey", false, e);
                createdKeyId = null;
            }

            // 8. ListAccessKeys
            try {
                ListAccessKeysResponse resp = iam.listAccessKeys(
                        ListAccessKeysRequest.builder().userName(userName).build());
                ctx.check("IAM ListAccessKeys", !resp.accessKeyMetadata().isEmpty());
            } catch (Exception e) {
                ctx.check("IAM ListAccessKeys", false, e);
            }

            // 9. UpdateAccessKey
            if (createdKeyId != null) {
                try {
                    iam.updateAccessKey(UpdateAccessKeyRequest.builder()
                            .userName(userName).accessKeyId(createdKeyId)
                            .status(StatusType.INACTIVE).build());
                    ctx.check("IAM UpdateAccessKey", true);
                } catch (Exception e) {
                    ctx.check("IAM UpdateAccessKey", false, e);
                }

                // 10. DeleteAccessKey
                try {
                    iam.deleteAccessKey(DeleteAccessKeyRequest.builder()
                            .userName(userName).accessKeyId(createdKeyId).build());
                    ctx.check("IAM DeleteAccessKey", true);
                } catch (Exception e) {
                    ctx.check("IAM DeleteAccessKey", false, e);
                }
            }

            // ── Groups ─────────────────────────────────────────────────────────

            // 11. CreateGroup
            try {
                CreateGroupResponse resp = iam.createGroup(CreateGroupRequest.builder()
                        .groupName(groupName).build());
                ctx.check("IAM CreateGroup", groupName.equals(resp.group().groupName()));
            } catch (Exception e) {
                ctx.check("IAM CreateGroup", false, e);
            }

            // 12. AddUserToGroup
            try {
                iam.addUserToGroup(AddUserToGroupRequest.builder()
                        .groupName(groupName).userName(userName).build());
                ctx.check("IAM AddUserToGroup", true);
            } catch (Exception e) {
                ctx.check("IAM AddUserToGroup", false, e);
            }

            // 13. GetGroup (includes members)
            try {
                GetGroupResponse resp = iam.getGroup(GetGroupRequest.builder()
                        .groupName(groupName).build());
                boolean hasMember = resp.users().stream().anyMatch(u -> userName.equals(u.userName()));
                ctx.check("IAM GetGroup", hasMember);
            } catch (Exception e) {
                ctx.check("IAM GetGroup", false, e);
            }

            // 14. ListGroupsForUser
            try {
                ListGroupsForUserResponse resp = iam.listGroupsForUser(
                        ListGroupsForUserRequest.builder().userName(userName).build());
                boolean found = resp.groups().stream().anyMatch(g -> groupName.equals(g.groupName()));
                ctx.check("IAM ListGroupsForUser", found);
            } catch (Exception e) {
                ctx.check("IAM ListGroupsForUser", false, e);
            }

            // ── Roles ──────────────────────────────────────────────────────────

            // 15. CreateRole
            try {
                CreateRoleResponse resp = iam.createRole(CreateRoleRequest.builder()
                        .roleName(roleName)
                        .assumeRolePolicyDocument(trustPolicy)
                        .description("SDK test role")
                        .build());
                ctx.check("IAM CreateRole",
                        roleName.equals(resp.role().roleName())
                        && resp.role().arn().contains(roleName));
            } catch (Exception e) {
                ctx.check("IAM CreateRole", false, e);
            }

            // 16. GetRole
            try {
                GetRoleResponse resp = iam.getRole(GetRoleRequest.builder().roleName(roleName).build());
                ctx.check("IAM GetRole", roleName.equals(resp.role().roleName()));
            } catch (Exception e) {
                ctx.check("IAM GetRole", false, e);
            }

            // 17. ListRoles
            try {
                ListRolesResponse resp = iam.listRoles();
                boolean found = resp.roles().stream().anyMatch(r -> roleName.equals(r.roleName()));
                ctx.check("IAM ListRoles", found);
            } catch (Exception e) {
                ctx.check("IAM ListRoles", false, e);
            }

            // ── Managed Policies ───────────────────────────────────────────────

            // 18. CreatePolicy
            String policyArn;
            try {
                CreatePolicyResponse resp = iam.createPolicy(CreatePolicyRequest.builder()
                        .policyName(policyName)
                        .policyDocument(policyDocument)
                        .description("SDK test policy")
                        .build());
                policyArn = resp.policy().arn();
                ctx.check("IAM CreatePolicy",
                        policyName.equals(resp.policy().policyName())
                        && policyArn != null);
            } catch (Exception e) {
                ctx.check("IAM CreatePolicy", false, e);
                policyArn = null;
            }

            final String finalPolicyArn = policyArn;
            if (finalPolicyArn != null) {
                // 19. GetPolicy
                try {
                    GetPolicyResponse resp = iam.getPolicy(
                            GetPolicyRequest.builder().policyArn(finalPolicyArn).build());
                    ctx.check("IAM GetPolicy", policyName.equals(resp.policy().policyName()));
                } catch (Exception e) {
                    ctx.check("IAM GetPolicy", false, e);
                }

                // 20. AttachRolePolicy
                try {
                    iam.attachRolePolicy(AttachRolePolicyRequest.builder()
                            .roleName(roleName).policyArn(finalPolicyArn).build());
                    ctx.check("IAM AttachRolePolicy", true);
                } catch (Exception e) {
                    ctx.check("IAM AttachRolePolicy", false, e);
                }

                // 21. ListAttachedRolePolicies
                try {
                    ListAttachedRolePoliciesResponse resp = iam.listAttachedRolePolicies(
                            ListAttachedRolePoliciesRequest.builder().roleName(roleName).build());
                    boolean found = resp.attachedPolicies().stream()
                            .anyMatch(p -> finalPolicyArn.equals(p.policyArn()));
                    ctx.check("IAM ListAttachedRolePolicies", found);
                } catch (Exception e) {
                    ctx.check("IAM ListAttachedRolePolicies", false, e);
                }

                // 22. AttachUserPolicy
                try {
                    iam.attachUserPolicy(AttachUserPolicyRequest.builder()
                            .userName(userName).policyArn(finalPolicyArn).build());
                    ctx.check("IAM AttachUserPolicy", true);
                } catch (Exception e) {
                    ctx.check("IAM AttachUserPolicy", false, e);
                }

                // 23. ListAttachedUserPolicies
                try {
                    ListAttachedUserPoliciesResponse resp = iam.listAttachedUserPolicies(
                            ListAttachedUserPoliciesRequest.builder().userName(userName).build());
                    boolean found = resp.attachedPolicies().stream()
                            .anyMatch(p -> finalPolicyArn.equals(p.policyArn()));
                    ctx.check("IAM ListAttachedUserPolicies", found);
                } catch (Exception e) {
                    ctx.check("IAM ListAttachedUserPolicies", false, e);
                }

                // 24. PutRolePolicy (inline)
                try {
                    iam.putRolePolicy(PutRolePolicyRequest.builder()
                            .roleName(roleName)
                            .policyName("inline-exec")
                            .policyDocument("{\"Version\":\"2012-10-17\"}")
                            .build());
                    ctx.check("IAM PutRolePolicy", true);
                } catch (Exception e) {
                    ctx.check("IAM PutRolePolicy", false, e);
                }

                // 25. GetRolePolicy
                try {
                    GetRolePolicyResponse resp = iam.getRolePolicy(GetRolePolicyRequest.builder()
                            .roleName(roleName).policyName("inline-exec").build());
                    ctx.check("IAM GetRolePolicy", "inline-exec".equals(resp.policyName()));
                } catch (Exception e) {
                    ctx.check("IAM GetRolePolicy", false, e);
                }

                // 26. ListRolePolicies
                try {
                    ListRolePoliciesResponse resp = iam.listRolePolicies(
                            ListRolePoliciesRequest.builder().roleName(roleName).build());
                    ctx.check("IAM ListRolePolicies", resp.policyNames().contains("inline-exec"));
                } catch (Exception e) {
                    ctx.check("IAM ListRolePolicies", false, e);
                }
            }

            // ── Instance Profiles ──────────────────────────────────────────────

            // 27. CreateInstanceProfile
            try {
                CreateInstanceProfileResponse resp = iam.createInstanceProfile(
                        CreateInstanceProfileRequest.builder()
                                .instanceProfileName(instanceProfileName).build());
                ctx.check("IAM CreateInstanceProfile",
                        instanceProfileName.equals(resp.instanceProfile().instanceProfileName()));
            } catch (Exception e) {
                ctx.check("IAM CreateInstanceProfile", false, e);
            }

            // 28. AddRoleToInstanceProfile
            try {
                iam.addRoleToInstanceProfile(AddRoleToInstanceProfileRequest.builder()
                        .instanceProfileName(instanceProfileName).roleName(roleName).build());
                ctx.check("IAM AddRoleToInstanceProfile", true);
            } catch (Exception e) {
                ctx.check("IAM AddRoleToInstanceProfile", false, e);
            }

            // 29. GetInstanceProfile
            try {
                GetInstanceProfileResponse resp = iam.getInstanceProfile(
                        GetInstanceProfileRequest.builder()
                                .instanceProfileName(instanceProfileName).build());
                boolean hasRole = resp.instanceProfile().roles().stream()
                        .anyMatch(r -> roleName.equals(r.roleName()));
                ctx.check("IAM GetInstanceProfile", hasRole);
            } catch (Exception e) {
                ctx.check("IAM GetInstanceProfile", false, e);
            }

            // 30. ListInstanceProfiles
            try {
                ListInstanceProfilesResponse resp = iam.listInstanceProfiles();
                boolean found = resp.instanceProfiles().stream()
                        .anyMatch(p -> instanceProfileName.equals(p.instanceProfileName()));
                ctx.check("IAM ListInstanceProfiles", found);
            } catch (Exception e) {
                ctx.check("IAM ListInstanceProfiles", false, e);
            }

            // ── Error Cases ────────────────────────────────────────────────────

            // 31. GetUser not found
            try {
                iam.getUser(GetUserRequest.builder().userName("nonexistent-user-xyz").build());
                ctx.check("IAM GetUser NotFound", false);
            } catch (NoSuchEntityException e) {
                ctx.check("IAM GetUser NotFound", true);
            } catch (Exception e) {
                ctx.check("IAM GetUser NotFound", false, e);
            }

            // ── Cleanup ────────────────────────────────────────────────────────

            try {
                iam.removeRoleFromInstanceProfile(RemoveRoleFromInstanceProfileRequest.builder()
                        .instanceProfileName(instanceProfileName).roleName(roleName).build());
                iam.deleteInstanceProfile(DeleteInstanceProfileRequest.builder()
                        .instanceProfileName(instanceProfileName).build());
                iam.deleteRolePolicy(DeleteRolePolicyRequest.builder()
                        .roleName(roleName).policyName("inline-exec").build());
                if (finalPolicyArn != null) {
                    iam.detachRolePolicy(DetachRolePolicyRequest.builder()
                            .roleName(roleName).policyArn(finalPolicyArn).build());
                    iam.detachUserPolicy(DetachUserPolicyRequest.builder()
                            .userName(userName).policyArn(finalPolicyArn).build());
                }
                iam.deleteRole(DeleteRoleRequest.builder().roleName(roleName).build());
                if (finalPolicyArn != null) {
                    iam.deletePolicy(DeletePolicyRequest.builder().policyArn(finalPolicyArn).build());
                }
                iam.removeUserFromGroup(RemoveUserFromGroupRequest.builder()
                        .groupName(groupName).userName(userName).build());
                iam.deleteGroup(DeleteGroupRequest.builder().groupName(groupName).build());
                iam.deleteUser(DeleteUserRequest.builder().userName(userName).build());
                ctx.check("IAM Cleanup", true);
            } catch (Exception e) {
                ctx.check("IAM Cleanup", false, e);
            }
        }
    }
}
