package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.*;

@FlociTestGroup(2)
public class SsmTests implements TestGroup {

    @Override
    public String name() { return "ssm"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- SSM Tests ---");

        try (SsmClient ssm = SsmClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            String name = "/sdk-test/param";
            String value = "param-value-123";

            // 1. PutParameter
            try {
                PutParameterResponse putResp = ssm.putParameter(PutParameterRequest.builder()
                        .name(name).value(value)
                        .type(ParameterType.STRING).overwrite(true)
                        .build());
                ctx.check("SSM PutParameter", putResp.version() != null && putResp.version() > 0);
            } catch (Exception e) {
                ctx.check("SSM PutParameter", false, e);
            }

            // 2. GetParameter
            try {
                GetParameterResponse getResp = ssm.getParameter(GetParameterRequest.builder()
                        .name(name).withDecryption(false).build());
                ctx.check("SSM GetParameter", getResp.parameter() != null
                        && value.equals(getResp.parameter().value()));
            } catch (Exception e) {
                ctx.check("SSM GetParameter", false, e);
            }

            // 2b. LabelParameterVersion
            try {
                ssm.labelParameterVersion(LabelParameterVersionRequest.builder()
                        .name(name).labels("test-label").parameterVersion(1L).build());
                ctx.check("SSM LabelParameterVersion", true);
            } catch (Exception e) {
                ctx.check("SSM LabelParameterVersion", false, e);
            }

            // 2c. GetParameterHistory
            try {
                GetParameterHistoryResponse hist = ssm.getParameterHistory(
                        GetParameterHistoryRequest.builder().name(name).withDecryption(false).build());
                boolean foundHist = hist.parameters().stream().anyMatch(p -> value.equals(p.value()));
                ctx.check("SSM GetParameterHistory", foundHist);
            } catch (Exception e) {
                ctx.check("SSM GetParameterHistory", false, e);
            }

            // 3. GetParameters
            try {
                GetParametersResponse gps = ssm.getParameters(
                        GetParametersRequest.builder().names(name).build());
                boolean ok = gps.parameters() != null && gps.parameters().stream()
                        .anyMatch(p -> name.equals(p.name()) && value.equals(p.value()));
                ctx.check("SSM GetParameters", ok);
            } catch (Exception e) {
                ctx.check("SSM GetParameters", false, e);
            }

            // 4. DescribeParameters
            try {
                DescribeParametersResponse dpr = ssm.describeParameters(
                        DescribeParametersRequest.builder().build());
                boolean found = dpr.parameters().stream().anyMatch(p -> name.equals(p.name()));
                ctx.check("SSM DescribeParameters", found);
            } catch (Exception e) {
                ctx.check("SSM DescribeParameters", false, e);
            }

            // 5. GetParametersByPath
            try {
                GetParametersByPathResponse byPath = ssm.getParametersByPath(
                        GetParametersByPathRequest.builder()
                                .path("/sdk-test").recursive(false).build());
                boolean found = byPath.parameters().stream().anyMatch(p -> name.equals(p.name()));
                ctx.check("SSM GetParametersByPath", found);
            } catch (Exception e) {
                ctx.check("SSM GetParametersByPath", false, e);
            }

            // 6. AddTagsToResource
            try {
                ssm.addTagsToResource(AddTagsToResourceRequest.builder()
                        .resourceType("Parameter").resourceId(name)
                        .tags(
                                Tag.builder().key("env").value("test").build(),
                                Tag.builder().key("team").value("backend").build()
                        )
                        .build());
                ctx.check("SSM AddTagsToResource", true);
            } catch (Exception e) {
                ctx.check("SSM AddTagsToResource", false, e);
            }

            // 7. ListTagsForResource
            try {
                ListTagsForResourceResponse ltr = ssm.listTagsForResource(
                        ListTagsForResourceRequest.builder()
                                .resourceType("Parameter").resourceId(name).build());
                boolean envFound = ltr.tagList().stream()
                        .anyMatch(t -> "env".equals(t.key()) && "test".equals(t.value()));
                boolean teamFound = ltr.tagList().stream()
                        .anyMatch(t -> "team".equals(t.key()) && "backend".equals(t.value()));
                ctx.check("SSM ListTagsForResource", envFound && teamFound);
            } catch (Exception e) {
                ctx.check("SSM ListTagsForResource", false, e);
            }

            // 8. RemoveTagsFromResource
            try {
                ssm.removeTagsFromResource(RemoveTagsFromResourceRequest.builder()
                        .resourceType("Parameter").resourceId(name).tagKeys("team").build());

                ListTagsForResourceResponse ltr2 = ssm.listTagsForResource(
                        ListTagsForResourceRequest.builder()
                                .resourceType("Parameter").resourceId(name).build());
                boolean stillEnv = ltr2.tagList().stream()
                        .anyMatch(t -> "env".equals(t.key()) && "test".equals(t.value()));
                boolean removedTeam = ltr2.tagList().stream().noneMatch(t -> "team".equals(t.key()));
                ctx.check("SSM RemoveTagsFromResource", stillEnv && removedTeam);
            } catch (Exception e) {
                ctx.check("SSM RemoveTagsFromResource", false, e);
            }

            // 9. DeleteParameter
            try {
                ssm.deleteParameter(DeleteParameterRequest.builder().name(name).build());
                try {
                    ssm.getParameter(GetParameterRequest.builder().name(name).withDecryption(false).build());
                    ctx.check("SSM DeleteParameter", false);
                } catch (ParameterNotFoundException e) {
                    ctx.check("SSM DeleteParameter", true);
                } catch (Exception e) {
                    ctx.check("SSM DeleteParameter", false, e);
                }
            } catch (Exception e) {
                ctx.check("SSM DeleteParameter", false, e);
            }

            // 10. DeleteParameters (batch)
            try {
                String p1 = "/sdk-test/param1";
                String p2 = "/sdk-test/param2";
                ssm.putParameter(PutParameterRequest.builder()
                        .name(p1).value("v1").type(ParameterType.STRING).overwrite(true).build());
                ssm.putParameter(PutParameterRequest.builder()
                        .name(p2).value("v2").type(ParameterType.STRING).overwrite(true).build());

                DeleteParametersResponse delResp = ssm.deleteParameters(
                        DeleteParametersRequest.builder().names(p1, p2).build());
                ctx.check("SSM DeleteParameters", delResp.deletedParameters().size() == 2);
            } catch (Exception e) {
                ctx.check("SSM DeleteParameters", false, e);
            }
        }
    }
}
