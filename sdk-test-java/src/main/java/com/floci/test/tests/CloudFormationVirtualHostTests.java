package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.ListStacksResponse;

import java.net.URI;

/**
 * Ensures that requests to non-S3 service hostnames (e.g. cloudformation.amazonaws.com)
 * are not incorrectly hijacked by the S3 virtual host filter.
 */
@FlociTestGroup
public class CloudFormationVirtualHostTests implements TestGroup {

    @Override
    public String name() { return "cfn-vhost"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- CloudFormation Virtual Host Tests ---");

        // We override the endpoint to simulate a virtual host style request for CloudFormation
        URI cfnEndpoint = URI.create("http://cloudformation.us-east-1.amazonaws.com:" + ctx.endpoint.getPort());
        if (ctx.endpoint.getHost().equals("localhost")) {
            cfnEndpoint = URI.create("http://cloudformation.localhost:" + ctx.endpoint.getPort());
        }

        try (CloudFormationClient cfn = CloudFormationClient.builder()
                .endpointOverride(cfnEndpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            try {
                ListStacksResponse resp = cfn.listStacks();
                ctx.check("CFN VHost ListStacks", resp.sdkHttpResponse().isSuccessful());
            } catch (Exception e) {
                ctx.check("CFN VHost ListStacks", false, e);
            }
        } catch (Exception e) {
            ctx.check("CFN VHost Client", false, e);
        }
    }
}
