package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.CreateStackRequest;
import software.amazon.awssdk.services.cloudformation.model.DescribeStackResourcesResponse;
import software.amazon.awssdk.services.cloudformation.model.DescribeStacksResponse;
import software.amazon.awssdk.services.cloudformation.model.StackResource;
import software.amazon.awssdk.services.cloudformation.model.StackStatus;

import java.util.List;
import java.util.Locale;

@FlociTestGroup
public class CloudFormationNamingTests implements TestGroup {

    @Override
    public String name() { return "cloudformation-naming"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- CloudFormation Naming Tests ---");

        try (CloudFormationClient cfn = CloudFormationClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            String token = Long.toString(System.currentTimeMillis(), 36);

            runAutoNamingCase(cfn, ctx, token);
            runExplicitNamingCase(cfn, ctx, token);
        }
    }

    private void runAutoNamingCase(CloudFormationClient cfn, TestContext ctx, String token) {
        String stackName = "cfn-auto-naming-" + token;
        String template = """
                {
                  "Resources": {
                    "AutoBucket": {
                      "Type": "AWS::S3::Bucket"
                    },
                    "AutoQueue": {
                      "Type": "AWS::SQS::Queue"
                    },
                    "AutoTopic": {
                      "Type": "AWS::SNS::Topic"
                    },
                    "AutoParameter": {
                      "Type": "AWS::SSM::Parameter",
                      "Properties": {
                        "Type": "String",
                        "Value": "v1"
                      }
                    },
                    "CrossRefQueue": {
                      "Type": "AWS::SQS::Queue",
                      "Properties": {
                        "QueueName": {
                          "Fn::Sub": "${AutoBucket}-cross"
                        }
                      }
                    }
                  }
                }
                """;

        try {
            cfn.createStack(CreateStackRequest.builder().stackName(stackName).templateBody(template).build());
            ctx.check("CFN Naming auto CreateStack", true);
        } catch (Exception e) {
            ctx.check("CFN Naming auto CreateStack", false, e);
            return;
        }

        WaitResult autoWait = waitForStackTerminalState(cfn, stackName, true);
        ctx.check("CFN Naming auto terminal status", autoWait.ok, new RuntimeException(autoWait.status));
        if (!autoWait.ok) {
          deleteStack(cfn, ctx, stackName, "CFN Naming auto DeleteStack");
          return;
        }

        List<StackResource> resources;
        try {
            DescribeStackResourcesResponse resp = cfn.describeStackResources(b -> b.stackName(stackName));
            resources = resp.stackResources();
            ctx.check("CFN Naming auto DescribeStackResources", !resources.isEmpty());
        } catch (Exception e) {
            ctx.check("CFN Naming auto DescribeStackResources", false, e);
            deleteStack(cfn, ctx, stackName, "CFN Naming auto DeleteStack");
            return;
        }

        String bucketName = physicalId(resources, "AutoBucket");
        String queueId = physicalId(resources, "AutoQueue");
        String topicId = physicalId(resources, "AutoTopic");
        String parameterName = physicalId(resources, "AutoParameter");
        String crossRefQueue = physicalId(resources, "CrossRefQueue");

        ctx.check("CFN Naming auto S3 generated name present", bucketName != null && !bucketName.isBlank());
        if (bucketName != null) {
            ctx.check("CFN Naming auto S3 naming constraints",
                    bucketName.length() >= 3
                            && bucketName.length() <= 63
                            && bucketName.matches("[a-z0-9.-]+")
                            && bucketName.equals(bucketName.toLowerCase(Locale.ROOT)));
        }

        ctx.check("CFN Naming auto SQS generated id present", queueId != null && !queueId.isBlank());
        if (queueId != null) {
            String queueName = queueId.contains("/") ? queueId.substring(queueId.lastIndexOf('/') + 1) : queueId;
            ctx.check("CFN Naming auto SQS naming constraints", queueName.length() > 0 && queueName.length() <= 80);
        }

        ctx.check("CFN Naming auto SNS generated id present", topicId != null && !topicId.isBlank());
        if (topicId != null) {
            String topicName = topicId.contains(":") ? topicId.substring(topicId.lastIndexOf(':') + 1) : topicId;
            ctx.check("CFN Naming auto SNS naming constraints", topicName.length() > 0 && topicName.length() <= 256);
        }

        ctx.check("CFN Naming auto SSM generated name present", parameterName != null && !parameterName.isBlank());
        if (parameterName != null) {
            ctx.check("CFN Naming auto SSM naming constraints", parameterName.length() <= 2048);
        }

        ctx.check("CFN Naming cross-reference queue created", crossRefQueue != null && !crossRefQueue.isBlank());
        if (bucketName != null && crossRefQueue != null) {
            String queueName = crossRefQueue.contains("/")
                    ? crossRefQueue.substring(crossRefQueue.lastIndexOf('/') + 1)
                    : crossRefQueue;
            ctx.check("CFN Naming cross-reference queue uses AutoBucket name",
                    queueName.startsWith(bucketName + "-cross"));
        }

        deleteStack(cfn, ctx, stackName, "CFN Naming auto DeleteStack");
    }

    private void runExplicitNamingCase(CloudFormationClient cfn, TestContext ctx, String token) {
        String stackName = "cfn-explicit-naming-" + token;
        String bucketName = "cfn-explicit-" + token;
        String queueName = "cfn-explicit-" + token;
        String topicName = "cfn-explicit-" + token;
        String parameterName = "/cfn-explicit/" + token;

        String template = """
                {
                  "Resources": {
                    "NamedBucket": {
                      "Type": "AWS::S3::Bucket",
                      "Properties": {
                        "BucketName": "%s"
                      }
                    },
                    "NamedQueue": {
                      "Type": "AWS::SQS::Queue",
                      "Properties": {
                        "QueueName": "%s"
                      }
                    },
                    "NamedTopic": {
                      "Type": "AWS::SNS::Topic",
                      "Properties": {
                        "TopicName": "%s"
                      }
                    },
                    "NamedParameter": {
                      "Type": "AWS::SSM::Parameter",
                      "Properties": {
                        "Name": "%s",
                        "Type": "String",
                        "Value": "explicit"
                      }
                    }
                  }
                }
                """.formatted(bucketName, queueName, topicName, parameterName);

        try {
            cfn.createStack(CreateStackRequest.builder().stackName(stackName).templateBody(template).build());
            ctx.check("CFN Naming explicit CreateStack", true);
        } catch (Exception e) {
            ctx.check("CFN Naming explicit CreateStack", false, e);
            return;
        }

        WaitResult explicitWait = waitForStackTerminalState(cfn, stackName, true);
        ctx.check("CFN Naming explicit terminal status", explicitWait.ok, new RuntimeException(explicitWait.status));
        if (!explicitWait.ok) {
          deleteStack(cfn, ctx, stackName, "CFN Naming explicit DeleteStack");
          return;
        }

        try {
            List<StackResource> resources = cfn.describeStackResources(b -> b.stackName(stackName)).stackResources();
            String actualBucket = physicalId(resources, "NamedBucket");
            String actualQueue = physicalId(resources, "NamedQueue");
            String actualTopic = physicalId(resources, "NamedTopic");
            String actualParameter = physicalId(resources, "NamedParameter");

            ctx.check("CFN Naming explicit S3 name respected", bucketName.equals(actualBucket));
            ctx.check("CFN Naming explicit SQS name respected",
                    actualQueue != null && actualQueue.contains(queueName));
            ctx.check("CFN Naming explicit SNS name respected",
                    actualTopic != null && actualTopic.contains(topicName));
            ctx.check("CFN Naming explicit SSM name respected", parameterName.equals(actualParameter));
        } catch (Exception e) {
            ctx.check("CFN Naming explicit DescribeStackResources", false, e);
        }

        deleteStack(cfn, ctx, stackName, "CFN Naming explicit DeleteStack");
    }

      private WaitResult waitForStackTerminalState(CloudFormationClient cfn, String stackName, boolean expectedSuccess) {
        for (int i = 0; i < 40; i++) {
          try {
            DescribeStacksResponse resp = cfn.describeStacks(b -> b.stackName(stackName));
            if (resp.stacks().isEmpty()) {
              return new WaitResult(false, "STACK_NOT_FOUND");
            }
            StackStatus status = resp.stacks().get(0).stackStatus();
            if (status == StackStatus.CREATE_COMPLETE || status == StackStatus.UPDATE_COMPLETE) {
              return new WaitResult(expectedSuccess, status.toString());
            }
            if (status == StackStatus.CREATE_FAILED
                || status == StackStatus.ROLLBACK_IN_PROGRESS
                || status == StackStatus.ROLLBACK_FAILED
                || status == StackStatus.ROLLBACK_COMPLETE
                || status == StackStatus.DELETE_FAILED
                || status == StackStatus.DELETE_COMPLETE) {
              return new WaitResult(!expectedSuccess, status.toString());
            }
            Thread.sleep(1000);
          } catch (Exception e) {
            return new WaitResult(false, e.getMessage() != null ? e.getMessage() : "UNKNOWN_ERROR");
          }
        }
        return new WaitResult(false, "TIMEOUT");
      }

      private record WaitResult(boolean ok, String status) {}

    private void deleteStack(CloudFormationClient cfn, TestContext ctx, String stackName, String checkName) {
        try {
            cfn.deleteStack(b -> b.stackName(stackName));
            ctx.check(checkName, true);
        } catch (Exception e) {
            ctx.check(checkName, false, e);
        }
    }

    private String physicalId(List<StackResource> resources, String logicalId) {
        return resources.stream()
                .filter(r -> logicalId.equals(r.logicalResourceId()))
                .map(StackResource::physicalResourceId)
                .findFirst()
                .orElse(null);
    }
}
