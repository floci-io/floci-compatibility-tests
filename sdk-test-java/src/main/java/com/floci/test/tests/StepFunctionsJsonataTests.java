package com.floci.test.tests;

import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.SfnClientBuilder;
import software.amazon.awssdk.services.sfn.model.*;

public class StepFunctionsJsonataTests implements TestGroup {

    /** Override with a real IAM role ARN when testing against AWS. */
    private static final String ROLE_ARN = System.getenv("SFN_ROLE_ARN") != null
            ? System.getenv("SFN_ROLE_ARN")
            : "arn:aws:iam::000000000000:role/service-role/test-role";

    /** Set to "aws" to use real AWS credentials and no endpoint override. */
    private static final boolean USE_REAL_AWS = "aws".equalsIgnoreCase(System.getenv("SFN_TARGET"));

    @Override
    public String name() { return "sfn-jsonata"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- Step Functions JSONata Tests ---");
        if (USE_REAL_AWS) System.out.println("  (targeting real AWS)");

        SfnClientBuilder builder = SfnClient.builder().region(ctx.region);
        if (USE_REAL_AWS) {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        } else {
            builder.endpointOverride(ctx.endpoint).credentialsProvider(ctx.credentials);
        }

        try (SfnClient sfn = builder.build()) {

            testPassStateWithOutput(ctx, sfn);
            testChoiceStateWithCondition(ctx, sfn);
            testMapStateWithItems(ctx, sfn);
            testStatesInputAccess(ctx, sfn);
            testOutputWithJsonataExpression(ctx, sfn);
            testJsonataConcatenationInOutput(ctx, sfn);
            testMixedModeJsonPathDefaultWithJsonataOverride(ctx, sfn);
            testStatesContext(ctx, sfn);
            testFailStateWithJsonataExpressions(ctx, sfn);

        } catch (Exception e) {
            ctx.check("SFN-JSONata Client", false, e);
        }
    }

    // ── 1. Pass state with Output ──────────────────────────────────────

    private void testPassStateWithOutput(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "Transform",
                  "States": {
                    "Transform": {
                      "Type": "Pass",
                      "Output": {
                        "greeting": "{% 'Hello ' & $states.input.name %}"
                      },
                      "End": true
                    }
                  }
                }""";
        String input = """
                {"name": "Alice"}""";

        try {
            String output = executeAndWait(sfn, "jsonata-pass-output", definition, input);
            boolean ok = output != null
                    && output.contains("\"greeting\"")
                    && output.contains("Hello Alice");
            ctx.check("SFN-JSONata Pass with Output", ok);
        } catch (Exception e) {
            ctx.check("SFN-JSONata Pass with Output", false, e);
        }
    }

    // ── 2. Choice state with Condition ─────────────────────────────────

    private void testChoiceStateWithCondition(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "Route",
                  "States": {
                    "Route": {
                      "Type": "Choice",
                      "Choices": [
                        {
                          "Condition": "{% $states.input.score > 50 %}",
                          "Next": "High"
                        }
                      ],
                      "Default": "Low"
                    },
                    "High": {
                      "Type": "Pass",
                      "Output": {"result": "high"},
                      "End": true
                    },
                    "Low": {
                      "Type": "Pass",
                      "Output": {"result": "low"},
                      "End": true
                    }
                  }
                }""";
        String input = """
                {"score": 85}""";

        try {
            String output = executeAndWait(sfn, "jsonata-choice", definition, input);
            boolean ok = output != null && output.contains("\"high\"");
            ctx.check("SFN-JSONata Choice with Condition", ok);
        } catch (Exception e) {
            ctx.check("SFN-JSONata Choice with Condition", false, e);
        }
    }

    // ── 3. Map state with Items ────────────────────────────────────────

    private void testMapStateWithItems(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "MapItems",
                  "States": {
                    "MapItems": {
                      "Type": "Map",
                      "Items": "{% $states.input.values %}",
                      "ItemProcessor": {
                        "StartAt": "AddPrefix",
                        "States": {
                          "AddPrefix": {
                            "Type": "Pass",
                            "End": true
                          }
                        }
                      },
                      "End": true
                    }
                  }
                }""";
        String input = """
                {"values": [1, 2, 3]}""";

        try {
            String output = executeAndWait(sfn, "jsonata-map-items", definition, input);
            boolean ok = output != null && output.contains("1") && output.contains("2") && output.contains("3");
            ctx.check("SFN-JSONata Map with Items", ok);
        } catch (Exception e) {
            ctx.check("SFN-JSONata Map with Items", false, e);
        }
    }

    // ── 4. $states.input access ────────────────────────────────────────

    private void testStatesInputAccess(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "Echo",
                  "States": {
                    "Echo": {
                      "Type": "Pass",
                      "Output": "{% $states.input %}",
                      "End": true
                    }
                  }
                }""";
        String input = """
                {"foo": "bar", "num": 42}""";

        try {
            String output = executeAndWait(sfn, "jsonata-states-input", definition, input);
            boolean ok = output != null
                    && output.contains("\"foo\"")
                    && output.contains("\"bar\"")
                    && output.contains("42");
            ctx.check("SFN-JSONata $states.input access", ok);
        } catch (Exception e) {
            ctx.check("SFN-JSONata $states.input access", false, e);
        }
    }

    // ── 5. Output with JSONata expression (arithmetic) ─────────────────

    private void testOutputWithJsonataExpression(TestContext ctx, SfnClient sfn) {
        // Pass state with Output doing arithmetic on input
        String definition = """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "Compute",
                  "States": {
                    "Compute": {
                      "Type": "Pass",
                      "Output": {
                        "doubled": "{% $states.input.value * 2 %}"
                      },
                      "End": true
                    }
                  }
                }""";
        String input = """
                {"value": 21}""";

        try {
            String output = executeAndWait(sfn, "jsonata-output-arithmetic", definition, input);
            boolean ok = output != null && output.contains("42");
            ctx.check("SFN-JSONata Output with arithmetic", ok);
        } catch (Exception e) {
            ctx.check("SFN-JSONata Output with arithmetic", false, e);
        }
    }

    // ── 6. JSONata string concatenation in Output ────────────────────

    private void testJsonataConcatenationInOutput(TestContext ctx, SfnClient sfn) {
        // Use JSONata's & operator inside a single {% %} expression for string building
        String definition = """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "Greet",
                  "States": {
                    "Greet": {
                      "Type": "Pass",
                      "Output": {
                        "message": "{% 'Hello ' & $states.input.name & ', you are ' & $string($states.input.age) & ' years old' %}"
                      },
                      "End": true
                    }
                  }
                }""";
        String input = """
                {"name": "Bob", "age": 25}""";

        try {
            String output = executeAndWait(sfn, "jsonata-concat", definition, input);
            boolean ok = output != null
                    && output.contains("Hello Bob")
                    && output.contains("you are 25 years old");
            ctx.check("SFN-JSONata String concatenation in Output", ok);
        } catch (Exception e) {
            ctx.check("SFN-JSONata String concatenation in Output", false, e);
        }
    }

    // ── 7. Mixed mode: default JSONPath with per-state JSONata override ──

    private void testMixedModeJsonPathDefaultWithJsonataOverride(TestContext ctx, SfnClient sfn) {
        // Top-level defaults to JSONPath; one state overrides to JSONata
        String definition = """
                {
                  "StartAt": "JsonPathState",
                  "States": {
                    "JsonPathState": {
                      "Type": "Pass",
                      "Next": "JsonataState"
                    },
                    "JsonataState": {
                      "Type": "Pass",
                      "QueryLanguage": "JSONata",
                      "Output": {
                        "x": "{% $states.input.val + 1 %}"
                      },
                      "End": true
                    }
                  }
                }""";
        String input = """
                {"val": 10}""";

        try {
            String output = executeAndWait(sfn, "jsonata-mixed-mode", definition, input);
            boolean ok = output != null && output.contains("11");
            ctx.check("SFN-JSONata Mixed mode (per-state JSONata override)", ok);
        } catch (Exception e) {
            ctx.check("SFN-JSONata Mixed mode (per-state JSONata override)", false, e);
        }
    }

    // ── 8. $states.context access ──────────────────────────────────────

    private void testStatesContext(TestContext ctx, SfnClient sfn) {
        // Extract context fields into the output so we can inspect them
        String definition = """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "ShowContext",
                  "States": {
                    "ShowContext": {
                      "Type": "Pass",
                      "Output": {
                        "execId": "{% $states.context.Execution.Id %}",
                        "execName": "{% $states.context.Execution.Name %}",
                        "smName": "{% $states.context.StateMachine.Name %}",
                        "stateName": "{% $states.context.State.Name %}"
                      },
                      "End": true
                    }
                  }
                }""";
        String input = "{}";

        try {
            String smName = "test-jsonata-context-" + System.currentTimeMillis();
            String smArn = createStateMachineWithName(sfn, smName, definition);
            try {
                String execArn = startExecution(sfn, smArn, input);
                DescribeExecutionResponse resp = waitForExecution(sfn, execArn);

                if (resp.status() != ExecutionStatus.SUCCEEDED) {
                    ctx.check("SFN-JSONata $states.context", false,
                            new RuntimeException("Execution " + resp.status() + " error=" + resp.error()));
                    return;
                }

                String output = resp.output();
                boolean ok = output != null
                        && output.contains("execId")
                        && output.contains("smName")
                        && output.contains(smName)
                        && output.contains("ShowContext"); // State.Name
                ctx.check("SFN-JSONata $states.context", ok);
            } finally {
                sfn.deleteStateMachine(b -> b.stateMachineArn(smArn));
            }
        } catch (Exception e) {
            ctx.check("SFN-JSONata $states.context", false, e);
        }
    }

    private String createStateMachineWithName(SfnClient sfn, String name, String definition) {
        CreateStateMachineResponse resp = sfn.createStateMachine(b -> b
                .name(name)
                .definition(definition)
                .roleArn(ROLE_ARN));
        return resp.stateMachineArn();
    }

    // ── 9. Fail state with JSONata expressions ─────────────────────────

    private void testFailStateWithJsonataExpressions(TestContext ctx, SfnClient sfn) {
        String definition = """
                {
                  "QueryLanguage": "JSONata",
                  "StartAt": "FailNow",
                  "States": {
                    "FailNow": {
                      "Type": "Fail",
                      "Error": "{% 'CustomError.' & $states.input.code %}",
                      "Cause": "{% 'Failed because: ' & $states.input.reason %}"
                    }
                  }
                }""";
        String input = """
                {"code": "42", "reason": "test failure"}""";

        try {
            String smArn = createStateMachine(sfn, "jsonata-fail", definition);
            String execArn = startExecution(sfn, smArn, input);
            DescribeExecutionResponse resp = waitForExecution(sfn, execArn);

            boolean ok = resp.status() == ExecutionStatus.FAILED
                    && resp.error() != null && resp.error().contains("CustomError.42")
                    && resp.cause() != null && resp.cause().contains("test failure");
            ctx.check("SFN-JSONata Fail state with expressions", ok);

            sfn.deleteStateMachine(b -> b.stateMachineArn(smArn));
        } catch (Exception e) {
            ctx.check("SFN-JSONata Fail state with expressions", false, e);
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────

    private String executeAndWait(SfnClient sfn, String nameSuffix, String definition, String input) {
        String smArn = createStateMachine(sfn, nameSuffix, definition);
        try {
            String execArn = startExecution(sfn, smArn, input);
            DescribeExecutionResponse resp = waitForExecution(sfn, execArn);

            if (resp.status() != ExecutionStatus.SUCCEEDED) {
                throw new RuntimeException("Execution " + resp.status()
                        + " error=" + resp.error() + " cause=" + resp.cause());
            }
            return resp.output();
        } finally {
            sfn.deleteStateMachine(b -> b.stateMachineArn(smArn));
        }
    }

    private String createStateMachine(SfnClient sfn, String nameSuffix, String definition) {
        CreateStateMachineResponse resp = sfn.createStateMachine(b -> b
                .name("test-" + nameSuffix + "-" + System.currentTimeMillis())
                .definition(definition)
                .roleArn(ROLE_ARN));
        return resp.stateMachineArn();
    }

    private String startExecution(SfnClient sfn, String smArn, String input) {
        StartExecutionResponse resp = sfn.startExecution(b -> b
                .stateMachineArn(smArn)
                .input(input));
        return resp.executionArn();
    }

    private DescribeExecutionResponse waitForExecution(SfnClient sfn, String execArn) {
        for (int i = 0; i < 30; i++) {
            DescribeExecutionResponse resp = sfn.describeExecution(b -> b.executionArn(execArn));
            if (resp.status() != ExecutionStatus.RUNNING) {
                return resp;
            }
            try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
        throw new RuntimeException("Execution did not complete within timeout");
    }
}
