package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.*;

@FlociTestGroup
public class StepFunctionsTests implements TestGroup {

    @Override
    public String name() { return "stepfunctions"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- Step Functions Tests ---");

        try (SfnClient sfn = SfnClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            // 1. CreateStateMachine
            String smArn;
            String definition = "{\"StartAt\":\"Hello\",\"States\":{\"Hello\":{\"Type\":\"Pass\",\"Result\":\"World\",\"End\":true}}}";
            try {
                CreateStateMachineResponse resp = sfn.createStateMachine(b -> b
                        .name("test-sm-" + System.currentTimeMillis())
                        .definition(definition)
                        .roleArn("arn:aws:iam::000000000000:role/service-role/test-role"));
                smArn = resp.stateMachineArn();
                ctx.check("SFN CreateStateMachine", smArn != null);
            } catch (Exception e) {
                ctx.check("SFN CreateStateMachine", false, e);
                return;
            }

            // 2. DescribeStateMachine
            try {
                DescribeStateMachineResponse resp = sfn.describeStateMachine(b -> b.stateMachineArn(smArn));
                ctx.check("SFN DescribeStateMachine", resp.definition().equals(definition));
            } catch (Exception e) { ctx.check("SFN DescribeStateMachine", false, e); }

            // 3. ListStateMachines
            try {
                ListStateMachinesResponse resp = sfn.listStateMachines();
                boolean found = resp.stateMachines().stream().anyMatch(sm -> sm.stateMachineArn().equals(smArn));
                ctx.check("SFN ListStateMachines", found);
            } catch (Exception e) { ctx.check("SFN ListStateMachines", false, e); }

            // 4. StartExecution
            String execArn;
            String input = "{\"key\":\"value\"}";
            try {
                StartExecutionResponse resp = sfn.startExecution(b -> b
                        .stateMachineArn(smArn)
                        .input(input));
                execArn = resp.executionArn();
                ctx.check("SFN StartExecution", execArn != null);
            } catch (Exception e) {
                ctx.check("SFN StartExecution", false, e);
                execArn = null;
            }

            // 5. DescribeExecution
            if (execArn != null) {
                try {
                    final String fExecArn = execArn;
                    DescribeExecutionResponse resp = sfn.describeExecution(b -> b.executionArn(fExecArn));
                    ctx.check("SFN DescribeExecution", resp.status() == ExecutionStatus.SUCCEEDED);
                } catch (Exception e) { ctx.check("SFN DescribeExecution", false, e); }

                // 6. GetExecutionHistory
                try {
                    final String fExecArn = execArn;
                    GetExecutionHistoryResponse resp = sfn.getExecutionHistory(b -> b.executionArn(fExecArn));
                    ctx.check("SFN GetExecutionHistory", !resp.events().isEmpty());
                } catch (Exception e) { ctx.check("SFN GetExecutionHistory", false, e); }
            }

            // 7. DeleteStateMachine
            try {
                sfn.deleteStateMachine(b -> b.stateMachineArn(smArn));
                ctx.check("SFN DeleteStateMachine", true);
            } catch (Exception e) { ctx.check("SFN DeleteStateMachine", false, e); }

        } catch (Exception e) {
            ctx.check("SFN Client", false, e);
        }
    }
}
