package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.*;

import java.util.List;
import java.util.Map;

@FlociTestGroup
public class CloudWatchLogsTests implements TestGroup {

    private static final String GROUP_NAME = "/test/cloudwatch-logs-group";
    private static final String STREAM_NAME = "test-stream-01";

    @Override
    public String name() { return "cloudwatch-logs"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- CloudWatch Logs Tests ---");

        try (CloudWatchLogsClient cw = CloudWatchLogsClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            testCreateLogGroup(ctx, cw);
            testRetentionPolicy(ctx, cw);
            testTagging(ctx, cw);
            testCreateLogStream(ctx, cw);
            testPutAndGetLogEvents(ctx, cw);
            testGetLogEventsTimeFilter(ctx, cw);
            testFilterLogEvents(ctx, cw);
            testDeleteLogStream(ctx, cw);
            testDeleteLogGroup(ctx, cw);
        }
    }

    private void testCreateLogGroup(TestContext ctx, CloudWatchLogsClient cw) {
        try {
            cw.createLogGroup(b -> b.logGroupName(GROUP_NAME));
            List<LogGroup> groups = cw.describeLogGroups(b -> b.logGroupNamePrefix(GROUP_NAME))
                    .logGroups();
            ctx.check("CWL CreateLogGroup", groups.stream().anyMatch(g -> GROUP_NAME.equals(g.logGroupName())));
        } catch (Exception e) {
            ctx.check("CWL CreateLogGroup", false, e);
        }
    }

    private void testRetentionPolicy(TestContext ctx, CloudWatchLogsClient cw) {
        try {
            cw.putRetentionPolicy(b -> b.logGroupName(GROUP_NAME).retentionInDays(14));
            List<LogGroup> groups = cw.describeLogGroups(b -> b.logGroupNamePrefix(GROUP_NAME)).logGroups();
            boolean found = groups.stream()
                    .anyMatch(g -> GROUP_NAME.equals(g.logGroupName()) && Integer.valueOf(14).equals(g.retentionInDays()));
            ctx.check("CWL PutRetentionPolicy retentionInDays=14", found);
        } catch (Exception e) {
            ctx.check("CWL PutRetentionPolicy", false, e);
        }
    }

    private void testTagging(TestContext ctx, CloudWatchLogsClient cw) {
        try {
            cw.tagLogGroup(b -> b.logGroupName(GROUP_NAME).tags(Map.of("env", "test", "team", "platform")));
            Map<String, String> tags = cw.listTagsLogGroup(b -> b.logGroupName(GROUP_NAME)).tags();
            ctx.check("CWL TagLogGroup", "test".equals(tags.get("env")) && "platform".equals(tags.get("team")));

            cw.untagLogGroup(b -> b.logGroupName(GROUP_NAME).tags(List.of("team")));
            Map<String, String> tagsAfter = cw.listTagsLogGroup(b -> b.logGroupName(GROUP_NAME)).tags();
            ctx.check("CWL UntagLogGroup", !tagsAfter.containsKey("team") && tagsAfter.containsKey("env"));
        } catch (Exception e) {
            ctx.check("CWL Tagging", false, e);
        }
    }

    private void testCreateLogStream(TestContext ctx, CloudWatchLogsClient cw) {
        try {
            cw.createLogStream(b -> b.logGroupName(GROUP_NAME).logStreamName(STREAM_NAME));
            List<LogStream> streams = cw.describeLogStreams(b -> b.logGroupName(GROUP_NAME)).logStreams();
            ctx.check("CWL CreateLogStream", streams.stream().anyMatch(s -> STREAM_NAME.equals(s.logStreamName())));
        } catch (Exception e) {
            ctx.check("CWL CreateLogStream", false, e);
        }
    }

    private void testPutAndGetLogEvents(TestContext ctx, CloudWatchLogsClient cw) {
        try {
            long now = System.currentTimeMillis();
            PutLogEventsResponse putResp = cw.putLogEvents(b -> b
                    .logGroupName(GROUP_NAME)
                    .logStreamName(STREAM_NAME)
                    .logEvents(
                            InputLogEvent.builder().timestamp(now - 2000).message("msg-alpha").build(),
                            InputLogEvent.builder().timestamp(now - 1000).message("msg-beta").build(),
                            InputLogEvent.builder().timestamp(now).message("msg-gamma").build()
                    ));
            ctx.check("CWL PutLogEvents returns nextSequenceToken", putResp.nextSequenceToken() != null);

            GetLogEventsResponse getResp = cw.getLogEvents(b -> b
                    .logGroupName(GROUP_NAME)
                    .logStreamName(STREAM_NAME)
                    .startFromHead(true));
            ctx.check("CWL GetLogEvents count=3", getResp.events().size() == 3);
            ctx.check("CWL GetLogEvents first message",
                    getResp.events().get(0).message().equals("msg-alpha"));
        } catch (Exception e) {
            ctx.check("CWL PutAndGetLogEvents", false, e);
        }
    }

    private void testGetLogEventsTimeFilter(TestContext ctx, CloudWatchLogsClient cw) {
        try {
            String stream2 = "stream-time-filter";
            cw.createLogStream(b -> b.logGroupName(GROUP_NAME).logStreamName(stream2));

            long base = System.currentTimeMillis() - 10000;
            cw.putLogEvents(b -> b
                    .logGroupName(GROUP_NAME)
                    .logStreamName(stream2)
                    .logEvents(
                            InputLogEvent.builder().timestamp(base).message("early-event").build(),
                            InputLogEvent.builder().timestamp(base + 5000).message("mid-event").build(),
                            InputLogEvent.builder().timestamp(base + 9000).message("late-event").build()
                    ));

            GetLogEventsResponse filtered = cw.getLogEvents(b -> b
                    .logGroupName(GROUP_NAME)
                    .logStreamName(stream2)
                    .startTime(base + 3000)
                    .endTime(base + 7000)
                    .startFromHead(true));
            ctx.check("CWL GetLogEvents time filter", filtered.events().size() == 1
                    && "mid-event".equals(filtered.events().get(0).message()));
        } catch (Exception e) {
            ctx.check("CWL GetLogEventsTimeFilter", false, e);
        }
    }

    private void testFilterLogEvents(TestContext ctx, CloudWatchLogsClient cw) {
        try {
            String stream3 = "stream-filter-test";
            cw.createLogStream(b -> b.logGroupName(GROUP_NAME).logStreamName(stream3));

            long now = System.currentTimeMillis();
            cw.putLogEvents(b -> b
                    .logGroupName(GROUP_NAME)
                    .logStreamName(stream3)
                    .logEvents(
                            InputLogEvent.builder().timestamp(now - 2000).message("ERROR: database connection lost").build(),
                            InputLogEvent.builder().timestamp(now - 1000).message("INFO: request processed").build(),
                            InputLogEvent.builder().timestamp(now).message("ERROR: timeout occurred").build()
                    ));

            FilterLogEventsResponse resp = cw.filterLogEvents(b -> b
                    .logGroupName(GROUP_NAME)
                    .logStreamNames(stream3)
                    .filterPattern("ERROR"));
            ctx.check("CWL FilterLogEvents matches ERROR only",
                    resp.events().size() == 2
                    && resp.events().stream().allMatch(e -> e.message().contains("ERROR")));
        } catch (Exception e) {
            ctx.check("CWL FilterLogEvents", false, e);
        }
    }

    private void testDeleteLogStream(TestContext ctx, CloudWatchLogsClient cw) {
        try {
            cw.deleteLogStream(b -> b.logGroupName(GROUP_NAME).logStreamName(STREAM_NAME));
            List<LogStream> streams = cw.describeLogStreams(b -> b.logGroupName(GROUP_NAME)).logStreams();
            ctx.check("CWL DeleteLogStream", streams.stream().noneMatch(s -> STREAM_NAME.equals(s.logStreamName())));
        } catch (Exception e) {
            ctx.check("CWL DeleteLogStream", false, e);
        }
    }

    private void testDeleteLogGroup(TestContext ctx, CloudWatchLogsClient cw) {
        try {
            cw.deleteLogGroup(b -> b.logGroupName(GROUP_NAME));
            List<LogGroup> groups = cw.describeLogGroups(b -> b.logGroupNamePrefix(GROUP_NAME)).logGroups();
            ctx.check("CWL DeleteLogGroup", groups.stream().noneMatch(g -> GROUP_NAME.equals(g.logGroupName())));
        } catch (Exception e) {
            ctx.check("CWL DeleteLogGroup", false, e);
        }
    }
}
