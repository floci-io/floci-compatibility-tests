package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;

import java.time.Instant;
import java.util.List;

@FlociTestGroup
public class CloudWatchMetricsTests implements TestGroup {

    private static final String NAMESPACE_A = "TestApp/CloudWatchMetricsTests/" + System.currentTimeMillis();
    private static final String NAMESPACE_B = "TestApp/OtherNamespace";

    @Override
    public String name() { return "cloudwatch-metrics"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- CloudWatch Metrics Tests ---");

        try (CloudWatchClient cw = CloudWatchClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            testPutMetricData(ctx, cw);
            testPutMetricDataWithDimensions(ctx, cw);
            testListMetrics(ctx, cw);
            testListMetricsNamespaceFilter(ctx, cw);
            testGetMetricStatistics(ctx, cw);
            testGetMetricStatisticsWithDimensions(ctx, cw);
            testAlarms(ctx, cw);
        }
    }

    private void testAlarms(TestContext ctx, CloudWatchClient cw) {
        String alarmName = "sdk-test-alarm-" + System.currentTimeMillis();
        try {
            // 1. PutMetricAlarm
            cw.putMetricAlarm(b -> b
                    .alarmName(alarmName)
                    .metricName("CPUUtilization")
                    .namespace("AWS/EC2")
                    .statistic(Statistic.AVERAGE)
                    .period(60)
                    .threshold(80.0)
                    .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                    .evaluationPeriods(1)
                    .alarmActions("arn:aws:sns:us-east-1:000000000000:my-topic")
            );
            ctx.check("CWM PutMetricAlarm", true);

            // 2. DescribeAlarms
            DescribeAlarmsResponse desc = cw.describeAlarms(b -> b.alarmNames(alarmName));
            boolean found = desc.metricAlarms().stream().anyMatch(a -> a.alarmName().equals(alarmName));
            ctx.check("CWM DescribeAlarms found", found);
            if (found) {
                MetricAlarm alarm = desc.metricAlarms().stream().filter(a -> a.alarmName().equals(alarmName)).findFirst().get();
                ctx.check("CWM Alarm state initialized", alarm.stateValue() == StateValue.INSUFFICIENT_DATA);
            }

            // 3. SetAlarmState
            cw.setAlarmState(b -> b
                    .alarmName(alarmName)
                    .stateValue(StateValue.ALARM)
                    .stateReason("Threshold breached")
            );
            DescribeAlarmsResponse desc2 = cw.describeAlarms(b -> b.alarmNames(alarmName));
            ctx.check("CWM SetAlarmState verified",
                    desc2.metricAlarms().get(0).stateValue() == StateValue.ALARM);

            // 4. DeleteAlarms
            cw.deleteAlarms(b -> b.alarmNames(alarmName));
            DescribeAlarmsResponse desc3 = cw.describeAlarms(b -> b.alarmNames(alarmName));
            ctx.check("CWM DeleteAlarms verified", desc3.metricAlarms().isEmpty());

        } catch (Exception e) {
            ctx.check("CWM Alarms tests", false, e);
        }
    }

    private void testPutMetricData(TestContext ctx, CloudWatchClient cw) {
        try {
            cw.putMetricData(b -> b
                    .namespace(NAMESPACE_A)
                    .metricData(MetricDatum.builder()
                            .metricName("RequestCount")
                            .value(42.0)
                            .unit(StandardUnit.COUNT)
                            .timestamp(Instant.now())
                            .build()));
            ctx.check("CWM PutMetricData no exception", true);
        } catch (Exception e) {
            ctx.check("CWM PutMetricData", false, e);
        }
    }

    private void testPutMetricDataWithDimensions(TestContext ctx, CloudWatchClient cw) {
        try {
            cw.putMetricData(b -> b
                    .namespace(NAMESPACE_A)
                    .metricData(MetricDatum.builder()
                            .metricName("Latency")
                            .value(125.5)
                            .unit(StandardUnit.MILLISECONDS)
                            .timestamp(Instant.now())
                            .dimensions(Dimension.builder().name("Host").value("web01").build())
                            .build()));
            ctx.check("CWM PutMetricData with dimensions no exception", true);
        } catch (Exception e) {
            ctx.check("CWM PutMetricData with dimensions", false, e);
        }
    }

    private void testListMetrics(TestContext ctx, CloudWatchClient cw) {
        try {
            ListMetricsResponse resp = cw.listMetrics(b -> b.namespace(NAMESPACE_A));
            boolean hasRequestCount = resp.metrics().stream()
                    .anyMatch(m -> "RequestCount".equals(m.metricName()) && NAMESPACE_A.equals(m.namespace()));
            boolean hasLatency = resp.metrics().stream()
                    .anyMatch(m -> "Latency".equals(m.metricName()) && NAMESPACE_A.equals(m.namespace()));
            ctx.check("CWM ListMetrics contains RequestCount and Latency", hasRequestCount && hasLatency);
        } catch (Exception e) {
            ctx.check("CWM ListMetrics", false, e);
        }
    }

    private void testListMetricsNamespaceFilter(TestContext ctx, CloudWatchClient cw) {
        try {
            // Put something in namespace B
            cw.putMetricData(b -> b
                    .namespace(NAMESPACE_B)
                    .metricData(MetricDatum.builder()
                            .metricName("OtherMetric")
                            .value(1.0)
                            .unit(StandardUnit.COUNT)
                            .timestamp(Instant.now())
                            .build()));

            ListMetricsResponse respA = cw.listMetrics(b -> b.namespace(NAMESPACE_A));
            ListMetricsResponse respB = cw.listMetrics(b -> b.namespace(NAMESPACE_B));

            boolean noAinB = respB.metrics().stream().noneMatch(m -> NAMESPACE_A.equals(m.namespace()));
            boolean noBinA = respA.metrics().stream().noneMatch(m -> NAMESPACE_B.equals(m.namespace()));
            ctx.check("CWM ListMetrics namespace isolation", noAinB && noBinA);
        } catch (Exception e) {
            ctx.check("CWM ListMetrics namespace filter", false, e);
        }
    }

    private void testGetMetricStatistics(TestContext ctx, CloudWatchClient cw) {
        try {
            Instant now = Instant.now();
            // Put 5 data points with known values
            cw.putMetricData(b -> b
                    .namespace(NAMESPACE_A)
                    .metricData(
                            metricDatum("CPUUtil", 10.0, now.minusSeconds(250)),
                            metricDatum("CPUUtil", 20.0, now.minusSeconds(200)),
                            metricDatum("CPUUtil", 30.0, now.minusSeconds(150)),
                            metricDatum("CPUUtil", 40.0, now.minusSeconds(100)),
                            metricDatum("CPUUtil", 50.0, now.minusSeconds(50))
                    ));

            GetMetricStatisticsResponse stats = cw.getMetricStatistics(b -> b
                    .namespace(NAMESPACE_A)
                    .metricName("CPUUtil")
                    .startTime(now.minusSeconds(3600))
                    .endTime(now.plusSeconds(60))
                    .period(3600)
                    .statistics(Statistic.SUM, Statistic.AVERAGE, Statistic.SAMPLE_COUNT));

            ctx.check("CWM GetMetricStatistics has datapoints", !stats.datapoints().isEmpty());

            double totalSum = stats.datapoints().stream().mapToDouble(Datapoint::sum).sum();
            double totalSc = stats.datapoints().stream().mapToDouble(Datapoint::sampleCount).sum();
            ctx.check("CWM GetMetricStatistics sum=150", Math.abs(totalSum - 150.0) < 0.001);
            ctx.check("CWM GetMetricStatistics sampleCount=5", Math.abs(totalSc - 5.0) < 0.001);
        } catch (Exception e) {
            ctx.check("CWM GetMetricStatistics", false, e);
        }
    }

    private void testGetMetricStatisticsWithDimensions(TestContext ctx, CloudWatchClient cw) {
        try {
            Instant now = Instant.now();
            String metric = "DimMetric";
            // Put data for two different hosts
            cw.putMetricData(b -> b
                    .namespace(NAMESPACE_A)
                    .metricData(
                            MetricDatum.builder().metricName(metric).value(100.0)
                                    .unit(StandardUnit.COUNT)
                                    .dimensions(Dimension.builder().name("Host").value("web01").build())
                                    .timestamp(now.minusSeconds(60)).build(),
                            MetricDatum.builder().metricName(metric).value(999.0)
                                    .unit(StandardUnit.COUNT)
                                    .dimensions(Dimension.builder().name("Host").value("web02").build())
                                    .timestamp(now.minusSeconds(60)).build()
                    ));

            GetMetricStatisticsResponse statsWeb01 = cw.getMetricStatistics(b -> b
                    .namespace(NAMESPACE_A)
                    .metricName(metric)
                    .dimensions(Dimension.builder().name("Host").value("web01").build())
                    .startTime(now.minusSeconds(3600))
                    .endTime(now.plusSeconds(60))
                    .period(3600)
                    .statistics(Statistic.SUM));

            double sumWeb01 = statsWeb01.datapoints().stream().mapToDouble(Datapoint::sum).sum();
            ctx.check("CWM GetMetricStatistics dimension filter web01 sum=100",
                    Math.abs(sumWeb01 - 100.0) < 0.001);

            GetMetricStatisticsResponse statsWeb02 = cw.getMetricStatistics(b -> b
                    .namespace(NAMESPACE_A)
                    .metricName(metric)
                    .dimensions(Dimension.builder().name("Host").value("web02").build())
                    .startTime(now.minusSeconds(3600))
                    .endTime(now.plusSeconds(60))
                    .period(3600)
                    .statistics(Statistic.SUM));

            double sumWeb02 = statsWeb02.datapoints().stream().mapToDouble(Datapoint::sum).sum();
            ctx.check("CWM GetMetricStatistics dimension filter web02 sum=999",
                    Math.abs(sumWeb02 - 999.0) < 0.001);
        } catch (Exception e) {
            ctx.check("CWM GetMetricStatisticsWithDimensions", false, e);
        }
    }

    private static MetricDatum metricDatum(String name, double value, Instant ts) {
        return MetricDatum.builder()
                .metricName(name)
                .value(value)
                .unit(StandardUnit.PERCENT)
                .timestamp(ts)
                .build();
    }
}
