package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.nio.charset.StandardCharsets;
import java.util.List;

@FlociTestGroup
public class KinesisTests implements TestGroup {

    @Override
    public String name() { return "kinesis"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- Kinesis Tests ---");

        // Disable CBOR to use JSON 1.1 (which our emulator supports)
        System.setProperty("aws.cborEnabled", "false");

        try (KinesisClient kinesis = KinesisClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build()) {

            String streamName = "sdk-test-stream-" + System.currentTimeMillis();

            // 1. CreateStream
            try {
                kinesis.createStream(CreateStreamRequest.builder()
                        .streamName(streamName)
                        .shardCount(1)
                        .build());
                ctx.check("Kinesis CreateStream", true);
            } catch (Exception e) {
                ctx.check("Kinesis CreateStream", false, e);
                return;
            }

            // 2. ListStreams
            try {
                ListStreamsResponse resp = kinesis.listStreams();
                ctx.check("Kinesis ListStreams", resp.streamNames().contains(streamName));
            } catch (Exception e) {
                ctx.check("Kinesis ListStreams", false, e);
            }

            // 3. DescribeStream
            String shardId = null;
            try {
                DescribeStreamResponse resp = kinesis.describeStream(DescribeStreamRequest.builder()
                        .streamName(streamName)
                        .build());
                shardId = resp.streamDescription().shards().get(0).shardId();
                ctx.check("Kinesis DescribeStream", 
                        resp.streamDescription().streamName().equals(streamName) && shardId != null);
            } catch (Exception e) {
                ctx.check("Kinesis DescribeStream", false, e);
            }

            // 4. PutRecord
            String data = "hello kinesis";
            try {
                PutRecordResponse resp = kinesis.putRecord(PutRecordRequest.builder()
                        .streamName(streamName)
                        .data(SdkBytes.fromString(data, StandardCharsets.UTF_8))
                        .partitionKey("pk1")
                        .build());
                ctx.check("Kinesis PutRecord", resp.sequenceNumber() != null);
            } catch (Exception e) {
                ctx.check("Kinesis PutRecord", false, e);
            }

            // 5. GetRecords (TRIM_HORIZON)
            try {
                String iterator = kinesis.getShardIterator(GetShardIteratorRequest.builder()
                        .streamName(streamName)
                        .shardId(shardId)
                        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                        .build()).shardIterator();
                
                GetRecordsResponse recs = kinesis.getRecords(GetRecordsRequest.builder()
                        .shardIterator(iterator)
                        .build());
                
                boolean found = recs.records().stream()
                        .anyMatch(r -> data.equals(r.data().asUtf8String()));
                ctx.check("Kinesis GetRecords", found);
            } catch (Exception e) {
                ctx.check("Kinesis GetRecords", false, e);
            }

            // 6. PutRecords (Batch)
            try {
                PutRecordsResponse resp = kinesis.putRecords(PutRecordsRequest.builder()
                        .streamName(streamName)
                        .records(
                            PutRecordsRequestEntry.builder()
                                .data(SdkBytes.fromString("batch1", StandardCharsets.UTF_8))
                                .partitionKey("pk1")
                                .build(),
                            PutRecordsRequestEntry.builder()
                                .data(SdkBytes.fromString("batch2", StandardCharsets.UTF_8))
                                .partitionKey("pk2")
                                .build()
                        )
                        .build());
                ctx.check("Kinesis PutRecords batch", resp.failedRecordCount() == 0 && resp.records().size() == 2);
            } catch (Exception e) {
                ctx.check("Kinesis PutRecords batch", false, e);
            }

            // 7. DescribeStreamSummary
            try {
                DescribeStreamSummaryResponse resp = kinesis.describeStreamSummary(DescribeStreamSummaryRequest.builder()
                        .streamName(streamName)
                        .build());
                ctx.check("Kinesis DescribeStreamSummary", 
                        resp.streamDescriptionSummary().streamName().equals(streamName) && 
                        resp.streamDescriptionSummary().openShardCount() == 1);
            } catch (Exception e) {
                ctx.check("Kinesis DescribeStreamSummary", false, e);
            }

            // 8. Tagging
            try {
                kinesis.addTagsToStream(AddTagsToStreamRequest.builder()
                        .streamName(streamName)
                        .tags(java.util.Map.of("project", "floci"))
                        .build());
                ListTagsForStreamResponse tagsResp = kinesis.listTagsForStream(ListTagsForStreamRequest.builder()
                        .streamName(streamName)
                        .build());
                boolean found = tagsResp.tags().stream().anyMatch(t -> t.key().equals("project") && t.value().equals("floci"));
                ctx.check("Kinesis Tagging", found);
            } catch (Exception e) {
                ctx.check("Kinesis Tagging", false, e);
            }

            // 9. Encryption
            try {
                kinesis.startStreamEncryption(StartStreamEncryptionRequest.builder()
                        .streamName(streamName)
                        .encryptionType(EncryptionType.KMS)
                        .keyId("my-key")
                        .build());
                DescribeStreamSummaryResponse summary = kinesis.describeStreamSummary(DescribeStreamSummaryRequest.builder()
                        .streamName(streamName)
                        .build());
                ctx.check("Kinesis Encryption", summary.streamDescriptionSummary().encryptionType() == EncryptionType.KMS);
            } catch (Exception e) {
                ctx.check("Kinesis Encryption", false, e);
            }

            // 10. SplitShard
            try {
                kinesis.splitShard(SplitShardRequest.builder()
                        .streamName(streamName)
                        .shardToSplit(shardId)
                        .newStartingHashKey("170141183460469231731687303715884105728")
                        .build());
                DescribeStreamSummaryResponse summary = kinesis.describeStreamSummary(DescribeStreamSummaryRequest.builder()
                        .streamName(streamName)
                        .build());
                ctx.check("Kinesis SplitShard", summary.streamDescriptionSummary().openShardCount() == 2);
            } catch (Exception e) {
                ctx.check("Kinesis SplitShard", false, e);
            }

            // 11. Enhanced Fan-Out (EFO)
            try {
                String streamArn = kinesis.describeStream(DescribeStreamRequest.builder().streamName(streamName).build())
                        .streamDescription().streamARN();
                
                RegisterStreamConsumerResponse regResp = kinesis.registerStreamConsumer(RegisterStreamConsumerRequest.builder()
                        .streamARN(streamArn)
                        .consumerName("efo-consumer")
                        .build());
                ctx.check("Kinesis RegisterConsumer", regResp.consumer().consumerName().equals("efo-consumer"));

                ListStreamConsumersResponse listResp = kinesis.listStreamConsumers(ListStreamConsumersRequest.builder()
                        .streamARN(streamArn)
                        .build());
                ctx.check("Kinesis ListConsumers", listResp.consumers().size() == 1);

                DescribeStreamConsumerResponse descResp = kinesis.describeStreamConsumer(DescribeStreamConsumerRequest.builder()
                        .consumerARN(regResp.consumer().consumerARN())
                        .build());
                ctx.check("Kinesis DescribeConsumer", descResp.consumerDescription().consumerName().equals("efo-consumer"));

                kinesis.deregisterStreamConsumer(DeregisterStreamConsumerRequest.builder()
                        .consumerARN(regResp.consumer().consumerARN())
                        .build());
                ctx.check("Kinesis DeregisterConsumer", true);
            } catch (Exception e) {
                ctx.check("Kinesis EFO", false, e);
            }

            // Cleanup
            try {
                kinesis.deleteStream(DeleteStreamRequest.builder().streamName(streamName).build());
                ctx.check("Kinesis DeleteStream", true);
            } catch (Exception e) {
                ctx.check("Kinesis DeleteStream", false, e);
            }

        } catch (Exception e) {
            ctx.check("Kinesis Client", false, e);
        }
    }
}
