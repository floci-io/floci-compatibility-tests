package com.floci.test.tests;

import com.floci.test.FlociTestGroup;
import com.floci.test.TestContext;
import com.floci.test.TestGroup;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.util.Map;

@FlociTestGroup
public class DynamoDbStreamsTests implements TestGroup {

    private static final String SHARD_ID = "shardId-0000000001-00000000001";

    @Override
    public String name() { return "dynamodb-streams"; }

    @Override
    public void run(TestContext ctx) {
        System.out.println("--- DynamoDB Streams Tests ---");

        try (DynamoDbClient ddb = DynamoDbClient.builder()
                .endpointOverride(ctx.endpoint)
                .region(ctx.region)
                .credentialsProvider(ctx.credentials)
                .build();
             DynamoDbStreamsClient streams = DynamoDbStreamsClient.builder()
                     .endpointOverride(ctx.endpoint)
                     .region(ctx.region)
                     .credentialsProvider(ctx.credentials)
                     .build()) {

            runStreamTests(ctx, ddb, streams);
            runKeysOnlyStreamTest(ctx, ddb, streams);
            runDisableStreamTest(ctx, ddb, streams);
        }
    }

    private void runStreamTests(TestContext ctx, DynamoDbClient ddb, DynamoDbStreamsClient streams) {
        String tableName = "streams-test-table";
        deleteSilently(ddb, tableName);

        String streamArn;
        String shardIterator;

        // Test 1: CreateTable with StreamEnabled=true, ViewType=NEW_AND_OLD_IMAGES
        try {
            CreateTableResponse createResp = ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build()
                    )
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("pk")
                                    .attributeType(ScalarAttributeType.S).build()
                    )
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits(5L).writeCapacityUnits(5L).build())
                    .streamSpecification(StreamSpecification.builder()
                            .streamEnabled(true)
                            .streamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
                            .build())
                    .build());

            streamArn = createResp.tableDescription().latestStreamArn();
            ctx.check("CreateTable with stream enabled returns LatestStreamArn",
                    streamArn != null && !streamArn.isEmpty());
        } catch (Exception e) {
            ctx.check("CreateTable with stream enabled returns LatestStreamArn", false, e);
            deleteSilently(ddb, tableName);
            return;
        }

        // Test 2: ListStreams with TableName filter
        try {
            ListStreamsResponse listResp = streams.listStreams(ListStreamsRequest.builder()
                    .tableName(tableName)
                    .build());
            ctx.check("ListStreams with TableName filter returns 1 stream",
                    listResp.streams().size() == 1
                            && listResp.streams().get(0).tableName().equals(tableName));
        } catch (Exception e) {
            ctx.check("ListStreams with TableName filter returns 1 stream", false, e);
        }

        // Test 3: DescribeStream
        try {
            DescribeStreamResponse descResp = streams.describeStream(DescribeStreamRequest.builder()
                    .streamArn(streamArn)
                    .build());
            StreamDescription sd = descResp.streamDescription();
            ctx.check("DescribeStream returns ENABLED status with 1 shard",
                    "ENABLED".equals(sd.streamStatusAsString())
                            && sd.shards().size() == 1
                            && tableName.equals(sd.tableName())
                            && "NEW_AND_OLD_IMAGES".equals(sd.streamViewTypeAsString()));
        } catch (Exception e) {
            ctx.check("DescribeStream returns ENABLED status with 1 shard", false, e);
        }

        // Test 4: GetShardIterator TRIM_HORIZON
        try {
            GetShardIteratorResponse iterResp = streams.getShardIterator(GetShardIteratorRequest.builder()
                    .streamArn(streamArn)
                    .shardId(SHARD_ID)
                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                    .build());
            shardIterator = iterResp.shardIterator();
            ctx.check("GetShardIterator TRIM_HORIZON returns non-null iterator",
                    shardIterator != null && !shardIterator.isEmpty());
        } catch (Exception e) {
            ctx.check("GetShardIterator TRIM_HORIZON returns non-null iterator", false, e);
            deleteSilently(ddb, tableName);
            return;
        }

        // Test 5: GetRecords on empty stream
        try {
            GetRecordsResponse getResp = streams.getRecords(GetRecordsRequest.builder()
                    .shardIterator(shardIterator)
                    .build());
            ctx.check("GetRecords on empty stream returns empty list with NextShardIterator",
                    getResp.records().isEmpty() && getResp.nextShardIterator() != null);
            shardIterator = getResp.nextShardIterator();
        } catch (Exception e) {
            ctx.check("GetRecords on empty stream returns empty list with NextShardIterator", false, e);
            deleteSilently(ddb, tableName);
            return;
        }

        // Test 6: PutItem (new) → INSERT with NewImage; OldImage absent
        try {
            ddb.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(Map.of("pk", AttributeValue.fromS("item-1"),
                            "data", AttributeValue.fromS("hello")))
                    .build());

            Thread.sleep(100); // Give time for stream capture
            GetRecordsResponse getResp = streams.getRecords(GetRecordsRequest.builder()
                    .shardIterator(shardIterator)
                    .build());
            shardIterator = getResp.nextShardIterator();

            if (getResp.records().isEmpty()) {
                ctx.check("PutItem (new) produces INSERT with NewImage, no OldImage", false);
            } else {
                software.amazon.awssdk.services.dynamodb.model.Record r = getResp.records().get(0);
                boolean hasInsert = "INSERT".equals(r.eventNameAsString())
                        && r.dynamodb().hasNewImage() && !r.dynamodb().newImage().isEmpty()
                        && r.dynamodb().newImage().containsKey("pk")
                        && !r.dynamodb().hasOldImage();
                ctx.check("PutItem (new) produces INSERT with NewImage, no OldImage", hasInsert);
            }
        } catch (Exception e) {
            ctx.check("PutItem (new) produces INSERT with NewImage, no OldImage", false, e);
        }

        // Test 7: PutItem (overwrite) → MODIFY with both NewImage and OldImage
        try {
            ddb.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(Map.of("pk", AttributeValue.fromS("item-1"),
                            "data", AttributeValue.fromS("updated")))
                    .build());

            Thread.sleep(100);
            GetRecordsResponse getResp = streams.getRecords(GetRecordsRequest.builder()
                    .shardIterator(shardIterator)
                    .build());
            shardIterator = getResp.nextShardIterator();

            if (getResp.records().isEmpty()) {
                ctx.check("PutItem (overwrite) produces MODIFY with both NewImage and OldImage", false);
            } else {
                software.amazon.awssdk.services.dynamodb.model.Record r = getResp.records().get(0);
                boolean hasModify = "MODIFY".equals(r.eventNameAsString())
                        && r.dynamodb().hasNewImage()
                        && r.dynamodb().hasOldImage();
                ctx.check("PutItem (overwrite) produces MODIFY with both NewImage and OldImage", hasModify);
            }
        } catch (Exception e) {
            ctx.check("PutItem (overwrite) produces MODIFY with both NewImage and OldImage", false, e);
        }

        // Test 8: UpdateItem → MODIFY with NewImage and OldImage
        try {
            ddb.updateItem(UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of("pk", AttributeValue.fromS("item-1")))
                    .updateExpression("SET #d = :val")
                    .expressionAttributeNames(Map.of("#d", "data"))
                    .expressionAttributeValues(Map.of(":val", AttributeValue.fromS("updated-again")))
                    .build());

            Thread.sleep(100);
            GetRecordsResponse getResp = streams.getRecords(GetRecordsRequest.builder()
                    .shardIterator(shardIterator)
                    .build());
            shardIterator = getResp.nextShardIterator();

            if (getResp.records().isEmpty()) {
                ctx.check("UpdateItem produces MODIFY with NewImage and OldImage", false);
            } else {
                software.amazon.awssdk.services.dynamodb.model.Record r = getResp.records().get(0);
                boolean hasModify = "MODIFY".equals(r.eventNameAsString())
                        && r.dynamodb().hasNewImage()
                        && r.dynamodb().hasOldImage();
                ctx.check("UpdateItem produces MODIFY with NewImage and OldImage", hasModify);
            }
        } catch (Exception e) {
            ctx.check("UpdateItem produces MODIFY with NewImage and OldImage", false, e);
        }

        // Test 9: DeleteItem → REMOVE with OldImage; NewImage absent
        try {
            ddb.deleteItem(DeleteItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of("pk", AttributeValue.fromS("item-1")))
                    .build());

            Thread.sleep(100);
            GetRecordsResponse getResp = streams.getRecords(GetRecordsRequest.builder()
                    .shardIterator(shardIterator)
                    .build());
            shardIterator = getResp.nextShardIterator();

            if (getResp.records().isEmpty()) {
                ctx.check("DeleteItem produces REMOVE with OldImage, no NewImage", false);
            } else {
                software.amazon.awssdk.services.dynamodb.model.Record r = getResp.records().get(0);
                boolean hasRemove = "REMOVE".equals(r.eventNameAsString())
                        && r.dynamodb().hasOldImage() && !r.dynamodb().oldImage().isEmpty()
                        && !r.dynamodb().hasNewImage();
                ctx.check("DeleteItem produces REMOVE with OldImage, no NewImage", hasRemove);
            }
        } catch (Exception e) {
            ctx.check("DeleteItem produces REMOVE with OldImage, no NewImage", false, e);
        }

        // Test 10: GetShardIterator LATEST → GetRecords empty, then picks up new write
        try {
            GetShardIteratorResponse latestIterResp = streams.getShardIterator(GetShardIteratorRequest.builder()
                    .streamArn(streamArn)
                    .shardId(SHARD_ID)
                    .shardIteratorType(ShardIteratorType.LATEST)
                    .build());
            String latestIter = latestIterResp.shardIterator();

            GetRecordsResponse emptyResp = streams.getRecords(GetRecordsRequest.builder()
                    .shardIterator(latestIter)
                    .build());
            boolean wasEmpty = emptyResp.records().isEmpty();

            ddb.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(Map.of("pk", AttributeValue.fromS("item-latest"),
                            "val", AttributeValue.fromS("new")))
                    .build());

            Thread.sleep(100);
            GetRecordsResponse newResp = streams.getRecords(GetRecordsRequest.builder()
                    .shardIterator(emptyResp.nextShardIterator())
                    .build());

            ctx.check("GetShardIterator LATEST: empty then picks up new write",
                    wasEmpty && !newResp.records().isEmpty());
        } catch (Exception e) {
            ctx.check("GetShardIterator LATEST: empty then picks up new write", false, e);
        }

        deleteSilently(ddb, tableName);
    }

    private void runKeysOnlyStreamTest(TestContext ctx, DynamoDbClient ddb, DynamoDbStreamsClient streams) {
        String tableName = "streams-keys-only-table";
        deleteSilently(ddb, tableName);

        // Test 11: CreateTable with StreamViewType=KEYS_ONLY
        try {
            CreateTableResponse createResp = ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build()
                    )
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("pk")
                                    .attributeType(ScalarAttributeType.S).build()
                    )
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits(5L).writeCapacityUnits(5L).build())
                    .streamSpecification(StreamSpecification.builder()
                            .streamEnabled(true)
                            .streamViewType(StreamViewType.KEYS_ONLY)
                            .build())
                    .build());

            String streamArn = createResp.tableDescription().latestStreamArn();

            GetShardIteratorResponse iterResp = streams.getShardIterator(GetShardIteratorRequest.builder()
                    .streamArn(streamArn)
                    .shardId(SHARD_ID)
                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                    .build());

            ddb.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(Map.of("pk", AttributeValue.fromS("key-item"),
                            "secret", AttributeValue.fromS("hidden")))
                    .build());

            Thread.sleep(100);
            GetRecordsResponse getResp = streams.getRecords(GetRecordsRequest.builder()
                    .shardIterator(iterResp.shardIterator())
                    .build());

            if (getResp.records().isEmpty()) {
                ctx.check("KEYS_ONLY stream: records have Keys but no NewImage/OldImage", false);
            } else {
                software.amazon.awssdk.services.dynamodb.model.Record r = getResp.records().get(0);
                boolean keysOnly = r.dynamodb().keys() != null && !r.dynamodb().keys().isEmpty()
                        && r.dynamodb().keys().containsKey("pk")
                        && !r.dynamodb().hasNewImage()
                        && !r.dynamodb().hasOldImage();
                ctx.check("KEYS_ONLY stream: records have Keys but no NewImage/OldImage", keysOnly);
            }
        } catch (Exception e) {
            ctx.check("KEYS_ONLY stream: records have Keys but no NewImage/OldImage", false, e);
        } finally {
            deleteSilently(ddb, tableName);
        }
    }

    private void runDisableStreamTest(TestContext ctx, DynamoDbClient ddb, DynamoDbStreamsClient streams) {
        String tableName = "streams-disable-table";
        deleteSilently(ddb, tableName);

        // Test 12: UpdateTable to disable stream → DescribeStream returns DISABLED
        try {
            CreateTableResponse createResp = ddb.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build()
                    )
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("pk")
                                    .attributeType(ScalarAttributeType.S).build()
                    )
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits(5L).writeCapacityUnits(5L).build())
                    .streamSpecification(StreamSpecification.builder()
                            .streamEnabled(true)
                            .streamViewType(StreamViewType.NEW_IMAGE)
                            .build())
                    .build());

            String streamArn = createResp.tableDescription().latestStreamArn();

            ddb.updateTable(UpdateTableRequest.builder()
                    .tableName(tableName)
                    .streamSpecification(StreamSpecification.builder()
                            .streamEnabled(false)
                            .build())
                    .build());

            DescribeStreamResponse descResp = streams.describeStream(DescribeStreamRequest.builder()
                    .streamArn(streamArn)
                    .build());

            ctx.check("UpdateTable disables stream → DescribeStream returns DISABLED",
                    "DISABLED".equals(descResp.streamDescription().streamStatusAsString()));
        } catch (Exception e) {
            ctx.check("UpdateTable disables stream → DescribeStream returns DISABLED", false, e);
        } finally {
            deleteSilently(ddb, tableName);
        }
    }

    private void deleteSilently(DynamoDbClient ddb, String tableName) {
        try {
            ddb.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
        } catch (Exception ignored) {
        }
    }
}
