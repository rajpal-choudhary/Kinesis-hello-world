package kinesis.consumers;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.*;
import software.amazon.awssdk.regions.Region;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class HelloConsumer3 {
    private static final String STREAM_NAME = "test-stream";

    private static ClientConfiguration getKinesisConsumer() {
        ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(1);
        config.setRequestTimeout(60000);
        return config;
    }

    public static void main(String[] args) {
        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        clientBuilder.setCredentials(new DefaultAWSCredentialsProviderChain());
        clientBuilder.setRegion(Region.AP_SOUTH_1.toString());
        clientBuilder.setClientConfiguration(getKinesisConsumer());
        AmazonKinesis kinesisClient = clientBuilder.build();

        // Retrieve the Shards from a Stream
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(STREAM_NAME);
        List<Shard> shards = new ArrayList<>();

        DescribeStreamResult streamRes;
        do {
            streamRes = kinesisClient.describeStream(describeStreamRequest);
            shards.addAll(streamRes.getStreamDescription().getShards());
            /*String lastShardId = null;
            if (shards.size() > 0) {
                lastShardId = shards.get(shards.size() - 1).getShardId();
            }*/
        } while (streamRes.getStreamDescription().isHasMoreShards());

        GetShardIteratorRequest itReq = new GetShardIteratorRequest()
                .withStreamName(STREAM_NAME)
                .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .withShardId(shards.get(0).getShardId());

        String shardIterator;
        GetShardIteratorResult shardIteratorResult = kinesisClient.getShardIterator(itReq);
        shardIterator = shardIteratorResult.getShardIterator();

        // Continuously read data records from shard.
        List<Record> records;

        // Create new GetRecordsRequest with existing shardIterator.
        // Set maximum records to return to 1000.
        GetRecordsRequest recordsRequest = new GetRecordsRequest();
        recordsRequest.setShardIterator(shardIterator);

        GetRecordsResult result = kinesisClient.getRecords(recordsRequest);

        // Put result into record list. Result may be empty.
        records = result.getRecords();

        // Print records
        for (Record record : records) {
            ByteBuffer byteBuffer = record.getData();
            System.out.println(String.format("Seq No: %s - %s", record.getSequenceNumber(),
                    new String(byteBuffer.array())));
        }
    }
}