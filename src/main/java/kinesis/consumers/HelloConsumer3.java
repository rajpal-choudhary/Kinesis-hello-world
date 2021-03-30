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

        //DescribeStreamResult streamRes;
        for (Shard shard : kinesisClient.describeStream(STREAM_NAME).getStreamDescription().getShards()) {
            GetShardIteratorRequest itReq = new GetShardIteratorRequest()
                    .withStreamName(STREAM_NAME)
                    .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)
                    .withShardId(shard.getShardId());

            GetShardIteratorResult shardIteratorResult = kinesisClient.getShardIterator(itReq);
            String shardIterator = shardIteratorResult.getShardIterator();

            GetRecordsRequest recordsRequest = new GetRecordsRequest();
            recordsRequest.setShardIterator(shardIterator);

            for (Record record : kinesisClient.getRecords(recordsRequest).getRecords()) {
                ByteBuffer byteBuffer = record.getData();
                System.out.println(String.format("Seq No: %s - %s", record.getSequenceNumber(),
                        new String(byteBuffer.array())));
            }
        }
    }
}