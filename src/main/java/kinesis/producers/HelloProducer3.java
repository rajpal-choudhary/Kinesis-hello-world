package kinesis.producers;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import kinesis.handlers.DataGenerator;
import software.amazon.awssdk.regions.Region;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class HelloProducer3 {
    private static final String STREAM_NAME="test-stream";

    private static ClientConfiguration getKinesisProducer() {
        ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(1);
        config.setRequestTimeout(60000);
        return config;
    }

    public static void main(String[] args) {
        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        clientBuilder.setCredentials(new DefaultAWSCredentialsProviderChain());
        clientBuilder.setRegion(Region.AP_SOUTHEAST_1.toString());
        clientBuilder.setClientConfiguration(getKinesisProducer());
        AmazonKinesis kinesisClient = clientBuilder.build();

        PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
        putRecordsRequest.setStreamName(STREAM_NAME);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>();
        DataGenerator dg = new DataGenerator();
        for (int i = 0; i < 500; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(dg.generateData());
            putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        PutRecordsResult putRecordsResult  = kinesisClient.putRecords(putRecordsRequest);
        System.out.println("Put Result" + putRecordsResult);
    }
}
