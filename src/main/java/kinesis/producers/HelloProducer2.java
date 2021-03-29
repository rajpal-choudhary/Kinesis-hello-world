package kinesis.producers;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.producer.*;
import com.fasterxml.jackson.databind.util.JSONPObject;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class HelloProducer2 {

    private static final Logger log = LoggerFactory.getLogger(HelloProducer2.class);

    private static final String DEFAULT_REGION_NAME = "ap-south-1";
    private static final String STREAM_NAME = "test-stream";

    @SuppressWarnings("squid:S2245")
    private static final Random RANDOM = new Random();

    private static final String TIMESTAMP = Long.toString(System.currentTimeMillis());
    private static final int RECORDS_PER_SECOND = 100;
    private static final int SECONDS_TO_RUN_DEFAULT = 5;
    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);

    private static final String[] TICKERS = {"AAPL", "AMZN", "MSFT", "INTC", "TBV"};

    private static KinesisProducer getKinesisProducer(final String region) {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        config.setRegion(region);
        config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
        config.setMaxConnections(1);
        config.setRequestTimeout(60000);
        config.setRecordMaxBufferedTime(2000);
        config.setAggregationEnabled(false);

        return new KinesisProducer(config);
    }

    private static String getArgIfPresent(final String[] args, final int index, final String defaultValue) {
        return args.length > index ? args[index] : defaultValue;
    }

    public static void main(String[] args) throws Exception {
        final String streamName = getArgIfPresent(args, 0, STREAM_NAME);
        final String region = getArgIfPresent(args, 1, DEFAULT_REGION_NAME);
        final String secondsToRunString = getArgIfPresent(args, 2, String.valueOf(SECONDS_TO_RUN_DEFAULT));
        final int secondsToRun = Integer.parseInt(secondsToRunString);

        //PropertyConfigurator.configure("log4j.properties");

        if (secondsToRun <= 0) {
            log.error("Seconds to Run should be a positive integer");
            System.exit(1);
        }

        final KinesisProducer producer = getKinesisProducer(region);
        System.out.println(String.format("Stream name: %s; Region: %s", streamName, region));
        List<Future<UserRecordResult>> putFutures = new LinkedList();

        for (int i = 0; i < 1000; i++) {
            ByteBuffer data = generateData();
            putFutures.add(producer.addUserRecord(streamName, TIMESTAMP, data));
            Thread.sleep(1000);
        }

        // Wait for puts to finish and check the results
        for (Future<UserRecordResult> f : putFutures) {
            UserRecordResult result = f.get(); // this does block
            if (result.isSuccessful()) {
                System.out.println("Put record into shard " +
                        result.getShardId());
                System.out.println(result.toString());
            } else {
                for (Attempt attempt : result.getAttempts()) {
                    System.out.println(attempt.toString());
                }
            }
        }
    }

    public static ByteBuffer generateData() {
        int index = RANDOM.nextInt(TICKERS.length);

        JSONObject record = new JSONObject();
        record.put("EVENT_TIME", Instant.now().toString());
        record.put("TICKER", TICKERS[index]);
        record.put("PRICE", RANDOM.nextDouble() * 100);
        String str = record.toString();

        System.out.println(str);

        byte[] sendData = str.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(sendData);
    }
}
