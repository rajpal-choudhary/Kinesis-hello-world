package kinesis.handlers;

import org.json.simple.JSONObject;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Random;

public class DataGenerator {
    private final String[] TICKERS = {"AAPL", "AMZN", "MSFT", "INTC", "TBV"};

    public ByteBuffer generateData() {
        int index = new Random().nextInt(TICKERS.length);

        JSONObject record = new JSONObject();
        record.put("EVENT_TIME", Instant.now().toString());
        record.put("TICKER", TICKERS[index]);
        record.put("PRICE", new Random().nextDouble() * 100);
        String str = record.toString();

        System.out.println(str);

        byte[] sendData = str.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(sendData);
    }
}
