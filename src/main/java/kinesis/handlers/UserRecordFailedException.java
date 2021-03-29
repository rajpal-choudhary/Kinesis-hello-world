package kinesis.handlers;

import com.amazonaws.services.kinesis.producer.UserRecordResult;

public class UserRecordFailedException extends Exception {
    private static final long serialVersionUID = 3168271192277927600L;

    private UserRecordResult result;

    public UserRecordFailedException(UserRecordResult result) {
        this.result = result;
    }

    public UserRecordResult getResult() {
        return result;
    }
}
