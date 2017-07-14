import com.dataspec.connection.ConnectionHandle;
import exception.ExpectationNeverMetException;

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Predicate;

public class DatabaseUtil {

    private ConnectionHandle handle;
    private Duration timeout = Duration.ofSeconds(5);


    public DatabaseUtil(ConnectionHandle handle) {
        this.handle = handle;
    }


    public void execute(String query) {
        handle.execute(query);
    }

    public <T> Iterable<T> query(String query) {
        return handle.execute(query);
    }

    public <T> void waitForExpectedValue(String query, Predicate<Iterable<T>> predicate) {
        final Boolean[] timedOut = {false};
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                timedOut[0] = true;
            }
        }, timeout.toMillis());

        boolean success;
        Iterable<?> result;
        do {
            result = query(query);
            success = predicate.test((Iterable<T>) result);
        }
        while (!timedOut[0] && !success);

        if (timedOut[0]) {
            throw new ExpectationNeverMetException("aw shit");
        }
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }
}
