package com.dataspec;

import com.dataspec.connection.ConnectionHandle;
import com.dataspec.exception.ExpectationNeverMetException;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Predicate;

import static java.time.Instant.now;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DatabaseUtil {

    private ConnectionHandle handle;
    private Duration timeout = Duration.ofSeconds(5);
    private Duration interval = Duration.ofMillis(500);

    public DatabaseUtil(ConnectionHandle handle) {
        this.handle = handle;
    }

    public void executeStatement(String query) {
        handle.execute(query);
    }

    public <T> Iterable<T> runQuery(String query) {
        return handle.execute(query);
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    public <T> void waitForExpectedValue(String query, Predicate<Iterable<T>> predicate) {
        boolean success;
        Instant endTime = now().plus(timeout);
        do {
            success = pollForValue(query, predicate);
        }
        while (notYet(endTime) && !success);

        if (!success) throw new ExpectationNeverMetException("aw shit");
    }

    private <T> boolean pollForValue(String query, Predicate<Iterable<T>> predicate) {
        boolean success = checkForValue(query, predicate);
        if (!success) waitFor(interval);
        return success;
    }

    private <T> boolean checkForValue(String queryString, Predicate<Iterable<T>> predicate) {
        return predicate.test(runQuery(queryString));
    }

    private boolean notYet(Instant endTime) {
        return now().isBefore(endTime);
    }

    private void waitFor(Duration duration) {
        try {
            MILLISECONDS.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
