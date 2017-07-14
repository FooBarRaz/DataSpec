package com.dataspec;

import com.dataspec.connection.ConnectionHandle;
import com.dataspec.exception.ExpectationNeverMetException;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.time.Instant.now;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    public <T> void waitForExpectedValue(String query, Predicate<Iterable<T>> predicate) {
        boolean success;
        Instant endTime = now().plus(timeout);
        do {
            success = checkForValue(query, predicate);
            if (!success) waitFor(500, MILLISECONDS);
        }
        while (notYet(endTime) && !success);

        if (!success) throw new ExpectationNeverMetException("aw shit");
    }

    private <T> boolean checkForValue(String query, Predicate<Iterable<T>> predicate) {
        Iterable<?> result;
        boolean success;
        result = query(query);
        success = predicate.test((Iterable<T>) result);
        return success;
    }

    private boolean notYet(Instant endTime) {
        return now().isBefore(endTime);
    }

    private void waitFor(int timeout, TimeUnit timeUnit) {
        try {
            timeUnit.sleep(timeout);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
