package com.dataspec.connection;

public interface ConnectionHandle<T> {
    Iterable<T> execute(String query);
}
