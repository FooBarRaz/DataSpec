package com.dataspec.cassandra;

import java.net.InetSocketAddress;

public class CassandraConfiguration {

    private final InetSocketAddress address;
    private String keyspace;

    public CassandraConfiguration(int i, String keyspace, String... hosts) {
        this.keyspace = keyspace;
        address = new InetSocketAddress(hosts[0], i);
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public String getKeyspace() {
        return keyspace;
    }
}
