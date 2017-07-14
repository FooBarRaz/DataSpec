package com.dataspec.cassandra;

import com.dataspec.connection.ConnectionHandle;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraConnectionHandle implements ConnectionHandle<Row> {

    private final Cluster cluster;
    private CassandraConfiguration config;
    private Session session;

    public CassandraConnectionHandle(CassandraConfiguration config) {
        cluster = Cluster.builder().addContactPointsWithPorts(config.getAddress()).build();
        this.config = config;
    }

    public Iterable<Row> execute(String query) {
        connect();
        return session.execute(query);
    }

    private void connect() {
        if (session == null || session.isClosed()) {
            session = cluster.connect(config.getKeyspace());
        }
    }

}
