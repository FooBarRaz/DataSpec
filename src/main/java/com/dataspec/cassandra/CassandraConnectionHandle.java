package com.dataspec.cassandra;

import com.dataspec.connection.ConnectionHandle;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.sql.ResultSet;

public class CassandraConnectionHandle implements ConnectionHandle {

    private final Cluster cluster;
    private CassandraConfiguration config;
    private Session session;

    public CassandraConnectionHandle(CassandraConfiguration config) {
        cluster = Cluster.builder().addContactPointsWithPorts(config.getAddress()).build();
        this.config = config;
    }

    public ResultSet execute(String query) {
        connect();
        session.execute(query);
        return null;
    }

    private void connect() {
        if (session == null || session.isClosed()) {
            session = cluster.connect(config.getKeyspace());
        }
    }

}
