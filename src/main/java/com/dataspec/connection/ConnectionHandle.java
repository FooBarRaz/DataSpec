package com.dataspec.connection;

import java.sql.ResultSet;

public interface ConnectionHandle {
    ResultSet execute(String query);
}
