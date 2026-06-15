package org.kendar.cqrses.db;

import java.sql.SQLException;

public class DbException extends RuntimeException {
    public DbException(SQLException cause) {
        super(cause.getMessage(), cause);
    }
}
