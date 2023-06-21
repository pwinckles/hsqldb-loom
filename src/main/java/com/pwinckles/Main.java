package com.pwinckles;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.hsqldb.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final List<String> MODES = List.of("LOCKS", "MVLOCKS", "MVCC");
    private static volatile ComboPooledDataSource c3p0;

    public static void main(String[] args) throws Exception {
        if (args.length != 1 || !MODES.contains(args[0].toUpperCase())) {
            throw new IllegalArgumentException("Must specify transaction control mode: " + MODES);
        }

        var server = new Server();
        server.setDatabaseName(0, "testdb");
        server.setDatabasePath(0, "mem:testdb");
        server.start();

        initC3P0Pool();

        try (var conn = newConnection()) {
            setupDb(args[0]);

            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < 1_000; i++) {
                    var index = i;
                    executor.execute(() -> {
                        log.info("Before {}", index);
                        try (var c = newConnection();
                                var statement = c.createStatement()) {
                            c.setAutoCommit(false);
                            statement.execute("INSERT INTO test (value) VALUES ('abc')");
                            TimeUnit.SECONDS.sleep(1);
                            statement.execute("COMMIT");
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        log.info("After {}", index);
                    });
                }
            }
        } finally {
            server.stop();
        }
    }

    private static void setupDb(String mode) throws SQLException {
        try (var conn = newConnection();
                var statement = conn.createStatement()) {
            statement.execute("SET DATABASE TRANSACTION CONTROL " + mode);
            statement.execute("CREATE TABLE test (id IDENTITY PRIMARY KEY, value VARCHAR(255))");
        }
    }

    private static Connection newConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:hsqldb:hsql://localhost/testdb", "SA", "");
//        return c3p0.getConnection();
    }

    private static void initC3P0Pool() throws PropertyVetoException {
        c3p0 = new ComboPooledDataSource();
        c3p0.setDriverClass("org.hsqldb.jdbcDriver");
        c3p0.setJdbcUrl("jdbc:hsqldb:hsql://localhost/testdb");
        c3p0.setUser("SA");
        c3p0.setPassword("");
    }

}
