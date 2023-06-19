package com.pwinckles;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final List<String> MODES = List.of("LOCKS", "MVLOCKS", "MVCC");

    public static void main(String[] args) throws SQLException {
        if (args.length != 1 || !MODES.contains(args[0].toUpperCase())) {
            throw new IllegalArgumentException("Must specify transaction control mode: " + MODES);
        }

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
        return DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "SA", "");
    }
}
