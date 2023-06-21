package com.pwinckles;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.hsqldb.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final List<String> MODES = List.of("LOCKS", "MVLOCKS", "MVCC");
    private static final HikariDataSource hikari = new HikariDataSource();

    static {
        hikari.setJdbcUrl("jdbc:hsqldb:hsql://localhost/testdb");
        hikari.setUsername("SA");
        hikari.setPassword("");
        hikari.setMaximumPoolSize(50);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1 || !MODES.contains(args[0].toUpperCase())) {
            throw new IllegalArgumentException("Must specify transaction control mode: " + MODES);
        }

        var server = new Server();
        server.setDatabaseName(0, "testdb");
        server.setDatabasePath(0, "mem:testdb");
        server.start();

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
                            var start = Instant.now();
                            statement.execute("INSERT INTO test (value) VALUES ('abc')");
                            var duration = Duration.between(start, Instant.now());
                            var remaining = Duration.ofSeconds(1).minus(duration);
                            if (remaining.isPositive()) {
                                TimeUnit.NANOSECONDS.sleep(remaining.getNano());
                            }
                            statement.execute("COMMIT");
                        } catch (Exception e) {
                            log.error("Failure " + index, e);
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
        return hikari.getConnection();
    }
}
