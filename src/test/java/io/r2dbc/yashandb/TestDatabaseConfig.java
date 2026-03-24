package io.r2dbc.yashandb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Loads test database connection parameters.
 *
 * <p>Resolution order (highest priority first):
 * <ol>
 *   <li>System properties: {@code -Ddb.host=... -Ddb.port=... -Ddb.user=... -Ddb.password=...}</li>
 *   <li>File {@code test-database.properties} on the test classpath</li>
 * </ol>
 */
final class TestDatabaseConfig {

    static final String HOST;
    static final int    PORT;
    static final String DATABASE;
    static final String USER;
    static final String PASSWORD;

    static {
        Properties props = new Properties();
        // Load from classpath file first (lowest priority)
        try (InputStream is = TestDatabaseConfig.class
                .getClassLoader().getResourceAsStream("test-database.properties")) {
            if (is != null) {
                props.load(is);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load test-database.properties", e);
        }

        // System properties override file values
        HOST     = System.getProperty("db.host",     props.getProperty("db.host",     "localhost"));
        PORT     = Integer.parseInt(System.getProperty("db.port",     props.getProperty("db.port",     "1688")));
        DATABASE = System.getProperty("db.database", props.getProperty("db.database", ""));
        USER     = System.getProperty("db.user",     props.getProperty("db.user",     "sys"));
        PASSWORD = System.getProperty("db.password", props.getProperty("db.password", ""));
    }

    private TestDatabaseConfig() {}
}
