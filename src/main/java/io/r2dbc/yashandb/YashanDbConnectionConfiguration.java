package io.r2dbc.yashandb;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for creating a {@link YashanDbConnectionFactory}.
 * Use {@link #builder()} to construct an instance.
 */
public final class YashanDbConnectionConfiguration {

    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final CharSequence password;
    private final Duration connectTimeout;
    private final boolean ssl;

    private YashanDbConnectionConfiguration(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.database = builder.database;
        this.username = builder.username;
        this.password = builder.password;
        this.connectTimeout = builder.connectTimeout;
        this.ssl = builder.ssl;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public CharSequence getPassword() {
        return password;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public boolean isSsl() {
        return ssl;
    }

    /**
     * Build a JDBC-style connection URL for the underlying protocol layer.
     * Format: jdbc:yasdb://host:port/database
     */
    public String toJdbcUrl() {
        return "jdbc:yasdb://" + host + ":" + port + "/" + database;
    }

    @Override
    public String toString() {
        return "YashanDbConnectionConfiguration{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", database='" + database + '\'' +
                ", username='" + username + '\'' +
                ", connectTimeout=" + connectTimeout +
                ", ssl=" + ssl +
                '}';
    }

    public static final class Builder {

        private String host = "localhost";
        private int port = 1688;
        private String database;
        private String username;
        private CharSequence password;
        private Duration connectTimeout = Duration.ofSeconds(10);
        private boolean ssl = false;

        private Builder() {}

        /**
         * Database server hostname or IP address. Default: {@code localhost}.
         */
        public Builder host(String host) {
            this.host = Objects.requireNonNull(host, "host must not be null");
            return this;
        }

        /**
         * Database server port. Default: {@code 1688}.
         */
        public Builder port(int port) {
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("port must be between 1 and 65535");
            }
            this.port = port;
            return this;
        }

        /**
         * Database (service) name.
         */
        public Builder database(String database) {
            this.database = Objects.requireNonNull(database, "database must not be null");
            return this;
        }

        /**
         * Login username.
         */
        public Builder username(String username) {
            this.username = Objects.requireNonNull(username, "username must not be null");
            return this;
        }

        /**
         * Login password.
         */
        public Builder password(CharSequence password) {
            this.password = Objects.requireNonNull(password, "password must not be null");
            return this;
        }

        /**
         * TCP connection timeout. Default: {@code 10s}.
         */
        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = Objects.requireNonNull(connectTimeout, "connectTimeout must not be null");
            return this;
        }

        /**
         * Whether to use SSL/TLS. Default: {@code false}.
         */
        public Builder ssl(boolean ssl) {
            this.ssl = ssl;
            return this;
        }

        public YashanDbConnectionConfiguration build() {
            Objects.requireNonNull(database, "database must not be null");
            Objects.requireNonNull(username, "username must not be null");
            Objects.requireNonNull(password, "password must not be null");
            return new YashanDbConnectionConfiguration(this);
        }
    }
}
