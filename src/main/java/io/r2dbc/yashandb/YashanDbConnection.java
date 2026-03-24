package io.r2dbc.yashandb;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import io.r2dbc.yashandb.util.ExceptionFactory;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * R2DBC {@link Connection} implementation for YashanDB.
 *
 * <p>Wraps a standard {@link java.sql.Connection} (JDBC {@code ConnectionImpl})
 * and exposes all lifecycle operations as reactive {@link Mono} / {@link Publisher}
 * sequences that execute on {@link Schedulers#boundedElastic()} to avoid blocking
 * the Reactor event loop.</p>
 */
public final class YashanDbConnection implements Connection {

    private static final Logger log = LoggerFactory.getLogger(YashanDbConnection.class);

    private final java.sql.Connection jdbcConnection;
    private final YashanDbConnectionConfiguration configuration;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    /** Savepoint name -> JDBC Savepoint object, for rollback-to-savepoint support. */
    private final Map<String, Savepoint> savepoints = new HashMap<>();

    private YashanDbConnection(java.sql.Connection jdbcConnection, YashanDbConnectionConfiguration configuration) {
        this.jdbcConnection = jdbcConnection;
        this.configuration = configuration;
    }

    /**
     * Open a new JDBC connection synchronously (called from a boundedElastic thread).
     */
    static YashanDbConnection connect(YashanDbConnectionConfiguration configuration) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", configuration.getUsername());
        props.setProperty("password", configuration.getPassword().toString());

        if (configuration.getConnectTimeout() != null) {
            props.setProperty("connectTimeout",
                    String.valueOf(configuration.getConnectTimeout().toMillis() / 1000));
        }

        if (configuration.isSsl()) {
            props.setProperty("ssl", "true");
        }

        String url = configuration.toJdbcUrl();
        log.debug("Opening JDBC connection: {}", url);

        // Load the YashanDB JDBC driver explicitly (in case ServiceLoader hasn't picked it up)
        try {
            Class.forName("com.yashandb.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("YashanDB JDBC driver class not found", e);
        }

        java.sql.Connection jdbcConn = DriverManager.getConnection(url, props);
        // Disable auto-commit by default so that R2DBC transaction semantics apply
        jdbcConn.setAutoCommit(true);
        return new YashanDbConnection(jdbcConn, configuration);
    }

    // -------------------------------------------------------------------------
    // Connection lifecycle
    // -------------------------------------------------------------------------

    @Override
    public Mono<Void> close() {
        if (closed.compareAndSet(false, true)) {
            return Mono.<Void>fromRunnable(() -> {
                        try {
                            log.debug("Closing YashanDB connection");
                            jdbcConnection.close();
                        } catch (SQLException e) {
                            throw ExceptionFactory.convert(e);
                        }
                    })
                    .subscribeOn(Schedulers.boundedElastic());
        }
        return Mono.empty();
    }

    @Override
    public Publisher<Boolean> validate(ValidationDepth depth) {
        if (closed.get()) {
            return Mono.just(false);
        }
        if (depth == ValidationDepth.LOCAL) {
            return Mono.fromCallable(() -> {
                        try {
                            return !jdbcConnection.isClosed();
                        } catch (SQLException e) {
                            return false;
                        }
                    })
                    .subscribeOn(Schedulers.boundedElastic());
        }
        // REMOTE: send a lightweight ping
        return Mono.fromCallable(() -> {
                    try {
                        return jdbcConnection.isValid(5);
                    } catch (SQLException e) {
                        return false;
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    // -------------------------------------------------------------------------
    // Transaction management
    // -------------------------------------------------------------------------

    @Override
    public Mono<Void> beginTransaction() {
        return Mono.<Void>fromRunnable(() -> {
                    try {
                        if (jdbcConnection.getAutoCommit()) {
                            jdbcConnection.setAutoCommit(false);
                            log.debug("Transaction started (autoCommit=false)");
                        }
                    } catch (SQLException e) {
                        throw ExceptionFactory.convert(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> beginTransaction(TransactionDefinition definition) {
        return Mono.<Void>fromRunnable(() -> {
                    try {
                        IsolationLevel isolationLevel = definition.getAttribute(TransactionDefinition.ISOLATION_LEVEL);
                        if (isolationLevel != null) {
                            int jdbcLevel = toJdbcIsolationLevel(isolationLevel);
                            jdbcConnection.setTransactionIsolation(jdbcLevel);
                        }
                        if (jdbcConnection.getAutoCommit()) {
                            jdbcConnection.setAutoCommit(false);
                        }
                    } catch (SQLException e) {
                        throw ExceptionFactory.convert(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> commitTransaction() {
        return Mono.<Void>fromRunnable(() -> {
                    try {
                        if (!jdbcConnection.getAutoCommit()) {
                            jdbcConnection.commit();
                            jdbcConnection.setAutoCommit(true);
                            log.debug("Transaction committed");
                        }
                    } catch (SQLException e) {
                        throw ExceptionFactory.convert(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return Mono.<Void>fromRunnable(() -> {
                    try {
                        if (!jdbcConnection.getAutoCommit()) {
                            jdbcConnection.rollback();
                            jdbcConnection.setAutoCommit(true);
                            log.debug("Transaction rolled back");
                        }
                    } catch (SQLException e) {
                        throw ExceptionFactory.convert(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        return Mono.<Void>fromRunnable(() -> {
                    try {
                        Savepoint sp = savepoints.get(name);
                        if (sp == null) {
                            throw new IllegalArgumentException("No savepoint named '" + name + "'");
                        }
                        jdbcConnection.rollback(sp);
                        // Remove this savepoint and any later ones from cache
                        savepoints.remove(name);
                    } catch (SQLException e) {
                        throw ExceptionFactory.convert(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        return Mono.<Void>fromRunnable(() -> {
                    try {
                        // R2DBC spec: createSavepoint implicitly starts a transaction
                        if (jdbcConnection.getAutoCommit()) {
                            jdbcConnection.setAutoCommit(false);
                        }
                        Savepoint sp = jdbcConnection.setSavepoint(name);
                        savepoints.put(name, sp);
                    } catch (SQLException e) {
                        throw ExceptionFactory.convert(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        return Mono.<Void>fromRunnable(() -> {
                    try {
                        jdbcConnection.releaseSavepoint(jdbcConnection.setSavepoint(name));
                    } catch (SQLException e) {
                        throw ExceptionFactory.convert(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    // -------------------------------------------------------------------------
    // Statement factory
    // -------------------------------------------------------------------------

    @Override
    public Statement createStatement(String sql) {
        return new YashanDbStatement(this, sql);
    }

    @Override
    public Batch createBatch() {
        return new YashanDbBatch(this);
    }

    // -------------------------------------------------------------------------
    // Isolation level
    // -------------------------------------------------------------------------

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        return Mono.<Void>fromRunnable(() -> {
                    try {
                        jdbcConnection.setTransactionIsolation(toJdbcIsolationLevel(isolationLevel));
                    } catch (SQLException e) {
                        throw ExceptionFactory.convert(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        try {
            return fromJdbcIsolationLevel(jdbcConnection.getTransactionIsolation());
        } catch (SQLException e) {
            return IsolationLevel.READ_COMMITTED;
        }
    }

    @Override
    public Mono<Void> setLockWaitTimeout(java.time.Duration lockWaitTimeout) {
        // YashanDB does not expose a direct lock wait timeout via JDBC; no-op
        return Mono.empty();
    }

    @Override
    public Mono<Void> setStatementTimeout(java.time.Duration statementTimeout) {
        return Mono.<Void>fromRunnable(() -> {
                    try {
                        // JDBC setQueryTimeout is in seconds
                        int seconds = (int) statementTimeout.getSeconds();
                        // Apply to the underlying connection's default statement timeout
                        // by setting a connection-level property where supported
                        // (YashanDB SOCKET_TIMEOUT is a connect-time property; best effort here)
                        if (seconds > 0) {
                            java.sql.Statement stmt = jdbcConnection.createStatement();
                            stmt.setQueryTimeout(seconds);
                            stmt.close();
                        }
                    } catch (SQLException e) {
                        throw ExceptionFactory.convert(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public ConnectionMetadata getMetadata() {
        return YashanDbConnectionMetadata.INSTANCE;
    }

    @Override
    public boolean isAutoCommit() {
        try {
            return jdbcConnection.getAutoCommit();
        } catch (SQLException e) {
            return true;
        }
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {
        return Mono.<Void>fromRunnable(() -> {
                    try {
                        jdbcConnection.setAutoCommit(autoCommit);
                    } catch (SQLException e) {
                        throw ExceptionFactory.convert(e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    // -------------------------------------------------------------------------
    // Package-private accessors used by Statement / Result
    // -------------------------------------------------------------------------

    java.sql.Connection getJdbcConnection() {
        return jdbcConnection;
    }

    YashanDbConnectionConfiguration getConfiguration() {
        return configuration;
    }

    boolean isClosed() {
        return closed.get();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static int toJdbcIsolationLevel(IsolationLevel level) {
        if (IsolationLevel.READ_UNCOMMITTED.equals(level)) {
            return java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
        } else if (IsolationLevel.READ_COMMITTED.equals(level)) {
            return java.sql.Connection.TRANSACTION_READ_COMMITTED;
        } else if (IsolationLevel.REPEATABLE_READ.equals(level)) {
            return java.sql.Connection.TRANSACTION_REPEATABLE_READ;
        } else if (IsolationLevel.SERIALIZABLE.equals(level)) {
            return java.sql.Connection.TRANSACTION_SERIALIZABLE;
        }
        return java.sql.Connection.TRANSACTION_READ_COMMITTED;
    }

    private static IsolationLevel fromJdbcIsolationLevel(int level) {
        return switch (level) {
            case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED -> IsolationLevel.READ_UNCOMMITTED;
            case java.sql.Connection.TRANSACTION_READ_COMMITTED -> IsolationLevel.READ_COMMITTED;
            case java.sql.Connection.TRANSACTION_REPEATABLE_READ -> IsolationLevel.REPEATABLE_READ;
            case java.sql.Connection.TRANSACTION_SERIALIZABLE -> IsolationLevel.SERIALIZABLE;
            default -> IsolationLevel.READ_COMMITTED;
        };
    }
}
