package io.r2dbc.yashandb;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Objects;

/**
 * R2DBC {@link ConnectionFactory} implementation for YashanDB.
 *
 * <p>Creates new {@link YashanDbConnection} instances by delegating to the
 * underlying JDBC/protocol layer on a bounded-elastic thread to avoid blocking
 * the Reactor event loop.</p>
 */
public final class YashanDbConnectionFactory implements ConnectionFactory {

    private static final Logger log = LoggerFactory.getLogger(YashanDbConnectionFactory.class);

    private final YashanDbConnectionConfiguration configuration;

    public YashanDbConnectionFactory(YashanDbConnectionConfiguration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
    }

    /**
     * Creates a new database connection.
     *
     * <p>The blocking JDBC connect call is executed on {@link Schedulers#boundedElastic()}
     * so that the caller's Reactor thread is never blocked.</p>
     *
     * @return a {@link Mono} that emits a single {@link Connection} when the
     *         connection is established, or an error signal if the connection fails
     */
    @Override
    public Mono<? extends Connection> create() {
        return Mono.fromCallable(() -> {
                    log.debug("Creating YashanDB connection to {}:{}/{}",
                            configuration.getHost(), configuration.getPort(), configuration.getDatabase());
                    return YashanDbConnection.connect(configuration);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .cast(Connection.class);
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return YashanDbConnectionFactoryMetadata.INSTANCE;
    }

    public YashanDbConnectionConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public String toString() {
        return "YashanDbConnectionFactory{configuration=" + configuration + '}';
    }
}
