package io.r2dbc.yashandb;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Result;
import io.r2dbc.yashandb.util.ExceptionFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * R2DBC {@link Batch} implementation for YashanDB.
 *
 * <p>Accumulates SQL statements and executes them as a JDBC batch on a
 * bounded-elastic thread when {@link #execute()} is called.</p>
 */
public final class YashanDbBatch implements Batch {

    private final YashanDbConnection connection;
    private final List<String> statements = new ArrayList<>();

    YashanDbBatch(YashanDbConnection connection) {
        this.connection = Objects.requireNonNull(connection, "connection must not be null");
    }

    @Override
    public Batch add(String sql) {
        Objects.requireNonNull(sql, "sql must not be null");
        statements.add(sql);
        return this;
    }

    @Override
    public Publisher<? extends Result> execute() {
        List<String> sqlList = new ArrayList<>(statements);
        return Flux.fromIterable(sqlList)
                .flatMap(sql -> Flux.defer(() -> {
                    try {
                        Statement stmt = connection.getJdbcConnection().createStatement();
                        boolean hasResultSet = stmt.execute(sql);
                        if (hasResultSet) {
                            return Flux.just(YashanDbResult.ofResultSet(stmt.getResultSet(), stmt));
                        } else {
                            long updateCount = stmt.getLargeUpdateCount();
                            stmt.close();
                            return Flux.just(YashanDbResult.ofUpdateCount(updateCount));
                        }
                    } catch (SQLException e) {
                        return Flux.error(ExceptionFactory.convert(e));
                    }
                }).subscribeOn(Schedulers.boundedElastic()));
    }
}
