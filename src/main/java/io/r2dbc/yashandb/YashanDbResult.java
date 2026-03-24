package io.r2dbc.yashandb;

import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.yashandb.util.ExceptionFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * R2DBC {@link Result} implementation for YashanDB.
 *
 * <p>Supports three kinds of results:
 * <ol>
 *   <li><strong>Query result</strong> – a JDBC {@link ResultSet} emitted row-by-row.</li>
 *   <li><strong>Update count</strong> – the number of rows affected by a DML statement.</li>
 *   <li><strong>Generated keys</strong> – update count + a ResultSet of generated key values.</li>
 * </ol>
 * </p>
 */
public final class YashanDbResult implements Result {

    // ------ factory fields ------
    private final ResultSet resultSet;       // nullable
    private final ResultSet generatedKeys;   // nullable
    private final long updateCount;
    private final Statement owningStatement; // nullable; closed after ResultSet is exhausted

    private YashanDbResult(ResultSet resultSet, ResultSet generatedKeys, long updateCount, Statement owningStatement) {
        this.resultSet = resultSet;
        this.generatedKeys = generatedKeys;
        this.updateCount = updateCount;
        this.owningStatement = owningStatement;
    }

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    /** Create a result backed by a query {@link ResultSet}. */
    static YashanDbResult ofResultSet(ResultSet resultSet, Statement statement) {
        return new YashanDbResult(resultSet, null, -1, statement);
    }

    /** Create a result for a DML statement (rows updated). */
    static YashanDbResult ofUpdateCount(long updateCount) {
        return new YashanDbResult(null, null, updateCount, null);
    }

    /** Create a result with both update count and generated keys. */
    static YashanDbResult ofGeneratedKeys(long updateCount, ResultSet generatedKeys, Statement statement) {
        return new YashanDbResult(null, generatedKeys, updateCount, statement);
    }

    // -------------------------------------------------------------------------
    // Result SPI
    // -------------------------------------------------------------------------

    @Override
    public Mono<Long> getRowsUpdated() {
        if (resultSet != null) {
            return Mono.empty();
        }
        return Mono.just(Math.max(updateCount, 0L));
    }

    @Override
    public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
        ResultSet rs = resultSet != null ? resultSet : generatedKeys;
        if (rs != null) {
            return mapResultSet(rs, owningStatement, mappingFunction);
        }
        return Flux.empty();
    }

    @Override
    public Result filter(Predicate<Segment> filter) {
        return new FilteredResult(this, filter);
    }

    /**
     * R2DBC 1.0 SPI: flatMap over {@link Segment} stream.
     *
     * <p>This implementation emits two segment types:
     * <ul>
     *   <li>{@link UpdateCount} segments when there is an update count.</li>
     *   <li>{@link RowSegment} segments for each data row.</li>
     * </ul>
     * </p>
     */
    @Override
    public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
        return Flux.defer(() -> {
            if (resultSet != null) {
                return flatMapResultSet(resultSet, owningStatement, mappingFunction);
            }
            if (generatedKeys != null) {
                return flatMapResultSet(generatedKeys, owningStatement, mappingFunction);
            }
            // Update-count segment
            long count = Math.max(updateCount, 0L);
            UpdateCount seg = () -> count;
            return Flux.from(mappingFunction.apply(seg));
        });
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private static <T> Flux<T> mapResultSet(
            ResultSet rs,
            Statement stmt,
            BiFunction<Row, RowMetadata, ? extends T> mapper) {

        return Flux.<T>create(sink -> {
                    try {
                        YashanDbRowMetadata metadata = YashanDbRowMetadata.fromJdbc(rs.getMetaData());
                        while (rs.next()) {
                            YashanDbRow row = new YashanDbRow(rs, metadata);
                            T value = mapper.apply(row, metadata);
                            sink.next(value);
                        }
                        sink.complete();
                    } catch (SQLException e) {
                        sink.error(ExceptionFactory.convert(e));
                    } finally {
                        closeQuietly(rs, stmt);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private static <T> Flux<T> flatMapResultSet(
            ResultSet rs,
            Statement stmt,
            Function<Segment, ? extends Publisher<? extends T>> mapper) {

        return Flux.<T>create(sink -> {
                    try {
                        YashanDbRowMetadata metadata = YashanDbRowMetadata.fromJdbc(rs.getMetaData());
                        while (rs.next()) {
                            YashanDbRow row = new YashanDbRow(rs, metadata);
                            RowSegment seg = () -> row;
                            Publisher<? extends T> pub = mapper.apply(seg);
                            // Materialise synchronously (we are on boundedElastic)
                            Flux.from(pub).toIterable().forEach(sink::next);
                        }
                        sink.complete();
                    } catch (SQLException e) {
                        sink.error(ExceptionFactory.convert(e));
                    } finally {
                        closeQuietly(rs, stmt);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private static void closeQuietly(ResultSet rs, Statement stmt) {
        try {
            if (rs != null && !rs.isClosed()) rs.close();
        } catch (SQLException ignored) {}
        try {
            if (stmt != null && !stmt.isClosed()) stmt.close();
        } catch (SQLException ignored) {}
    }

    // -------------------------------------------------------------------------
    // FilteredResult: wraps a Result and applies a Predicate<Segment> filter
    // -------------------------------------------------------------------------

    private static final class FilteredResult implements Result {
        private final YashanDbResult delegate;
        private final Predicate<Segment> predicate;

        FilteredResult(YashanDbResult delegate, Predicate<Segment> predicate) {
            this.delegate = delegate;
            this.predicate = predicate;
        }

        @Override
        public Mono<Long> getRowsUpdated() {
            return delegate.getRowsUpdated();
        }

        @Override
        public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
            // Filter RowSegments: if predicate rejects a RowSegment, skip the row
            ResultSet rs = delegate.resultSet != null ? delegate.resultSet : delegate.generatedKeys;
            if (rs != null) {
                return mapResultSetFiltered(rs, delegate.owningStatement, mappingFunction, predicate);
            }
            return Flux.empty();
        }

        @Override
        public Result filter(Predicate<Segment> filter) {
            return new FilteredResult(delegate, s -> predicate.test(s) && filter.test(s));
        }

        @Override
        public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
            return Flux.defer(() -> {
                ResultSet rs = delegate.resultSet != null ? delegate.resultSet : delegate.generatedKeys;
                if (rs != null) {
                    return flatMapResultSetFiltered(rs, delegate.owningStatement, mappingFunction, predicate);
                }
                long count = Math.max(delegate.updateCount, 0L);
                UpdateCount seg = () -> count;
                if (predicate.test(seg)) {
                    return Flux.from(mappingFunction.apply(seg));
                }
                return Flux.empty();
            });
        }
    }

    private static <T> Flux<T> mapResultSetFiltered(
            ResultSet rs,
            Statement stmt,
            BiFunction<Row, RowMetadata, ? extends T> mapper,
            Predicate<Segment> predicate) {

        return Flux.<T>create(sink -> {
                    try {
                        YashanDbRowMetadata metadata = YashanDbRowMetadata.fromJdbc(rs.getMetaData());
                        while (rs.next()) {
                            YashanDbRow row = new YashanDbRow(rs, metadata);
                            RowSegment seg = () -> row;
                            if (predicate.test(seg)) {
                                T value = mapper.apply(row, metadata);
                                sink.next(value);
                            }
                        }
                        sink.complete();
                    } catch (SQLException e) {
                        sink.error(ExceptionFactory.convert(e));
                    } finally {
                        closeQuietly(rs, stmt);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private static <T> Flux<T> flatMapResultSetFiltered(
            ResultSet rs,
            Statement stmt,
            Function<Segment, ? extends Publisher<? extends T>> mapper,
            Predicate<Segment> predicate) {

        return Flux.<T>create(sink -> {
                    try {
                        YashanDbRowMetadata metadata = YashanDbRowMetadata.fromJdbc(rs.getMetaData());
                        while (rs.next()) {
                            YashanDbRow row = new YashanDbRow(rs, metadata);
                            RowSegment seg = () -> row;
                            if (predicate.test(seg)) {
                                Publisher<? extends T> pub = mapper.apply(seg);
                                Flux.from(pub).toIterable().forEach(sink::next);
                            }
                        }
                        sink.complete();
                    } catch (SQLException e) {
                        sink.error(ExceptionFactory.convert(e));
                    } finally {
                        closeQuietly(rs, stmt);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }
}
