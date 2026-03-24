package io.r2dbc.yashandb;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.yashandb.codec.DefaultCodecs;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * R2DBC {@link Row} implementation for YashanDB.
 *
 * <p>Wraps a JDBC {@link ResultSet} row snapshot and delegates type conversions
 * to {@link DefaultCodecs}.</p>
 */
public final class YashanDbRow implements Row {

    private final ResultSet resultSet;
    private final YashanDbRowMetadata metadata;
    private final DefaultCodecs codecs;

    YashanDbRow(ResultSet resultSet, YashanDbRowMetadata metadata) {
        this.resultSet = Objects.requireNonNull(resultSet, "resultSet must not be null");
        this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
        this.codecs = DefaultCodecs.INSTANCE;
    }

    @Override
    public RowMetadata getMetadata() {
        return metadata;
    }

    @Override
    public <T> T get(int index, Class<T> type) {
        Objects.requireNonNull(type, "type must not be null");
        if (index < 0 || index >= metadata.size()) {
            throw new IndexOutOfBoundsException("Column index " + index + " out of range");
        }
        try {
            YashanDbColumnMetadata colMeta = (YashanDbColumnMetadata) metadata.getColumnMetadata(index);
            return codecs.decode(resultSet, index + 1, colMeta, type);
        } catch (SQLException e) {
            throw io.r2dbc.yashandb.util.ExceptionFactory.convert(e);
        }
    }

    @Override
    public <T> T get(String name, Class<T> type) {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(type, "type must not be null");
        int index = metadata.indexOf(name);
        return get(index, type);
    }
}
