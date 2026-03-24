package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Strategy for decoding a JDBC column value into a target Java type.
 *
 * @param <T> the Java type this codec produces
 */
public interface Codec<T> {

    /**
     * Returns {@code true} if this codec can decode the given column into {@code targetType}.
     *
     * @param columnMetadata metadata of the column to decode
     * @param targetType     the requested Java type
     */
    boolean canDecode(YashanDbColumnMetadata columnMetadata, Class<?> targetType);

    /**
     * Decode the value at JDBC column {@code jdbcIndex} (1-based) from {@code rs}.
     *
     * @param rs         the open {@link ResultSet} positioned at the current row
     * @param jdbcIndex  1-based column index in the ResultSet
     * @param metadata   column metadata
     * @param targetType target Java type
     * @return the decoded value, or {@code null} if the column is SQL NULL
     * @throws SQLException if a database access error occurs
     */
    T decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends T> targetType)
            throws SQLException;
}
