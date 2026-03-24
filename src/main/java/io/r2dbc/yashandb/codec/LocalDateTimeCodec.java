package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;

/**
 * Codec for TIMESTAMP columns, targeting {@link LocalDateTime}.
 */
public final class LocalDateTimeCodec implements Codec<LocalDateTime> {

    public static final LocalDateTimeCodec INSTANCE = new LocalDateTimeCodec();

    private LocalDateTimeCodec() {}

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!LocalDateTime.class.isAssignableFrom(targetType)) return false;
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.TIMESTAMP
                || jdbcType == Types.TIMESTAMP_WITH_TIMEZONE;
    }

    @Override
    public LocalDateTime decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends LocalDateTime> targetType)
            throws SQLException {
        Timestamp ts = rs.getTimestamp(jdbcIndex);
        return ts == null ? null : ts.toLocalDateTime();
    }
}
