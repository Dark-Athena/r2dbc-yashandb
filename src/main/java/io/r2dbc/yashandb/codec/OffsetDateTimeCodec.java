package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Codec for TIMESTAMP_TZ / TIMESTAMP_LTZ columns, targeting {@link OffsetDateTime}.
 */
public final class OffsetDateTimeCodec implements Codec<OffsetDateTime> {

    public static final OffsetDateTimeCodec INSTANCE = new OffsetDateTimeCodec();

    private OffsetDateTimeCodec() {}

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!OffsetDateTime.class.isAssignableFrom(targetType)) return false;
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.TIMESTAMP_WITH_TIMEZONE
                || jdbcType == Types.TIMESTAMP;
    }

    @Override
    public OffsetDateTime decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends OffsetDateTime> targetType)
            throws SQLException {
        Timestamp ts = rs.getTimestamp(jdbcIndex);
        if (ts == null) return null;
        return ts.toInstant().atOffset(ZoneOffset.UTC);
    }
}
