package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.time.LocalTime;

/**
 * Codec for TIME columns, targeting {@link LocalTime}.
 */
public final class LocalTimeCodec implements Codec<LocalTime> {

    public static final LocalTimeCodec INSTANCE = new LocalTimeCodec();

    private LocalTimeCodec() {}

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!LocalTime.class.isAssignableFrom(targetType)) return false;
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.TIME;
    }

    @Override
    public LocalTime decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends LocalTime> targetType)
            throws SQLException {
        Time time = rs.getTime(jdbcIndex);
        return time == null ? null : time.toLocalTime();
    }
}
