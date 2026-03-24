package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;

/**
 * Codec for DATE columns, targeting {@link LocalDate}.
 */
public final class LocalDateCodec implements Codec<LocalDate> {

    public static final LocalDateCodec INSTANCE = new LocalDateCodec();

    private LocalDateCodec() {}

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!LocalDate.class.isAssignableFrom(targetType)) return false;
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.DATE;
    }

    @Override
    public LocalDate decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends LocalDate> targetType)
            throws SQLException {
        Date date = rs.getDate(jdbcIndex);
        return date == null ? null : date.toLocalDate();
    }
}
