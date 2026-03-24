package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Codec for FLOAT / DOUBLE / REAL columns, targeting {@link Double}.
 */
public final class DoubleCodec implements Codec<Double> {

    public static final DoubleCodec INSTANCE = new DoubleCodec();

    private DoubleCodec() {}

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!Double.class.isAssignableFrom(targetType) && targetType != double.class) return false;
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.DOUBLE
                || jdbcType == Types.FLOAT
                || jdbcType == Types.REAL;
    }

    @Override
    public Double decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends Double> targetType)
            throws SQLException {
        double value = rs.getDouble(jdbcIndex);
        return rs.wasNull() ? null : value;
    }
}
