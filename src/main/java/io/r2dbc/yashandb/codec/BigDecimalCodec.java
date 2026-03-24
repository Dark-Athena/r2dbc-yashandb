package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Codec for NUMBER / DECIMAL / NUMERIC columns, targeting {@link BigDecimal}.
 */
public final class BigDecimalCodec implements Codec<BigDecimal> {

    public static final BigDecimalCodec INSTANCE = new BigDecimalCodec();

    private BigDecimalCodec() {}

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!BigDecimal.class.isAssignableFrom(targetType)) return false;
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.NUMERIC
                || jdbcType == Types.DECIMAL
                || jdbcType == Types.BIGINT
                || jdbcType == Types.INTEGER
                || jdbcType == Types.SMALLINT
                || jdbcType == Types.TINYINT
                || jdbcType == Types.FLOAT
                || jdbcType == Types.DOUBLE
                || jdbcType == Types.REAL;
    }

    @Override
    public BigDecimal decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends BigDecimal> targetType)
            throws SQLException {
        return rs.getBigDecimal(jdbcIndex);
    }
}
