package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Fallback codec that accepts any column type and any target type, delegating to
 * {@link ResultSet#getObject(int, Class)} for the conversion.
 *
 * <p>This codec has the lowest priority and is checked last in {@link DefaultCodecs}.</p>
 */
public final class ObjectCodec implements Codec<Object> {

    public static final ObjectCodec INSTANCE = new ObjectCodec();

    private ObjectCodec() {}

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        // Accepts everything as fallback
        return true;
    }

    @Override
    public Object decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends Object> targetType)
            throws SQLException {
        if (targetType == Object.class) {
            return rs.getObject(jdbcIndex);
        }
        try {
            return rs.getObject(jdbcIndex, targetType);
        } catch (SQLException e) {
            // If the driver doesn't support getObject(int, Class), fall back
            Object raw = rs.getObject(jdbcIndex);
            if (raw == null) return null;
            if (targetType.isInstance(raw)) {
                return targetType.cast(raw);
            }
            return raw;
        }
    }
}
