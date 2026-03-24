package io.r2dbc.yashandb.codec;

import io.r2dbc.spi.Clob;
import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Codec for CLOB / NCLOB columns.
 *
 * <ul>
 *   <li>When the target type is {@link Clob}: returns an R2DBC {@link Clob} wrapper.</li>
 *   <li>When the target type is {@link String} or {@link Object}: returns the content as
 *       a plain {@link String}.</li>
 * </ul>
 */
public final class ClobCodec implements Codec<Object> {

    public static final ClobCodec INSTANCE = new ClobCodec();

    private ClobCodec() {}

    private static boolean isClobColumn(YashanDbColumnMetadata metadata) {
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.CLOB
                || jdbcType == Types.NCLOB
                || jdbcType == Types.LONGVARCHAR
                || jdbcType == Types.LONGNVARCHAR;
    }

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!isClobColumn(metadata)) return false;
        return Clob.class.isAssignableFrom(targetType)
                || String.class.isAssignableFrom(targetType)
                || targetType == Object.class;
    }

    @Override
    public Object decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<?> targetType)
            throws SQLException {
        if (Clob.class.isAssignableFrom(targetType)) {
            String content = rs.getString(jdbcIndex);
            if (content == null) return null;
            return new Clob() {
                @Override
                public org.reactivestreams.Publisher<CharSequence> stream() {
                    return Flux.just(content);
                }

                @Override
                public org.reactivestreams.Publisher<Void> discard() {
                    return Mono.empty();
                }
            };
        }
        // String or Object: return raw string
        return rs.getString(jdbcIndex);
    }
}
