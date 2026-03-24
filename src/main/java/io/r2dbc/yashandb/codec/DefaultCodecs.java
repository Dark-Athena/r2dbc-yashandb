package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Registry of all built-in {@link Codec} implementations.
 *
 * <p>Codecs are checked in registration order; the first codec that returns
 * {@code true} from {@link Codec#canDecode} wins.</p>
 */
public final class DefaultCodecs {

    public static final DefaultCodecs INSTANCE = new DefaultCodecs();

    /**
     * Ordered list of codecs from most specific to least specific.
     * {@link ObjectCodec} must always be last.
     */
    @SuppressWarnings("rawtypes")
    private static final List<Codec> CODECS = List.of(
            BooleanCodec.INSTANCE,
            IntegerCodec.INSTANCE,
            LongCodec.INSTANCE,
            FloatCodec.INSTANCE,
            DoubleCodec.INSTANCE,
            BigDecimalCodec.INSTANCE,
            LocalDateCodec.INSTANCE,
            LocalTimeCodec.INSTANCE,
            LocalDateTimeCodec.INSTANCE,
            OffsetDateTimeCodec.INSTANCE,
            ByteArrayCodec.INSTANCE,
            BlobCodec.INSTANCE,
            ClobCodec.INSTANCE,
            StringCodec.INSTANCE,
            ObjectCodec.INSTANCE   // fallback — must be last
    );

    private DefaultCodecs() {}

    /**
     * Decode the value at JDBC column {@code jdbcIndex} (1-based) into {@code targetType}.
     *
     * @param rs         the open {@link ResultSet} positioned at the current row
     * @param jdbcIndex  1-based JDBC column index
     * @param metadata   column metadata
     * @param targetType the desired Java type
     * @param <T>        the return type
     * @return the decoded value, or {@code null} if the column value is SQL NULL
     * @throws SQLException if a JDBC error occurs during decoding
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> T decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<T> targetType)
            throws SQLException {
        for (Codec codec : CODECS) {
            if (codec.canDecode(metadata, targetType)) {
                Object result = codec.decode(rs, jdbcIndex, metadata, targetType);
                if (result == null) return null;
                // Safe cast: codec guarantees the returned type is compatible with targetType
                if (targetType.isInstance(result)) {
                    return targetType.cast(result);
                }
                // targetType == Object.class: return as-is
                if (targetType == Object.class) {
                    return (T) result;
                }
                return targetType.cast(result);
            }
        }
        // Should never reach here because ObjectCodec accepts everything
        throw new IllegalStateException("No codec found for column '" + metadata.getName()
                + "' and target type " + targetType.getName());
    }
}
