package io.r2dbc.yashandb.codec;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.Type;
import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Unit tests for individual {@link Codec} implementations using a mock {@link ResultSet}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CodecTest {

    @Mock
    ResultSet rs;

    @Mock
    ResultSetMetaData rsmd;

    /**
     * Build a YashanDbColumnMetadata via the fromJdbc factory using a mock ResultSetMetaData.
     */
    private YashanDbColumnMetadata meta(YashanDbType type) throws Exception {
        int sqlType = type.getJdbcType();
        when(rsmd.getColumnLabel(1)).thenReturn("test_col");
        when(rsmd.getColumnName(1)).thenReturn("test_col");
        when(rsmd.getColumnType(1)).thenReturn(sqlType);
        when(rsmd.getColumnTypeName(1)).thenReturn(type.getName());
        when(rsmd.isNullable(1)).thenReturn(ResultSetMetaData.columnNullable);
        when(rsmd.getPrecision(1)).thenReturn(0);
        when(rsmd.getScale(1)).thenReturn(0);
        return YashanDbColumnMetadata.fromJdbc(rsmd, 1);
    }

    // -------------------------------------------------------------------------
    // BooleanCodec
    // -------------------------------------------------------------------------

    @Test
    void booleanCodecDecodesTrue() throws Exception {
        when(rs.getBoolean(1)).thenReturn(true);
        when(rs.wasNull()).thenReturn(false);

        YashanDbColumnMetadata m = meta(YashanDbType.BOOLEAN);
        assertThat(BooleanCodec.INSTANCE.canDecode(m, Boolean.class)).isTrue();
        assertThat(BooleanCodec.INSTANCE.decode(rs, 1, m, Boolean.class)).isTrue();
    }

    @Test
    void booleanCodecDecodesNull() throws Exception {
        when(rs.getBoolean(1)).thenReturn(false);
        when(rs.wasNull()).thenReturn(true);

        YashanDbColumnMetadata m = meta(YashanDbType.BOOLEAN);
        assertThat(BooleanCodec.INSTANCE.decode(rs, 1, m, Boolean.class)).isNull();
    }

    @Test
    void booleanCodecDoesNotDecodeInteger() throws Exception {
        assertThat(BooleanCodec.INSTANCE.canDecode(meta(YashanDbType.INTEGER), Boolean.class)).isFalse();
    }

    // -------------------------------------------------------------------------
    // IntegerCodec
    // -------------------------------------------------------------------------

    @Test
    void integerCodecDecodesValue() throws Exception {
        when(rs.getInt(1)).thenReturn(42);
        when(rs.wasNull()).thenReturn(false);

        YashanDbColumnMetadata m = meta(YashanDbType.INTEGER);
        assertThat(IntegerCodec.INSTANCE.canDecode(m, Integer.class)).isTrue();
        assertThat(IntegerCodec.INSTANCE.decode(rs, 1, m, Integer.class)).isEqualTo(42);
    }

    @Test
    void integerCodecDecodesNull() throws Exception {
        when(rs.getInt(1)).thenReturn(0);
        when(rs.wasNull()).thenReturn(true);

        assertThat(IntegerCodec.INSTANCE.decode(rs, 1, meta(YashanDbType.INTEGER), Integer.class)).isNull();
    }

    // -------------------------------------------------------------------------
    // LongCodec
    // -------------------------------------------------------------------------

    @Test
    void longCodecDecodes() throws Exception {
        when(rs.getLong(1)).thenReturn(1234567890123L);
        when(rs.wasNull()).thenReturn(false);

        assertThat(LongCodec.INSTANCE.decode(rs, 1, meta(YashanDbType.BIGINT), Long.class)).isEqualTo(1234567890123L);
    }

    // -------------------------------------------------------------------------
    // DoubleCodec
    // -------------------------------------------------------------------------

    @Test
    void doubleCodecDecodes() throws Exception {
        when(rs.getDouble(1)).thenReturn(3.14);
        when(rs.wasNull()).thenReturn(false);

        assertThat(DoubleCodec.INSTANCE.decode(rs, 1, meta(YashanDbType.DOUBLE), Double.class)).isEqualTo(3.14);
    }

    // -------------------------------------------------------------------------
    // BigDecimalCodec
    // -------------------------------------------------------------------------

    @Test
    void bigDecimalCodecDecodes() throws Exception {
        BigDecimal expected = new BigDecimal("123456.789");
        when(rs.getBigDecimal(1)).thenReturn(expected);

        assertThat(BigDecimalCodec.INSTANCE.decode(rs, 1, meta(YashanDbType.NUMBER), BigDecimal.class)).isEqualTo(expected);
    }

    // -------------------------------------------------------------------------
    // StringCodec
    // -------------------------------------------------------------------------

    @Test
    void stringCodecDecodes() throws Exception {
        when(rs.getString(1)).thenReturn("hello");

        assertThat(StringCodec.INSTANCE.canDecode(meta(YashanDbType.VARCHAR), String.class)).isTrue();
        assertThat(StringCodec.INSTANCE.decode(rs, 1, meta(YashanDbType.VARCHAR), String.class)).isEqualTo("hello");
    }

    @Test
    void stringCodecDecodesNull() throws Exception {
        when(rs.getString(1)).thenReturn(null);

        assertThat(StringCodec.INSTANCE.decode(rs, 1, meta(YashanDbType.VARCHAR), String.class)).isNull();
    }

    // -------------------------------------------------------------------------
    // LocalDateCodec
    // -------------------------------------------------------------------------

    @Test
    void localDateCodecDecodes() throws Exception {
        when(rs.getDate(1)).thenReturn(Date.valueOf(LocalDate.of(2024, 3, 15)));

        LocalDate result = LocalDateCodec.INSTANCE.decode(rs, 1, meta(YashanDbType.DATE), LocalDate.class);
        assertThat(result).isEqualTo(LocalDate.of(2024, 3, 15));
    }

    @Test
    void localDateCodecDecodesNull() throws Exception {
        when(rs.getDate(1)).thenReturn(null);

        assertThat(LocalDateCodec.INSTANCE.decode(rs, 1, meta(YashanDbType.DATE), LocalDate.class)).isNull();
    }

    // -------------------------------------------------------------------------
    // LocalTimeCodec
    // -------------------------------------------------------------------------

    @Test
    void localTimeCodecDecodes() throws Exception {
        when(rs.getTime(1)).thenReturn(Time.valueOf(LocalTime.of(10, 30, 0)));

        LocalTime result = LocalTimeCodec.INSTANCE.decode(rs, 1, meta(YashanDbType.TIME), LocalTime.class);
        assertThat(result).isEqualTo(LocalTime.of(10, 30, 0));
    }

    // -------------------------------------------------------------------------
    // LocalDateTimeCodec
    // -------------------------------------------------------------------------

    @Test
    void localDateTimeCodecDecodes() throws Exception {
        LocalDateTime expected = LocalDateTime.of(2024, 3, 15, 10, 30, 0);
        when(rs.getTimestamp(1)).thenReturn(Timestamp.valueOf(expected));

        LocalDateTime result = LocalDateTimeCodec.INSTANCE.decode(rs, 1, meta(YashanDbType.TIMESTAMP), LocalDateTime.class);
        assertThat(result).isEqualTo(expected);
    }

    // -------------------------------------------------------------------------
    // ByteArrayCodec
    // -------------------------------------------------------------------------

    @Test
    void byteArrayCodecDecodes() throws Exception {
        byte[] bytes = {1, 2, 3};
        when(rs.getBytes(1)).thenReturn(bytes);

        byte[] result = ByteArrayCodec.INSTANCE.decode(rs, 1, meta(YashanDbType.RAW), byte[].class);
        assertThat(result).isEqualTo(bytes);
    }

    // -------------------------------------------------------------------------
    // DefaultCodecs dispatcher
    // -------------------------------------------------------------------------

    @Test
    void defaultCodecsDispatchesToInteger() throws Exception {
        when(rs.getInt(1)).thenReturn(99);
        when(rs.wasNull()).thenReturn(false);

        DefaultCodecs codecs = DefaultCodecs.INSTANCE;
        Integer result = codecs.decode(rs, 1, meta(YashanDbType.INTEGER), Integer.class);
        assertThat(result).isEqualTo(99);
    }

    @Test
    void defaultCodecsDispatchesToString() throws Exception {
        when(rs.getString(1)).thenReturn("world");

        DefaultCodecs codecs = DefaultCodecs.INSTANCE;
        String result = codecs.decode(rs, 1, meta(YashanDbType.VARCHAR), String.class);
        assertThat(result).isEqualTo("world");
    }
}
