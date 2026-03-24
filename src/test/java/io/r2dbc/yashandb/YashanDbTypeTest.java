package io.r2dbc.yashandb;

import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link YashanDbType} type mapping.
 */
class YashanDbTypeTest {

    @Test
    void integerMapsCorrectly() {
        YashanDbType type = YashanDbType.of(Types.INTEGER, "INTEGER");
        assertThat(type).isEqualTo(YashanDbType.INTEGER);
        assertThat(type.getJavaType()).isEqualTo(Integer.class);
        assertThat(type.getName()).isEqualTo("INTEGER");
    }

    @Test
    void bigintMapsCorrectly() {
        YashanDbType type = YashanDbType.of(Types.BIGINT, null);
        assertThat(type).isEqualTo(YashanDbType.BIGINT);
        assertThat(type.getJavaType()).isEqualTo(Long.class);
    }

    @Test
    void varcharMapsCorrectly() {
        YashanDbType type = YashanDbType.of(Types.VARCHAR, "VARCHAR");
        assertThat(type).isEqualTo(YashanDbType.VARCHAR);
        assertThat(type.getJavaType()).isEqualTo(String.class);
    }

    @Test
    void timestampMapsCorrectly() {
        YashanDbType type = YashanDbType.of(Types.TIMESTAMP, "TIMESTAMP");
        assertThat(type).isEqualTo(YashanDbType.TIMESTAMP);
        assertThat(type.getJavaType()).isEqualTo(LocalDateTime.class);
    }

    @Test
    void dateMapsCorrectly() {
        YashanDbType type = YashanDbType.of(Types.DATE, "DATE");
        assertThat(type).isEqualTo(YashanDbType.DATE);
        assertThat(type.getJavaType()).isEqualTo(LocalDate.class);
    }

    @Test
    void timeMapsCorrectly() {
        YashanDbType type = YashanDbType.of(Types.TIME, null);
        assertThat(type).isEqualTo(YashanDbType.TIME);
        assertThat(type.getJavaType()).isEqualTo(LocalTime.class);
    }

    @Test
    void timestampTzMapsCorrectly() {
        YashanDbType type = YashanDbType.of(Types.TIMESTAMP_WITH_TIMEZONE, null);
        assertThat(type).isEqualTo(YashanDbType.TIMESTAMP_TZ);
        assertThat(type.getJavaType()).isEqualTo(OffsetDateTime.class);
    }

    @Test
    void blobMapsCorrectly() {
        YashanDbType type = YashanDbType.of(Types.BLOB, "BLOB");
        assertThat(type).isEqualTo(YashanDbType.BLOB);
        assertThat(type.getJavaType()).isEqualTo(io.r2dbc.spi.Blob.class);
    }

    @Test
    void clobMapsCorrectly() {
        YashanDbType type = YashanDbType.of(Types.CLOB, "CLOB");
        assertThat(type).isEqualTo(YashanDbType.CLOB);
        assertThat(type.getJavaType()).isEqualTo(io.r2dbc.spi.Clob.class);
    }

    @Test
    void typeNameTakesPriorityOverJdbcType() {
        // NUMBER has JDBC type NUMERIC (2), but "NUMBER" name should resolve to NUMBER
        YashanDbType type = YashanDbType.of(Types.NUMERIC, "NUMBER");
        assertThat(type).isEqualTo(YashanDbType.NUMBER);
    }

    @Test
    void unknownTypeFallsBack() {
        YashanDbType type = YashanDbType.of(Types.OTHER, "SOME_EXOTIC_TYPE");
        assertThat(type).isEqualTo(YashanDbType.UNKNOWN);
    }

    @Test
    void caseInsensitiveNameLookup() {
        YashanDbType type = YashanDbType.of(Types.INTEGER, "integer");
        assertThat(type).isEqualTo(YashanDbType.INTEGER);
    }
}
