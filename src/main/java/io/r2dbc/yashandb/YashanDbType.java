package io.r2dbc.yashandb;

import io.r2dbc.spi.Type;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Maps JDBC SQL types to R2DBC {@link Type} descriptors and Java types.
 */
public enum YashanDbType implements Type {

    // Numeric
    BIT("BIT", Boolean.class, Types.BIT),
    BOOLEAN("BOOLEAN", Boolean.class, Types.BOOLEAN),
    TINYINT("TINYINT", Byte.class, Types.TINYINT),
    SMALLINT("SMALLINT", Short.class, Types.SMALLINT),
    INTEGER("INTEGER", Integer.class, Types.INTEGER),
    BIGINT("BIGINT", Long.class, Types.BIGINT),
    UTINYINT("UTINYINT", Short.class, Types.SMALLINT),
    USMALLINT("USMALLINT", Integer.class, Types.INTEGER),
    UINTEGER("UINTEGER", Long.class, Types.BIGINT),
    UBIGINT("UBIGINT", BigDecimal.class, Types.NUMERIC),
    FLOAT("FLOAT", Float.class, Types.FLOAT),
    BINARY_FLOAT("BINARY_FLOAT", Float.class, Types.FLOAT),
    DOUBLE("DOUBLE", Double.class, Types.DOUBLE),
    NUMBER("NUMBER", BigDecimal.class, Types.NUMERIC),
    DECIMAL("DECIMAL", BigDecimal.class, Types.DECIMAL),

    // Date/Time
    DATE("DATE", LocalDate.class, Types.DATE),
    SHORTDATE("SHORTDATE", LocalDate.class, Types.DATE),
    TIME("TIME", LocalTime.class, Types.TIME),
    SHORTTIME("SHORTTIME", LocalTime.class, Types.TIME),
    TIMESTAMP("TIMESTAMP", LocalDateTime.class, Types.TIMESTAMP),
    TIMESTAMP_TZ("TIMESTAMP_TZ", OffsetDateTime.class, Types.TIMESTAMP_WITH_TIMEZONE),
    TIMESTAMP_LTZ("TIMESTAMP_LTZ", OffsetDateTime.class, Types.TIMESTAMP_WITH_TIMEZONE),

    // Character
    CHAR("CHAR", String.class, Types.CHAR),
    NCHAR("NCHAR", String.class, Types.NCHAR),
    VARCHAR("VARCHAR", String.class, Types.VARCHAR),
    NVARCHAR("NVARCHAR", String.class, Types.NVARCHAR),
    CLOB("CLOB", io.r2dbc.spi.Clob.class, Types.CLOB),
    NCLOB("NCLOB", io.r2dbc.spi.Clob.class, Types.NCLOB),

    // Binary
    RAW("RAW", byte[].class, Types.BINARY),
    BINARY("BINARY", byte[].class, Types.BINARY),
    VARBINARY("VARBINARY", byte[].class, Types.VARBINARY),
    BLOB("BLOB", io.r2dbc.spi.Blob.class, Types.BLOB),

    // Other
    ROWID("ROWID", String.class, Types.ROWID),
    JSON("JSON", String.class, Types.VARCHAR),
    XMLTYPE("XMLTYPE", String.class, Types.SQLXML),
    CURSOR("CURSOR", Object.class, Types.REF_CURSOR),
    UNKNOWN("UNKNOWN", Object.class, Types.OTHER);

    private final String name;
    private final Class<?> javaType;
    private final int jdbcType;

    YashanDbType(String name, Class<?> javaType, int jdbcType) {
        this.name = name;
        this.javaType = javaType;
        this.jdbcType = jdbcType;
    }

    @Override
    public Class<?> getJavaType() {
        return javaType;
    }

    @Override
    public String getName() {
        return name;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    /**
     * Resolve a {@link YashanDbType} from a JDBC SQL type code and optional type name.
     */
    public static YashanDbType of(int sqlType, String typeName) {
        // Try by type name first (more precise for vendor-specific types)
        if (typeName != null && !typeName.isEmpty()) {
            String upper = typeName.toUpperCase().replace(" ", "_");
            for (YashanDbType t : values()) {
                if (t.name.equalsIgnoreCase(upper)) {
                    return t;
                }
            }
        }
        // Fall back to JDBC type code
        return switch (sqlType) {
            case Types.BIT -> BIT;
            case Types.BOOLEAN -> BOOLEAN;
            case Types.TINYINT -> TINYINT;
            case Types.SMALLINT -> SMALLINT;
            case Types.INTEGER -> INTEGER;
            case Types.BIGINT -> BIGINT;
            case Types.FLOAT -> FLOAT;
            case Types.REAL -> FLOAT;
            case Types.DOUBLE -> DOUBLE;
            case Types.NUMERIC -> NUMBER;
            case Types.DECIMAL -> DECIMAL;
            case Types.DATE -> DATE;
            case Types.TIME -> TIME;
            case Types.TIMESTAMP -> TIMESTAMP;
            case Types.TIMESTAMP_WITH_TIMEZONE -> TIMESTAMP_TZ;
            case Types.CHAR -> CHAR;
            case Types.NCHAR -> NCHAR;
            case Types.VARCHAR, Types.LONGVARCHAR -> VARCHAR;
            case Types.NVARCHAR, Types.LONGNVARCHAR -> NVARCHAR;
            case Types.CLOB -> CLOB;
            case Types.NCLOB -> NCLOB;
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> BINARY;
            case Types.BLOB -> BLOB;
            case Types.ROWID -> ROWID;
            case Types.SQLXML -> XMLTYPE;
            case Types.REF_CURSOR -> CURSOR;
            default -> UNKNOWN;
        };
    }
}
