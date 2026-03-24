package io.r2dbc.yashandb;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.Type;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * R2DBC {@link ColumnMetadata} implementation for a single YashanDB column.
 */
public final class YashanDbColumnMetadata implements ColumnMetadata {

    private final String name;
    private final YashanDbType type;
    private final Nullability nullability;
    private final Integer precision;
    private final Integer scale;

    private YashanDbColumnMetadata(
            String name,
            YashanDbType type,
            Nullability nullability,
            Integer precision,
            Integer scale) {
        this.name = name;
        this.type = type;
        this.nullability = nullability;
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * Build a {@link YashanDbColumnMetadata} from JDBC {@link ResultSetMetaData} at column {@code index} (1-based).
     */
    public static YashanDbColumnMetadata fromJdbc(ResultSetMetaData rsmd, int index) throws SQLException {
        String name = rsmd.getColumnLabel(index);
        if (name == null || name.isEmpty()) {
            name = rsmd.getColumnName(index);
        }

        int sqlType = rsmd.getColumnType(index);
        String typeName = rsmd.getColumnTypeName(index);
        YashanDbType type = YashanDbType.of(sqlType, typeName);

        int nullable = rsmd.isNullable(index);
        Nullability nullability;
        if (nullable == ResultSetMetaData.columnNullable) {
            nullability = Nullability.NULLABLE;
        } else if (nullable == ResultSetMetaData.columnNoNulls) {
            nullability = Nullability.NON_NULL;
        } else {
            nullability = Nullability.UNKNOWN;
        }

        int precision = rsmd.getPrecision(index);
        int scale = rsmd.getScale(index);

        return new YashanDbColumnMetadata(
                name,
                type,
                nullability,
                precision > 0 ? precision : null,
                scale > 0 ? scale : null
        );
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public Nullability getNullability() {
        return nullability;
    }

    @Override
    public Integer getPrecision() {
        return precision;
    }

    @Override
    public Integer getScale() {
        return scale;
    }

    @Override
    public Class<?> getJavaType() {
        return type.getJavaType();
    }

    @Override
    public String toString() {
        return "YashanDbColumnMetadata{name='" + name + "', type=" + type + ", nullability=" + nullability + '}';
    }
}
