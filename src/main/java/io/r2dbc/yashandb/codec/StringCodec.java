package io.r2dbc.yashandb.codec;

import io.r2dbc.yashandb.YashanDbColumnMetadata;
import io.r2dbc.yashandb.YashanDbType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Codec for character-type columns: CHAR, NCHAR, VARCHAR, NVARCHAR, JSON, XMLTYPE, ROWID.
 */
public final class StringCodec implements Codec<String> {

    public static final StringCodec INSTANCE = new StringCodec();

    private StringCodec() {}

    @Override
    public boolean canDecode(YashanDbColumnMetadata metadata, Class<?> targetType) {
        if (!String.class.isAssignableFrom(targetType)) return false;
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        return jdbcType == Types.CHAR
                || jdbcType == Types.NCHAR
                || jdbcType == Types.VARCHAR
                || jdbcType == Types.NVARCHAR
                || jdbcType == Types.LONGVARCHAR
                || jdbcType == Types.LONGNVARCHAR
                || jdbcType == Types.ROWID
                || jdbcType == Types.SQLXML
                || jdbcType == Types.CLOB
                || jdbcType == Types.NCLOB
                || jdbcType == Types.OTHER;   // JSON maps here
    }

    @Override
    public String decode(ResultSet rs, int jdbcIndex, YashanDbColumnMetadata metadata, Class<? extends String> targetType)
            throws SQLException {
        int jdbcType = ((YashanDbType) metadata.getType()).getJdbcType();
        if (jdbcType == Types.CLOB || jdbcType == Types.NCLOB) {
            java.sql.Clob clob = rs.getClob(jdbcIndex);
            if (clob == null) return null;
            try {
                return clob.getSubString(1, (int) clob.length());
            } finally {
                clob.free();
            }
        }
        return rs.getString(jdbcIndex);
    }
}
