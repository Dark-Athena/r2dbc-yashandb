package io.r2dbc.yashandb;

import io.r2dbc.spi.ConnectionMetadata;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * {@link ConnectionMetadata} for YashanDB connections, backed by JDBC {@link DatabaseMetaData}.
 */
public final class YashanDbConnectionMetadata implements ConnectionMetadata {

    private final String databaseProductName;
    private final String databaseVersion;

    private YashanDbConnectionMetadata(String databaseProductName, String databaseVersion) {
        this.databaseProductName = databaseProductName;
        this.databaseVersion = databaseVersion;
    }

    /**
     * Create a {@link YashanDbConnectionMetadata} from JDBC {@link DatabaseMetaData}.
     *
     * @param dbmd the JDBC database metadata
     * @return a new {@link YashanDbConnectionMetadata} instance
     * @throws SQLException if a database access error occurs
     */
    static YashanDbConnectionMetadata fromJdbc(DatabaseMetaData dbmd) throws SQLException {
        return new YashanDbConnectionMetadata(
                dbmd.getDatabaseProductName(),
                dbmd.getDatabaseProductVersion()
        );
    }

    @Override
    public String getDatabaseProductName() {
        return databaseProductName;
    }

    @Override
    public String getDatabaseVersion() {
        return databaseVersion;
    }
}
