package io.r2dbc.yashandb;

import io.r2dbc.spi.ConnectionMetadata;

/**
 * Static {@link ConnectionMetadata} for YashanDB connections.
 */
public final class YashanDbConnectionMetadata implements ConnectionMetadata {

    static final YashanDbConnectionMetadata INSTANCE = new YashanDbConnectionMetadata();

    private YashanDbConnectionMetadata() {}

    @Override
    public String getDatabaseProductName() {
        return "YashanDB";
    }

    @Override
    public String getDatabaseVersion() {
        return "unknown";
    }
}
