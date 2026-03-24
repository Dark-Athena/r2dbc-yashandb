package io.r2dbc.yashandb;

import io.r2dbc.spi.ConnectionFactoryMetadata;

/**
 * {@link ConnectionFactoryMetadata} for the YashanDB R2DBC driver.
 */
public final class YashanDbConnectionFactoryMetadata implements ConnectionFactoryMetadata {

    static final YashanDbConnectionFactoryMetadata INSTANCE = new YashanDbConnectionFactoryMetadata();

    static final String DRIVER_NAME = "YashanDB";

    private YashanDbConnectionFactoryMetadata() {}

    @Override
    public String getName() {
        return DRIVER_NAME;
    }
}
