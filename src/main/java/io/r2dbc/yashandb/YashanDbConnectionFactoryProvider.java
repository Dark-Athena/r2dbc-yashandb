package io.r2dbc.yashandb;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

/**
 * R2DBC {@link ConnectionFactoryProvider} for YashanDB.
 *
 * <p>Registered via {@code META-INF/services/io.r2dbc.spi.ConnectionFactoryProvider}
 * and discovered automatically by {@link io.r2dbc.spi.ConnectionFactories}.</p>
 *
 * <p>Supported URL format:
 * <pre>r2dbc:yashandb://user:password@host:port/database</pre>
 * or
 * <pre>r2dbcs:yashandb://user:password@host:port/database</pre>
 * (the {@code s} suffix enables SSL)</p>
 */
public final class YashanDbConnectionFactoryProvider implements ConnectionFactoryProvider {

    /** R2DBC driver identifier used in the URL scheme. */
    public static final String YASHANDB_DRIVER = "yashandb";

    /** Default YashanDB port. */
    static final int DEFAULT_PORT = 1688;

    @Override
    public ConnectionFactory create(ConnectionFactoryOptions options) {
        YashanDbConnectionConfiguration.Builder builder = YashanDbConnectionConfiguration.builder();

        String host = (String) options.getRequiredValue(HOST);
        builder.host(host);

        Object portObj = options.getValue(PORT);
        int port = portObj != null ? (Integer) portObj : DEFAULT_PORT;
        builder.port(port);

        Object dbObj = options.getValue(DATABASE);
        if (dbObj != null) {
            builder.database(dbObj.toString());
        } else {
            builder.database("");
        }

        Object userObj = options.getValue(USER);
        if (userObj != null) {
            builder.username(userObj.toString());
        }

        Object passwordObj = options.getValue(PASSWORD);
        if (passwordObj instanceof CharSequence cs) {
            builder.password(cs);
        }

        // SSL: enabled if driver is "r2dbcs:yashandb" or explicit option
        Object sslObj = options.getValue(SSL);
        if (Boolean.TRUE.equals(sslObj)) {
            builder.ssl(true);
        }

        return new YashanDbConnectionFactory(builder.build());
    }

    @Override
    public boolean supports(ConnectionFactoryOptions options) {
        Object driver = options.getValue(DRIVER);
        return YASHANDB_DRIVER.equals(driver);
    }

    @Override
    public String getDriver() {
        return YASHANDB_DRIVER;
    }
}
