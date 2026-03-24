package io.r2dbc.yashandb;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link YashanDbConnectionFactory} and {@link YashanDbConnectionFactoryProvider}.
 */
class YashanDbConnectionFactoryTest {

    @Test
    void providerSupportsYashanDbDriver() {
        YashanDbConnectionFactoryProvider provider = new YashanDbConnectionFactoryProvider();
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
                .option(DRIVER, "yashandb")
                .option(HOST, "localhost")
                .option(PORT, 1688)
                .option(DATABASE, "testdb")
                .option(USER, "admin")
                .option(PASSWORD, "secret")
                .build();

        assertThat(provider.supports(options)).isTrue();
    }

    @Test
    void providerDoesNotSupportOtherDrivers() {
        YashanDbConnectionFactoryProvider provider = new YashanDbConnectionFactoryProvider();
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
                .option(DRIVER, "postgresql")
                .option(HOST, "localhost")
                .option(DATABASE, "testdb")
                .option(USER, "admin")
                .option(PASSWORD, "secret")
                .build();

        assertThat(provider.supports(options)).isFalse();
    }

    @Test
    void getDriverReturnsYashandb() {
        assertThat(new YashanDbConnectionFactoryProvider().getDriver()).isEqualTo("yashandb");
    }

    @Test
    void providerCreatesFactoryWithFullOptions() {
        YashanDbConnectionFactoryProvider provider = new YashanDbConnectionFactoryProvider();
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
                .option(DRIVER, "yashandb")
                .option(HOST, "db.example.com")
                .option(PORT, 1688)
                .option(DATABASE, "mydb")
                .option(USER, "scott")
                .option(PASSWORD, "tiger")
                .option(SSL, false)
                .build();

        ConnectionFactory factory = provider.create(options);

        assertThat(factory).isInstanceOf(YashanDbConnectionFactory.class);
        YashanDbConnectionFactory yFactory = (YashanDbConnectionFactory) factory;
        YashanDbConnectionConfiguration cfg = yFactory.getConfiguration();

        assertThat(cfg.getHost()).isEqualTo("db.example.com");
        assertThat(cfg.getPort()).isEqualTo(1688);
        assertThat(cfg.getDatabase()).isEqualTo("mydb");
        assertThat(cfg.getUsername()).isEqualTo("scott");
        assertThat(cfg.getPassword().toString()).isEqualTo("tiger");
        assertThat(cfg.isSsl()).isFalse();
    }

    @Test
    void providerDefaultPort() {
        YashanDbConnectionFactoryProvider provider = new YashanDbConnectionFactoryProvider();
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
                .option(DRIVER, "yashandb")
                .option(HOST, "localhost")
                .option(DATABASE, "mydb")
                .option(USER, "u")
                .option(PASSWORD, "p")
                .build();

        YashanDbConnectionFactory factory = (YashanDbConnectionFactory) provider.create(options);
        assertThat(factory.getConfiguration().getPort()).isEqualTo(1688);
    }

    @Test
    void factoryMetadata() {
        YashanDbConnectionFactory factory = new YashanDbConnectionFactory(
                YashanDbConnectionConfiguration.builder()
                        .host("localhost")
                        .port(1688)
                        .database("testdb")
                        .username("user")
                        .password("pass")
                        .build());

        assertThat(factory.getMetadata().getName()).isEqualTo("YashanDB");
    }

    @Test
    void configurationToJdbcUrl() {
        YashanDbConnectionConfiguration cfg = YashanDbConnectionConfiguration.builder()
                .host("myhost")
                .port(5678)
                .database("myschema")
                .username("user")
                .password("pass")
                .build();

        assertThat(cfg.toJdbcUrl()).isEqualTo("jdbc:yasdb://myhost:5678/myschema");
    }

    @Test
    void configurationBuilderRequiresDatabase() {
        assertThatThrownBy(() ->
                YashanDbConnectionConfiguration.builder()
                        .host("localhost")
                        .username("user")
                        .password("pass")
                        .build()
        ).isInstanceOf(NullPointerException.class);
    }

    @Test
    void configurationBuilderInvalidPort() {
        assertThatThrownBy(() ->
                YashanDbConnectionConfiguration.builder()
                        .port(0)
        ).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() ->
                YashanDbConnectionConfiguration.builder()
                        .port(70000)
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void discoveredViaSpi() {
        // This verifies META-INF/services registration works
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
                .option(DRIVER, "yashandb")
                .option(HOST, "localhost")
                .option(DATABASE, "testdb")
                .option(USER, "u")
                .option(PASSWORD, "p")
                .build();

        ConnectionFactory factory = ConnectionFactories.get(options);
        assertThat(factory).isInstanceOf(YashanDbConnectionFactory.class);
    }
}
