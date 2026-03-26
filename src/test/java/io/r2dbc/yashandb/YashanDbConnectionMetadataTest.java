package io.r2dbc.yashandb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link YashanDbConnectionMetadata}.
 * Verifies that product name and version are read from JDBC {@link DatabaseMetaData}.
 */
@ExtendWith(MockitoExtension.class)
class YashanDbConnectionMetadataTest {

    @Mock
    private DatabaseMetaData jdbcMetaData;

    @Test
    void fromJdbcReturnsDatabaseProductName() throws SQLException {
        when(jdbcMetaData.getDatabaseProductName()).thenReturn("YashanDB");
        when(jdbcMetaData.getDatabaseProductVersion()).thenReturn("22.2.0.1");

        YashanDbConnectionMetadata metadata = YashanDbConnectionMetadata.fromJdbc(jdbcMetaData);

        assertThat(metadata.getDatabaseProductName()).isEqualTo("YashanDB");
    }

    @Test
    void fromJdbcReturnsDatabaseVersion() throws SQLException {
        when(jdbcMetaData.getDatabaseProductName()).thenReturn("YashanDB");
        when(jdbcMetaData.getDatabaseProductVersion()).thenReturn("22.2.0.1");

        YashanDbConnectionMetadata metadata = YashanDbConnectionMetadata.fromJdbc(jdbcMetaData);

        assertThat(metadata.getDatabaseVersion()).isEqualTo("22.2.0.1");
    }

    @Test
    void fromJdbcReflectsActualDriverValues() throws SQLException {
        when(jdbcMetaData.getDatabaseProductName()).thenReturn("OtherDB");
        when(jdbcMetaData.getDatabaseProductVersion()).thenReturn("1.0.0");

        YashanDbConnectionMetadata metadata = YashanDbConnectionMetadata.fromJdbc(jdbcMetaData);

        assertThat(metadata.getDatabaseProductName()).isEqualTo("OtherDB");
        assertThat(metadata.getDatabaseVersion()).isEqualTo("1.0.0");
    }
}
