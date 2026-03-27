package io.r2dbc.yashandb;

import io.r2dbc.spi.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link YashanDbStatement} — parameter binding and SQL parsing.
 * These tests do NOT require a live database.
 */
@ExtendWith(MockitoExtension.class)
class YashanDbStatementTest {

    @Mock
    private YashanDbConnection mockConnection;

    @Mock
    private Connection mockJdbcConnection;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @BeforeEach
    void setUp() throws SQLException {
        lenient().when(mockConnection.getJdbcConnection()).thenReturn(mockJdbcConnection);
        lenient().when(mockJdbcConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    }

    // -------------------------------------------------------------------------
    // Batch execution — verifies JDBC addBatch/executeBatch path
    // -------------------------------------------------------------------------

    @Test
    void batchExecutionUsesJdbcBatch() throws SQLException {
        // When there are multiple binding sets, the driver must call addBatch()
        // for each row and executeBatch() once — NOT execute() per row.
        when(mockPreparedStatement.executeBatch()).thenReturn(new int[]{1, 1, 1});

        YashanDbStatement stmt = new YashanDbStatement(mockConnection,
                "INSERT INTO t (a) VALUES (?)");

        stmt.bind(0, 1).add();
        stmt.bind(0, 2).add();
        stmt.bind(0, 3).add();

        List<? extends Result> results = Flux.from(stmt.execute()).collectList().block();

        // addBatch() must have been called 3 times, executeBatch() exactly once
        verify(mockPreparedStatement, times(3)).addBatch();
        verify(mockPreparedStatement, times(1)).executeBatch();
        // execute() (single-row path) must NOT have been called
        verify(mockPreparedStatement, never()).execute();

        assertThat(results).hasSize(3);
    }

    @Test
    void singleBindingSetUsesSingleExecute() throws SQLException {
        // A single binding set must still use the existing execute() path,
        // not JDBC batch.
        when(mockPreparedStatement.execute()).thenReturn(false);
        when(mockPreparedStatement.getLargeUpdateCount()).thenReturn(1L);

        YashanDbStatement stmt = new YashanDbStatement(mockConnection,
                "INSERT INTO t (a) VALUES (?)");
        stmt.bind(0, 42);

        Flux.from(stmt.execute()).blockLast();

        verify(mockPreparedStatement, times(1)).execute();
        verify(mockPreparedStatement, never()).addBatch();
        verify(mockPreparedStatement, never()).executeBatch();
    }

    @Test
    void batchUpdateCountsAreReturnedPerRow() throws SQLException {
        when(mockPreparedStatement.executeBatch()).thenReturn(new int[]{1, 1});

        YashanDbStatement stmt = new YashanDbStatement(mockConnection,
                "INSERT INTO t (a) VALUES (?)");
        stmt.bind(0, "row1").add();
        stmt.bind(0, "row2").add();

        List<Long> counts = Flux.from(stmt.execute())
                .flatMap(r -> r.getRowsUpdated())
                .collectList()
                .block();

        assertThat(counts).containsExactly(1L, 1L);
    }

    @Test
    void nullBindingsArePropagatedCorrectlyInBatch() throws SQLException {
        // Each batch entry must carry its own null-type information; the
        // NullValue wrapper must reach ps.setNull() with the right SQL type.
        when(mockPreparedStatement.executeBatch()).thenReturn(new int[]{1, 1});

        YashanDbStatement stmt = new YashanDbStatement(mockConnection,
                "INSERT INTO t (a) VALUES (?)");
        stmt.bindNull(0, String.class).add();
        stmt.bindNull(0, Integer.class).add();

        Flux.from(stmt.execute()).blockLast();

        verify(mockPreparedStatement, times(2)).addBatch();
        verify(mockPreparedStatement, times(1)).executeBatch();

        // Capture the SQL type constants actually passed to setNull() and verify
        // that each batch entry's null type is preserved independently.
        ArgumentCaptor<Integer> jdbcIndexCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> sqlTypeCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(mockPreparedStatement, times(2)).setNull(jdbcIndexCaptor.capture(), sqlTypeCaptor.capture());

        List<Integer> capturedTypes = sqlTypeCaptor.getAllValues();
        assertThat(capturedTypes).containsExactlyInAnyOrder(Types.VARCHAR, Types.INTEGER);
    }

    // -------------------------------------------------------------------------
    // Named parameter SQL conversion (convertToJdbcSql)
    // -------------------------------------------------------------------------

    @Test
    void convertToJdbcSqlReplacesNamedParameters() {
        String jdbcSql = YashanDbStatement.convertToJdbcSql(
                "SELECT * FROM users WHERE name = :name AND age = :age");
        assertThat(jdbcSql).isEqualTo("SELECT * FROM users WHERE name = ? AND age = ?");
    }

    @Test
    void convertToJdbcSqlLeavesPositionalPlaceholdersUnchanged() {
        String sql = "INSERT INTO t (a, b) VALUES (?, ?)";
        assertThat(YashanDbStatement.convertToJdbcSql(sql)).isEqualTo(sql);
    }

    @Test
    void convertToJdbcSqlSkipsStringLiterals() {
        String jdbcSql = YashanDbStatement.convertToJdbcSql(
                "SELECT ':notAParam' AS x, :realParam FROM t");
        assertThat(jdbcSql).isEqualTo("SELECT ':notAParam' AS x, ? FROM t");
    }

    @Test
    void convertToJdbcSqlHandlesMixedPlaceholders() {
        String jdbcSql = YashanDbStatement.convertToJdbcSql(
                "INSERT INTO t (a, b, c) VALUES (?, :name, ?)");
        assertThat(jdbcSql).isEqualTo("INSERT INTO t (a, b, c) VALUES (?, ?, ?)");
    }

    @Test
    void convertToJdbcSqlHandlesUnderscoreInName() {
        String jdbcSql = YashanDbStatement.convertToJdbcSql(
                "SELECT * FROM t WHERE first_name = :first_name");
        assertThat(jdbcSql).isEqualTo("SELECT * FROM t WHERE first_name = ?");
    }

    @Test
    void convertToJdbcSqlHandlesEscapedQuotesInLiterals() {
        // SQL standard: '' inside a single-quoted string is an escaped single quote
        String jdbcSql = YashanDbStatement.convertToJdbcSql(
                "SELECT * FROM t WHERE name = 'O''Brien' AND id = :id");
        assertThat(jdbcSql).isEqualTo("SELECT * FROM t WHERE name = 'O''Brien' AND id = ?");
    }

    @Test
    void convertToJdbcSqlHandlesNoParameters() {
        String sql = "SELECT 1 FROM dual";
        assertThat(YashanDbStatement.convertToJdbcSql(sql)).isEqualTo(sql);
    }

    @Test
    void namedParameterBindingUsesConvertedSql() throws SQLException {
        // Verify that the PreparedStatement is created with '?' instead of ':name'
        // so that the driver uses a true bind variable.
        when(mockPreparedStatement.execute()).thenReturn(false);
        when(mockPreparedStatement.getLargeUpdateCount()).thenReturn(1L);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(mockJdbcConnection.prepareStatement(sqlCaptor.capture())).thenReturn(mockPreparedStatement);

        YashanDbStatement stmt = new YashanDbStatement(mockConnection,
                "INSERT INTO t (a) VALUES (:val)");
        stmt.bind("val", 42);

        Flux.from(stmt.execute()).blockLast();

        // The SQL passed to JDBC must use '?' not ':val'
        assertThat(sqlCaptor.getValue()).isEqualTo("INSERT INTO t (a) VALUES (?)");
        verify(mockPreparedStatement).setInt(1, 42);
    }

    // -------------------------------------------------------------------------
    // Named parameter parsing
    // -------------------------------------------------------------------------

    @Test
    void parseSimpleNamedParameters() {
        Map<String, Integer> map = YashanDbStatement.parseParameterNames(
                "SELECT * FROM users WHERE name = :name AND age = :age");

        assertThat(map).containsEntry("name", 0)
                       .containsEntry("age", 1);
    }

    @Test
    void parseRepeatedNamedParameter() {
        Map<String, Integer> map = YashanDbStatement.parseParameterNames(
                "SELECT * FROM t WHERE a = :val OR b = :val");

        // :val appears twice but maps to the same index (first occurrence)
        assertThat(map).containsKey("val");
        assertThat(map).hasSize(1);
    }

    @Test
    void parsePositionalPlaceholders() {
        Map<String, Integer> map = YashanDbStatement.parseParameterNames(
                "INSERT INTO t (a, b) VALUES (?, ?)");

        // No named params — map should be empty
        assertThat(map).isEmpty();
    }

    @Test
    void parseSkipsStringLiterals() {
        Map<String, Integer> map = YashanDbStatement.parseParameterNames(
                "SELECT ':notAParam' AS x, :realParam FROM t");

        assertThat(map).containsKey("realParam");
        assertThat(map).doesNotContainKey("notAParam");
    }

    @Test
    void parseMixedPlaceholders() {
        Map<String, Integer> map = YashanDbStatement.parseParameterNames(
                "INSERT INTO t (a, b, c) VALUES (?, :name, ?)");

        // ? at index 0, :name at index 1, ? at index 2
        assertThat(map).containsEntry("name", 1);
    }

    @Test
    void parseEmptySql() {
        Map<String, Integer> map = YashanDbStatement.parseParameterNames("");
        assertThat(map).isEmpty();
    }

    @Test
    void parseNoParameters() {
        Map<String, Integer> map = YashanDbStatement.parseParameterNames(
                "SELECT 1 FROM dual");
        assertThat(map).isEmpty();
    }

    @Test
    void parseUnderscoreInName() {
        Map<String, Integer> map = YashanDbStatement.parseParameterNames(
                "SELECT * FROM t WHERE first_name = :first_name");
        assertThat(map).containsKey("first_name");
    }
}
