package io.r2dbc.yashandb;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link YashanDbStatement} — parameter binding and SQL parsing.
 * These tests do NOT require a live database.
 */
class YashanDbStatementTest {

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
