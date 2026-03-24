package io.r2dbc.yashandb;

import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.r2dbc.yashandb.util.Assert;
import io.r2dbc.yashandb.util.ExceptionFactory;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * R2DBC {@link Statement} implementation for YashanDB.
 *
 * <p>Wraps a JDBC {@link PreparedStatement} and translates R2DBC parameter
 * binding calls into the corresponding {@code setXxx()} invocations on the
 * underlying prepared statement. SQL execution is delegated to a
 * {@link reactor.core.scheduler.Schedulers#boundedElastic()} thread so that
 * the Reactor event loop is never blocked.</p>
 *
 * <h3>Parameter binding</h3>
 * <p>Parameters may be bound by index (0-based in R2DBC) or by name
 * (using {@code :paramName} placeholder syntax). The driver converts R2DBC's
 * 0-based index to JDBC's 1-based index internally.</p>
 *
 * <h3>Batch execution</h3>
 * <p>Call {@link #add()} after setting all parameters for a row to accumulate
 * batches; subsequent calls to {@link #execute()} will execute all batches and
 * emit one {@link Result} per batch entry.</p>
 */
public final class YashanDbStatement implements Statement {

    private static final Logger log = LoggerFactory.getLogger(YashanDbStatement.class);

    private final YashanDbConnection connection;
    private final String sql;

    /** Each element represents one set of bindings (for batch support). */
    private final List<Map<Integer, Object>> bindings = new ArrayList<>();
    /** Bindings for the current (not-yet-added) row. */
    private Map<Integer, Object> currentBindings = new HashMap<>();
    /** Null-type overrides: index -> SQL type constant from {@link Types}. */
    private final Map<Integer, Integer> nullTypes = new HashMap<>();

    /** Whether to return generated keys. */
    private boolean returnGeneratedValues = false;
    private String[] generatedColumns = null;

    /** Column name -> 0-based index cache (built lazily). */
    private Map<String, Integer> parameterNameIndex;

    /** Total number of parameters in the SQL (computed lazily). */
    private int parameterCount = -1;

    YashanDbStatement(YashanDbConnection connection, String sql) {
        this.connection = Objects.requireNonNull(connection, "connection must not be null");
        this.sql = Assert.requireNotEmpty(sql, "sql must not be empty");
    }

    /** Count total '?' + ':name' parameters in the SQL (cached). */
    private int getParameterCount() {
        if (parameterCount < 0) {
            parameterCount = countParameters(sql);
        }
        return parameterCount;
    }

    static int countParameters(String sql) {
        int count = 0;
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '"') {
                char q = c; i++;
                while (i < sql.length() && sql.charAt(i) != q) i++;
                i++;
                continue;
            }
            if (c == '?' ) {
                count++;
            } else if (c == ':' && i + 1 < sql.length() && Character.isLetter(sql.charAt(i + 1))) {
                count++;
                i++;
                while (i < sql.length() && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '_')) i++;
                continue;
            }
            i++;
        }
        return count;
    }

    // -------------------------------------------------------------------------
    // Binding by index (0-based)
    // -------------------------------------------------------------------------

    @Override
    public Statement bind(int index, Object value) {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null (use bindNull for null values)");
        }
        if (value instanceof Class) {
            throw new IllegalArgumentException("Cannot bind a Class object as a parameter value");
        }
        if (index < 0 || index >= getParameterCount()) {
            throw new IndexOutOfBoundsException("Parameter index " + index + " out of range");
        }
        currentBindings.put(index, value);
        return this;
    }

    @Override
    public Statement bind(String name, Object value) {
        Objects.requireNonNull(name, "name must not be null");
        if (value == null) {
            throw new IllegalArgumentException("value must not be null (use bindNull for null values)");
        }
        if (value instanceof Class) {
            throw new IllegalArgumentException("Cannot bind a Class object as a parameter value");
        }
        currentBindings.put(resolveParamIndex(name), value);
        return this;
    }

    @Override
    public Statement bindNull(int index, Class<?> type) {
        Objects.requireNonNull(type, "type must not be null");
        if (index < 0 || index >= getParameterCount()) {
            throw new IndexOutOfBoundsException("Parameter index " + index + " out of range");
        }
        currentBindings.put(index, null);
        nullTypes.put(index, toSqlType(type));
        return this;
    }

    @Override
    public Statement bindNull(String name, Class<?> type) {
        if (name == null) {
            throw new IllegalArgumentException("name must not be null");
        }
        Objects.requireNonNull(type, "type must not be null");
        int index = resolveParamIndex(name);
        currentBindings.put(index, null);
        nullTypes.put(index, toSqlType(type));
        return this;
    }

    // -------------------------------------------------------------------------
    // Batch accumulation
    // -------------------------------------------------------------------------

    @Override
    public Statement add() {
        int total = getParameterCount();
        if (total > 0 && currentBindings.size() < total) {
            throw new IllegalStateException(
                    "Not all parameters have been bound. Expected " + total
                    + " bindings but only " + currentBindings.size() + " were provided.");
        }
        bindings.add(new HashMap<>(currentBindings));
        currentBindings = new HashMap<>();
        nullTypes.clear();
        return this;
    }

    // -------------------------------------------------------------------------
    // Generated values
    // -------------------------------------------------------------------------

    @Override
    public Statement returnGeneratedValues(String... columns) {
        if (columns == null) {
            throw new IllegalArgumentException("columns must not be null");
        }
        this.returnGeneratedValues = true;
        this.generatedColumns = columns;
        return this;
    }

    // -------------------------------------------------------------------------
    // Execution
    // -------------------------------------------------------------------------

    @Override
    public Publisher<? extends Result> execute() {
        int total = getParameterCount();
        // Check for incomplete bindings in current (pending) batch
        if (total > 0 && !currentBindings.isEmpty() && currentBindings.size() < total) {
            throw new IllegalStateException(
                    "Not all parameters have been bound before execute(). Expected " + total
                    + " bindings but only " + currentBindings.size() + " were provided.");
        }
        // Trailing add(): bindings has entries but currentBindings is empty and has parameters
        // This means add() was called but no new bindings were provided for the next batch.
        if (total > 0 && !bindings.isEmpty() && currentBindings.isEmpty()) {
            IllegalStateException ex = new IllegalStateException(
                    "Trailing add() detected: add() was called but no new parameter bindings were provided.");
            return Flux.error(ex);
        }
        // Finalise: if there are pending bindings not yet added, treat as the last batch
        if (!currentBindings.isEmpty()) {
            bindings.add(new HashMap<>(currentBindings));
            currentBindings = new HashMap<>();
        }
        // If no batches at all, execute once with no parameters
        if (bindings.isEmpty()) {
            bindings.add(new HashMap<>());
        }

        List<Map<Integer, Object>> batchCopy = new ArrayList<>(bindings);
        bindings.clear();
        currentBindings = new HashMap<>();

        return Flux.fromIterable(batchCopy)
                .concatMap(params -> executeSingle(params).subscribeOn(Schedulers.boundedElastic()));
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private Mono<YashanDbResult> executeSingle(Map<Integer, Object> params) {
        return Mono.fromCallable(() -> {
            log.debug("Executing SQL: {}", sql);
            PreparedStatement ps = prepareStatement();
            bindParameters(ps, params);

            boolean hasResultSet = ps.execute();
            if (hasResultSet) {
                java.sql.ResultSet rs = ps.getResultSet();
                return YashanDbResult.ofResultSet(rs, ps);
            } else {
                long updateCount = ps.getLargeUpdateCount();
                // Check for generated keys
                if (returnGeneratedValues) {
                    java.sql.ResultSet keys = ps.getGeneratedKeys();
                    return YashanDbResult.ofGeneratedKeys(updateCount, keys, ps);
                }
                ps.close();
                return YashanDbResult.ofUpdateCount(updateCount);
            }
        });
    }

    private PreparedStatement prepareStatement() throws SQLException {
        java.sql.Connection jdbcConn = connection.getJdbcConnection();
        if (returnGeneratedValues && generatedColumns != null && generatedColumns.length > 0) {
            return jdbcConn.prepareStatement(sql, generatedColumns);
        } else if (returnGeneratedValues) {
            return jdbcConn.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS);
        }
        return jdbcConn.prepareStatement(sql);
    }

    private void bindParameters(PreparedStatement ps, Map<Integer, Object> params) throws SQLException {
        for (Map.Entry<Integer, Object> entry : params.entrySet()) {
            int r2dbcIndex = entry.getKey();
            int jdbcIndex = r2dbcIndex + 1; // R2DBC is 0-based, JDBC is 1-based
            Object value = entry.getValue();

            if (value == null) {
                int sqlType = nullTypes.getOrDefault(r2dbcIndex, Types.NULL);
                ps.setNull(jdbcIndex, sqlType);
            } else {
                setParameter(ps, jdbcIndex, value);
            }
        }
    }

    private void setParameter(PreparedStatement ps, int jdbcIndex, Object value) throws SQLException {
        // Unwrap R2DBC Parameter wrapper (e.g. Parameters.in(value), Parameters.in(Class))
        if (value instanceof Parameter param) {
            Object inner = param.getValue();
            if (inner == null) {
                // Typed null via Parameters.in(Class<?>)
                Class<?> javaType = param.getType().getJavaType();
                ps.setNull(jdbcIndex, toSqlType(javaType));
                return;
            }
            value = inner;
        }
        if (value instanceof String s) {
            ps.setString(jdbcIndex, s);
        } else if (value instanceof Boolean b) {
            ps.setBoolean(jdbcIndex, b);
        } else if (value instanceof Byte b) {
            ps.setByte(jdbcIndex, b);
        } else if (value instanceof Short s) {
            ps.setShort(jdbcIndex, s);
        } else if (value instanceof Integer i) {
            ps.setInt(jdbcIndex, i);
        } else if (value instanceof Long l) {
            ps.setLong(jdbcIndex, l);
        } else if (value instanceof Float f) {
            ps.setFloat(jdbcIndex, f);
        } else if (value instanceof Double d) {
            ps.setDouble(jdbcIndex, d);
        } else if (value instanceof BigDecimal bd) {
            ps.setBigDecimal(jdbcIndex, bd);
        } else if (value instanceof LocalDate ld) {
            ps.setDate(jdbcIndex, java.sql.Date.valueOf(ld));
        } else if (value instanceof LocalDateTime ldt) {
            ps.setTimestamp(jdbcIndex, java.sql.Timestamp.valueOf(ldt));
        } else if (value instanceof LocalTime lt) {
            ps.setTime(jdbcIndex, java.sql.Time.valueOf(lt));
        } else if (value instanceof java.sql.Date d) {
            ps.setDate(jdbcIndex, d);
        } else if (value instanceof java.sql.Timestamp t) {
            ps.setTimestamp(jdbcIndex, t);
        } else if (value instanceof byte[] bytes) {
            ps.setBytes(jdbcIndex, bytes);
        } else if (value instanceof io.r2dbc.spi.Blob blob) {
            // Convert R2DBC Blob to JDBC stream
            ps.setBlob(jdbcIndex, new R2dbcBlobInputStream(blob));
        } else if (value instanceof io.r2dbc.spi.Clob clob) {
            ps.setClob(jdbcIndex, new R2dbcClobReader(clob));
        } else {
            // Fallback: let JDBC driver figure it out
            ps.setObject(jdbcIndex, value);
        }
    }

    /**
     * Resolve a named parameter to its 0-based positional index.
     * Supports {@code :name} style placeholders.
     */
    private int resolveParamIndex(String name) {
        if (parameterNameIndex == null) {
            parameterNameIndex = parseParameterNames(sql);
        }
        Integer index = parameterNameIndex.get(name);
        if (index == null) {
            throw new NoSuchElementException(
                    "Parameter '" + name + "' not found in SQL: " + sql);
        }
        return index;
    }

    /**
     * Parse {@code :name} placeholders from SQL and build name → 0-based-index map.
     */
    static Map<String, Integer> parseParameterNames(String sql) {
        Map<String, Integer> result = new HashMap<>();
        int paramIndex = 0;
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            // Skip string literals
            if (c == '\'' || c == '"') {
                char quote = c;
                i++;
                while (i < sql.length() && sql.charAt(i) != quote) {
                    i++;
                }
                i++; // skip closing quote
                continue;
            }
            if (c == ':' && i + 1 < sql.length() && Character.isLetter(sql.charAt(i + 1))) {
                int start = i + 1;
                int end = start;
                while (end < sql.length() && (Character.isLetterOrDigit(sql.charAt(end)) || sql.charAt(end) == '_')) {
                    end++;
                }
                String paramName = sql.substring(start, end);
                result.putIfAbsent(paramName, paramIndex++);
                i = end;
                continue;
            }
            if (c == '?') {
                paramIndex++;
            }
            i++;
        }
        return result;
    }

    private static int toSqlType(Class<?> type) {
        if (type == String.class) return Types.VARCHAR;
        if (type == Boolean.class || type == boolean.class) return Types.BOOLEAN;
        if (type == Byte.class || type == byte.class) return Types.TINYINT;
        if (type == Short.class || type == short.class) return Types.SMALLINT;
        if (type == Integer.class || type == int.class) return Types.INTEGER;
        if (type == Long.class || type == long.class) return Types.BIGINT;
        if (type == Float.class || type == float.class) return Types.FLOAT;
        if (type == Double.class || type == double.class) return Types.DOUBLE;
        if (type == BigDecimal.class) return Types.DECIMAL;
        if (type == LocalDate.class) return Types.DATE;
        if (type == LocalDateTime.class) return Types.TIMESTAMP;
        if (type == LocalTime.class) return Types.TIME;
        if (type == byte[].class) return Types.BINARY;
        if (type == io.r2dbc.spi.Blob.class) return Types.BLOB;
        if (type == io.r2dbc.spi.Clob.class) return Types.CLOB;
        return Types.NULL;
    }
}
