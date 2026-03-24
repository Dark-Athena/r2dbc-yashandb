package io.r2dbc.yashandb.util;

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcRollbackException;
import io.r2dbc.spi.R2dbcTransientResourceException;

import java.sql.SQLException;

/**
 * Converts YashanDB / JDBC exceptions into the appropriate {@link R2dbcException} subclass.
 */
public final class ExceptionFactory {

    private ExceptionFactory() {}

    /**
     * Convert a {@link SQLException} (from the underlying JDBC/protocol layer)
     * into the most appropriate {@link R2dbcException} subclass.
     *
     * <p>SQL State classification follows the SQL-92 standard:
     * <ul>
     *   <li>{@code 08xxx} – connection / resource errors</li>
     *   <li>{@code 22xxx} – data exception</li>
     *   <li>{@code 23xxx} – integrity constraint violation</li>
     *   <li>{@code 28xxx} – invalid authorization</li>
     *   <li>{@code 40xxx} – transaction rollback</li>
     *   <li>{@code 42xxx} – syntax error or access rule violation</li>
     * </ul>
     * </p>
     *
     * @param e the JDBC exception
     * @return an appropriate R2DBC exception
     */
    public static R2dbcException convert(SQLException e) {
        String sqlState = e.getSQLState();
        String message = e.getMessage();
        int errorCode = e.getErrorCode();

        if (sqlState == null) {
            return new R2dbcNonTransientResourceException(message, sqlState, errorCode, e);
        }

        return switch (sqlState.substring(0, Math.min(2, sqlState.length()))) {
            case "08" -> new R2dbcNonTransientResourceException(message, sqlState, errorCode, e);
            case "22" -> new R2dbcDataIntegrityViolationException(message, sqlState, errorCode, e);
            case "23" -> new R2dbcDataIntegrityViolationException(message, sqlState, errorCode, e);
            case "28" -> new R2dbcPermissionDeniedException(message, sqlState, errorCode, e);
            case "40" -> new R2dbcRollbackException(message, sqlState, errorCode, e);
            case "42" -> new R2dbcBadGrammarException(message, sqlState, errorCode, e);
            default -> sqlState.startsWith("08") || isTransient(sqlState)
                    ? new R2dbcTransientResourceException(message, sqlState, errorCode, e)
                    : new R2dbcNonTransientResourceException(message, sqlState, errorCode, e);
        };
    }

    /**
     * Wrap a generic {@link Exception} that is not a {@link SQLException}.
     */
    public static R2dbcNonTransientResourceException wrap(String message, Throwable cause) {
        return new R2dbcNonTransientResourceException(message, cause);
    }

    private static boolean isTransient(String sqlState) {
        // 57 = operator intervention / resource unavailable (transient)
        return sqlState.startsWith("57") || sqlState.startsWith("53");
    }
}
