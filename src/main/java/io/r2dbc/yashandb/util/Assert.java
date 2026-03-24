package io.r2dbc.yashandb.util;

/**
 * Simple assertion utilities to validate method arguments without extra dependencies.
 */
public final class Assert {

    private Assert() {}

    /**
     * Assert that the given object is not {@code null}.
     *
     * @param value   the value to check
     * @param message the error message if null
     * @param <T>     the type of the value
     * @return the value itself (for chaining)
     * @throws IllegalArgumentException if {@code value} is null
     */
    public static <T> T requireNonNull(T value, String message) {
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    /**
     * Assert that the given string is not {@code null} and not empty.
     *
     * @param value   the string to check
     * @param message the error message
     * @return the value itself
     */
    public static String requireNotEmpty(String value, String message) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    /**
     * Assert that the given condition is {@code true}.
     *
     * @param condition the condition to check
     * @param message   the error message if {@code false}
     */
    public static void isTrue(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
