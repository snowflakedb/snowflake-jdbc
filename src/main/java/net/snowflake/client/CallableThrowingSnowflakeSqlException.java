package net.snowflake.client;

import net.snowflake.client.jdbc.SnowflakeSQLException;

/**
 * A functional interface similar to {@code Callable<V>}, but whose {@code call()}
 * method is permitted to throw a {@link SnowflakeSQLException}.
 *
 * @param <V> the result type of the call method
 */
@FunctionalInterface
public interface CallableThrowingSnowflakeSqlException<V> {
    /**
     * Computes a result, potentially throwing a {@code SnowflakeSQLException}.
     *
     * @return the computed result
     * @throws SnowflakeSQLException if a SQL exception occurs during execution
     */
    V call() throws SnowflakeSQLException;
}
