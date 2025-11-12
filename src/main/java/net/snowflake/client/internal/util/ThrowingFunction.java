package net.snowflake.client.internal.util;

@FunctionalInterface
public interface ThrowingFunction<A, R, T extends Throwable> {
  R apply(A a) throws T;
}
