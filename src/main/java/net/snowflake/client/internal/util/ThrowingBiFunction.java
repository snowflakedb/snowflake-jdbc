package net.snowflake.client.internal.util;

@FunctionalInterface
public interface ThrowingBiFunction<A, B, R, T extends Throwable> {
  R apply(A a, B b) throws T;
}
