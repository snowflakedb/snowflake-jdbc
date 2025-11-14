package net.snowflake.client.internal.util;

@FunctionalInterface
public interface ThrowingTriFunction<A, B, C, R, T extends Throwable> {
  R apply(A a, B b, C c) throws T;
}
