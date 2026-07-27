package net.snowflake.client.internal.util;

@FunctionalInterface
public interface ThrowingBiCallable<A, B, T extends Throwable> {
  void apply(A a, B b) throws T;
}
