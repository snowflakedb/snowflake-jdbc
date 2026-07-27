package net.snowflake.client.internal.util;

@FunctionalInterface
public interface ThrowingTriCallable<A, B, C, T extends Throwable> {
  void apply(A a, B b, C c) throws T;
}
