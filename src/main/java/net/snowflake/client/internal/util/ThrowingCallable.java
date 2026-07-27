package net.snowflake.client.internal.util;

@FunctionalInterface
public interface ThrowingCallable<A, T extends Throwable> {
  A call() throws T;
}
