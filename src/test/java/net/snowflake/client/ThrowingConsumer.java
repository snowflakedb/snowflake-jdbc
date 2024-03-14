package net.snowflake.client;

@FunctionalInterface
public interface ThrowingConsumer<A, T extends Throwable> {
  void accept(A parameter) throws T;
}
