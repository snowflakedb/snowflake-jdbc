package net.snowflake.client;

@FunctionalInterface
public interface ThrowingConsumer<T> {
  void call(T parameter) throws Exception;
}
