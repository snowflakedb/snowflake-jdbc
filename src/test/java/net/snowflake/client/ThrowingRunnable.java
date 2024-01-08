package net.snowflake.client;

@FunctionalInterface
public interface ThrowingRunnable {
  void run() throws Exception;
}
