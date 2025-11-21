package net.snowflake.client.internal.core.auth.oauth;

public interface StateProvider<T> {
  T getState();
}
