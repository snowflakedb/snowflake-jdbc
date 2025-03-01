package net.snowflake.client.core.structs;

import java.sql.SQLData;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class SnowflakeObjectTypeFactories {
  private static final Map<Class<?>, Supplier<SQLData>> factories = new ConcurrentHashMap<>();

  public static void register(Class<?> type, Supplier<SQLData> factory) {
    Objects.requireNonNull((Object) type, "type cannot be null");
    Objects.requireNonNull((Object) factory, "factory cannot be null");
    factories.put(type, factory);
  }

  public static void unregister(Class<?> type) {
    Objects.requireNonNull((Object) type, "type cannot be null");
    factories.remove(type);
  }

  public static Optional<Supplier<SQLData>> get(Class<?> type) {
    Objects.requireNonNull((Object) type, "type cannot be null");
    return Optional.ofNullable(factories.get(type));
  }
}
