package net.snowflake.client.core.structs;

import static net.snowflake.client.util.Predicates.notNull;

import java.sql.SQLData;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class SnowflakeObjectTypeFactories {
  private static final Map<Class<?>, Supplier<SQLData>> factories = new HashMap<>();

  public static synchronized void register(Class<?> type, Supplier<SQLData> factory) {
    notNull(type, "type cannot be null");
    notNull(factory, "factory cannot be null");
    factories.put(type, factory);
  }

  public static synchronized void unregister(Class<?> type) {
    notNull(type, "type cannot be null");
    factories.remove(type);
  }

  public static synchronized Optional<Supplier<SQLData>> get(Class<?> type) {
    notNull(type, "type cannot be null");
    return Optional.ofNullable(factories.get(type));
  }
}
