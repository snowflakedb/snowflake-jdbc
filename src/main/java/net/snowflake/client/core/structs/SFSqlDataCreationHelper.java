package net.snowflake.client.core.structs;

import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Supplier;

public class SFSqlDataCreationHelper {
  public static <T> T create(Class<T> type) throws SQLException {
    Optional<Supplier<SFSqlData>> typeFactory = SnowflakeObjectTypeFactories.get(type);
    SFSqlData instance =
        typeFactory
            .map(Supplier::get)
            .orElseGet(() -> createUsingReflection((Class<SFSqlData>) type));
    return (T) instance;
  }

  private static SFSqlData createUsingReflection(Class<? extends SFSqlData> type) {
    try {
      return type.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
