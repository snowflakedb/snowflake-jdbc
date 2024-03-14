/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core.structs;

import java.sql.SQLData;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Supplier;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class SQLDataCreationHelper {
  public static <T> T create(Class<T> type) throws SQLException {
    Optional<Supplier<SQLData>> typeFactory = SnowflakeObjectTypeFactories.get(type);
    SQLData instance =
        typeFactory
            .map(Supplier::get)
            .orElseGet(() -> createUsingReflection((Class<SQLData>) type));
    return (T) instance;
  }

  private static SQLData createUsingReflection(Class<? extends SQLData> type) {
    try {
      return type.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
