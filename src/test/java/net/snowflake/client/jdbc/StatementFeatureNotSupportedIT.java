/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import net.snowflake.client.category.TestCategoryStatement;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryStatement.class)
public class StatementFeatureNotSupportedIT extends BaseJDBCTest {
  @Test
  public void testFeatureNotSupportedException() throws Throwable {
    try (Connection connection = getConnection()) {
      try (Statement statement = connection.createStatement()) {
        expectFeatureNotSupportedException(() -> statement.execute("select 1", new int[] {}));
        expectFeatureNotSupportedException(() -> statement.execute("select 1", new String[] {}));
        expectFeatureNotSupportedException(
            () ->
                statement.executeUpdate(
                    "insert into a values(1)", Statement.RETURN_GENERATED_KEYS));
        expectFeatureNotSupportedException(
            () -> statement.executeUpdate("insert into a values(1)", new int[] {}));
        expectFeatureNotSupportedException(
            () -> statement.executeUpdate("insert into a values(1)", new String[] {}));
        expectFeatureNotSupportedException(
            () ->
                statement.executeLargeUpdate(
                    "insert into a values(1)", Statement.RETURN_GENERATED_KEYS));
        expectFeatureNotSupportedException(
            () -> statement.executeLargeUpdate("insert into a values(1)", new int[] {}));
        expectFeatureNotSupportedException(
            () -> statement.executeLargeUpdate("insert into a values(1)", new String[] {}));
        expectFeatureNotSupportedException(() -> statement.setCursorName("curname"));
        expectFeatureNotSupportedException(
            () -> statement.setFetchDirection(ResultSet.FETCH_REVERSE));
        expectFeatureNotSupportedException(
            () -> statement.setFetchDirection(ResultSet.FETCH_UNKNOWN));
        expectFeatureNotSupportedException(() -> statement.setMaxFieldSize(10));
        expectFeatureNotSupportedException(statement::closeOnCompletion);
        expectFeatureNotSupportedException(statement::isCloseOnCompletion);
      }
    }
  }
}
