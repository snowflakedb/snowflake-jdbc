/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeDatabaseMetaData.*;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import net.snowflake.client.category.TestCategoryOthers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * DatabaseMetaData test for the latest JDBC driver but not the oldest supported driver. Revisit
 * this tests whenever bumping up the oldest supported driver to examine if the tests still is not
 * applicable. If it is applicable, move tests to DatabaseMetaDataIT so that both the latest and
 * oldest supported driver run the tests.
 */
@Category(TestCategoryOthers.class)
public class DatabaseMetaDataLatestIT extends BaseJDBCTest {

  /**
   * Tests for getFunctions
   *
   * @throws SQLException arises if any error occurs
   */
  @Test
  public void testGetFunctions() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metadata = connection.getMetaData();
      String supportedStringFuncs = metadata.getStringFunctions();
      assertEquals(StringFunctionsSupported, supportedStringFuncs);

      String supportedNumberFuncs = metadata.getNumericFunctions();
      assertEquals(NumericFunctionsSupported, supportedNumberFuncs);

      String supportedSystemFuncs = metadata.getSystemFunctions();
      assertEquals(SystemFunctionsSupported, supportedSystemFuncs);
    }
  }
}
