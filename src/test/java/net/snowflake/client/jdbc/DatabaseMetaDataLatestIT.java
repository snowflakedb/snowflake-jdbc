/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.DatabaseMetaDataIT.verifyResultSetMetaDataColumns;
import static net.snowflake.client.jdbc.SnowflakeDatabaseMetaData.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.category.TestCategoryOthers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * DatabaseMetaData test for the latest JDBC driver. This doesn't work for the oldest supported
 * driver. Revisit this tests whenever bumping up the oldest supported driver to examine if the
 * tests still is not applicable. If it is applicable, move tests to DatabaseMetaDataIT so that both
 * the latest and oldest supported driver run the tests.
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

  @Test
  public void testGetStringValueFromColumnDef() throws SQLException {
    Map<String, String> params = getConnectionParameters();
    Properties properties = new Properties();
    for (Map.Entry<?, ?> entry : params.entrySet()) {
      if (entry.getValue() != null) {
        properties.put(entry.getKey(), entry.getValue());
      }
    }
    // test out connection parameter stringsQuoted to remove strings from quotes
    properties.put("stringsQuotedForColumnDef", "true");
    Connection connection = DriverManager.getConnection(params.get("uri"), properties);
    String database = connection.getCatalog();
    String schema = connection.getSchema();
    final String targetTable = "T0";

    connection
        .createStatement()
        .execute(
            "create or replace table "
                + targetTable
                + "(C1 string, C2 string default '', C3 string default 'apples', C4 string default '\"apples\"', C5 int, C6 "
                + "int default 5, C7 string default '''', C8 string default '''apples''''', C9  string default '%')");

    DatabaseMetaData metaData = connection.getMetaData();

    ResultSet resultSet = metaData.getColumns(database, schema, targetTable, "%");
    assertTrue(resultSet.next());
    assertNull(resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("''", resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("'apples'", resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("'\"apples\"'", resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertNull(resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("5", resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("''''", resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("'''apples'''''", resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("'%'", resultSet.getString("COLUMN_DEF"));
  }

  @Test
  public void testGetColumnsNullable() throws Throwable {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";

      connection
          .createStatement()
          .execute(
              "create or replace table "
                  + targetTable
                  + "(C1 int, C2 varchar(100), C3 string default '', C4 number(18,4), C5 double, C6 boolean, "
                  + "C7 date not null, C8 time, C9 timestamp_ntz(7), C10 binary,"
                  + "C11 variant, C12 timestamp_ltz(8), C13 timestamp_tz(3))");

      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet = metaData.getColumns(database, schema, targetTable, "%");
      verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_COLUMNS);

      // C1 metadata
      assertTrue(resultSet.next());
      assertTrue(resultSet.getBoolean("NULLABLE"));
    }
  }
}
