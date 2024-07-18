/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class DatabaseMetaDataResultSetLatestIT extends BaseJDBCTest {

  @Test(expected = SnowflakeLoggedFeatureNotSupportedException.class)
  public void testGetObjectNotSupported() throws SQLException {
    try (Connection con = getConnection();
        Statement st = con.createStatement()) {
      Object[][] rows = {{1.2F}};
      List<String> columnNames = Arrays.asList("float");
      List<String> columnTypeNames = Arrays.asList("FLOAT");
      List<Integer> columnTypes = Arrays.asList(Types.FLOAT);
      try (ResultSet resultSet =
          new SnowflakeDatabaseMetaDataResultSet(
              columnNames, columnTypeNames, columnTypes, rows, st)) {
        resultSet.next();
        assertEquals(1.2F, resultSet.getObject(1));
      }
    }
  }

  /** Added in > 3.17.0 */
  @Test
  public void testObjectColumn() throws SQLException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE OR REPLACE TABLE TABLEWITHOBJECTCOLUMN ("
              + "    col OBJECT("
              + "      str VARCHAR,"
              + "      num NUMBER(38,0)"
              + "      )"
              + "   )");
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet resultSet =
          metaData.getColumns(
              connection.getCatalog(), connection.getSchema(), "TABLEWITHOBJECTCOLUMN", null)) {
        assertTrue(resultSet.next());
        assertEquals("OBJECT", resultSet.getObject("TYPE_NAME"));
        assertFalse(resultSet.next());
      }
    }
  }
}
