/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class DatabaseMetaDataResultSetLatestIT extends BaseJDBCTest {

  @Test(expected = SnowflakeLoggedFeatureNotSupportedException.class)
  public void testGetObjectNotSupported() throws SQLException {
    Connection con = getConnection();
    Statement st = con.createStatement();
    Object[][] rows = {{1.2F}};
    List<String> columnNames = Arrays.asList("float");
    List<String> columnTypeNames = Arrays.asList("FLOAT");
    List<Integer> columnTypes = Arrays.asList(Types.FLOAT);
    ResultSet resultSet =
        new SnowflakeDatabaseMetaDataResultSet(columnNames, columnTypeNames, columnTypes, rows, st);
    resultSet.next();
    assertEquals(1.2F, resultSet.getObject(1));
  }
}
