/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StatementFeatureNotSupportedIT extends BaseJDBCTest
{
  private void expectFeatureNotSupportedException(MethodRaisesSQLException f)
  {
    try
    {
      f.run();
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertTrue(ex instanceof SQLFeatureNotSupportedException);
    }
  }

  @Test
  public void testFeatureNotSupportedException() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      try (Statement statement = connection.createStatement())
      {
        expectFeatureNotSupportedException(() -> statement.execute("select 1", new int[]{}));
        expectFeatureNotSupportedException(() -> statement.execute("select 1", new String[]{}));
        expectFeatureNotSupportedException(() -> statement.setCursorName("curname"));
        expectFeatureNotSupportedException(() -> statement.setFetchDirection(ResultSet.FETCH_REVERSE));
        expectFeatureNotSupportedException(() -> statement.setFetchDirection(ResultSet.FETCH_UNKNOWN));
        expectFeatureNotSupportedException(() -> statement.setMaxFieldSize(10));
        expectFeatureNotSupportedException(statement::closeOnCompletion);
        expectFeatureNotSupportedException(statement::isCloseOnCompletion);
      }
    }
  }
}
