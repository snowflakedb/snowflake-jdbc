/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class StatementFeatureNotSupportedIT extends BaseJDBCTest
{
  @Test
  public void testFeatureNotSupportedException() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      Statement statement = connection.createStatement();

      try
      {
        statement.execute("select 1", new int[]{});
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        statement.execute("select 1", new String[]{});
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        statement.setCursorName("curname");
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        statement.setFetchDirection(ResultSet.FETCH_REVERSE);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        statement.setFetchDirection(ResultSet.FETCH_UNKNOWN);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        statement.setMaxFieldSize(10);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        statement.closeOnCompletion();
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        statement.isCloseOnCompletion();
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
    }
  }
}
