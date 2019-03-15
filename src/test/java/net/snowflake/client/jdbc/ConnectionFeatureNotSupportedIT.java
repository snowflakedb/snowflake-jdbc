/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class ConnectionFeatureNotSupportedIT extends BaseJDBCTest
{

  @Test
  public void testFeatureNotSupportedException() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      try
      {
        connection.prepareCall("select 1");
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }

      try
      {
        connection.prepareCall(
            "select 1",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }

      try
      {
        connection.prepareCall(
            "select 1",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.HOLD_CURSORS_OVER_COMMIT);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }

      try
      {
        connection.rollback(new FakeSavepoint());
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.prepareStatement("select 1", 0);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        int[] columnIndexes = new int[]{1, 2};
        connection.prepareStatement("select 1", columnIndexes);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        String[] columnNames = new String[]{"c1", "c2"};
        connection.prepareStatement("select 1", columnNames);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.prepareStatement("select 1", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        Map<String, Class<?>> m = new HashMap<>();
        connection.setTypeMap(m);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.setSavepoint();
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.setSavepoint("fake");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.releaseSavepoint(new FakeSavepoint());
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.createBlob();
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.createNClob();
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.createSQLXML();
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.createArrayOf("fakeType", new Object[]{});
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.createStruct("fakeType", new Object[]{});
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        connection.createStruct("fakeType", new Object[]{});
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
    }
  }

  class FakeSavepoint implements Savepoint
  {
    @Override
    public int getSavepointId() throws SQLException
    {
      return 0;
    }

    @Override
    public String getSavepointName() throws SQLException
    {
      return "";
    }
  }

}
