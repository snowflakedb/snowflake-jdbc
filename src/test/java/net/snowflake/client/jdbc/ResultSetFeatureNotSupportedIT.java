package net.snowflake.client.jdbc;

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.Assert.fail;

public class ResultSetFeatureNotSupportedIT extends BaseJDBCTest
{
  @Test
  public void testNotSupportedException() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery("select 1");
      resultSet.close();

      try
      {
        resultSet.getAsciiStream(1);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.getBinaryStream(1);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.getAsciiStream("col1");
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.getBinaryStream("col1");
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.getCursorName();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.beforeFirst();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.afterLast();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.first();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.last();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.absolute(0);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.relative(0);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.previous();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.rowUpdated();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.rowInserted();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.rowDeleted();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateNull(1);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateBoolean(1, true);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateByte(1, (byte) 0x20);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateShort(1, (short) 1);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateInt(1, 2);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateLong(1, 3L);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateFloat(1, (float) 1.0);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateDouble(1, 2.0);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateBigDecimal(1, new BigDecimal(1));
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateString(1, "test1");
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateBytes(1, new byte[]{});
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateDate(1, new Date(2));
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateTime(1, new Time(1));
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateTimestamp(1, new Timestamp(3));
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateAsciiStream(1, null, 0);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateBinaryStream(1, null, 0);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateCharacterStream(1, null, 0);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateObject(1, new Object(), 134);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateObject(1, new Object());
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateNull("col1");
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateBoolean("col1", true);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateByte("col1", (byte) 0x20);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateShort("col1", (short) 1);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateInt("col1", 2);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateLong("col1", 3L);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateFloat("col1", (float) 1.0);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateDouble("col1", 2.0);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateBigDecimal("col1", new BigDecimal(1));
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateString("col1", "test1");
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateBytes("col1", new byte[]{});
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateDate("col1", new Date(2));
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateTime("col1", new Time(1));
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateTimestamp("col1", new Timestamp(3));
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateAsciiStream("col1", null, 0);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateBinaryStream("col1", null, 0);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateCharacterStream("col1", null, 0);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateObject("col1", new Object(), 134);
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateObject("col1", new Object());
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.insertRow();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.updateRow();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.deleteRow();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.refreshRow();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.cancelRowUpdates();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.moveToInsertRow();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
      try
      {
        resultSet.moveToCurrentRow();
        fail("must raise exception");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }
    }
  }
}
