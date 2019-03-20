package net.snowflake.client.jdbc;

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResultSetFeatureNotSupportedIT extends BaseJDBCTest
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
  public void testNotSupportedException() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      try (Statement statement = connection.createStatement())
      {
        ResultSet resultSet = statement.executeQuery("select 1");

        expectFeatureNotSupportedException(() -> resultSet.getAsciiStream(1));
        expectFeatureNotSupportedException(() -> resultSet.getBinaryStream(1));

        expectFeatureNotSupportedException(() -> resultSet.getAsciiStream("col1"));
        expectFeatureNotSupportedException(() -> resultSet.getBinaryStream("col2"));

        expectFeatureNotSupportedException(resultSet::getCursorName);
        expectFeatureNotSupportedException(resultSet::beforeFirst);
        expectFeatureNotSupportedException(resultSet::afterLast);
        expectFeatureNotSupportedException(resultSet::first);
        expectFeatureNotSupportedException(resultSet::last);
        expectFeatureNotSupportedException(resultSet::previous);

        expectFeatureNotSupportedException(() -> resultSet.absolute(0));
        expectFeatureNotSupportedException(() -> resultSet.relative(0));
        expectFeatureNotSupportedException(() -> resultSet.relative(0));

        expectFeatureNotSupportedException(resultSet::rowUpdated);
        expectFeatureNotSupportedException(resultSet::rowInserted);
        expectFeatureNotSupportedException(resultSet::rowDeleted);
        expectFeatureNotSupportedException(resultSet::rowUpdated);

        expectFeatureNotSupportedException(() -> resultSet.updateNull(1));
        expectFeatureNotSupportedException(() -> resultSet.updateBoolean(1, true));
        expectFeatureNotSupportedException(() -> resultSet.updateByte(1, (byte) 1));
        expectFeatureNotSupportedException(() -> resultSet.updateShort(1, (short) 2));
        expectFeatureNotSupportedException(() -> resultSet.updateInt(1, 3));
        expectFeatureNotSupportedException(() -> resultSet.updateLong(1, 4L));
        expectFeatureNotSupportedException(() -> resultSet.updateFloat(1, (float) 5.0));
        expectFeatureNotSupportedException(() -> resultSet.updateDouble(1, 6.0));
        expectFeatureNotSupportedException(() -> resultSet.updateBigDecimal(1, new BigDecimal(7)));
        expectFeatureNotSupportedException(() -> resultSet.updateString(1, "test1"));
        expectFeatureNotSupportedException(() -> resultSet.updateBytes(1, new byte[]{}));
        expectFeatureNotSupportedException(() -> resultSet.updateDate(1, new Date(1)));
        expectFeatureNotSupportedException(() -> resultSet.updateTime(1, new Time(0)));
        expectFeatureNotSupportedException(() -> resultSet.updateTimestamp(1, new Timestamp(3)));
        expectFeatureNotSupportedException(() -> resultSet.updateAsciiStream(1, null));
        expectFeatureNotSupportedException(() -> resultSet.updateBinaryStream(1, null));
        expectFeatureNotSupportedException(() -> resultSet.updateCharacterStream(1, null));
        expectFeatureNotSupportedException(() -> resultSet.updateAsciiStream(1, null));
        expectFeatureNotSupportedException(() -> resultSet.updateObject(1, new Object(), 124));
        expectFeatureNotSupportedException(() -> resultSet.updateObject(1, new Object()));

        expectFeatureNotSupportedException(() -> resultSet.updateNull("col1"));
        expectFeatureNotSupportedException(() -> resultSet.updateBoolean("col1", true));
        expectFeatureNotSupportedException(() -> resultSet.updateByte("col1", (byte) 1));
        expectFeatureNotSupportedException(() -> resultSet.updateShort("col1", (short) 2));
        expectFeatureNotSupportedException(() -> resultSet.updateInt("col1", 3));
        expectFeatureNotSupportedException(() -> resultSet.updateLong("col1", 4L));
        expectFeatureNotSupportedException(() -> resultSet.updateFloat("col1", (float) 5.0));
        expectFeatureNotSupportedException(() -> resultSet.updateDouble("col1", 6.0));
        expectFeatureNotSupportedException(() -> resultSet.updateBigDecimal("col1", new BigDecimal(7)));
        expectFeatureNotSupportedException(() -> resultSet.updateString("col1", "test1"));
        expectFeatureNotSupportedException(() -> resultSet.updateBytes("col1", new byte[]{}));
        expectFeatureNotSupportedException(() -> resultSet.updateDate("col1", new Date(0)));
        expectFeatureNotSupportedException(() -> resultSet.updateTime("col1", new Time(0)));
        expectFeatureNotSupportedException(() -> resultSet.updateTimestamp("col1", new Timestamp(3)));
        expectFeatureNotSupportedException(() -> resultSet.updateAsciiStream("col1", null));
        expectFeatureNotSupportedException(() -> resultSet.updateBinaryStream("col1", null));
        expectFeatureNotSupportedException(() -> resultSet.updateCharacterStream("col1", null));
        expectFeatureNotSupportedException(() -> resultSet.updateAsciiStream("col1", null));
        expectFeatureNotSupportedException(() -> resultSet.updateObject("col1", new Object(), 124));
        expectFeatureNotSupportedException(() -> resultSet.updateObject("col1", new Object()));

        expectFeatureNotSupportedException(resultSet::insertRow);
        expectFeatureNotSupportedException(resultSet::updateRow);
        expectFeatureNotSupportedException(resultSet::deleteRow);
        expectFeatureNotSupportedException(resultSet::refreshRow);
        expectFeatureNotSupportedException(resultSet::cancelRowUpdates);
        expectFeatureNotSupportedException(resultSet::moveToInsertRow);
        expectFeatureNotSupportedException(resultSet::moveToCurrentRow);
      }
    }
  }
}
