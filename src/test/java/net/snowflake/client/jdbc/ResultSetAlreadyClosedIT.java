/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import net.snowflake.client.category.TestCategoryResultSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryResultSet.class)
public class ResultSetAlreadyClosedIT extends BaseJDBCTest {
  @Test
  public void testQueryResultSetAlreadyClosed() throws Throwable {
    try (Connection connection = getConnection()) {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery("select 1");
      checkAlreadyClosed(resultSet);
    }
  }

  @Test
  public void testMetadataResultSetAlreadyClosed() throws Throwable {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      DatabaseMetaData metaData = connection.getMetaData();

      checkAlreadyClosed(metaData.getCatalogs());
      checkAlreadyClosed(metaData.getSchemas());
      checkAlreadyClosed(metaData.getSchemas(database, null));
      checkAlreadyClosed(metaData.getTables(database, schema, null, null));
      checkAlreadyClosed(metaData.getColumns(database, schema, null, null));
    }
  }

  @Test
  public void testEmptyResultSetAlreadyClosed() throws Throwable {
    ResultSet resultSet = new SnowflakeResultSetV1.EmptyResultSet();
    checkAlreadyClosed(resultSet);
  }

  private void checkAlreadyClosed(ResultSet resultSet) throws SQLException {
    resultSet.close();
    resultSet.close(); // second close won't raise exception
    assertTrue(resultSet.isClosed());
    assertFalse(resultSet.next()); // next after close should return false.

    expectResultSetAlreadyClosedException(resultSet::wasNull);
    expectResultSetAlreadyClosedException(() -> resultSet.getString(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getBoolean(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getByte(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getShort(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getInt(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getLong(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getFloat(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getDouble(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getBigDecimal(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getBytes(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getString(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getDate(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getTime(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getTimestamp(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getObject(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getCharacterStream(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getClob(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getDate(1, Calendar.getInstance()));
    expectResultSetAlreadyClosedException(() -> resultSet.getTime(1, Calendar.getInstance()));
    expectResultSetAlreadyClosedException(() -> resultSet.getTimestamp(1, Calendar.getInstance()));

    expectResultSetAlreadyClosedException(() -> resultSet.getString("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getBoolean("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getByte("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getShort("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getInt("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getLong("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getFloat("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getDouble("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getBigDecimal("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getBytes("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getString("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getDate("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getTime("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getTimestamp("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getObject("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getCharacterStream("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getClob("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getDate("col1", Calendar.getInstance()));
    expectResultSetAlreadyClosedException(() -> resultSet.getTime("col1", Calendar.getInstance()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.getTimestamp("col1", Calendar.getInstance()));

    expectResultSetAlreadyClosedException(resultSet::getWarnings);
    expectResultSetAlreadyClosedException(resultSet::clearWarnings);
    expectResultSetAlreadyClosedException(resultSet::getMetaData);

    expectResultSetAlreadyClosedException(() -> resultSet.findColumn("col1"));

    expectResultSetAlreadyClosedException(resultSet::isBeforeFirst);
    expectResultSetAlreadyClosedException(resultSet::isAfterLast);
    expectResultSetAlreadyClosedException(resultSet::isFirst);
    expectResultSetAlreadyClosedException(resultSet::isLast);
    expectResultSetAlreadyClosedException(resultSet::getRow);

    expectResultSetAlreadyClosedException(
        () -> resultSet.setFetchDirection(ResultSet.FETCH_FORWARD));
    expectResultSetAlreadyClosedException(() -> resultSet.setFetchSize(10));
    expectResultSetAlreadyClosedException(resultSet::getFetchDirection);
    expectResultSetAlreadyClosedException(resultSet::getFetchSize);
    expectResultSetAlreadyClosedException(resultSet::getType);
    expectResultSetAlreadyClosedException(resultSet::getConcurrency);
    expectResultSetAlreadyClosedException(resultSet::getHoldability);
    expectResultSetAlreadyClosedException(resultSet::getStatement);
  }
}
