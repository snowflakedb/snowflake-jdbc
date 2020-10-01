/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.*;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import net.snowflake.client.category.TestCategoryOthers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryOthers.class)
public class DatabaseMetaDataResultsetIT extends BaseJDBCTest {
  private static final int columnCount = 7;
  private static final int INT_DATA = 1;
  private static final String TEXT_DATA = "TEST";
  private static final String EPOCH_DATE = "1970-01-01";
  private static final double DOUBLE_DATA = 9.5555555;
  private static final double DELTA = 0.0001;
  private static final long NOW = System.currentTimeMillis();
  private static final Time TIME_DATA = new Time(NOW);
  private static final Timestamp TIMESTAMP_DATA = new Timestamp(NOW);
  private static final boolean BOOLEAN_DATA = true;
  private static final List<String> columnNames =
      Arrays.asList("int", "text", "date", "double", "time", "timestamp", "bool");
  private static final List<String> columnTypeNames =
      Arrays.asList("INTEGER", "TEXT", "DATA", "DOUBLE", "TIME", "TIMESTAMP", "BOOLEAN");
  private static final List<Integer> columnTypes =
      Arrays.asList(
          Types.INTEGER,
          Types.VARCHAR,
          Types.DATE,
          Types.DOUBLE,
          Types.TIME,
          Types.TIMESTAMP,
          Types.BOOLEAN);
  private static final Object[][] rows = {
    {
      INT_DATA,
      TEXT_DATA,
      Date.valueOf(EPOCH_DATE),
      DOUBLE_DATA,
      TIME_DATA,
      TIMESTAMP_DATA,
      BOOLEAN_DATA
    },
    {0, null, null, 0, null, null, false}
  };

  @Test
  public void testRowIndex() throws SQLException {
    ResultSet resultSet = getResultSet(false);

    assertEquals(columnCount, resultSet.getMetaData().getColumnCount());
    assertEquals(-1, resultSet.getRow());
    assertTrue(resultSet.isBeforeFirst());
    assertFalse(resultSet.isFirst());

    assertTrue(resultSet.next());
    assertEquals(0, resultSet.getRow());

    assertFalse(resultSet.isBeforeFirst());
    assertTrue(resultSet.isFirst());

    assertTrue(resultSet.next());
    assertEquals(1, resultSet.getRow());

    assertTrue(resultSet.isLast());
    assertFalse(resultSet.isAfterLast());

    assertFalse(resultSet.next());
    assertEquals(2, resultSet.getRow());

    assertFalse(resultSet.isLast());
    assertTrue(resultSet.isAfterLast());
  }

  private ResultSet getResultSet(boolean doNext) throws SQLException {
    Connection con = getConnection();
    Statement st = con.createStatement();
    ResultSet resultSet =
        new SnowflakeDatabaseMetaDataResultSet(columnNames, columnTypeNames, columnTypes, rows, st);
    if (doNext) {
      resultSet.next();
    }
    return resultSet;
  }

  @Test
  public void testGetInt() throws SQLException {
    ResultSet resultSet = getResultSet(true);
    assertEquals(INT_DATA, resultSet.getInt("int"));
  }

  @Test
  public void testGetString() throws SQLException {
    ResultSet resultSet = getResultSet(true);
    assertEquals(TEXT_DATA, resultSet.getString("text"));
  }

  @Test
  public void testGetDate() throws SQLException {
    ResultSet resultSet = getResultSet(true);
    assertEquals(Date.valueOf(EPOCH_DATE), resultSet.getDate("date"));
  }

  @Test
  public void testGetDouble() throws SQLException {
    ResultSet resultSet = getResultSet(true);
    assertEquals(DOUBLE_DATA, resultSet.getDouble("double"), DELTA);
  }

  @Test
  public void testGetTime() throws SQLException {
    ResultSet resultSet = getResultSet(true);
    assertEquals(TIME_DATA, resultSet.getTime("time"));
  }

  @Test
  public void testGetTimestamp() throws SQLException {
    ResultSet resultSet = getResultSet(true);
    assertEquals(TIMESTAMP_DATA, resultSet.getTimestamp("timestamp"));
  }

  @Test
  public void testGetBoolean() throws SQLException {
    ResultSet resultSet = getResultSet(true);
    assertEquals(BOOLEAN_DATA, resultSet.getBoolean("bool"));
  }
}
