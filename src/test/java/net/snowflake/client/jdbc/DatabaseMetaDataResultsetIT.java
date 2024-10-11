/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

//@Category(TestCategoryOthers.class)
public class DatabaseMetaDataResultsetIT extends BaseJDBCTest {
  private static final int columnCount = 9;
  private static final int INT_DATA = 1;
  private static final String TEXT_DATA = "TEST";
  private static final String EPOCH_DATE = "1970-01-01";
  private static final double DOUBLE_DATA = 9.5555555;
  private static final double DELTA = 0.0001;
  private static final long NOW = System.currentTimeMillis();
  private static final Time TIME_DATA = new Time(NOW);
  private static final Timestamp TIMESTAMP_DATA = new Timestamp(NOW);
  private static final boolean BOOLEAN_DATA = true;
  private static final BigDecimal DECIMAL_DATA = new BigDecimal(0.01);
  private static final long BIGINT_DATA = 10100000L;
  private static final List<String> columnNames =
      Arrays.asList(
          "int", "text", "date", "double", "time", "timestamp", "bool", "decimal", "bigint");
  private static final List<String> columnTypeNames =
      Arrays.asList(
          "INTEGER", "TEXT", "DATA", "DOUBLE", "TIME", "TIMESTAMP", "BOOLEAN", "DECIMAL", "BIGINT");
  private static final List<Integer> columnTypes =
      Arrays.asList(
          Types.INTEGER,
          Types.VARCHAR,
          Types.DATE,
          Types.DOUBLE,
          Types.TIME,
          Types.TIMESTAMP,
          Types.BOOLEAN,
          Types.DECIMAL,
          Types.BIGINT);
  private static final Object[][] rows = {
    {
      INT_DATA,
      TEXT_DATA,
      Date.valueOf(EPOCH_DATE),
      DOUBLE_DATA,
      TIME_DATA,
      TIMESTAMP_DATA,
      BOOLEAN_DATA,
      DECIMAL_DATA,
      BIGINT_DATA
    },
    {0, null, null, 0, null, null, false, null, 0}
  };

  @Test
  public void testRowIndex() throws SQLException {
    try (ResultSet resultSet = getResultSet(false)) {

      Assertions.assertEquals(columnCount, resultSet.getMetaData().getColumnCount());
      Assertions.assertEquals(-1, resultSet.getRow());
      Assertions.assertTrue(resultSet.isBeforeFirst());
      Assertions.assertFalse(resultSet.isFirst());

      Assertions.assertTrue(resultSet.next());
      Assertions.assertEquals(0, resultSet.getRow());

      Assertions.assertFalse(resultSet.isBeforeFirst());
      Assertions.assertTrue(resultSet.isFirst());

      Assertions.assertTrue(resultSet.next());
      Assertions.assertEquals(1, resultSet.getRow());

      Assertions.assertTrue(resultSet.isLast());
      Assertions.assertFalse(resultSet.isAfterLast());

      Assertions.assertFalse(resultSet.next());
      Assertions.assertEquals(2, resultSet.getRow());

      Assertions.assertFalse(resultSet.isLast());
      Assertions.assertTrue(resultSet.isAfterLast());
    }
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
    try (ResultSet resultSet = getResultSet(true)) {
      Assertions.assertEquals(INT_DATA, resultSet.getInt("int"));
    }
  }

  @Test
  public void testGetString() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      Assertions.assertEquals(TEXT_DATA, resultSet.getString("text"));
    }
  }

  @Test
  public void testGetDate() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      Assertions.assertEquals(Date.valueOf(EPOCH_DATE), resultSet.getDate("date"));
    }
  }

  @Test
  public void testGetDouble() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      Assertions.assertEquals(DOUBLE_DATA, resultSet.getDouble("double"), DELTA);
    }
  }

  @Test
  public void testGetTime() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      Assertions.assertEquals(TIME_DATA, resultSet.getTime("time"));
    }
  }

  @Test
  public void testGetTimestamp() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      Assertions.assertEquals(TIMESTAMP_DATA, resultSet.getTimestamp("timestamp"));
    }
  }

  @Test
  public void testGetBoolean() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      Assertions.assertEquals(BOOLEAN_DATA, resultSet.getBoolean("bool"));
    }
  }

  @Test
  public void testGetObject() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      Assertions.assertEquals(INT_DATA, resultSet.getObject(1));
      Assertions.assertEquals(TEXT_DATA, resultSet.getObject(2));
      Assertions.assertEquals(Date.valueOf(EPOCH_DATE), resultSet.getObject(3));
      Assertions.assertEquals(DOUBLE_DATA, resultSet.getObject(4));
      Assertions.assertEquals(TIME_DATA, resultSet.getObject(5));
      Assertions.assertEquals(TIMESTAMP_DATA, resultSet.getObject(6));
      Assertions.assertEquals(BOOLEAN_DATA, resultSet.getObject(7));
      Assertions.assertEquals(DECIMAL_DATA, resultSet.getObject(8));
      Assertions.assertEquals(BIGINT_DATA, resultSet.getObject(9));
    }
  }
}
