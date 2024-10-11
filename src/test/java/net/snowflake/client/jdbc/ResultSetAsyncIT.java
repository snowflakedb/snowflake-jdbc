/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import net.snowflake.client.TestUtil;
import net.snowflake.common.core.SqlState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test AsyncResultSet */
//@Category(TestCategoryResultSet.class)
public class ResultSetAsyncIT extends BaseJDBCWithSharedConnectionIT {

  @Test
  public void testAsyncResultSetFunctionsWithNewSession() throws SQLException {
    final Map<String, String> params = getConnectionParameters();
    String queryID = null;
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
        statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
        String createTableSql = "select * from test_rsmd";
        try (ResultSet rs =
            statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(createTableSql)) {
          queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
        }
      } finally {
        statement.execute("drop table if exists test_rsmd");
      }
    }
    try (Connection connection = getConnection();
        // open a new connection and create a result set
        ResultSet resultSet =
            connection.unwrap(SnowflakeConnection.class).createResultSet(queryID)) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      // getCatalogName(), getSchemaName(), and getTableName() are empty
      // when session is re-opened
      Assertions.assertEquals("", resultSetMetaData.getCatalogName(1).toUpperCase());
      Assertions.assertEquals("", resultSetMetaData.getSchemaName(1).toUpperCase());
      Assertions.assertEquals("", resultSetMetaData.getTableName(1));
      Assertions.assertEquals(String.class.getName(), resultSetMetaData.getColumnClassName(2));
      Assertions.assertEquals(2, resultSetMetaData.getColumnCount());
      Assertions.assertEquals(22, resultSetMetaData.getColumnDisplaySize(1));
      Assertions.assertEquals("COLA", resultSetMetaData.getColumnLabel(1));
      Assertions.assertEquals("COLA", resultSetMetaData.getColumnName(1));
      Assertions.assertEquals(3, resultSetMetaData.getColumnType(1));
      Assertions.assertEquals("NUMBER", resultSetMetaData.getColumnTypeName(1));
      Assertions.assertEquals(20, resultSetMetaData.getPrecision(1));
      Assertions.assertEquals(5, resultSetMetaData.getScale(1));
      Assertions.assertFalse(resultSetMetaData.isAutoIncrement(1));
      Assertions.assertFalse(resultSetMetaData.isCaseSensitive(1));
      Assertions.assertFalse(resultSetMetaData.isCurrency(1));
      Assertions.assertFalse(resultSetMetaData.isDefinitelyWritable(1));
      Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSetMetaData.isNullable(1));
      Assertions.assertTrue(resultSetMetaData.isReadOnly(1));
      Assertions.assertTrue(resultSetMetaData.isSearchable(1));
      Assertions.assertTrue(resultSetMetaData.isSigned(1));

      SnowflakeResultSetMetaData secretMetaData =
          resultSetMetaData.unwrap(SnowflakeResultSetMetaData.class);
      List<String> colNames = secretMetaData.getColumnNames();
      Assertions.assertEquals("COLA", colNames.get(0));
      Assertions.assertEquals("COLB", colNames.get(1));
      Assertions.assertEquals(Types.DECIMAL, secretMetaData.getInternalColumnType(1));
      Assertions.assertEquals(Types.VARCHAR, secretMetaData.getInternalColumnType(2));
      TestUtil.assertValidQueryId(secretMetaData.getQueryID());
      Assertions.assertEquals(secretMetaData.getQueryID(), resultSet.unwrap(SnowflakeResultSet.class).getQueryID());
    }
  }

  @Test
  public void testResultSetMetadata() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
        statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
        try (ResultSet resultSet =
            statement
                .unwrap(SnowflakeStatement.class)
                .executeAsyncQuery("select * from test_rsmd")) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          Assertions.assertEquals("", resultSetMetaData.getCatalogName(1).toUpperCase());
          Assertions.assertEquals("", resultSetMetaData.getSchemaName(1).toUpperCase());
          Assertions.assertEquals("", resultSetMetaData.getTableName(1));
          Assertions.assertEquals(String.class.getName(), resultSetMetaData.getColumnClassName(2));
          Assertions.assertEquals(2, resultSetMetaData.getColumnCount());
          Assertions.assertEquals(22, resultSetMetaData.getColumnDisplaySize(1));
          Assertions.assertEquals("COLA", resultSetMetaData.getColumnLabel(1));
          Assertions.assertEquals("COLA", resultSetMetaData.getColumnName(1));
          Assertions.assertEquals(3, resultSetMetaData.getColumnType(1));
          Assertions.assertEquals("NUMBER", resultSetMetaData.getColumnTypeName(1));
          Assertions.assertEquals(20, resultSetMetaData.getPrecision(1));
          Assertions.assertEquals(5, resultSetMetaData.getScale(1));
          Assertions.assertFalse(resultSetMetaData.isAutoIncrement(1));
          Assertions.assertFalse(resultSetMetaData.isCaseSensitive(1));
          Assertions.assertFalse(resultSetMetaData.isCurrency(1));
          Assertions.assertFalse(resultSetMetaData.isDefinitelyWritable(1));
          Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSetMetaData.isNullable(1));
          Assertions.assertTrue(resultSetMetaData.isReadOnly(1));
          Assertions.assertTrue(resultSetMetaData.isSearchable(1));
          Assertions.assertTrue(resultSetMetaData.isSigned(1));
          SnowflakeResultSetMetaData secretMetaData =
              resultSetMetaData.unwrap(SnowflakeResultSetMetaData.class);
          List<String> colNames = secretMetaData.getColumnNames();
          Assertions.assertEquals("COLA", colNames.get(0));
          Assertions.assertEquals("COLB", colNames.get(1));
          Assertions.assertEquals(Types.DECIMAL, secretMetaData.getInternalColumnType(1));
          Assertions.assertEquals(Types.VARCHAR, secretMetaData.getInternalColumnType(2));
          TestUtil.assertValidQueryId(secretMetaData.getQueryID());
          Assertions.assertEquals(secretMetaData.getQueryID(), resultSet.unwrap(SnowflakeResultSet.class).getQueryID());
        }
      } finally {
        statement.execute("drop table if exists test_rsmd");
      }
    }
  }

  @Test
  public void testOrderAndClosureFunctions() throws SQLException {
    // Set up environment
    String queryID = null;
    try (Statement statement = connection.createStatement()) {
      statement.execute("create or replace table test_rsmd(colA number(20, 5), colB string)");
      statement.execute("insert into test_rsmd values(1.00, 'str'),(2.00, 'str2')");
      try {
        ResultSet resultSet =
            statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("select * from test_rsmd");

        // test isFirst, isBeforeFirst
        Assertions.assertTrue(resultSet.isBeforeFirst(), "should be before the first");
        Assertions.assertFalse(resultSet.isFirst(), "should not be the first");
        resultSet.next();
        Assertions.assertFalse(resultSet.isBeforeFirst(), "should not be before the first");
        Assertions.assertTrue(resultSet.isFirst(), "should be the first");

        // test isClosed functions
        queryID = resultSet.unwrap(SnowflakeResultSet.class).getQueryID();
        Assertions.assertFalse(resultSet.isClosed());
        // close resultSet and test again
        resultSet.close();
        Assertions.assertTrue(resultSet.isClosed());
      } finally {
        statement.execute("drop table if exists test_rsmd");
      }
    }
    try (Connection connection = getConnection()) {
      ResultSet resultSet = connection.unwrap(SnowflakeConnection.class).createResultSet(queryID);
      // test out isClosed, isLast, and isAfterLast
      Assertions.assertFalse(resultSet.isClosed());
      resultSet.next();
      resultSet.next();
      // cursor should be on last row
      Assertions.assertTrue(resultSet.isLast());
      resultSet.next();
      // cursor is after last row
      Assertions.assertTrue(resultSet.isAfterLast());
      resultSet.close();
      // resultSet should be closed
      Assertions.assertTrue(resultSet.isClosed());
    }
  }

  @Test
  public void testWasNull() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        Clob emptyClob = connection.createClob();
        emptyClob.setString(1, "");
        statement.execute(
            "create or replace table test_null(colA number, colB string, colNull string, emptyClob string)");
        try (PreparedStatement prepst =
            connection.prepareStatement("insert into test_null values (?, ?, ?, ?)")) {
          prepst.setNull(1, Types.INTEGER);
          prepst.setString(2, "hello");
          prepst.setString(3, null);
          prepst.setClob(4, emptyClob);
          prepst.execute();

          try (ResultSet resultSet =
              statement
                  .unwrap(SnowflakeStatement.class)
                  .executeAsyncQuery("select * from test_null")) {
            resultSet.next();
            resultSet.getInt(1);
            Assertions.assertTrue(resultSet.wasNull()); // integer value is null
            resultSet.getString(2);
            Assertions.assertFalse(resultSet.wasNull()); // string value is not null
            Assertions.assertNull(resultSet.getClob(3));
            Assertions.assertNull(resultSet.getClob("COLNULL"));
            Assertions.assertEquals("", resultSet.getClob("EMPTYCLOB").toString());
          }
        }
      } finally {
        statement.execute("drop table if exists test_null");
      }
    }
  }

  @Test
  public void testGetMethods() throws Throwable {
    String prepInsertString =
        "insert into test_get values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    int bigInt = Integer.MAX_VALUE;
    long bigLong = Long.MAX_VALUE;
    short bigShort = Short.MAX_VALUE;
    String str = "hello";
    double bigDouble = Double.MAX_VALUE;
    float bigFloat = Float.MAX_VALUE;
    byte[] bytes = {(byte) 0xAB, (byte) 0xCD, (byte) 0x12};
    BigDecimal bigDecimal = new BigDecimal("10000000000");
    byte oneByte = (byte) 1;
    Date date = new Date(44);
    Time time = new Time(500);
    Timestamp ts = new Timestamp(333);

    Clob clob = connection.createClob();
    clob.setString(1, "hello world");
    try (Statement statement = connection.createStatement()) {
      try {
        // TODO structuredType - add to test when WRITE is ready - SNOW-1157904
        statement.execute(
            "create or replace table test_get(colA integer, colB number, colC number, colD string, colE double, colF float, colG boolean, colH text, colI binary(3), colJ number(38,9), colK int, colL date, colM time, colN timestamp_ltz)");

        try (PreparedStatement prepStatement = connection.prepareStatement(prepInsertString)) {
          prepStatement.setInt(1, bigInt);
          prepStatement.setLong(2, bigLong);
          prepStatement.setLong(3, bigShort);
          prepStatement.setString(4, str);
          prepStatement.setDouble(5, bigDouble);
          prepStatement.setFloat(6, bigFloat);
          prepStatement.setBoolean(7, true);
          prepStatement.setClob(8, clob);
          prepStatement.setBytes(9, bytes);
          prepStatement.setBigDecimal(10, bigDecimal);
          prepStatement.setByte(11, oneByte);
          prepStatement.setDate(12, date);
          prepStatement.setTime(13, time);
          prepStatement.setTimestamp(14, ts);
          prepStatement.execute();

          try (ResultSet resultSet =
              statement
                  .unwrap(SnowflakeStatement.class)
                  .executeAsyncQuery("select * from test_get")) {
            resultSet.next();
            Assertions.assertEquals(bigInt, resultSet.getInt(1));
            Assertions.assertEquals(bigInt, resultSet.getInt("COLA"));
            Assertions.assertEquals(bigLong, resultSet.getLong(2));
            Assertions.assertEquals(bigLong, resultSet.getLong("COLB"));
            Assertions.assertEquals(bigShort, resultSet.getShort(3));
            Assertions.assertEquals(bigShort, resultSet.getShort("COLC"));
            Assertions.assertEquals(str, resultSet.getString(4));
            Assertions.assertEquals(str, resultSet.getString("COLD"));
            Reader reader = resultSet.getCharacterStream("COLD");
            char[] sample = new char[str.length()];

            Assertions.assertEquals(str.length(), reader.read(sample));
            Assertions.assertEquals(str.charAt(0), sample[0]);
            Assertions.assertEquals(str, new String(sample));

            Assertions.assertEquals(bigDouble, resultSet.getDouble(5), 0);
            Assertions.assertEquals(bigDouble, resultSet.getDouble("COLE"), 0);
            Assertions.assertEquals(bigFloat, resultSet.getFloat(6), 0);
            Assertions.assertEquals(bigFloat, resultSet.getFloat("COLF"), 0);
            Assertions.assertTrue(resultSet.getBoolean(7));
            Assertions.assertTrue(resultSet.getBoolean("COLG"));
            Assertions.assertEquals("hello world", resultSet.getClob("COLH").toString());

            // TODO: figure out why getBytes returns an offset.
            // assertEquals(bytes, resultSet.getBytes(9));
            // assertEquals(bytes, resultSet.getBytes("COLI"));

            DecimalFormat df = new DecimalFormat("#.00");
            Assertions.assertEquals(df.format(bigDecimal), df.format(resultSet.getBigDecimal(10)));
            Assertions.assertEquals(df.format(bigDecimal), df.format(resultSet.getBigDecimal("COLJ")));

            Assertions.assertEquals(oneByte, resultSet.getByte(11));
            Assertions.assertEquals(oneByte, resultSet.getByte("COLK"));

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Assertions.assertEquals(sdf.format(date), sdf.format(resultSet.getDate(12)));
            Assertions.assertEquals(sdf.format(date), sdf.format(resultSet.getDate("COLL")));
            Assertions.assertEquals(time, resultSet.getTime(13));
            Assertions.assertEquals(time, resultSet.getTime("COLM"));
            Assertions.assertEquals(ts, resultSet.getTimestamp(14));
            Assertions.assertEquals(ts, resultSet.getTimestamp("COLN"));

            // test getObject
            Assertions.assertEquals(str, resultSet.getObject(4).toString());
            Assertions.assertEquals(str, resultSet.getObject("COLD").toString());

            // test getStatement method
            Assertions.assertEquals(statement, resultSet.getStatement());
          }
        }
      } finally {
        statement.execute("drop table if exists table_get");
      }
    }
  }

  /**
   * This is a corner case for if a user forgets to call one of these functions before attempting to
   * fetch real data. An empty ResultSet is initially returned form executeAsyncQuery() but is
   * replaced by a real ResultSet when next() or getMetaData() is called. If neither is called, user
   * can try to get results from empty result set but exceptions are thrown whenever a column name
   * cannot be found.
   *
   * @throws SQLException
   */
  @Test
  public void testEmptyResultSet() throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet rs =
            statement
                .unwrap(SnowflakeStatement.class)
                .executeAsyncQuery("select * from empty_table")) {
      // if user never calls getMetadata() or next(), empty result set is used to get results.
      // empty ResultSet returns all nulls, 0s, and empty values.
      Assertions.assertFalse(rs.isClosed());
      Assertions.assertEquals(0, rs.getInt(1));
      try {
        rs.getInt("col1");
        Assertions.fail("Fetching from a column name that does not exist should return a SQLException");
      } catch (SQLException e) {
        // findColumn fails with empty metadata with exception "Column not found".
        Assertions.assertEquals(SqlState.UNDEFINED_COLUMN, e.getSQLState());
      }
    }
  }
}
