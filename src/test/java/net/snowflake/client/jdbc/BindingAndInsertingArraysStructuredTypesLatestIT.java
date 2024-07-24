/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.TimeZone;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryResultSet;
import net.snowflake.client.core.structs.SnowflakeObjectTypeFactories;
import net.snowflake.client.jdbc.structuredtypes.sqldata.AllTypesClass;
import net.snowflake.client.jdbc.structuredtypes.sqldata.SimpleClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category(TestCategoryResultSet.class)
public class BindingAndInsertingArraysStructuredTypesLatestIT extends BaseJDBCTest {

  @Parameterized.Parameters(name = "format={0}")
  public static Object[][] data() {
    return new Object[][] {
      {ResultSetFormatType.JSON},
      {ResultSetFormatType.ARROW_WITH_JSON_STRUCTURED_TYPES},
      {ResultSetFormatType.NATIVE_ARROW}
    };
  }

  private final ResultSetFormatType queryResultFormat;

  public BindingAndInsertingArraysStructuredTypesLatestIT(ResultSetFormatType queryResultFormat) {
    this.queryResultFormat = queryResultFormat;
  }

  public Connection init() throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE = true");
      stmt.execute("alter session set IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE = true");
      stmt.execute("alter session set ENABLE_STRUCTURED_TYPES_IN_BINDS = enable");
      stmt.execute("alter session set ENABLE_OBJECT_TYPED_BINDS = true");
      stmt.execute("alter session set enable_structured_types_in_fdn_tables=true");
      stmt.execute("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'");
      stmt.execute(
          "alter session set jdbc_query_result_format = '"
              + queryResultFormat.sessionParameterTypeValue
              + "'");
      if (queryResultFormat == ResultSetFormatType.NATIVE_ARROW) {
        stmt.execute("alter session set ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT = true");
        stmt.execute("alter session set FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT = true");
      }
    }
    return conn;
  }

  @Before
  public void setup() {
    SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    SnowflakeObjectTypeFactories.register(AllTypesClass.class, AllTypesClass::new);
  }

  @After
  public void clean() {
    SnowflakeObjectTypeFactories.unregister(SimpleClass.class);
    SnowflakeObjectTypeFactories.unregister(AllTypesClass.class);
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteArrayInteger() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "INSERT INTO array_of_integers (arrayInt) SELECT ?;"); ) {

      statement.execute(" CREATE OR REPLACE TABLE array_of_integers(arrayInt ARRAY(INTEGER))");

      Array array = connection.createArrayOf("INTEGER", new Integer[] {1, 2, 3});
      stmt.setArray(1, array);
      stmt.executeUpdate();

      try (ResultSet resultSet = statement.executeQuery("SELECT * from array_of_integers"); ) {
        resultSet.next();

        Long[] resultArray = (Long[]) resultSet.getArray(1).getArray();
        assertEquals(Long.valueOf(1), resultArray[0]);
        assertEquals(Long.valueOf(2), resultArray[1]);
        assertEquals(Long.valueOf(3), resultArray[2]);
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteArrayString() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "INSERT INTO array_of_varchars (arrayType) SELECT ?;"); ) {

      statement.execute(" CREATE OR REPLACE TABLE array_of_varchars(arrayType ARRAY(VARCHAR))");

      Array array = connection.createArrayOf("VARCHAR", new String[] {"a", "b", "c"});
      stmt.setArray(1, array);
      stmt.executeUpdate();

      try (ResultSet resultSet = statement.executeQuery("SELECT * from array_of_varchars"); ) {
        resultSet.next();

        String[] resultArray = (String[]) resultSet.getArray(1).getArray();
        assertEquals("a", resultArray[0]);
        assertEquals("b", resultArray[1]);
        assertEquals("c", resultArray[2]);
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteArrayOfTimestampLtz() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "INSERT INTO array_of_timestamp_ltz (arrayInt) SELECT ?;"); ) {

      statement.execute(
          " CREATE OR REPLACE TABLE array_of_timestamp_ltz(arrayInt ARRAY(TIMESTAMP_LTZ))");

      Array array =
          connection.createArrayOf(
              "TIMESTAMP",
              new Timestamp[] {
                Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)),
                Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 45))
              });
      stmt.setArray(1, array);
      stmt.executeUpdate();

      try (ResultSet resultSet = statement.executeQuery("SELECT * from array_of_timestamp_ltz"); ) {
        resultSet.next();

        Timestamp[] resultArray = (Timestamp[]) resultSet.getArray(1).getArray();
//        assertEquals(Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)), resultArray[0]);
//        assertEquals(Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 45)), resultArray[1]);
                assertEquals(
                        LocalDateTime.of(2021, 12, 22, 9, 43, 44)
                                .atZone(ZoneId.of("Europe/Warsaw"))
                                .toInstant(),
                        resultArray[0].toInstant());
                assertEquals(
                        LocalDateTime.of(2021, 12, 22, 9, 43, 45)
                                .atZone(ZoneId.of("Europe/Warsaw"))
                                .toInstant(),
                        resultArray[1].toInstant());

      }
    }
  }
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteArrayOfTimestampNtz() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "INSERT INTO array_of_timestamp_ntz (arrayInt) SELECT ?;"); ) {

      statement.execute(
          " CREATE OR REPLACE TABLE array_of_timestamp_ntz(arrayInt ARRAY(TIMESTAMP_NTZ))");

      Array array =
          connection.createArrayOf(
              "TIMESTAMP",
              new Timestamp[] {
                Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)),
                Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 45))
              });
      stmt.setArray(1, array, SnowflakeType.TIMESTAMP_NTZ);
      stmt.executeUpdate();

      try (ResultSet resultSet = statement.executeQuery("SELECT * from array_of_timestamp_ntz"); ) {
        resultSet.next();

        Timestamp[] resultArray = (Timestamp[]) resultSet.getArray(1).getArray();
        assertEquals(Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)), resultArray[0]);
        assertEquals(Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 45)), resultArray[1]);
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteArrayOfTimestampTz() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "INSERT INTO array_of_timestamp_tz (arrayInt) SELECT ?;"); ) {

      statement.execute(" CREATE OR REPLACE TABLE array_of_timestamp_tz(arrayInt ARRAY(TIMESTAMP_TZ))");

      Array array =
          connection.createArrayOf(
              "TIMESTAMP",
              new Timestamp[] {
                Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)),
                Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 45))
              });
      stmt.setArray(1, array, SnowflakeType.TIMESTAMP_TZ);
      stmt.executeUpdate();

      try (ResultSet resultSet = statement.executeQuery("SELECT * from array_of_timestamp_tz"); ) {
        assertTrue(resultSet.next());

        Timestamp[] resultArray = (Timestamp[]) resultSet.getArray(1).getArray();
        assertEquals(
            LocalDateTime.of(2021, 12, 22, 9, 43, 44)
                .atZone(ZoneId.of("Europe/Warsaw"))
                .toInstant(),
            resultArray[0].toInstant());
        assertEquals(
            LocalDateTime.of(2021, 12, 22, 9, 43, 45)
                .atZone(ZoneId.of("Europe/Warsaw"))
                .toInstant(),
            resultArray[1].toInstant());
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteArrayOfTime() throws SQLException {
    TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("INSERT INTO array_of_time (arrayType) SELECT ?;"); ) {

      statement.execute(" CREATE OR REPLACE TABLE array_of_time(arrayType ARRAY(TIME))");

      Array array =
          connection.createArrayOf(
              "TIME", new Time[] {Time.valueOf("12:34:56"), Time.valueOf("12:34:57")});
      stmt.setArray(1, array);
      stmt.executeUpdate();

      try (ResultSet resultSet = statement.executeQuery("SELECT * from array_of_time"); ) {
        assertTrue(resultSet.next());

        Time[] resultArray = (Time[]) resultSet.getArray(1).getArray();
        assertEquals(Time.valueOf("12:34:56"), resultArray[0]);
        assertEquals(Time.valueOf("12:34:57"), resultArray[1]);
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteArrayOfDate() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("INSERT INTO array_of_date (arrayType) SELECT ?;"); ) {

      statement.execute(" CREATE OR REPLACE TABLE array_of_date(arrayType ARRAY(DATE))");

      Array array =
          connection.createArrayOf(
              "DATE", new Date[] {Date.valueOf("2023-12-24"), Date.valueOf("2023-12-24")});
      stmt.setArray(1, array);
      stmt.executeUpdate();

      try (ResultSet resultSet = statement.executeQuery("SELECT * from array_of_date"); ) {
        assertTrue(resultSet.next());

        Date[] resultArray = (Date[]) resultSet.getArray(1).getArray();
        assertEquals(Date.valueOf("2023-12-24").toString(), resultArray[0].toString());
        assertEquals(Date.valueOf("2023-12-24").toString(), resultArray[1].toString());
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteNullArray() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "INSERT INTO array_of_varchars (arrayType) SELECT ?;"); ) {

      statement.execute(" CREATE OR REPLACE TABLE array_of_varchars(arrayType ARRAY(VARCHAR))");

      Array nullArray = connection.createArrayOf("VARCHAR", null);
      stmt.setArray(1, nullArray);
      stmt.executeUpdate();

      try (ResultSet resultSet = statement.executeQuery("SELECT * from array_of_varchars"); ) {
        assertTrue(resultSet.next());

        Object resultArray = resultSet.getArray(1);
        assertNull(resultArray);
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteArrayOfSqlData() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "INSERT INTO array_of_struct (arrayType) SELECT ?;"); ) {

      statement.execute(
          " CREATE OR REPLACE TABLE array_of_struct(arrayType ARRAY(OBJECT(string VARCHAR, intValue INTEGER)) )");

      Array array =
          connection.createArrayOf(
              "STRUCT",
              new SimpleClass[] {new SimpleClass("string1", 1), new SimpleClass("string12", 2)});
      stmt.setArray(1, array);
      stmt.executeUpdate();

      try (ResultSet resultSet = statement.executeQuery("SELECT * from array_of_struct"); ) {
        assertTrue(resultSet.next());

        SimpleClass[] resultArray =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, SimpleClass.class);
        assertEquals(Integer.valueOf(1), resultArray[0].getIntValue());
        assertEquals("string1", resultArray[0].getString());
        assertEquals(Integer.valueOf(2), resultArray[1].getIntValue());
        assertEquals("string12", resultArray[1].getString());
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteArrayOfAllTypesObject() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "INSERT INTO array_of_all_types (arrayType) SELECT ?;"); ) {

      statement.execute(
          " CREATE OR REPLACE TABLE array_of_all_types(arrayType ARRAY(OBJECT(string VARCHAR, "
              + "                  b TINYINT, "
              + "                  s SMALLINT, "
              + "                  i INTEGER, "
              + "                  l BIGINT, "
              + "                  f FLOAT, "
              + "                  d DOUBLE, "
              + "                  bd NUMBER(38,2), "
              + "                  bool BOOLEAN, "
              + "                  timestampLtz TIMESTAMP_LTZ, "
              + "                  timestampNtz TIMESTAMP_NTZ, "
              + "                  timestampTz TIMESTAMP_TZ, "
              + "                  date DATE,"
              + "                  time TIME, "
              + "                   binary BINARY, "
              + "                  simpleClass OBJECT(string VARCHAR, intValue INTEGER)"
              + "                  ) ) )");

      Array array =
          connection.createArrayOf(
              "STRUCT",
              new AllTypesClass[] {
                new AllTypesClass(
                    "string",
                    "1".getBytes(StandardCharsets.UTF_8)[0],
                    Short.valueOf("2"),
                    Integer.valueOf(3),
                    Long.valueOf(4),
                    1.1f,
                    2.24,
                    new BigDecimal("999999999999999999999999999999999999.55"),
                    Boolean.TRUE,
                    Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)),
                    toTimestamp(ZonedDateTime.of(2021, 12, 23, 9, 44, 44, 0, ZoneId.of("UTC"))),
                    toTimestamp(
                        ZonedDateTime.of(2021, 12, 23, 9, 44, 44, 0, ZoneId.of("Asia/Tokyo"))),
                    Date.valueOf("2023-12-24"),
                    Time.valueOf("12:34:56"),
                    new byte[] {'a', 'b', 'c'},
                    new SimpleClass("testString", 2))
              });
      stmt.setArray(1, array);
      stmt.executeUpdate();

      try (ResultSet resultSet = statement.executeQuery("SELECT * from array_of_all_types"); ) {
        assertTrue(resultSet.next());

        AllTypesClass[] resultArray =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, AllTypesClass.class);
        System.out.println(resultArray);
        assertEquals("string", resultArray[0].getString());
        assertEquals(49, (long) resultArray[0].getB());
        assertEquals(2, (long) resultArray[0].getS());
        assertEquals(3, (long) resultArray[0].getI());
        assertEquals(4, (long) resultArray[0].getL());
        assertEquals(1.1, (double) resultArray[0].getF(), 0.01);
        assertEquals(2.24, (double) resultArray[0].getD(), 0.01);
        assertEquals(
            new BigDecimal("999999999999999999999999999999999999.55"), resultArray[0].getBd());
        assertEquals(Boolean.TRUE, resultArray[0].getBool());
        assertEquals(
            Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)),
            resultArray[0].getTimestampLtz());
        assertEquals(
            Timestamp.valueOf(LocalDateTime.of(2021, 12, 23, 9, 44, 44)),
            resultArray[0].getTimestampNtz());
        assertEquals(
            toTimestamp(ZonedDateTime.of(2021, 12, 23, 9, 44, 44, 0, ZoneId.of("Asia/Tokyo"))),
            resultArray[0].getTimestampTz());
        // TODO uncomment after merge SNOW-928973: Date field is returning one day less when getting
        // through getString method
        //        assertEquals(Date.valueOf(LocalDate.of(2023, 12, 24)), resultArray[0].getDate());
        assertEquals(Time.valueOf(LocalTime.of(12, 34, 56)), resultArray[0].getTime());
        assertArrayEquals(new byte[] {'a', 'b', 'c'}, resultArray[0].getBinary());
        assertEquals("testString", resultArray[0].getSimpleClass().getString());
        assertEquals(Integer.valueOf("2"), resultArray[0].getSimpleClass().getIntValue());
      }
    }
  }

  public static Timestamp toTimestamp(ZonedDateTime dateTime) {
    return new Timestamp(dateTime.toInstant().getEpochSecond() * 1000L);
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteArrayNoBinds() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "insert into array_no_binds select ([1, 2, 3]::array(integer));"); ) {

      statement.execute(" CREATE OR REPLACE TABLE array_no_binds(arrayInt ARRAY(INTEGER))");

      stmt.executeUpdate();

      try (ResultSet resultSet = statement.executeQuery("SELECT * from array_no_binds"); ) {
        assertTrue(resultSet.next());
        Long[] resultArray = (Long[]) resultSet.getArray(1).getArray();
        assertEquals(Long.valueOf(1), resultArray[0]);
        assertEquals(Long.valueOf(2), resultArray[1]);
        assertEquals(Long.valueOf(3), resultArray[2]);
      }
    }
  }
}
