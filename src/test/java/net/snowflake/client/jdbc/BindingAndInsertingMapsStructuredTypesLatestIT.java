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
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
public class BindingAndInsertingMapsStructuredTypesLatestIT extends BaseJDBCTest {

  @Parameterized.Parameters(name = "format={0}")
  public static Object[][] data() {
    return new Object[][] {
      {ResultSetFormatType.JSON},
      {ResultSetFormatType.ARROW_WITH_JSON_STRUCTURED_TYPES},
      {ResultSetFormatType.NATIVE_ARROW}
    };
  }

  private final ResultSetFormatType queryResultFormat;

  public BindingAndInsertingMapsStructuredTypesLatestIT(ResultSetFormatType queryResultFormat) {
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
  public void testWriteMapOfSqlData() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("INSERT INTO map_of_objects (mapp) SELECT ?;");
        SnowflakePreparedStatementV1 stmt2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("select * from map_of_objects where mapp=?"); ) {

      statement.execute(
          " CREATE OR REPLACE TABLE map_of_objects(mapp MAP(VARCHAR, OBJECT(string VARCHAR, intValue INTEGER)))");

      Map<String, SimpleClass> mapStruct = new HashMap<>();
      mapStruct.put("x", new SimpleClass("string1", 1));
      mapStruct.put("y", new SimpleClass("string2", 2));
      mapStruct.put("z", null);

      stmt.setMap(1, mapStruct, Types.STRUCT);
      stmt.executeUpdate();

      stmt2.setMap(1, mapStruct, Types.STRUCT);

      try (ResultSet resultSet = stmt2.executeQuery()) {
        assertTrue(resultSet.next());
        Map<String, SimpleClass> map =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, SimpleClass.class);
        assertEquals("string1", map.get("x").getString());
        assertEquals(Integer.valueOf(1), map.get("x").getIntValue());
        assertEquals("string2", map.get("y").getString());
        assertEquals(Integer.valueOf(2), map.get("y").getIntValue());
        assertNull(map.get("z"));
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteMapOfAllTypes() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "INSERT INTO map_of_objects_all_types (mapp) SELECT ?;");
        SnowflakePreparedStatementV1 stmt2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "select * from map_of_objects_all_types where mapp=?"); ) {

      statement.execute(
          " CREATE OR REPLACE TABLE map_of_objects_all_types(mapp MAP(VARCHAR, "
              + "                             OBJECT(string VARCHAR, "
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
              + "                  )))");

      Map<String, AllTypesClass> mapStruct =
          Stream.of(
                  new Object[][] {
                    {
                      "x",
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
                          toTimestamp(
                              ZonedDateTime.of(2021, 12, 23, 9, 44, 44, 0, ZoneId.of("UTC"))),
                          toTimestamp(
                              ZonedDateTime.of(
                                  2021, 12, 23, 9, 44, 44, 0, ZoneId.of("Asia/Tokyo"))),
                          Date.valueOf("2023-12-24"),
                          Time.valueOf("12:34:56"),
                          new byte[] {'a', 'b', 'c'},
                          new SimpleClass("testString", 2))
                    },
                    {
                      "y",
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
                          toTimestamp(
                              ZonedDateTime.of(2021, 12, 23, 9, 44, 44, 0, ZoneId.of("UTC"))),
                          toTimestamp(
                              ZonedDateTime.of(
                                  2021, 12, 23, 9, 44, 44, 0, ZoneId.of("Asia/Tokyo"))),
                          Date.valueOf("2023-12-24"),
                          Time.valueOf("12:34:56"),
                          new byte[] {'a', 'b', 'c'},
                          new SimpleClass("testString", 2))
                    },
                  })
              .collect(Collectors.toMap(data -> (String) data[0], data -> (AllTypesClass) data[1]));

      stmt.setMap(1, mapStruct, Types.STRUCT);
      stmt.executeUpdate();

      stmt2.setMap(1, mapStruct, Types.STRUCT);

      try (ResultSet resultSet = stmt2.executeQuery()) {
        assertTrue(resultSet.next());
        Map<String, AllTypesClass> map =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, AllTypesClass.class);
        assertEquals("string", map.get("x").getString());
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteMapOfInteger() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("INSERT INTO map_of_integers (mapp) SELECT ?;");
        SnowflakePreparedStatementV1 stmt2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("select * from map_of_integers where mapp=?"); ) {

      statement.execute(" CREATE OR REPLACE TABLE map_of_integers(mapp MAP(VARCHAR, INTEGER))");

      Map<String, Integer> mapStruct = new HashMap<>();
      mapStruct.put("x", 1);
      mapStruct.put("y", 2);
      mapStruct.put("z", null);

      stmt.setMap(1, mapStruct, Types.INTEGER);
      stmt.executeUpdate();

      stmt2.setMap(1, mapStruct, Types.INTEGER);

      try (ResultSet resultSet = stmt2.executeQuery()) {
        assertTrue(resultSet.next());
        Map<String, Integer> resultMap =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Integer.class);
        assertEquals(Integer.valueOf(1), resultMap.get("x"));
        assertEquals(Integer.valueOf(2), resultMap.get("y"));
        assertNull(resultMap.get("z"));
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteMapOfTimestampLtz() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("INSERT INTO map_of_timestamp_ltz (mapp) SELECT ?;");
        SnowflakePreparedStatementV1 stmt2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("select * from map_of_timestamp_ltz where mapp=?"); ) {

      statement.execute(
          " CREATE OR REPLACE TABLE map_of_timestamp_ltz(mapp MAP(VARCHAR, TIMESTAMP_LTZ))");

      Map<String, Timestamp> mapStruct = new HashMap<>();
      mapStruct.put("x", Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)));
      mapStruct.put("y", Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 45)));

      stmt.setMap(1, mapStruct, Types.TIMESTAMP, SnowflakeType.TIMESTAMP_LTZ);
      stmt.executeUpdate();

      stmt2.setMap(1, mapStruct, Types.TIMESTAMP, SnowflakeType.TIMESTAMP_LTZ);

      try (ResultSet resultSet = stmt2.executeQuery()) {
        assertTrue(resultSet.next());
        Map<String, Timestamp> map =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Timestamp.class);
        assertEquals(
            LocalDateTime.of(2021, 12, 22, 9, 43, 44)
                .atZone(ZoneId.of("Europe/Warsaw"))
                .toInstant(),
            map.get("x").toInstant());
        assertEquals(
            LocalDateTime.of(2021, 12, 22, 9, 43, 45)
                .atZone(ZoneId.of("Europe/Warsaw"))
                .toInstant(),
            map.get("y").toInstant());
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteMapOfTimestampNtz() throws SQLException {
    TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("INSERT INTO map_of_timestamp_ntz (mapp) SELECT ?;");
        SnowflakePreparedStatementV1 stmt2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("select * from map_of_timestamp_ntz where mapp=?"); ) {

      statement.execute(
          " CREATE OR REPLACE TABLE map_of_timestamp_ntz(mapp MAP(VARCHAR, TIMESTAMP_NTZ))");

      Map<String, Timestamp> mapStruct = new HashMap<>();
      mapStruct.put("x", Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)));
      mapStruct.put("y", Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 45)));

      stmt.setMap(1, mapStruct, Types.TIMESTAMP, SnowflakeType.TIMESTAMP_NTZ);
      stmt.executeUpdate();

      stmt2.setMap(1, mapStruct, Types.TIMESTAMP, SnowflakeType.TIMESTAMP_NTZ);

      try (ResultSet resultSet = stmt2.executeQuery()) {
        assertTrue(resultSet.next());
        Map<String, Timestamp> map =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Timestamp.class);
        assertEquals(
            Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)).toInstant(),
            map.get("x").toInstant());
        assertEquals(
            Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 45)).toInstant(),
            map.get("y").toInstant());
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteMapOfTimestampTz() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("INSERT INTO map_of_timestamp_tz (mapp) SELECT ?;");
        SnowflakePreparedStatementV1 stmt2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("select * from map_of_timestamp_tz where mapp=?"); ) {

      statement.execute(
          " CREATE OR REPLACE TABLE map_of_timestamp_tz(mapp MAP(VARCHAR, TIMESTAMP_TZ))");

      Map<String, Timestamp> mapStruct = new HashMap<>();
      mapStruct.put("x", Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)));
      mapStruct.put("y", Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 45)));

      stmt.setMap(1, mapStruct, Types.TIMESTAMP, SnowflakeType.TIMESTAMP_TZ);
      stmt.executeUpdate();

      stmt2.setMap(1, mapStruct, Types.TIMESTAMP, SnowflakeType.TIMESTAMP_TZ);

      try (ResultSet resultSet = stmt2.executeQuery()) {
        assertTrue(resultSet.next());
        Map<String, Timestamp> map =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Timestamp.class);
        assertEquals(
            LocalDateTime.of(2021, 12, 22, 9, 43, 44)
                .atZone(ZoneId.of("Europe/Warsaw"))
                .toInstant(),
            map.get("x").toInstant());
        assertEquals(
            LocalDateTime.of(2021, 12, 22, 9, 43, 45)
                .atZone(ZoneId.of("Europe/Warsaw"))
                .toInstant(),
            map.get("y").toInstant());
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteMapOfTime() throws SQLException {
    TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("INSERT INTO map_of_time (mapp) SELECT ?;");
        SnowflakePreparedStatementV1 stmt2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("select * from map_of_time where mapp=?"); ) {

      statement.execute(" CREATE OR REPLACE TABLE map_of_time(mapp MAP(VARCHAR, TIME))");

      Map<String, Time> mapStruct = new HashMap<>();
      mapStruct.put("x", Time.valueOf("12:34:56"));
      mapStruct.put("y", Time.valueOf("12:34:57"));

      stmt.setMap(1, mapStruct, Types.TIME);
      stmt.executeUpdate();

      stmt2.setMap(1, mapStruct, Types.TIME);

      try (ResultSet resultSet = stmt2.executeQuery()) {
        assertTrue(resultSet.next());
        Map<String, Time> map =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Time.class);
        assertEquals(Time.valueOf("12:34:56"), map.get("x"));
        assertEquals(Time.valueOf("12:34:57"), map.get("y"));
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteMapOfDate() throws SQLException {
    TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("INSERT INTO map_of_date (mapp) SELECT ?;");
        SnowflakePreparedStatementV1 stmt2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("select * from map_of_date where mapp=?"); ) {

      statement.execute(" CREATE OR REPLACE TABLE map_of_date(mapp MAP(VARCHAR, DATE))");

      Map<String, Date> mapStruct = new HashMap<>();
      mapStruct.put("x", Date.valueOf("2023-12-24"));
      mapStruct.put("y", Date.valueOf("2023-12-25"));

      stmt.setMap(1, mapStruct, Types.DATE);
      stmt.executeUpdate();

      stmt2.setMap(1, mapStruct, Types.DATE);

      try (ResultSet resultSet = stmt2.executeQuery()) {
        assertTrue(resultSet.next());
        Map<String, Date> map =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Date.class);
        assertEquals(Date.valueOf("2023-12-24"), map.get("x"));
        assertEquals(Date.valueOf("2023-12-25"), map.get("y"));
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteMapOfBinary() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("INSERT INTO map_of_binary (mapp) SELECT ?;");
        SnowflakePreparedStatementV1 stmt2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("select * from map_of_binary where mapp=?"); ) {

      statement.execute(" CREATE OR REPLACE TABLE map_of_binary(mapp MAP(VARCHAR, BINARY))");

      Map<String, byte[]> mapStruct = new HashMap<>();
      mapStruct.put("x", new byte[] {'a', 'b', 'c'});
      mapStruct.put("y", new byte[] {'d', 'e', 'f'});

      stmt.setMap(1, mapStruct, Types.BINARY);
      stmt.executeUpdate();

      stmt2.setMap(1, mapStruct, Types.BINARY);

      try (ResultSet resultSet = stmt2.executeQuery()) {
        assertTrue(resultSet.next());
        Map<String, byte[]> map =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, byte[].class);

        assertArrayEquals(new byte[] {'a', 'b', 'c'}, map.get("x"));
        assertArrayEquals(new byte[] {'d', 'e', 'f'}, map.get("y"));
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteMapWithNulls() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "INSERT INTO map_of_object_with_nulls (mapp) SELECT ?;");
        SnowflakePreparedStatementV1 stmt2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "select * from map_of_object_with_nulls where mapp=?"); ) {

      statement.execute(
          " CREATE OR REPLACE TABLE map_of_object_with_nulls(mapp MAP(VARCHAR, VARCHAR))");

      Map<String, String> mapStruct = new HashMap<>();
      mapStruct.put("x", null);
      mapStruct.put("y", "abc");

      stmt.setMap(1, mapStruct, Types.VARCHAR);
      stmt.executeUpdate();

      stmt2.setMap(1, mapStruct, Types.VARCHAR);

      try (ResultSet resultSet = stmt2.executeQuery()) {
        assertTrue(resultSet.next());
        Map<String, String> map =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, String.class);
        assertNull(map.get("x"));
        assertEquals("abc", map.get("y"));
      }
    }
  }

  private static Timestamp toTimestamp(ZonedDateTime dateTime) {
    return new Timestamp(dateTime.toInstant().getEpochSecond() * 1000L);
  }
}
