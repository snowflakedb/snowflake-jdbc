/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

@Category(TestCategoryResultSet.class)
public class BindingAndInsertingStructuredTypesLatestIT extends BaseJDBCTest {

  @Parameterized.Parameters(name = "format={0}")
  public static Object[][] data() {
    return new Object[][] {
      {ResultSetFormatType.JSON},
      {ResultSetFormatType.ARROW_WITH_JSON_STRUCTURED_TYPES},
      {ResultSetFormatType.NATIVE_ARROW}
    };
  }

  private final ResultSetFormatType queryResultFormat;

  public BindingAndInsertingStructuredTypesLatestIT(ResultSetFormatType queryResultFormat) {
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

  // TODO Structured types feature exists only on QA environments
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteObject() throws SQLException {
    SimpleClass sc = new SimpleClass("text1", 2);
    SimpleClass sc2 = new SimpleClass("text2", 3);
    try (Connection connection = init()) {
      Statement statement = connection.createStatement();
      statement.execute(
          "CREATE OR REPLACE TABLE test_table (ob OBJECT(string varchar, intValue NUMBER))");
      try (SnowflakePreparedStatementV1 stmt =
              (SnowflakePreparedStatementV1)
                  connection.prepareStatement("insert into test_table select ?");
          SnowflakePreparedStatementV1 stmt3 =
              (SnowflakePreparedStatementV1)
                  connection.prepareStatement("SELECT ob FROM test_table where ob = ?"); ) {

        stmt.setObject(1, sc);
        stmt.executeUpdate();

        stmt.setObject(1, sc2);
        stmt.executeUpdate();

        stmt3.setObject(1, sc2);

        try (ResultSet resultSet = stmt3.executeQuery()) {

          resultSet.next();
          SimpleClass object = resultSet.getObject(1, SimpleClass.class);
          assertEquals("text2", object.getString());
          assertEquals(Integer.valueOf("3"), object.getIntValue());
          assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteNullObject() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmtement2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("insert into test_table select null");
        SnowflakePreparedStatementV1 statement3 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("SELECT * FROM test_table"); ) {

      statement.execute(
          "CREATE OR REPLACE TABLE test_table (ob OBJECT(string varchar, intValue NUMBER))");

      stmtement2.executeUpdate();

      try (ResultSet resultSet = statement3.executeQuery()) {
        assertTrue(resultSet.next());
        assertNull(resultSet.getObject(1));
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteObjectBindingNull() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("insert into test_table select ?");
        SnowflakePreparedStatementV1 stmt2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("SELECT * FROM test_table"); ) {
      statement.execute(
          "CREATE OR REPLACE TABLE test_table (ob OBJECT(string varchar, intValue NUMBER))");
      stmt.setObject(1, null);
      stmt.executeUpdate();
      try (ResultSet resultSet = stmt2.executeQuery()) {
        resultSet.next();
        SimpleClass object = resultSet.getObject(1, SimpleClass.class);
        assertNull(object);
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteObjectAllTypes() throws SQLException {
    TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("insert into test_all_types_object select ?");
        SnowflakePreparedStatementV1 stmt2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("select * from test_all_types_object where ob=?"); ) {

      statement.execute(
          " CREATE OR REPLACE TABLE test_all_types_object ("
              + "                 ob OBJECT(string VARCHAR, "
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
              + "                  ) )");

      AllTypesClass allTypeInstance =
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
              toTimestamp(ZonedDateTime.of(2021, 12, 23, 9, 44, 44, 0, ZoneId.of("Asia/Tokyo"))),
              Date.valueOf("2023-12-24"),
              Time.valueOf("12:34:56"),
              new byte[] {'a', 'b', 'c'},
              new SimpleClass("testString", 2));
      stmt.setObject(1, allTypeInstance);
      stmt.executeUpdate();
      statement.execute("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'");

      stmt2.setObject(1, allTypeInstance);
      try (ResultSet resultSet = stmt2.executeQuery()) {
        resultSet.next();
        AllTypesClass object = resultSet.getObject(1, AllTypesClass.class);
        assertEquals("string", object.getString());
        assertEquals(49, (long) object.getB());
        assertEquals(2, (long) object.getS());
        assertEquals(3, (long) object.getI());
        assertEquals(4, (long) object.getL());
        assertEquals(1.1, (double) object.getF(), 0.01);
        assertEquals(2.24, (double) object.getD(), 0.01);
        assertEquals(new BigDecimal("999999999999999999999999999999999999.55"), object.getBd());
        assertEquals(Boolean.TRUE, object.getBool());
        assertEquals(
            Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)), object.getTimestampLtz());
        assertEquals(
            Timestamp.valueOf(LocalDateTime.of(2021, 12, 23, 9, 44, 44)), object.getTimestampNtz());
        assertEquals(
            toTimestamp(ZonedDateTime.of(2021, 12, 23, 9, 44, 44, 0, ZoneId.of("Asia/Tokyo"))),
            object.getTimestampTz());
        // TODO uncomment after merge SNOW-928973: Date field is returning one day less when getting
        // through getString method
        //        assertEquals(Date.valueOf(LocalDate.of(2023, 12, 24)), object.getDate());
        assertEquals(Time.valueOf(LocalTime.of(12, 34, 56)), object.getTime());
        assertArrayEquals(new byte[] {'a', 'b', 'c'}, object.getBinary());
        assertEquals("testString", object.getSimpleClass().getString());
        assertEquals(Integer.valueOf("2"), object.getSimpleClass().getIntValue());
      }
    }
  }

  public static Timestamp toTimestamp(ZonedDateTime dateTime) {
    return new Timestamp(dateTime.toInstant().getEpochSecond() * 1000L);
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testWriteArray() throws SQLException {
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
  public void testWriteArrayNoBinds() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        SnowflakePreparedStatementV1 stmt =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement(
                    "insert into array_of_integers select ([1, 2, 3]::array(integer));"); ) {

      statement.execute(" CREATE OR REPLACE TABLE array_of_integers(arrayInt ARRAY(INTEGER))");

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

      Map<String, SimpleClass> mapStruct =
          Stream.of(
                  new Object[][] {
                    {"x", new SimpleClass("string1", 1)},
                    {"y", new SimpleClass("string2", 2)},
                  })
              .collect(Collectors.toMap(data -> (String) data[0], data -> (SimpleClass) data[1]));

      stmt.setMap(1, mapStruct, Types.STRUCT);
      stmt.executeUpdate();

      stmt2.setMap(1, mapStruct, Types.STRUCT);

      try (ResultSet resultSet = stmt2.executeQuery()) {
        resultSet.next();
        Map<String, SimpleClass> map =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, SimpleClass.class);
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
                connection.prepareStatement("INSERT INTO map_of_objects (mapp) SELECT ?;");
        SnowflakePreparedStatementV1 stmt2 =
            (SnowflakePreparedStatementV1)
                connection.prepareStatement("select * from map_of_objects where mapp=?"); ) {

      statement.execute(" CREATE OR REPLACE TABLE map_of_objects(mapp MAP(VARCHAR, INTEGER))");

      Map<String, Integer> mapStruct = new HashMap<>();
      mapStruct.put("x", 1);
      mapStruct.put("y", 2);

      stmt.setMap(1, mapStruct, Types.INTEGER);
      stmt.executeUpdate();

      stmt2.setMap(1, mapStruct, Types.INTEGER);

      try (ResultSet resultSet = stmt2.executeQuery()) {
        resultSet.next();
        Map<String, Integer> map =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Integer.class);
      }
    }
  }
}
