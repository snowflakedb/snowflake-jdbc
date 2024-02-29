/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryStructuredType;
import net.snowflake.client.core.structs.SnowflakeObjectTypeFactories;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryStructuredType.class)
public class ResultSetStructuredTypesLatestIT extends BaseJDBCTest {
  private final String queryResultFormat;

  public ResultSetStructuredTypesLatestIT() {
    this("JSON");
  }

  protected ResultSetStructuredTypesLatestIT(String queryResultFormat) {
    this.queryResultFormat = queryResultFormat;
  }

  public Connection init() throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE = true");
      stmt.execute("alter session set IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE = true");
      stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    }
    return conn;
  }

  // TODO Structured types feature exists only on QA environments
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapStructToObjectWithFactory() throws SQLException {
    testMapJson(true);
  }

  // TODO Structured types feature exists only on QA environments
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapStructToObjectWithReflection() throws SQLException {
    testMapJson(false);
    testMapJson(true);
  }

  private void testMapJson(boolean registerFactory) throws SQLException {
    if (registerFactory) {
      SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    } else {
      SnowflakeObjectTypeFactories.unregister(SimpleClass.class);
    }
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("select {'string':'a'}::OBJECT(string VARCHAR)"); ) {
      resultSet.next();
      SimpleClass object = resultSet.getObject(1, SimpleClass.class);
      assertEquals("a", object.getString());
    }
  }

  // TODO Structured types feature exists only on QA environments
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapStructAllTypes() throws SQLException {
    testMapAllTypes(false);
    testMapAllTypes(true);
  }

  private void testMapAllTypes(boolean registerFactory) throws SQLException {
    if (registerFactory) {
      SnowflakeObjectTypeFactories.register(AllTypesClass.class, AllTypesClass::new);
    } else {
      SnowflakeObjectTypeFactories.unregister(AllTypesClass.class);
    }
    try (Connection connection = init();
        Statement statement = connection.createStatement()) {
      statement.execute("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'");
      try (ResultSet resultSet =
          statement.executeQuery(
              "select {"
                  + "'string': 'a', "
                  + "'b': 1, "
                  + "'s': 2, "
                  + "'i': 3, "
                  + "'l': 4, "
                  + "'f': 1.1, "
                  + "'d': 2.2, "
                  + "'bd': 3.3, "
                  + "'bool': true, "
                  + "'timestamp_ltz': '2021-12-22 09:43:44'::TIMESTAMP_LTZ, "
                  + "'timestamp_ntz': '2021-12-23 09:44:44'::TIMESTAMP_NTZ, "
                  + "'timestamp_tz': '2021-12-24 09:45:45 +0800'::TIMESTAMP_TZ, "
                  + "'date': '2023-12-24'::DATE, "
                  + "'time': '12:34:56'::TIME, "
                  + "'binary': TO_BINARY('616263', 'HEX'), "
                  + "'simpleClass': {'string': 'b'}"
                  + "}::OBJECT("
                  + "string VARCHAR, "
                  + "b TINYINT, "
                  + "s SMALLINT, "
                  + "i INTEGER, "
                  + "l BIGINT, "
                  + "f FLOAT, "
                  + "d DOUBLE, "
                  + "bd DOUBLE, "
                  + "bool BOOLEAN, "
                  + "timestamp_ltz TIMESTAMP_LTZ, "
                  + "timestamp_ntz TIMESTAMP_NTZ, "
                  + "timestamp_tz TIMESTAMP_TZ, "
                  + "date DATE, "
                  + "time TIME, "
                  + "binary BINARY, "
                  + "simpleClass OBJECT(string VARCHAR)"
                  + ")"); ) {
        resultSet.next();
        AllTypesClass object = resultSet.getObject(1, AllTypesClass.class);
        assertEquals("a", object.getString());
        assertEquals(1, (long) object.getB());
        assertEquals(2, (long) object.getS());
        assertEquals(3, (long) object.getI());
        assertEquals(4, (long) object.getL());
        assertEquals(1.1, (double) object.getF(), 0.01);
        assertEquals(2.2, (double) object.getD(), 0.01);
        assertEquals(BigDecimal.valueOf(3.3), object.getBd());
        assertEquals(
            Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)), object.getTimestampLtz());
        assertEquals(
            Timestamp.valueOf(LocalDateTime.of(2021, 12, 23, 10, 44, 44)),
            object.getTimestampNtz());
        assertEquals(
            Timestamp.valueOf(LocalDateTime.of(2021, 12, 24, 2, 45, 45)), object.getTimestampTz());
        assertEquals(Date.valueOf(LocalDate.of(2023, 12, 24)), object.getDate());
        assertEquals(Time.valueOf(LocalTime.of(12, 34, 56)), object.getTime());
        assertArrayEquals(new byte[] {'a', 'b', 'c'}, object.getBinary());
        assertTrue(object.getBool());
        assertEquals("b", object.getSimpleClass().getString());
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapJsonToMap() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("SELECT OBJECT_CONSTRUCT('string','a','string2','s')");
    resultSet.next();
    Map map = resultSet.getObject(1, Map.class);
    assertEquals("a", map.get("string"));
    assertEquals("s", map.get("string2"));
    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testReturnAsMap() throws SQLException {
    SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "select {'x':{'string':'one'},'y':{'string':'two'},'z':{'string':'three'}}::MAP(VARCHAR, OBJECT(string VARCHAR));");
    resultSet.next();
    Map<String, SimpleClass> map =
        resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, SimpleClass.class);
    assertEquals(map.get("x").getString(), "one");
    assertEquals(map.get("y").getString(), "two");
    assertEquals(map.get("z").getString(), "three");
    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testReturnAsList() throws SQLException {
    SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "select [{'string':'one'},{'string': 'two'}]::ARRAY(OBJECT(string varchar))");
    resultSet.next();
    List<SimpleClass> map =
        resultSet.unwrap(SnowflakeBaseResultSet.class).getList(1, SimpleClass.class);
    assertEquals(map.get(0).getString(), "one");
    assertEquals(map.get(1).getString(), "two");
    statement.close();
    connection.close();
  }

  // TODO Structured types feature exists only on QA environments
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapStructsFromChunks() throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select {'string':'a'}::OBJECT(string VARCHAR) FROM TABLE(GENERATOR(ROWCOUNT=>30000))"); ) {
      while (resultSet.next()) {
        SimpleClass object = resultSet.getObject(1, SimpleClass.class);
        assertEquals("a", object.getString());
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapIntegerArray() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("SELECT ARRAY_CONSTRUCT(10, 20, 30)::ARRAY(INTEGER)");
    resultSet.next();
    Long[] resultArray = (Long[]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0], Long.valueOf(10));
    assertEquals(resultArray[1], Long.valueOf(20));
    assertEquals(resultArray[2], Long.valueOf(30));
    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapVarcharArray() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("SELECT ARRAY_CONSTRUCT('10', '20','30')::ARRAY(VARCHAR)");
    resultSet.next();
    String[] resultArray = (String[]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0], "10");
    assertEquals(resultArray[1], "20");
    assertEquals(resultArray[2], "30");
    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapDatesArray() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "SELECT ARRAY_CONSTRUCT(to_date('2023-12-24', 'YYYY-MM-DD'), to_date('2023-12-25', 'YYYY-MM-DD'))::ARRAY(DATE)");
    resultSet.next();
    Date[] resultArray = (Date[]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0], Date.valueOf(LocalDate.of(2023, 12, 24)));
    assertEquals(resultArray[1], Date.valueOf(LocalDate.of(2023, 12, 25)));
    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapTimeArray() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "SELECT ARRAY_CONSTRUCT(to_time('15:39:20.123'), to_time('15:39:20.123'))::ARRAY(TIME)");
    resultSet.next();
    Time[] resultArray = (Time[]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0], Time.valueOf(LocalTime.of(15, 39, 20)));
    assertEquals(resultArray[1], Time.valueOf(LocalTime.of(15, 39, 20)));
    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapTimestampArray() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "SELECT ARRAY_CONSTRUCT(TO_TIMESTAMP_NTZ('2021-12-23 09:44:44'), TO_TIMESTAMP_NTZ('2021-12-24 09:55:55'))::ARRAY(TIMESTAMP)");
    resultSet.next();
    Timestamp[] resultArray = (Timestamp[]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0], Timestamp.valueOf(LocalDateTime.of(2021, 12, 23, 10, 44, 44)));
    assertEquals(resultArray[1], Timestamp.valueOf(LocalDateTime.of(2021, 12, 24, 10, 55, 55)));
    // TODO test timestamps LTZ and TZ
    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapBooleanArray() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("SELECT ARRAY_CONSTRUCT(true,false)::ARRAY(BOOLEAN)");
    resultSet.next();
    Boolean[] resultArray = (Boolean[]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0], true);
    assertEquals(resultArray[1], false);
    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapBinaryArray() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "SELECT ARRAY_CONSTRUCT(TO_BINARY('616263', 'HEX'),TO_BINARY('616263', 'HEX'))::ARRAY(BINARY)");
    resultSet.next();
    Object[] resultArray = (Object[]) resultSet.getArray(1).getArray();
    assertArrayEquals((byte[]) resultArray[0], new byte[] {'a', 'b', 'c'});
    assertArrayEquals((byte[]) resultArray[1], new byte[] {'a', 'b', 'c'});
    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapStructArray() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "SELECT ARRAY_CONSTRUCT({'x': 'abc', 'y': 1}, {'x': 'def', 'y': 2} )::ARRAY(OBJECT(x VARCHAR, y Integer))");
    resultSet.next();
    Map[] resultArray = (Map[]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0].toString(), "{x=abc, y=1}");
    assertEquals(resultArray[1].toString(), "{x=def, y=2}");
    statement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapArrayOfArrays() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT({'x': 'abc', 'y': 1}, {'x': 'def', 'y': 2}) )::ARRAY(ARRAY(OBJECT(x VARCHAR, y Integer)))");
    resultSet.next();
    Map[][] resultArray = (Map[][]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0][0].toString(), "{x=abc, y=1}");
    assertEquals(resultArray[0][1].toString(), "{x=def, y=2}");
    statement.close();
    connection.close();
  }
}
