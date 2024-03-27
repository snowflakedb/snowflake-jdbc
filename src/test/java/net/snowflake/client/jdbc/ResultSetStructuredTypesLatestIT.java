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
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.ThrowingConsumer;
import net.snowflake.client.ThrowingRunnable;
import net.snowflake.client.category.TestCategoryStructuredType;
import net.snowflake.client.core.structs.SnowflakeObjectTypeFactories;
import net.snowflake.client.core.structs.StructureTypeHelper;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryStructuredType.class)
public class ResultSetStructuredTypesLatestIT extends BaseJDBCTest {
  private final ResultSetFormatType queryResultFormat;

  public ResultSetStructuredTypesLatestIT() {
    this(ResultSetFormatType.NATIVE_ARROW);
  }

  protected ResultSetStructuredTypesLatestIT(ResultSetFormatType queryResultFormat) {
    this.queryResultFormat = queryResultFormat;
  }

  public Connection init() throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE = true");
      stmt.execute("alter session set IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE = true");
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
  public void clean() throws Exception {
    SnowflakeObjectTypeFactories.unregister(SimpleClass.class);
    SnowflakeObjectTypeFactories.unregister(AllTypesClass.class);
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
    }
    withFirstRow(
        "select {'string':'a'}::OBJECT(string VARCHAR)",
        (resultSet) -> {
          SimpleClass object = resultSet.getObject(1, SimpleClass.class);
          assertEquals("a", object.getString());
        });
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
        assertEquals(new Byte("1"), object.getB());
        assertEquals(Short.valueOf("2"), object.getS());
        assertEquals(Integer.valueOf(3), object.getI());
        assertEquals(Long.valueOf(4), object.getL());
        assertEquals(Float.valueOf(1.1f), object.getF(), 0.01);
        assertEquals(Double.valueOf(2.2), object.getD(), 0.01);
        assertEquals(BigDecimal.valueOf(3.3), object.getBd());
        assertEquals(
            Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)), object.getTimestampLtz());
        assertEquals(
            Timestamp.valueOf(LocalDateTime.of(2021, 12, 23, 9, 44, 44)), object.getTimestampNtz());
        assertEquals(
            Timestamp.valueOf(LocalDateTime.of(2021, 12, 24, 2, 45, 45)), object.getTimestampTz());
        assertEquals(
            Date.valueOf(LocalDate.of(2023, 12, 24)).toString(), object.getDate().toString());
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
    withFirstRow(
        "SELECT OBJECT_CONSTRUCT('string','a','string2',1)",
        (resultSet) -> {
          Map map = resultSet.getObject(1, Map.class);
          assertEquals("a", map.get("string"));
          assertEquals(1, map.get("string2"));
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testReturnAsArrayOfSqlData() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT({'string':'one'}, {'string':'two'}, {'string':'three'})::ARRAY(OBJECT(string VARCHAR))",
        (resultSet) -> {
          SimpleClass[] resultArray =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, SimpleClass.class);
          assertEquals("one", resultArray[0].getString());
          assertEquals("two", resultArray[1].getString());
          assertEquals("three", resultArray[2].getString());
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testReturnAsArrayOfString() throws SQLException {
      Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
      withFirstRow(
        "SELECT ARRAY_CONSTRUCT('one', 'two','three')::ARRAY(VARCHAR)",
        (resultSet) -> {
          String[] resultArray =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, String.class);
          assertEquals("one", resultArray[0]);
          assertEquals("two", resultArray[1]);
          assertEquals("three", resultArray[2]);
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testReturnAsListOfIntegers() throws SQLException {
      Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
      withFirstRow(
        "SELECT ARRAY_CONSTRUCT(1,2,3)::ARRAY(INTEGER)",
        (resultSet) -> {
          List<Integer> resultList =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getList(1, Integer.class);
          assertEquals(Integer.valueOf(1), resultList.get(0));
          assertEquals(Integer.valueOf(2), resultList.get(1));
          assertEquals(Integer.valueOf(3), resultList.get(2));
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testReturnAsMap() throws SQLException {
    SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    withFirstRow(
        "select {'x':{'string':'one'},'y':{'string':'two'},'z':{'string':'three'}}::MAP(VARCHAR, OBJECT(string VARCHAR));",
        (resultSet) -> {
          Map<String, SimpleClass> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, SimpleClass.class);
          assertEquals("one", map.get("x").getString());
          assertEquals("two", map.get("y").getString());
          assertEquals("three", map.get("z").getString());
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testReturnAsMapOfLong() throws SQLException {
    withFirstRow(
        "SELECT {'x':1, 'y':2, 'z':3}::MAP(VARCHAR, BIGINT)",
        (resultSet) -> {
          Map<String, Long> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Long.class);
          assertEquals(Long.valueOf(1), map.get("x"));
          assertEquals(Long.valueOf(2), map.get("y"));
          assertEquals(Long.valueOf(3), map.get("z"));
        });
  }

    @Test
    @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
    public void testReturnAsMapOfTimestampsNTz() throws SQLException {
        withFirstRow(
                "SELECT {'x': TO_TIMESTAMP_NTZ('2021-12-23 09:44:44'), 'y': TO_TIMESTAMP_NTZ('2021-12-24 09:55:55')}::MAP(VARCHAR, TIMESTAMP)",
                (resultSet) -> {
                    Map<String, Timestamp> map = resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Timestamp.class);
                    assertEquals(
                            Timestamp.valueOf(LocalDateTime.of(2021, 12, 23, 10, 44, 44)), map.get("x"));
                    assertEquals(
                            Timestamp.valueOf(LocalDateTime.of(2021, 12, 24, 10, 55, 55)), map.get("y"));
                });
    }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testReturnAsMapOfBoolean() throws SQLException {
    withFirstRow(
        "SELECT {'x':'true', 'y':0}::MAP(VARCHAR, BOOLEAN)",
        (resultSet) -> {
          Map<String, Boolean> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Boolean.class);
          assertEquals(Boolean.TRUE, map.get("x"));
          assertEquals(Boolean.FALSE, map.get("y"));
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testReturnAsList() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    withFirstRow(
        "select [{'string':'one'},{'string': 'two'}]::ARRAY(OBJECT(string varchar))",
        (resultSet) -> {
          List<SimpleClass> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getList(1, SimpleClass.class);
          assertEquals("one", map.get(0).getString());
          assertEquals("two", map.get(1).getString());
        });
  }

  // TODO Structured types feature exists only on QA environments
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapStructsFromChunks() throws SQLException {
    withFirstRow(
        "select {'string':'a'}::OBJECT(string VARCHAR) FROM TABLE(GENERATOR(ROWCOUNT=>30000))",
        (resultSet) -> {
          while (resultSet.next()) {
            SimpleClass object = resultSet.getObject(1, SimpleClass.class);
            assertEquals("a", object.getString());
          }
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapIntegerArray() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(10, 20, 30)::ARRAY(INTEGER)",
        (resultSet) -> {
          Long[] resultArray = (Long[]) resultSet.getArray(1).getArray();
          assertEquals(Long.valueOf(10), resultArray[0]);
          assertEquals(Long.valueOf(20), resultArray[1]);
          assertEquals(Long.valueOf(30), resultArray[2]);
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapFixedToLongArray() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(10, 20, 30)::ARRAY(SMALLINT)",
        (resultSet) -> {
          Long[] resultArray = (Long[]) resultSet.getArray(1).getArray();
          assertEquals(Long.valueOf("10"), resultArray[0]);
          assertEquals(Long.valueOf("20"), resultArray[1]);
          assertEquals(Long.valueOf("30"), resultArray[2]);
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapDecimalArray() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    //    when: jdbc_treat_decimal_as_int=true scale=0
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT ARRAY_CONSTRUCT(10.2, 20.02, 30)::ARRAY(DECIMAL(20,0))"); ) {
      resultSet.next();
      Long[] resultArray = (Long[]) resultSet.getArray(1).getArray();
      assertEquals(resultArray[0], Long.valueOf(10));
      assertEquals(resultArray[1], Long.valueOf(20));
      assertEquals(resultArray[2], Long.valueOf(30));
    }

    //    when: jdbc_treat_decimal_as_int=true scale=2
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "SELECT ARRAY_CONSTRUCT(10.2, 20.02, 30)::ARRAY(DECIMAL(20,2))"); ) {
      resultSet.next();
      BigDecimal[] resultArray2 = (BigDecimal[]) resultSet.getArray(1).getArray();
      assertEquals(BigDecimal.valueOf(10.2), resultArray2[0]);
      assertEquals(BigDecimal.valueOf(20.02), resultArray2[1]);
      assertEquals(BigDecimal.valueOf(30), resultArray2[2]);
    }

    //    when: jdbc_treat_decimal_as_int=false scale=0
    try (Connection connection = init();
        Statement statement = connection.createStatement(); ) {
      statement.execute("alter session set jdbc_treat_decimal_as_int = false");
      try (ResultSet resultSet =
          statement.executeQuery("SELECT ARRAY_CONSTRUCT(10.2, 20.02, 30)::ARRAY(DECIMAL(20,0))")) {
        resultSet.next();
        BigDecimal[] resultArray = (BigDecimal[]) resultSet.getArray(1).getArray();
        assertEquals(BigDecimal.valueOf(10), resultArray[0]);
        assertEquals(BigDecimal.valueOf(20), resultArray[1]);
        assertEquals(BigDecimal.valueOf(30), resultArray[2]);
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapVarcharArray() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    withFirstRow(
        "SELECT 'text', ARRAY_CONSTRUCT('10', '20','30')::ARRAY(VARCHAR)",
        (resultSet) -> {
          String t = resultSet.getString(1);
          String[] resultArray = (String[]) resultSet.getArray(2).getArray();
          assertEquals("10", resultArray[0]);
          assertEquals("20", resultArray[1]);
          assertEquals("30", resultArray[2]);
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapDatesArray() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(to_date('2023-12-24', 'YYYY-MM-DD'), to_date('2023-12-25', 'YYYY-MM-DD'))::ARRAY(DATE)",
        (resultSet) -> {
          Date[] resultArray = (Date[]) resultSet.getArray(1).getArray();
          assertEquals(Date.valueOf(LocalDate.of(2023, 12, 24)), resultArray[0]);
          assertEquals(Date.valueOf(LocalDate.of(2023, 12, 25)), resultArray[1]);
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapTimeArray() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(to_time('15:39:20.123'), to_time('15:39:20.123'))::ARRAY(TIME)",
        (resultSet) -> {
          Time[] resultArray = (Time[]) resultSet.getArray(1).getArray();
          assertEquals(Time.valueOf(LocalTime.of(15, 39, 20)), resultArray[0]);
          assertEquals(Time.valueOf(LocalTime.of(15, 39, 20)), resultArray[1]);
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapTimestampArray() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(TO_TIMESTAMP_NTZ('2021-12-23 09:44:44'), TO_TIMESTAMP_NTZ('2021-12-24 09:55:55'))::ARRAY(TIMESTAMP)",
        (resultSet) -> {
          Timestamp[] resultArray = (Timestamp[]) resultSet.getArray(1).getArray();
          assertEquals(
              Timestamp.valueOf(LocalDateTime.of(2021, 12, 23, 10, 44, 44)), resultArray[0]);
          assertEquals(
              Timestamp.valueOf(LocalDateTime.of(2021, 12, 24, 10, 55, 55)), resultArray[1]);
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapBooleanArray() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(true,false)::ARRAY(BOOLEAN)",
        (resultSet) -> {
          Boolean[] resultArray = (Boolean[]) resultSet.getArray(1).getArray();
          assertEquals(true, resultArray[0]);
          assertEquals(false, resultArray[1]);
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapBinaryArray() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(TO_BINARY('616263', 'HEX'),TO_BINARY('616263', 'HEX'))::ARRAY(BINARY)",
        (resultSet) -> {
          Byte[][] resultArray = (Byte[][]) resultSet.getArray(1).getArray();
          assertArrayEquals(new Byte[] {'a', 'b', 'c'}, resultArray[0]);
          assertArrayEquals(new Byte[] {'a', 'b', 'c'}, resultArray[1]);
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapArrayOfStructToMap() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT({'x': 'abc', 'y': 1}, {'x': 'def', 'y': 2} )::ARRAY(OBJECT(x VARCHAR, y INTEGER))",
        (resultSet) -> {
          Map[] resultArray = (Map[]) resultSet.getArray(1).getArray();
          assertEquals("{x=abc, y=1}", resultArray[0].toString());
          assertEquals("{x=def, y=2}", resultArray[1].toString());
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapArrayOfArrays() throws SQLException {
    Assume.assumeTrue(queryResultFormat != ResultSetFormatType.NATIVE_ARROW);
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT({'x': 'abc', 'y': 1}, {'x': 'def', 'y': 2}) )::ARRAY(ARRAY(OBJECT(x VARCHAR, y INTEGER)))",
        (resultSet) -> {
          Map[][] resultArray = (Map[][]) resultSet.getArray(1).getArray();
          assertEquals("{x=abc, y=1}", resultArray[0][0].toString());
          assertEquals("{x=def, y=2}", resultArray[0][1].toString());
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testColumnTypeWhenStructureTypeIsDisabled() throws Exception {
    withStructureTypeTemporaryDisabled(
        () -> {
          withFirstRow(
              "SELECT {'string':'a'}::OBJECT(string VARCHAR)",
              resultSet -> {
                assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(1));
              });
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testColumnTypeWhenStructureTypeIsEnabled() throws Exception {
    withStructureTypeTemporaryEnabled(
        () -> {
          withFirstRow(
              "SELECT {'string':'a'}::OBJECT(string VARCHAR)",
              resultSet -> {
                assertEquals(Types.STRUCT, resultSet.getMetaData().getColumnType(1));
              });
        });
  }

  private void withFirstRow(String sqlText, ThrowingConsumer<ResultSet, SQLException> consumer)
      throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sqlText); ) {
      assertTrue(rs.next());
      consumer.accept(rs);
    }
  }

  private void withStructureTypeTemporaryEnabled(ThrowingRunnable action) throws Exception {
    boolean isStructureTypeEnabled = StructureTypeHelper.isStructureTypeEnabled();
    try {
      StructureTypeHelper.enableStructuredType();
      action.run();
    } finally {
      if (!isStructureTypeEnabled) {
        StructureTypeHelper.disableStructuredType();
      }
    }
  }

  private void withStructureTypeTemporaryDisabled(ThrowingRunnable action) throws Exception {
    boolean isStructureTypeEnabled = StructureTypeHelper.isStructureTypeEnabled();
    try {
      StructureTypeHelper.disableStructuredType();
      action.run();
    } finally {
      if (isStructureTypeEnabled) {
        StructureTypeHelper.enableStructuredType();
      }
    }
  }

  enum ResultSetFormatType {
    JSON("JSON"),
    ARROW_WITH_JSON_STRUCTURED_TYPES("ARROW"),
    NATIVE_ARROW("ARROW");

    public final String sessionParameterTypeValue;

    ResultSetFormatType(String sessionParameterTypeValue) {
      this.sessionParameterTypeValue = sessionParameterTypeValue;
    }
  }
}
