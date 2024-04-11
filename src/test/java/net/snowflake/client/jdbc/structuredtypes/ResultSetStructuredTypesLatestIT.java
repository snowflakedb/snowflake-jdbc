/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc.structuredtypes;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.SnowflakeBaseResultSet;
import net.snowflake.client.jdbc.SnowflakeResultSetMetaData;
import net.snowflake.client.jdbc.structuredtypes.sqldata.AllTypesClass;
import net.snowflake.client.jdbc.structuredtypes.sqldata.FewTypesSqlData;
import net.snowflake.client.jdbc.structuredtypes.sqldata.SimpleClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category(TestCategoryStructuredType.class)
@Ignore
public class ResultSetStructuredTypesLatestIT extends BaseJDBCTest {

  @Parameterized.Parameters(name = "format={0}")
  public static Object[][] data() {
    return new Object[][] {
      {ResultSetFormatType.JSON},
      {ResultSetFormatType.ARROW_WITH_JSON_STRUCTURED_TYPES},
      {ResultSetFormatType.NATIVE_ARROW}
    };
  }

  private final ResultSetFormatType queryResultFormat;

  public ResultSetStructuredTypesLatestIT(ResultSetFormatType queryResultFormat) {
    this.queryResultFormat = queryResultFormat;
  }

  public Connection init() throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE = true");
      stmt.execute("alter session set IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE = true");
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
  public void clean() throws Exception {
    SnowflakeObjectTypeFactories.unregister(SimpleClass.class);
    SnowflakeObjectTypeFactories.unregister(AllTypesClass.class);
  }

  @Test
  public void testMapStructToObjectWithFactory() throws SQLException {
    testMapJson(true);
  }

  @Test
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

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapStructAllTypes() throws SQLException {
    testMapAllTypes(false);
    testMapAllTypes(true);
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMapNullStruct() throws SQLException {
    withFirstRow(
        "select null::OBJECT(string VARCHAR)",
        (resultSet) -> {
          SimpleClass object = resultSet.getObject(1, SimpleClass.class);
          assertNull(object);
        });
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
  public void testReturnAsArrayOfSqlData() throws SQLException {
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
  public void testReturnAsArrayOfNullableFieldsInSqlData() throws SQLException {
    SnowflakeObjectTypeFactories.register(FewTypesSqlData.class, FewTypesSqlData::new);
    withFirstRow(
        "SELECT OBJECT_CONSTRUCT_KEEP_NULL('string', null, 'nullableIntValue', null, 'nullableLongValue', null, "
            + "'date', null, 'bd', null, 'bytes', null, 'longValue', null)"
            + "::OBJECT(string VARCHAR, nullableIntValue INTEGER, nullableLongValue INTEGER, date DATE, bd DOUBLE, bytes BINARY, longValue INTEGER)",
        (resultSet) -> {
          FewTypesSqlData result =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getObject(1, FewTypesSqlData.class);
          assertNull(result.getString());
          assertNull(result.getNullableIntValue());
          assertNull(result.getNullableLongValue());
          assertNull(result.getDate());
          assertNull(result.getBd());
          assertNull(result.getBytes());
          assertEquals(Long.valueOf(0), result.getLongValue());
        });
  }

  @Test
  public void testReturnNullsForAllTpesInSqlData() throws SQLException {
    SnowflakeObjectTypeFactories.register(AllTypesClass.class, AllTypesClass::new);
    try (Connection connection = init();
        Statement statement = connection.createStatement()) {
      statement.execute("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'");
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT OBJECT_CONSTRUCT_KEEP_NULL('string', null, 'b', null, 's', null, 'i', null, 'l', null, 'f', null,'d', null, 'bd', null, 'bool', null,"
                  + " 'timestamp_ltz', null, 'timestamp_ntz', null, 'timestamp_tz', null, 'date', null, 'time', null, 'binary', null, 'simpleClass', null)"
                  + "::OBJECT(string VARCHAR, b TINYINT, s SMALLINT, i INTEGER, l BIGINT, f FLOAT, d DOUBLE, bd DOUBLE, bool BOOLEAN, timestamp_ltz TIMESTAMP_LTZ, "
                  + "timestamp_ntz TIMESTAMP_NTZ, timestamp_tz TIMESTAMP_TZ, date DATE, time TIME, binary BINARY, simpleClass OBJECT(string VARCHAR))"); ) {
        resultSet.next();
        AllTypesClass object = resultSet.getObject(1, AllTypesClass.class);
        assertNull(object.getString());
        assertNull(object.getB());
        assertNull(object.getS());
        assertNull(object.getI());
        assertNull(object.getL());
        assertNull(object.getF());
        assertNull(object.getD());
        assertNull(object.getBd());
        assertNull(object.getTimestampLtz());
        assertNull(object.getTimestampNtz());
        assertNull(object.getTimestampTz());
        assertNull(object.getDate());
        assertNull(object.getTime());
        assertNull(object.getBinary());
        assertNull(object.getBool());
        assertNull(object.getSimpleClass());
      }
    }
  }

  @Test
  public void testReturnAsArrayOfString() throws SQLException {
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
  @Ignore // Arrays containing nulls aren't supported: SNOW-720936
  public void testReturnAsArrayOfNullableString() throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT('one', 'two', null)::ARRAY(VARCHAR)",
        (resultSet) -> {
          String[] resultArray =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, String.class);
          assertEquals("one", resultArray[0]);
          assertEquals("two", resultArray[1]);
          assertNull(resultArray[2]);
        });
  }

  @Test
  public void testReturnNullAsArray() throws SQLException {
    withFirstRow(
        "SELECT null::ARRAY(VARCHAR)",
        (resultSet) -> {
          String[] resultArray =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, String.class);
          assertNull(resultArray);
        });
  }

  @Test
  public void testReturnAsListOfIntegers() throws SQLException {
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
  public void testReturnAsListOfFloat() throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(1.1,2.2,3.3)::ARRAY(FLOAT)",
        (resultSet) -> {
          List<Float> resultList =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getList(1, Float.class);
          assertEquals(Float.valueOf(1.1f), resultList.get(0));
          assertEquals(Float.valueOf(2.2f), resultList.get(1));
          assertEquals(Float.valueOf(3.3f), resultList.get(2));
        });
  }

  @Test
  public void testReturnAsListOfDouble() throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(1.1,2.2,3.3)::ARRAY(DOUBLE)",
        (resultSet) -> {
          List<Double> resultList =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getList(1, Double.class);
          assertEquals(Double.valueOf(1.1), resultList.get(0));
          assertEquals(Double.valueOf(2.2), resultList.get(1));
          assertEquals(Double.valueOf(3.3), resultList.get(2));
        });
  }

  @Test
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
  public void testReturnAsMapWithNullableValues() throws SQLException {
    SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    withFirstRow(
        "select {'x':{'string':'one'},'y':null,'z':{'string':'three'}}::MAP(VARCHAR, OBJECT(string VARCHAR));",
        (resultSet) -> {
          Map<String, SimpleClass> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, SimpleClass.class);
          assertEquals("one", map.get("x").getString());
          assertNull(map.get("y"));
          assertEquals("three", map.get("z").getString());
        });
  }

  @Test
  public void testReturnNullAsObjectOfTypeMap() throws SQLException {
    SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    withFirstRow(
        "select null::MAP(VARCHAR, OBJECT(string VARCHAR));",
        (resultSet) -> {
          Map<String, String> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getObject(1, Map.class);
          assertNull(map);
        });
  }

  @Test
  public void testReturnNullAsMap() throws SQLException {
    SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    withFirstRow(
        "select null::MAP(VARCHAR, OBJECT(string VARCHAR));",
        (resultSet) -> {
          Map<String, SimpleClass> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, SimpleClass.class);
          assertNull(map);
        });
  }

  @Test
  public void testReturnAsMapOfTimestampsNtz() throws SQLException {
    withFirstRow(
        "SELECT {'x': TO_TIMESTAMP_NTZ('2021-12-23 09:44:44'), 'y': TO_TIMESTAMP_NTZ('2021-12-24 09:55:55')}::MAP(VARCHAR, TIMESTAMP)",
        (resultSet) -> {
          Map<String, Timestamp> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Timestamp.class);
          assertEquals(Timestamp.valueOf(LocalDateTime.of(2021, 12, 23, 9, 44, 44)), map.get("x"));
          assertEquals(Timestamp.valueOf(LocalDateTime.of(2021, 12, 24, 9, 55, 55)), map.get("y"));
        });
  }

  @Test
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
  public void testReturnAsMapOfTimestamp() throws SQLException {
    withFirstRow(
        "SELECT {'x':'2021-12-22 09:43:44.000 +0100', 'y':'2021-12-22 10:43:44.000 +0100'}::MAP(VARCHAR, TIMESTAMP)",
        (resultSet) -> {
          Map<String, Timestamp> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Timestamp.class);
          assertEquals(Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)), map.get("x"));
          assertEquals(Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 10, 43, 44)), map.get("y"));
        });
  }

  @Test
  public void testReturnAsMapOfDate() throws SQLException {
    withFirstRow(
        "SELECT {'x':'2023-12-24', 'y':'2023-12-25'}::MAP(VARCHAR, DATE)",
        (resultSet) -> {
          Map<String, Date> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Date.class);
          assertEquals(
              Date.valueOf(LocalDate.of(2023, 12, 24)).toString(), map.get("x").toString());
          assertEquals(
              Date.valueOf(LocalDate.of(2023, 12, 25)).toString(), map.get("y").toString());
        });
  }

  @Test
  public void testReturnAsMapOfTime() throws SQLException {
    withFirstRow(
        "SELECT {'x':'12:34:56', 'y':'12:34:58'}::MAP(VARCHAR, TIME)",
        (resultSet) -> {
          Map<String, Time> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Time.class);
          assertEquals(Time.valueOf(LocalTime.of(12, 34, 56)), map.get("x"));
          assertEquals(Time.valueOf(LocalTime.of(12, 34, 58)), map.get("y"));
        });
  }

  @Test
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
  public void testReturnAsList() throws SQLException {
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

  @Test
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
  public void testMapIntegerArray() throws SQLException {
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
  public void testMapFixedToLongArray() throws SQLException {
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
  public void testMapDecimalArray() throws SQLException {
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
      assertEquals(BigDecimal.valueOf(10.20).doubleValue(), resultArray2[0].doubleValue(), 0);
      assertEquals(BigDecimal.valueOf(20.02).doubleValue(), resultArray2[1].doubleValue(), 0);
      assertEquals(BigDecimal.valueOf(30.00).doubleValue(), resultArray2[2].doubleValue(), 0);
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
  public void testMapVarcharArray() throws SQLException {
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
  public void testMapDatesArray() throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(to_date('2023-12-24', 'YYYY-MM-DD'), to_date('2023-12-25', 'YYYY-MM-DD'))::ARRAY(DATE)",
        (resultSet) -> {
          Date[] resultArray = (Date[]) resultSet.getArray(1).getArray();
          assertEquals(
              Date.valueOf(LocalDate.of(2023, 12, 24)).toString(), resultArray[0].toString());
          assertEquals(
              Date.valueOf(LocalDate.of(2023, 12, 25)).toString(), resultArray[1].toString());
        });
  }

  @Test
  public void testMapTimeArray() throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(to_time('15:39:20.123'), to_time('09:12:20.123'))::ARRAY(TIME)",
        (resultSet) -> {
          Time[] resultArray = (Time[]) resultSet.getArray(1).getArray();
          assertEquals(
              Time.valueOf(LocalTime.of(15, 39, 20)).toString(), resultArray[0].toString());
          assertEquals(Time.valueOf(LocalTime.of(9, 12, 20)).toString(), resultArray[1].toString());
        });
  }

  @Test
  public void testMapTimestampArray() throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(TO_TIMESTAMP_NTZ('2021-12-23 09:44:44'), TO_TIMESTAMP_NTZ('2021-12-24 09:55:55'))::ARRAY(TIMESTAMP)",
        (resultSet) -> {
          Timestamp[] resultArray = (Timestamp[]) resultSet.getArray(1).getArray();
          assertEquals(
              Timestamp.valueOf(LocalDateTime.of(2021, 12, 23, 9, 44, 44)), resultArray[0]);
          assertEquals(
              Timestamp.valueOf(LocalDateTime.of(2021, 12, 24, 9, 55, 55)), resultArray[1]);
        });
  }

  @Test
  public void testMapBooleanArray() throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(true,false)::ARRAY(BOOLEAN)",
        (resultSet) -> {
          Boolean[] resultArray = (Boolean[]) resultSet.getArray(1).getArray();
          assertEquals(true, resultArray[0]);
          assertEquals(false, resultArray[1]);
        });
  }

  @Test
  public void testMapBinaryArray() throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(TO_BINARY('616263', 'HEX'),TO_BINARY('616263', 'HEX'))::ARRAY(BINARY)",
        (resultSet) -> {
          Byte[][] resultArray = (Byte[][]) resultSet.getArray(1).getArray();
          assertArrayEquals(new Byte[] {'a', 'b', 'c'}, resultArray[0]);
          assertArrayEquals(new Byte[] {'a', 'b', 'c'}, resultArray[1]);
        });
  }

  @Test
  public void testMapArrayOfStructToMap() throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT({'x': 'abc', 'y': 1}, {'x': 'def', 'y': 2} )::ARRAY(OBJECT(x VARCHAR, y INTEGER))",
        (resultSet) -> {
          Map[] resultArray = (Map[]) resultSet.getArray(1).getArray();
          Map<String, Object> firstEntry = resultArray[0];
          Map<String, Object> secondEntry = resultArray[1];
          assertEquals(firstEntry.get("x").toString(), "abc");
          assertEquals(firstEntry.get("y").toString(), "1");
          assertEquals(secondEntry.get("x").toString(), "def");
          assertEquals(secondEntry.get("y").toString(), "2");
        });
  }

  @Test
  public void testMapArrayOfArrays() throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT({'x': 'abc', 'y': 1}, {'x': 'def', 'y': 2}) )::ARRAY(ARRAY(OBJECT(x VARCHAR, y INTEGER)))",
        (resultSet) -> {
          Map[][] resultArray = (Map[][]) resultSet.getArray(1).getArray();
          Map<String, Object> firstEntry = resultArray[0][0];
          Map<String, Object> secondEntry = resultArray[0][1];
          assertEquals(firstEntry.get("x").toString(), "abc");
          assertEquals(firstEntry.get("y").toString(), "1");
          assertEquals(secondEntry.get("x").toString(), "def");
          assertEquals(secondEntry.get("y").toString(), "2");
        });
  }

  @Test
  public void testColumnTypeWhenStructureTypeIsNotReturned() throws Exception {
    withStructureTypeTemporaryDisabled(
        () -> {
          withFirstRow(
              "SELECT {'string':'a'}",
              resultSet -> {
                assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(1));
              });
        });
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testColumnTypeAndFieldsWhenStructureTypeIsReturned() throws Exception {
    withStructureTypeTemporaryEnabled(
        () -> {
          withFirstRow(
              "SELECT {'string':'a'}::OBJECT(string VARCHAR)",
              resultSet -> {
                assertEquals(Types.STRUCT, resultSet.getMetaData().getColumnType(1));
                assertEquals(
                    1,
                    resultSet
                        .getMetaData()
                        .unwrap(SnowflakeResultSetMetaData.class)
                        .getColumnFields(1)
                        .size());
                assertEquals(
                    "VARCHAR",
                    resultSet
                        .getMetaData()
                        .unwrap(SnowflakeResultSetMetaData.class)
                        .getColumnFields(1)
                        .get(0)
                        .getTypeName());
                assertEquals(
                    "string",
                    resultSet
                        .getMetaData()
                        .unwrap(SnowflakeResultSetMetaData.class)
                        .getColumnFields(1)
                        .get(0)
                        .getName());
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
