package net.snowflake.client.jdbc.structuredtypes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import net.snowflake.client.TestUtil;
import net.snowflake.client.ThrowingConsumer;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.structs.SnowflakeObjectTypeFactories;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.ResultSetFormatType;
import net.snowflake.client.jdbc.SnowflakeBaseResultSet;
import net.snowflake.client.jdbc.SnowflakeResultSetMetaData;
import net.snowflake.client.jdbc.structuredtypes.sqldata.AllTypesClass;
import net.snowflake.client.jdbc.structuredtypes.sqldata.NestedStructSqlData;
import net.snowflake.client.jdbc.structuredtypes.sqldata.NullableFieldsSqlData;
import net.snowflake.client.jdbc.structuredtypes.sqldata.SimpleClass;
import net.snowflake.client.jdbc.structuredtypes.sqldata.StringClass;
import net.snowflake.client.providers.ResultFormatProvider;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(TestTags.RESULT_SET)
public class ResultSetStructuredTypesLatestIT extends BaseJDBCTest {
  @BeforeEach
  public void setup() {
    SnowflakeObjectTypeFactories.register(StringClass.class, StringClass::new);
    SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    SnowflakeObjectTypeFactories.register(AllTypesClass.class, AllTypesClass::new);
    SnowflakeObjectTypeFactories.register(NullableFieldsSqlData.class, NullableFieldsSqlData::new);
  }

  @AfterEach
  public void clean() {
    SnowflakeObjectTypeFactories.unregister(StringClass.class);
    SnowflakeObjectTypeFactories.unregister(SimpleClass.class);
    SnowflakeObjectTypeFactories.unregister(AllTypesClass.class);
    SnowflakeObjectTypeFactories.unregister(NullableFieldsSqlData.class);
  }

  public Connection init(ResultSetFormatType format) throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE = true");
      stmt.execute("alter session set IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE = true");
      stmt.execute("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'");
      stmt.execute(
          "alter session set jdbc_query_result_format = '"
              + format.sessionParameterTypeValue
              + "'");
      if (format == ResultSetFormatType.NATIVE_ARROW) {
        stmt.execute("alter session set ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT = true");
        stmt.execute("alter session set FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT = true");
      }
    }
    return conn;
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapStructToObjectWithFactory(ResultSetFormatType format) throws SQLException {
    testMapJson(true, format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapStructToObjectWithReflection(ResultSetFormatType format) throws SQLException {
    testMapJson(false, format);
    testMapJson(true, format);
  }

  private void testMapJson(boolean registerFactory, ResultSetFormatType format)
      throws SQLException {
    if (registerFactory) {
      SnowflakeObjectTypeFactories.register(StringClass.class, StringClass::new);
    } else {
      SnowflakeObjectTypeFactories.unregister(StringClass.class);
    }
    withFirstRow(
        "select {'string':'a'}::OBJECT(string VARCHAR)",
        (resultSet) -> {
          StringClass object = resultSet.getObject(1, StringClass.class);
          assertEquals("a", object.getString());
        },
        format);
    SnowflakeObjectTypeFactories.register(StringClass.class, StringClass::new);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapNullStruct(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "select null::OBJECT(string VARCHAR)",
        (resultSet) -> {
          StringClass object = resultSet.getObject(1, StringClass.class);
          assertNull(object);
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapStructAllTypes(ResultSetFormatType format) throws SQLException {
    try (Connection connection = init(format);
        Statement statement = connection.createStatement()) {
      statement.execute("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'");
      try (ResultSet resultSet = statement.executeQuery(AllTypesClass.ALL_TYPES_QUERY); ) {
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
            LocalDateTime.of(2021, 12, 22, 9, 43, 44)
                .atZone(ZoneId.of("Europe/Warsaw"))
                .toInstant(),
            object.getTimestampLtz().toInstant());
        assertEquals(
            Timestamp.valueOf(LocalDateTime.of(2021, 12, 23, 9, 44, 44)), object.getTimestampNtz());
        assertEquals(
            LocalDateTime.of(2021, 12, 24, 2, 45, 45)
                .atZone(ZoneId.of("Europe/Warsaw"))
                .toInstant(),
            object.getTimestampTz().toInstant());
        // TODO uncomment after merge SNOW-928973: Date field is returning one day less when getting
        // through getString method
        //                assertEquals(
        //                    Date.valueOf(LocalDate.of(2023, 12, 24)).toString(),
        //         object.getDate().toString());
        assertEquals(Time.valueOf(LocalTime.of(12, 34, 56)), object.getTime());
        assertArrayEquals(new byte[] {'a', 'b', 'c'}, object.getBinary());
        assertTrue(object.getBool());
        assertEquals("b", object.getSimpleClass().getString());
        assertEquals(Integer.valueOf(2), object.getSimpleClass().getIntValue());

        if (format == ResultSetFormatType.NATIVE_ARROW) {
          // Only verify getString for Arrow since JSON representations have difficulties with
          // floating point toString conversion (3.300000000000000e+00 vs 3.3 in native arrow)
          String expectedArrowGetStringResult =
              "{\"string\": \"a\",\"b\": 1,\"s\": 2,\"i\": 3,\"l\": 4,\"f\": 1.1,\"d\": 2.2,\"bd\": 3.3,\"bool\": true,\"timestamp_ltz\": \"Wed, 22 Dec 2021 09:43:44 +0100\",\"timestamp_ntz\": \"Thu, 23 Dec 2021 09:44:44 Z\",\"timestamp_tz\": \"Fri, 24 Dec 2021 09:45:45 +0800\",\"date\": \"2023-12-24\",\"time\": \"12:34:56\",\"binary\": \"616263\",\"simpleClass\": {\"string\": \"b\",\"intValue\": 2}}";
          assertEquals(expectedArrowGetStringResult, resultSet.getString(1));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnStructAsStringIfTypeWasNotIndicated(ResultSetFormatType format)
      throws SQLException {
    Assumptions.assumeTrue(format != ResultSetFormatType.NATIVE_ARROW);
    try (Connection connection = init(format);
        Statement statement = connection.createStatement()) {
      statement.execute(
          "alter session set "
              + "TIMEZONE='Europe/Warsaw',"
              + "TIME_OUTPUT_FORMAT = 'HH24:MI:SS',"
              + "DATE_OUTPUT_FORMAT = 'YYYY-MM-DD',"
              + "TIMESTAMP_TYPE_MAPPING='TIMESTAMP_LTZ',"
              + "TIMESTAMP_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM',"
              + "TIMESTAMP_TZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM',"
              + "TIMESTAMP_LTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM',"
              + "TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD HH24:MI:SS.FF3'");

      try (ResultSet resultSet = statement.executeQuery(AllTypesClass.ALL_TYPES_QUERY); ) {
        resultSet.next();
        String object = (String) resultSet.getObject(1);
        String expectedJson =
            "{\n"
                + "  \"string\": \"a\",\n"
                + "  \"b\": 1,\n"
                + "  \"s\": 2,\n"
                + "  \"i\": 3,\n"
                + "  \"l\": 4,\n"
                + "  \"f\": 1.100000000000000e+00,\n"
                + "  \"d\": 2.200000000000000e+00,\n"
                + "  \"bd\": 3.300000000000000e+00,\n"
                + "  \"bool\": true,\n"
                + "  \"timestamp_ltz\": \"2021-12-22 09:43:44.000 +0100\",\n"
                + "  \"timestamp_ntz\": \"2021-12-23 09:44:44.000\",\n"
                + "  \"timestamp_tz\": \"2021-12-24 09:45:45.000 +0800\",\n"
                + "  \"date\": \"2023-12-24\",\n"
                + "  \"time\": \"12:34:56\",\n"
                + "  \"binary\": \"616263\",\n"
                + "  \"simpleClass\": {\n"
                + "    \"string\": \"b\",\n"
                + "    \"intValue\": 2\n"
                + "  }\n"
                + "}";
        String expectedJsonFromArrow =
            "{\"string\": \"a\",\"b\": 1,\"s\": 2,\"i\": 3,\"l\": 4,\"f\": 1.1,\"d\": 2.2,\"bd\": 3.3,"
                + "\"bool\": true,\"timestamp_ltz\": \"2021-12-22 09:43:44.000 +0100\",\"timestamp_ntz\": \"2021-12-23 09:44:44.000\","
                + "\"timestamp_tz\": \"2021-12-24 09:45:45.000 +0800\",\"date\": \"2023-12-24\",\"time\": \"12:34:56\",\"binary\": \"616263\","
                + "\"simpleClass\": {\"string\": \"b\",\"intValue\": 2}}";
        if (format == ResultSetFormatType.NATIVE_ARROW) {
          Assert.assertEquals(expectedJsonFromArrow, object);
        } else {
          Assert.assertEquals(expectedJson, object);
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsArrayOfSqlData(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT({'string':'one'}, {'string':'two'}, {'string':'three'})::ARRAY(OBJECT(string VARCHAR))",
        (resultSet) -> {
          StringClass[] resultArray =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, StringClass.class);
          assertEquals("one", resultArray[0].getString());
          assertEquals("two", resultArray[1].getString());
          assertEquals("three", resultArray[2].getString());
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsArrayOfNullableFieldsInSqlData(ResultSetFormatType format)
      throws SQLException {
    withFirstRow(
        "SELECT OBJECT_CONSTRUCT_KEEP_NULL('string', null, 'nullableIntValue', null, 'nullableLongValue', null, "
            + "'date', null, 'bd', null, 'bytes', null, 'longValue', null)"
            + "::OBJECT(string VARCHAR, nullableIntValue INTEGER, nullableLongValue INTEGER, date DATE, bd DOUBLE, bytes BINARY, longValue INTEGER)",
        (resultSet) -> {
          NullableFieldsSqlData result = resultSet.getObject(1, NullableFieldsSqlData.class);
          assertNull(result.getString());
          assertNull(result.getNullableIntValue());
          assertNull(result.getNullableLongValue());
          assertNull(result.getDate());
          assertNull(result.getBd());
          assertNull(result.getBytes());
          assertEquals(Long.valueOf(0), result.getLongValue());
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnNullsForAllTpesInSqlData(ResultSetFormatType format) throws SQLException {
    try (Connection connection = init(format);
        Statement statement = connection.createStatement()) {
      statement.execute("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'");
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT OBJECT_CONSTRUCT_KEEP_NULL('string', null, 'b', null, 's', null, 'i', null, 'l', null, 'f', null,'d', null, 'bd', null, 'bool', null,"
                  + " 'timestamp_ltz', null, 'timestamp_ntz', null, 'timestamp_tz', null, 'date', null, 'time', null, 'binary', null, 'StringClass', null)"
                  + "::OBJECT(string VARCHAR, b TINYINT, s SMALLINT, i INTEGER, l BIGINT, f FLOAT, d DOUBLE, bd DOUBLE, bool BOOLEAN, timestamp_ltz TIMESTAMP_LTZ, "
                  + "timestamp_ntz TIMESTAMP_NTZ, timestamp_tz TIMESTAMP_TZ, date DATE, time TIME, binary BINARY, StringClass OBJECT(string VARCHAR))"); ) {
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

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsArrayOfString(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT('one', 'two','three')::ARRAY(VARCHAR)",
        (resultSet) -> {
          String[] resultArray =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, String.class);
          assertEquals("one", resultArray[0]);
          assertEquals("two", resultArray[1]);
          assertEquals("three", resultArray[2]);
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsArrayOfNullableString(ResultSetFormatType format) throws SQLException {
    Assumptions.assumeTrue(format == ResultSetFormatType.NATIVE_ARROW);
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT('one', 'two', null)::ARRAY(VARCHAR)",
        (resultSet) -> {
          String[] resultArray =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, String.class);
          assertEquals("one", resultArray[0]);
          assertEquals("two", resultArray[1]);
          assertNull(resultArray[2]);
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnNullAsArray(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT null::ARRAY(VARCHAR)",
        (resultSet) -> {
          String[] resultArray =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, String.class);
          assertNull(resultArray);
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsListOfIntegers(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(1,2,3)::ARRAY(INTEGER)",
        (resultSet) -> {
          List<Integer> resultList =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getList(1, Integer.class);
          assertEquals(Integer.valueOf(1), resultList.get(0));
          assertEquals(Integer.valueOf(2), resultList.get(1));
          assertEquals(Integer.valueOf(3), resultList.get(2));
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsListOfFloat(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(1.1,2.2,3.3)::ARRAY(FLOAT)",
        (resultSet) -> {
          Float[] resultList =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Float.class);
          assertEquals(Float.valueOf(1.1f), resultList[0]);
          assertEquals(Float.valueOf(2.2f), resultList[1]);
          assertEquals(Float.valueOf(3.3f), resultList[2]);
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsListOfDouble(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(1.1,2.2,3.3)::ARRAY(DOUBLE)",
        (resultSet) -> {
          List<Double> resultList =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getList(1, Double.class);
          assertEquals(Double.valueOf(1.1), resultList.get(0));
          assertEquals(Double.valueOf(2.2), resultList.get(1));
          assertEquals(Double.valueOf(3.3), resultList.get(2));
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsMap(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "select {'x':{'string':'one'},'y':{'string':'two'},'z':{'string':'three'}}::MAP(VARCHAR, OBJECT(string VARCHAR));",
        (resultSet) -> {
          Map<String, StringClass> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, StringClass.class);
          assertEquals("one", map.get("x").getString());
          assertEquals("two", map.get("y").getString());
          assertEquals("three", map.get("z").getString());
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsMapByGetObject(ResultSetFormatType format) throws SQLException {
    Assumptions.assumeTrue(format != ResultSetFormatType.NATIVE_ARROW);
    withFirstRow(
        "select {'x':{'string':'one'},'y':{'string':'two'},'z':{'string':'three'}}::MAP(VARCHAR, OBJECT(string VARCHAR));",
        (resultSet) -> {
          Map<String, Map<String, Object>> map = resultSet.getObject(1, Map.class);
          assertEquals("one", map.get("x").get("string"));
          assertEquals("two", map.get("y").get("string"));
          assertEquals("three", map.get("z").get("string"));
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsMapWithNullableValues(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "select {'x':{'string':'one'},'y':null,'z':{'string':'three'}}::MAP(VARCHAR, OBJECT(string VARCHAR));",
        (resultSet) -> {
          Map<String, StringClass> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, StringClass.class);
          assertEquals("one", map.get("x").getString());
          assertNull(map.get("y"));
          assertEquals("three", map.get("z").getString());
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnNullAsObjectOfTypeMap(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "select null::MAP(VARCHAR, OBJECT(string VARCHAR));",
        (resultSet) -> {
          Map<String, Object> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getObject(1, Map.class);
          assertNull(map);
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnNullAsMap(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "select null::MAP(VARCHAR, OBJECT(string VARCHAR));",
        (resultSet) -> {
          Map<String, StringClass> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, StringClass.class);
          assertNull(map);
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsMapOfTimestampsNtz(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT {'x': TO_TIMESTAMP_NTZ('2021-12-23 09:44:44'), 'y': TO_TIMESTAMP_NTZ('2021-12-24 09:55:55')}::MAP(VARCHAR, TIMESTAMP)",
        (resultSet) -> {
          Map<String, Timestamp> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Timestamp.class);
          assertEquals(
              LocalDateTime.of(2021, 12, 23, 9, 44, 44)
                  .atZone(ZoneId.of("Europe/Warsaw"))
                  .toInstant(),
              map.get("x").toInstant());
          assertEquals(
              LocalDateTime.of(2021, 12, 24, 9, 55, 55)
                  .atZone(ZoneId.of("Europe/Warsaw"))
                  .toInstant(),
              map.get("y").toInstant());
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsMapOfTimestampsLtz(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT {'x': TO_TIMESTAMP_LTZ('2021-12-23 09:44:44'), 'y': TO_TIMESTAMP_LTZ('2021-12-24 09:55:55')}::MAP(VARCHAR, TIMESTAMP_LTZ)",
        (resultSet) -> {
          Map<String, Timestamp> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Timestamp.class);
          assertEquals(
              LocalDateTime.of(2021, 12, 23, 9, 44, 44)
                  .atZone(ZoneId.of("Europe/Warsaw"))
                  .toInstant(),
              map.get("x").toInstant());
          assertEquals(
              LocalDateTime.of(2021, 12, 24, 9, 55, 55)
                  .atZone(ZoneId.of("Europe/Warsaw"))
                  .toInstant(),
              map.get("y").toInstant());
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsMapOfLong(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT {'x':1, 'y':2, 'z':3}::MAP(VARCHAR, BIGINT)",
        (resultSet) -> {
          Map<String, Long> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Long.class);
          assertEquals(Long.valueOf(1), map.get("x"));
          assertEquals(Long.valueOf(2), map.get("y"));
          assertEquals(Long.valueOf(3), map.get("z"));
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsMapOfDate(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT {'x':'2023-12-24', 'y':'2023-12-25'}::MAP(VARCHAR, DATE)",
        (resultSet) -> {
          Map<String, Date> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Date.class);
          assertEquals(
              Date.valueOf(LocalDate.of(2023, 12, 24)).toString(), map.get("x").toString());
          assertEquals(
              Date.valueOf(LocalDate.of(2023, 12, 25)).toString(), map.get("y").toString());
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsMapOfTime(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT {'x':'12:34:56', 'y':'12:34:58'}::MAP(VARCHAR, TIME)",
        (resultSet) -> {
          Map<String, Time> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Time.class);
          assertEquals(Time.valueOf(LocalTime.of(12, 34, 56)), map.get("x"));
          assertEquals(Time.valueOf(LocalTime.of(12, 34, 58)), map.get("y"));
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsMapOfBoolean(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT {'x':'true', 'y':0}::MAP(VARCHAR, BOOLEAN)",
        (resultSet) -> {
          Map<String, Boolean> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, Boolean.class);
          assertEquals(Boolean.TRUE, map.get("x"));
          assertEquals(Boolean.FALSE, map.get("y"));
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testReturnAsList(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "select [{'string':'one'},{'string': 'two'}]::ARRAY(OBJECT(string varchar))",
        (resultSet) -> {
          List<StringClass> map =
              resultSet.unwrap(SnowflakeBaseResultSet.class).getList(1, StringClass.class);
          assertEquals("one", map.get(0).getString());
          assertEquals("two", map.get(1).getString());
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapStructsFromChunks(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "select {'string':'a'}::OBJECT(string VARCHAR) FROM TABLE(GENERATOR(ROWCOUNT=>30000))",
        (resultSet) -> {
          while (resultSet.next()) {
            StringClass object = resultSet.getObject(1, StringClass.class);
            assertEquals("a", object.getString());
          }
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapIntegerArray(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(10, 20, 30)::ARRAY(INTEGER)",
        (resultSet) -> {
          Long[] resultArray = (Long[]) resultSet.getArray(1).getArray();
          assertEquals(Long.valueOf(10), resultArray[0]);
          assertEquals(Long.valueOf(20), resultArray[1]);
          assertEquals(Long.valueOf(30), resultArray[2]);
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapIntegerArrayGetObject(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(10, 20, 30)::ARRAY(INTEGER)",
        (resultSet) -> {
          Object resultArray = resultSet.getObject(1);
          TestUtil.assertEqualsIgnoringWhitespace("[10,20,30]", (String) resultArray);
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapFixedToLongArray(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(10, 20, 30)::ARRAY(SMALLINT)",
        (resultSet) -> {
          Long[] resultArray = (Long[]) resultSet.getArray(1).getArray();
          assertEquals(Long.valueOf("10"), resultArray[0]);
          assertEquals(Long.valueOf("20"), resultArray[1]);
          assertEquals(Long.valueOf("30"), resultArray[2]);
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapDecimalArray(ResultSetFormatType format) throws SQLException {
    //    when: jdbc_treat_decimal_as_int=true scale=0
    try (Connection connection = init(format);
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
    try (Connection connection = init(format);
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
    try (Connection connection = init(format);
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

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapVarcharArray(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT 'text', ARRAY_CONSTRUCT('10', '20','30')::ARRAY(VARCHAR)",
        (resultSet) -> {
          String t = resultSet.getString(1);
          String[] resultArray = (String[]) resultSet.getArray(2).getArray();
          assertEquals("10", resultArray[0]);
          assertEquals("20", resultArray[1]);
          assertEquals("30", resultArray[2]);
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapDatesArray(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(to_date('2023-12-24', 'YYYY-MM-DD'), to_date('2023-12-25', 'YYYY-MM-DD'))::ARRAY(DATE)",
        (resultSet) -> {
          Date[] resultArray = (Date[]) resultSet.getArray(1).getArray();
          assertEquals(
              Date.valueOf(LocalDate.of(2023, 12, 24)).toString(), resultArray[0].toString());
          assertEquals(
              Date.valueOf(LocalDate.of(2023, 12, 25)).toString(), resultArray[1].toString());
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapTimeArray(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(to_time('15:39:20.123'), to_time('09:12:20.123'))::ARRAY(TIME)",
        (resultSet) -> {
          Time[] resultArray = (Time[]) resultSet.getArray(1).getArray();
          assertEquals(
              Time.valueOf(LocalTime.of(15, 39, 20)).toString(), resultArray[0].toString());
          assertEquals(Time.valueOf(LocalTime.of(9, 12, 20)).toString(), resultArray[1].toString());
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapTimestampArray(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(TO_TIMESTAMP_NTZ('2021-12-23 09:44:44'), TO_TIMESTAMP_NTZ('2021-12-24 09:55:55'))::ARRAY(TIMESTAMP)",
        (resultSet) -> {
          Timestamp[] resultArray = (Timestamp[]) resultSet.getArray(1).getArray();
          assertEquals(
              LocalDateTime.of(2021, 12, 23, 9, 44, 44)
                  .atZone(ZoneId.of("Europe/Warsaw"))
                  .toInstant(),
              resultArray[0].toInstant());
          assertEquals(
              LocalDateTime.of(2021, 12, 24, 9, 55, 55)
                  .atZone(ZoneId.of("Europe/Warsaw"))
                  .toInstant(),
              resultArray[1].toInstant());
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapBooleanArray(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(true,false)::ARRAY(BOOLEAN)",
        (resultSet) -> {
          Boolean[] resultArray = (Boolean[]) resultSet.getArray(1).getArray();
          assertEquals(true, resultArray[0]);
          assertEquals(false, resultArray[1]);
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapBinaryArray(ResultSetFormatType format) throws SQLException {
    withFirstRow(
        "SELECT ARRAY_CONSTRUCT(TO_BINARY('616263', 'HEX'),TO_BINARY('616263', 'HEX'))::ARRAY(BINARY)",
        (resultSet) -> {
          Byte[][] resultArray = (Byte[][]) resultSet.getArray(1).getArray();
          assertArrayEquals(new Byte[] {'a', 'b', 'c'}, resultArray[0]);
          assertArrayEquals(new Byte[] {'a', 'b', 'c'}, resultArray[1]);
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapArrayOfStructToMap(ResultSetFormatType format) throws SQLException {
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
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapArrayOfArrays(ResultSetFormatType format) throws SQLException {
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
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testMapNestedStructures(ResultSetFormatType format) throws SQLException {
    String structSelectStatement =
        "SELECT {'simpleClass': {'string': 'a', 'intValue': 2}, "
            + "'simpleClasses': ARRAY_CONSTRUCT({'string': 'a', 'intValue': 2}, {'string': 'b', 'intValue': 2}), "
            + "'arrayOfSimpleClasses': ARRAY_CONSTRUCT({'string': 'a', 'intValue': 2}, {'string': 'b', 'intValue': 2}), "
            + "'mapOfSimpleClasses':{'x':{'string': 'c', 'intValue': 2}, 'y':{'string': 'd', 'intValue': 2}},"
            + "'texts': ARRAY_CONSTRUCT('string', 'a'), "
            + "'arrayOfDates': ARRAY_CONSTRUCT(to_date('2023-12-24', 'YYYY-MM-DD'), to_date('2023-12-25', 'YYYY-MM-DD')), "
            + "'mapOfIntegers':{'x':3, 'y':4}}"
            + "::OBJECT(simpleClass OBJECT(string VARCHAR, intValue INTEGER), "
            + "simpleClasses ARRAY(OBJECT(string VARCHAR, intValue INTEGER)),"
            + "arrayOfSimpleClasses ARRAY(OBJECT(string VARCHAR, intValue INTEGER)),"
            + "mapOfSimpleClasses MAP(VARCHAR, OBJECT(string VARCHAR, intValue INTEGER)),"
            + "texts ARRAY(VARCHAR),"
            + "arrayOfDates ARRAY(DATE),"
            + "mapOfIntegers MAP(VARCHAR, INTEGER))";
    String expectedQueryResult =
        "{\"simpleClass\": {\"string\": \"a\",\"intValue\": 2},\"simpleClasses\": [{\"string\": \"a\",\"intValue\": 2},{\"string\": \"b\",\"intValue\": 2}],\"arrayOfSimpleClasses\": [{\"string\": \"a\",\"intValue\": 2},{\"string\": \"b\",\"intValue\": 2}],\"mapOfSimpleClasses\": {\"x\": {\"string\": \"c\",\"intValue\": 2},\"y\": {\"string\": \"d\",\"intValue\": 2}},\"texts\": [\"string\",\"a\"],\"arrayOfDates\": [\"2023-12-24\",\"2023-12-25\"],\"mapOfIntegers\": {\"x\": 3,\"y\": 4}}";
    withFirstRow(
        structSelectStatement,
        (resultSet) -> {
          NestedStructSqlData nestedStructSqlData =
              resultSet.getObject(1, NestedStructSqlData.class);
          ;
          assertEquals("a", nestedStructSqlData.getSimpleClass().getString());
          assertEquals(Integer.valueOf(2), nestedStructSqlData.getSimpleClass().getIntValue());

          assertEquals("a", nestedStructSqlData.getSimpleClassses().get(0).getString());
          assertEquals(
              Integer.valueOf(2), nestedStructSqlData.getSimpleClassses().get(0).getIntValue());
          assertEquals("b", nestedStructSqlData.getSimpleClassses().get(1).getString());
          assertEquals(
              Integer.valueOf(2), nestedStructSqlData.getSimpleClassses().get(1).getIntValue());

          assertEquals("a", nestedStructSqlData.getArrayOfSimpleClasses()[0].getString());
          assertEquals(
              Integer.valueOf(2), nestedStructSqlData.getArrayOfSimpleClasses()[0].getIntValue());
          assertEquals("b", nestedStructSqlData.getArrayOfSimpleClasses()[1].getString());
          assertEquals(
              Integer.valueOf(2), nestedStructSqlData.getArrayOfSimpleClasses()[1].getIntValue());

          assertEquals("c", nestedStructSqlData.getMapOfSimpleClasses().get("x").getString());
          assertEquals(
              Integer.valueOf(2),
              nestedStructSqlData.getMapOfSimpleClasses().get("x").getIntValue());
          assertEquals("d", nestedStructSqlData.getMapOfSimpleClasses().get("y").getString());
          assertEquals(
              Integer.valueOf(2),
              nestedStructSqlData.getMapOfSimpleClasses().get("y").getIntValue());

          assertEquals("string", nestedStructSqlData.getTexts().get(0));
          assertEquals("a", nestedStructSqlData.getTexts().get(1));

          // TODO uncomment after merge SNOW-928973: Date field is returning one day less when
          // getting
          //          assertEquals(
          //              Date.valueOf(LocalDate.of(2023, 12, 24)).toString(),
          //              nestedStructSqlData.getArrayOfDates()[0].toString());
          //          assertEquals(
          //              Date.valueOf(LocalDate.of(2023, 12, 25)).toString(),
          //              nestedStructSqlData.getArrayOfDates()[1].toString());

          assertEquals(Integer.valueOf(3), nestedStructSqlData.getMapOfIntegers().get("x"));
          assertEquals(Integer.valueOf(4), nestedStructSqlData.getMapOfIntegers().get("y"));
          TestUtil.assertEqualsIgnoringWhitespace(expectedQueryResult, resultSet.getString(1));
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testColumnTypeWhenStructureTypeIsDisabled(ResultSetFormatType format)
      throws Exception {
    withFirstRow(
        "SELECT {'string':'a'}",
        resultSet -> {
          assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(1));
        },
        format);
  }

  @ParameterizedTest
  @ArgumentsSource(ResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testColumnTypeAndFieldsWhenStructureTypeIsReturned(ResultSetFormatType format)
      throws Exception {
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
        },
        format);
  }

  private void withFirstRow(
      String sqlText,
      ThrowingConsumer<ResultSet, SQLException> consumer,
      ResultSetFormatType format)
      throws SQLException {
    try (Connection connection = init(format);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sqlText); ) {
      assertTrue(rs.next());
      consumer.accept(rs);
    }
  }
}
