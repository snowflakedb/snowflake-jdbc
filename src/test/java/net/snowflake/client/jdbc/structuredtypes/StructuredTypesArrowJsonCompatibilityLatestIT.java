package net.snowflake.client.jdbc.structuredtypes;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.ResultSetFormatType;
import net.snowflake.client.providers.ProvidersUtil;
import net.snowflake.client.providers.ResultFormatProvider;
import net.snowflake.client.providers.SnowflakeArgumentsProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(TestTags.RESULT_SET)
public class StructuredTypesArrowJsonCompatibilityLatestIT extends StructuredTypesGetStringBaseIT {
  private static Map<ResultSetFormatType, Connection> connections = new HashMap<>();

  @BeforeAll
  public static void setUpConnections() throws SQLException {
    // We initialize connection here since we need to set server properties that cannot be set in GH
    // actions and before class is running even when all the tests have conditional ignore of tests
    for (ResultSetFormatType queryResultFormat : ResultSetFormatType.values()) {
      connections.put(queryResultFormat, initConnection(queryResultFormat));
    }
  }

  @AfterAll
  public static void closeConnections() throws SQLException {
    for (Connection connection : connections.values()) {
      connection.close();
    }
  }

  @ParameterizedTest
  @DontRunOnGithubActions
  @ArgumentsSource(DataProvider.class)
  public void testArrowJsonCompatibility(
      ResultSetFormatType queryResultFormat,
      String selectSql,
      String expectedStructureTypeRepresentation)
      throws SQLException {
    withFirstRow(
        connections.get(queryResultFormat),
        selectSql,
        (resultSet) -> assertResultSetIsCompatible(resultSet, expectedStructureTypeRepresentation));
  }

  public static class SampleProvider extends SnowflakeArgumentsProvider {
    @Override
    protected List<Arguments> rawArguments(ExtensionContext context) {
      List<Arguments> samples = new LinkedList<>();
      samples.add(Arguments.of("select {'a':3}::map(text, int);", "{\"a\":3}"));
      samples.add(
          Arguments.of(
              "select {'a':'zażółć gęślą jaźń'}::map(text, text);",
              "{\"a\":\"zażółć gęślą jaźń\"}"));
      samples.add(Arguments.of("select {'a':'bla'}::map(text, text);", "{\"a\":\"bla\"}"));
      samples.add(Arguments.of("select {'1':'bla'}::map(int, text);", "{\"1\":\"bla\"}"));
      samples.add(Arguments.of("select {'1':[1,2,3]}::map(int, ARRAY(int));", "{\"1\":[1,2,3]}"));
      samples.add(
          Arguments.of(
              "select {'1':{'string':'a'}}::map(int, OBJECT(string VARCHAR));",
              "{\"1\":{\"string\":\"a\"}}"));
      samples.add(
          Arguments.of(
              "select {'1':{'string':'a'}}::map(int, map(string, string));",
              "{\"1\":{\"string\":\"a\"}}"));
      samples.add(
          Arguments.of(
              "select {'1':[{'string':'a'},{'bla':'ble'}]}::map(int, array(map(string, string)));",
              "{\"1\":[{\"string\":\"a\"},{\"bla\":\"ble\"}]}"));
      samples.add(Arguments.of("select [1,2,3]::array(int)", "[1,2,3]"));
      samples.add(
          Arguments.of(
              "select [{'a':'a'}, {'b':'b'}]::array(map(string, string))",
              "[{\"a\":\"a\"}, {\"b\":\"b\"}]"));
      samples.add(
          Arguments.of(
              "select [{'a':true}, {'b':false}]::array(map(string, boolean))",
              "[{\"a\":true}, {\"b\":false}]"));
      samples.add(
          Arguments.of(
              "select [{'string':'a'}, {'string':'b'}]::array(object(string varchar))",
              "[{\"string\":\"a\"}, {\"string\":\"b\"}]"));
      samples.add(
          Arguments.of("select {'string':'a'}::object(string varchar)", "{\"string\":\"a\"}"));
      samples.add(
          Arguments.of(
              "select {'x':'a','b':'a','c':'a','d':'a','e':'a'}::object(x varchar,b varchar,c varchar,d varchar,e varchar)",
              "{\"x\":\"a\",\"b\":\"a\",\"c\":\"a\",\"d\":\"a\",\"e\":\"a\"}"));
      samples.add(
          Arguments.of(
              "select {'string':[1,2,3]}::object(string array(int))", "{\"string\":[1,2,3]}"));
      samples.add(
          Arguments.of(
              "select {'string':{'a':15}}::object(string object(a int))",
              "{\"string\":{\"a\":15}}"));
      samples.add(
          Arguments.of(
              "select {'string':{'a':15}}::object(string map(string,int))",
              "{\"string\":{\"a\":15}}"));
      samples.add(
          Arguments.of(
              "select {'string':{'a':{'b':15}}}::object(string object(a map(string, int)))",
              "{\"string\":{\"a\":{\"b\":15}}}"));

      samples.add(
          Arguments.of(
              "select {'string':{'a':{'b':[{'c': 15}]}}}::object(string map(string, object(b array(object(c int)))))",
              "{\"string\":{\"a\":{\"b\":[{\"c\":15}]}}}"));
      // DY, DD MON YYYY HH24:MI:SS TZHTZM
      samples.add(
          Arguments.of(
              "select {'ltz': '2024-05-20 11:22:33'::TIMESTAMP_LTZ}::object(ltz TIMESTAMP_LTZ)",
              "{\"ltz\":\"Mon, 20 May 2024 11:22:33 +0200\"}"));
      samples.add(
          Arguments.of(
              "select {'ntz': '2024-05-20 11:22:33'::TIMESTAMP_NTZ}::object(ntz TIMESTAMP_NTZ)",
              "{\"ntz\":\"Mon, 20 May 2024 11:22:33 Z\"}"));
      samples.add(
          Arguments.of(
              "select {'tz': '2024-05-20 11:22:33+0800'::TIMESTAMP_TZ}::object(tz TIMESTAMP_TZ)",
              "{\"tz\":\"Mon, 20 May 2024 11:22:33 +0800\"}"));
      samples.add(
          Arguments.of(
              "select {'date': '2024-05-20'::DATE}::object(date DATE)",
              "{\"date\":\"2024-05-20\"}"));
      samples.add(
          Arguments.of(
              "select {'time': '22:14:55'::TIME}::object(time TIME)", "{\"time\":\"22:14:55\"}"));
      samples.add(Arguments.of("select {'bool': TRUE}::object(bool BOOLEAN)", "{\"bool\":true}"));
      samples.add(Arguments.of("select {'bool': 'y'}::object(bool BOOLEAN)", "{\"bool\":true}"));
      samples.add(
          Arguments.of(
              "select {'binary': TO_BINARY('616263', 'HEX')}::object(binary BINARY)",
              "{\"binary\":\"616263\"}"));
      samples.add(Arguments.of("select [1,2,3]::VECTOR(INT, 3)", "[1,2,3]"));
      samples.add(Arguments.of("select ['a','b','c']::ARRAY(varchar)", "[\"a\",\"b\",\"c\"]"));
      samples.add(Arguments.of("select ['a','b','c']::ARRAY(variant)", "[\"a\",\"b\",\"c\"]"));

      return samples;
    }
  }

  private static class DataProvider extends SnowflakeArgumentsProvider {

    @Override
    protected List<Arguments> rawArguments(ExtensionContext context) {
      return ProvidersUtil.cartesianProduct(
          context, new ResultFormatProvider(), new SampleProvider());
    }
  }
}
