package net.snowflake.client.jdbc.structuredtypes;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import net.snowflake.client.category.TestCategoryResultSet;
import net.snowflake.client.jdbc.ResultSetFormatType;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category(TestCategoryResultSet.class)
public class StructuredTypesArrowJsonCompatibilityLatestIT extends StructuredTypesGetStringBaseIT {

  private final String expectedStructureTypeRepresentation;
  private final String selectSql;
  private static Map<ResultSetFormatType, Connection> connections = new HashMap<>();

  public StructuredTypesArrowJsonCompatibilityLatestIT(
      ResultSetFormatType queryResultFormat,
      String selectSql,
      String expectedStructureTypeRepresentation) {
    super(queryResultFormat);
    this.selectSql = selectSql;
    this.expectedStructureTypeRepresentation = expectedStructureTypeRepresentation;
  }

  @Before
  public void setUpConnection() throws SQLException {
    // We initialize connection here since we need to set server properties that cannot be set in GH
    // actions and before class is running even when all the tests have conditional ignore of tests
    Connection connection = connections.get(queryResultFormat);
    if (connection == null) {
      connections.put(queryResultFormat, initConnection(queryResultFormat));
    }
  }

  @AfterClass
  public static void closeConnections() throws SQLException {
    for (Connection connection : connections.values()) {
      connection.close();
    }
  }

  @Test
  public void testRunAsGetString() throws SQLException {
    withFirstRow(
        connections.get(queryResultFormat),
        selectSql,
        (resultSet) -> assertGetStringIsCompatible(resultSet, expectedStructureTypeRepresentation));
  }

  @Test
  public void testRunAsGetObject() throws SQLException {
    withFirstRow(
        connections.get(queryResultFormat),
        selectSql,
        (resultSet) -> assertGetObjectIsCompatible(resultSet, expectedStructureTypeRepresentation));
  }

  @Test
  public void testRunAsGetBytes() throws SQLException {
    withFirstRow(
        connections.get(queryResultFormat),
        selectSql,
        (resultSet) -> assertGetBytesIsCompatible(resultSet, expectedStructureTypeRepresentation));
  }

  @Parameterized.Parameters(name = "format={0},sql={1}")
  public static Collection<Object[]> data() {
    Map<String, String> samples = new LinkedHashMap<>();
    samples.put("select {'a':3}::map(text, int);", "{\"a\":3}");
    samples.put(
        "select {'a':'zażółć gęślą jaźń'}::map(text, text);", "{\"a\":\"zażółć gęślą jaźń\"}");
    samples.put("select {'a':'bla'}::map(text, text);", "{\"a\":\"bla\"}");
    samples.put("select {'1':'bla'}::map(int, text);", "{\"1\":\"bla\"}");
    samples.put("select {'1':[1,2,3]}::map(int, ARRAY(int));", "{\"1\":[1,2,3]}");
    samples.put(
        "select {'1':{'string':'a'}}::map(int, OBJECT(string VARCHAR));",
        "{\"1\":{\"string\":\"a\"}}");
    samples.put(
        "select {'1':{'string':'a'}}::map(int, map(string, string));",
        "{\"1\":{\"string\":\"a\"}}");
    samples.put(
        "select {'1':[{'string':'a'},{'bla':'ble'}]}::map(int, array(map(string, string)));",
        "{\"1\":[{\"string\":\"a\"},{\"bla\":\"ble\"}]}");
    samples.put("select [1,2,3]::array(int)", "[1,2,3]");
    samples.put(
        "select [{'a':'a'}, {'b':'b'}]::array(map(string, string))",
        "[{\"a\":\"a\"}, {\"b\":\"b\"}]");
    samples.put(
        "select [{'a':true}, {'b':false}]::array(map(string, boolean))",
        "[{\"a\":true}, {\"b\":false}]");
    samples.put(
        "select [{'string':'a'}, {'string':'b'}]::array(object(string varchar))",
        "[{\"string\":\"a\"}, {\"string\":\"b\"}]");
    samples.put("select {'string':'a'}::object(string varchar)", "{\"string\":\"a\"}");
    samples.put(
        "select {'x':'a','b':'a','c':'a','d':'a','e':'a'}::object(x varchar,b varchar,c varchar,d varchar,e varchar)",
        "{\"x\":\"a\",\"b\":\"a\",\"c\":\"a\",\"d\":\"a\",\"e\":\"a\"}");
    samples.put("select {'string':[1,2,3]}::object(string array(int))", "{\"string\":[1,2,3]}");
    samples.put(
        "select {'string':{'a':15}}::object(string object(a int))", "{\"string\":{\"a\":15}}");
    samples.put(
        "select {'string':{'a':15}}::object(string map(string,int))", "{\"string\":{\"a\":15}}");
    samples.put(
        "select {'string':{'a':{'b':15}}}::object(string object(a map(string, int)))",
        "{\"string\":{\"a\":{\"b\":15}}}");

    samples.put(
        "select {'string':{'a':{'b':[{'c': 15}]}}}::object(string map(string, object(b array(object(c int)))))",
        "{\"string\":{\"a\":{\"b\":[{\"c\":15}]}}}");
    // DY, DD MON YYYY HH24:MI:SS TZHTZM
    samples.put(
        "select {'ltz': '2024-05-20 11:22:33'::TIMESTAMP_LTZ}::object(ltz TIMESTAMP_LTZ)",
        "{\"ltz\":\"Mon, 20 May 2024 11:22:33 +0200\"}");
    samples.put(
        "select {'ntz': '2024-05-20 11:22:33'::TIMESTAMP_NTZ}::object(ntz TIMESTAMP_NTZ)",
        "{\"ntz\":\"Mon, 20 May 2024 11:22:33 Z\"}");
    samples.put(
        "select {'tz': '2024-05-20 11:22:33+0800'::TIMESTAMP_TZ}::object(tz TIMESTAMP_TZ)",
        "{\"tz\":\"Mon, 20 May 2024 11:22:33 +0800\"}");
    samples.put(
        "select {'date': '2024-05-20'::DATE}::object(date DATE)", "{\"date\":\"2024-05-20\"}");
    samples.put("select {'time': '22:14:55'::TIME}::object(time TIME)", "{\"time\":\"22:14:55\"}");
    samples.put("select {'bool': TRUE}::object(bool BOOLEAN)", "{\"bool\":true}");
    samples.put("select {'bool': 'y'}::object(bool BOOLEAN)", "{\"bool\":true}");
    samples.put(
        "select {'binary': TO_BINARY('616263', 'HEX')}::object(binary BINARY)",
        "{\"binary\":\"616263\"}");
    samples.put("select [1,2,3]::VECTOR(INT, 3)", "[1,2,3]");
    samples.put("select ['a','b','c']::ARRAY(varchar)", "[\"a\",\"b\",\"c\"]");
    samples.put("select ['a','b','c']::ARRAY(variant)", "[\"a\",\"b\",\"c\"]");

    Collection<Object[]> parameters = new ArrayList<>();
    for (ResultSetFormatType resultSetFormatType : ResultSetFormatType.values()) {
      samples.forEach(
          (sql, expected) -> parameters.add(new Object[] {resultSetFormatType, sql, expected}));
    }

    return parameters;
  }
}
