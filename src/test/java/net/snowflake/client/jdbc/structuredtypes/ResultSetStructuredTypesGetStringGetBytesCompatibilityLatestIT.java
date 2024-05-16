/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc.structuredtypes;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryResultSet;
import net.snowflake.client.jdbc.ResultSetFormatType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Added in > 3.16.0 */
@RunWith(Parameterized.class)
@Category(TestCategoryResultSet.class)
public class ResultSetStructuredTypesGetStringGetBytesCompatibilityLatestIT
    extends ResultSetStructuredTypesBaseIT {

  private static Map<ResultSetFormatType, Connection> connections;

  @BeforeClass
  public static void prepareConnections() throws SQLException {
    connections = new HashMap<>();
    for (ResultSetFormatType format : ResultSetFormatType.values()) {
      connections.put(format, initConnection(format));
    }
  }

  @AfterClass
  public static void closeConnections() throws SQLException {
    for (Connection connection : connections.values()) {
      connection.close();
    }
  }

  @Parameterized.Parameters(name = "format={0},sql={1}")
  public static Collection<Object[]> data() {
    Map<String, String> samples = new LinkedHashMap<>();
    samples.put("select {'a':3}::map(text, int);", "{\"a\":3}");
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
        "select [{'string':'a'}, {'string':'b'}]::array(object(string varchar))",
        "[{\"string\":\"a\"}, {\"string\":\"b\"}]");
    samples.put("select {'string':'a'}::object(string varchar)", "{\"string\":\"a\"}");
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
    samples.put("select [1,2,3]::VECTOR(INT, 3)", "[1,2,3]");

    Collection<Object[]> parameters = new ArrayList<>();
    for (ResultSetFormatType resultSetFormatType : ResultSetFormatType.values()) {
      samples.forEach(
          (sql, expected) -> parameters.add(new Object[] {resultSetFormatType, sql, expected}));
    }
    return parameters;
  }

  private final String selectSql;
  private final String expectedStructureTypeRepresentation;

  public ResultSetStructuredTypesGetStringGetBytesCompatibilityLatestIT(
      ResultSetFormatType queryResultFormat,
      String selectSql,
      String expectedStructureTypeRepresentation) {
    super(queryResultFormat);
    this.selectSql = selectSql;
    this.expectedStructureTypeRepresentation = expectedStructureTypeRepresentation;
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testReturnAsGetStringAndGetBytes() throws SQLException {
    withFirstRow(
        connections.get(queryResultFormat),
        selectSql,
        (resultSet) ->
            assertGetStringAndGetBytesAreCompatible(
                resultSet, expectedStructureTypeRepresentation));
  }
}
