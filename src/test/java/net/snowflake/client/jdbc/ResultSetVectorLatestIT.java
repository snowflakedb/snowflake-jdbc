package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import net.snowflake.client.category.TestCategoryResultSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * ResultSet integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the oldest supported driver to examine
 * if the tests still is not applicable. If it is applicable, move tests to ResultSetIT so that both
 * the latest and oldest supported driver run the tests.
 */
@Category(TestCategoryResultSet.class)
@RunWith(Parameterized.class)
public class ResultSetVectorLatestIT extends ResultSet0IT {

  private final String queryResultFormat;

  public ResultSetVectorLatestIT(String queryResultFormat) {
    super(queryResultFormat);
    this.queryResultFormat = queryResultFormat;
  }

  @Parameterized.Parameters(name = "format={0}")
  public static List<String> queryResultFormats() {
    return Arrays.asList("json", "arrow");
  }

  @Test
  public void testGetIntVectorAsIntArray() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      enforceQueryResultFormat(stmt);
      Integer[] vector = {-1, 5};
      ResultSet resultSet = stmt.executeQuery("select " + vectorToString(vector, "int"));
      assertTrue(resultSet.next());
      Integer[] result = resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Integer.class);
      assertEquals(vector, result);
    }
  }

  @Test
  public void testGetIntVectorAsLongArray() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      enforceQueryResultFormat(stmt);
      Long[] vector = {-1L, 5L};
      ResultSet resultSet = stmt.executeQuery("select " + vectorToString(vector, "int"));
      assertTrue(resultSet.next());
      Long[] result = resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Long.class);
      assertEquals(vector, result);
    }
  }

  @Test
  public void testGetFloatVectorAsFloatArray() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      enforceQueryResultFormat(stmt);
      Float[] vector = {-1.2f, 5.1f, 15.87f};
      ResultSet resultSet = stmt.executeQuery("select " + vectorToString(vector, "float"));
      assertTrue(resultSet.next());
      Float[] result = resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Float.class);
      assertEquals(vector, result);
    }
  }

  @Test
  public void testGetIntVectorFromTable() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      enforceQueryResultFormat(stmt);
      stmt.execute("create or replace table test_vector_int(x vector(int, 2), y int)");
      stmt.execute("insert into test_vector_int select [3, 7]::vector(int, 2), 15");
      ResultSet resultSet = stmt.executeQuery("select * from test_vector_int");
      assertTrue(resultSet.next());
      Integer[] result = resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Integer.class);
      assertEquals(new Integer[] {3, 7}, result);
    }
  }

  @Test
  public void testGetFloatVectorFromTable() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      enforceQueryResultFormat(stmt);
      stmt.execute("create or replace table test_vector_float(x vector(float, 2), y float)");
      stmt.execute("insert into test_vector_float select [-3, 7.1]::vector(float, 2), 20.3");
      ResultSet resultSet = stmt.executeQuery("select * from test_vector_float");
      assertTrue(resultSet.next());
      Float[] result = resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Float.class);
      assertEquals(new Float[] {-3f, 7.1f}, result);
    }
  }

  private <T extends Number> String vectorToString(T[] vector, String vectorType) {
    return Arrays.toString(vector) + "::vector(" + vectorType + ", " + vector.length + ")";
  }

  private void enforceQueryResultFormat(Statement stmt) throws SQLException {
    String sql =
        String.format(
            "alter session set jdbc_query_result_format = '%s'", queryResultFormat.toUpperCase());
    stmt.execute(sql);
  }
}
