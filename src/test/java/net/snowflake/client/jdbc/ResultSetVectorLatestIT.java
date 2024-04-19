package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeUtil.EXTRA_TYPES_VECTOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import net.snowflake.client.category.TestCategoryResultSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * ResultSet integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. It works for drivers with version bigger than 3.15.1. Revisit this tests
 * whenever bumping up the oldest supported driver to examine if the tests still is not applicable.
 * If it is applicable, move tests to ResultSetVectorIT so that both the latest and oldest supported
 * driver run the tests.
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
      try (ResultSet resultSet = stmt.executeQuery("select " + vectorToString(vector, "int"))) {
        assertTrue(resultSet.next());
        Integer[] result =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Integer.class);
        assertEquals(vector, result);
        assertVectorMetadata(resultSet, 1, Types.INTEGER, 1);
      }
    }
  }

  @Test
  public void testGetIntVectorAsLongArray() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      enforceQueryResultFormat(stmt);
      Long[] vector = {-1L, 5L};
      try (ResultSet resultSet = stmt.executeQuery("select " + vectorToString(vector, "int"))) {
        assertTrue(resultSet.next());
        Long[] result = resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Long.class);
        assertEquals(vector, result);
        assertVectorMetadata(resultSet, 1, Types.INTEGER, 1);
      }
    }
  }

  @Test
  public void testGetFloatVectorAsFloatArray() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      enforceQueryResultFormat(stmt);
      Float[] vector = {-1.2f, 5.1f, 15.87f};
      try (ResultSet resultSet = stmt.executeQuery("select " + vectorToString(vector, "float"))) {
        assertTrue(resultSet.next());
        Float[] result = resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Float.class);
        assertEquals(vector, result);
        assertVectorMetadata(resultSet, 1, Types.FLOAT, 1);
      }
    }
  }

  @Test
  public void testGetNullAsIntVector() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      enforceQueryResultFormat(stmt);
      try (ResultSet resultSet = stmt.executeQuery("select null::vector(int, 2)")) {
        assertTrue(resultSet.next());
        Integer[] result =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Integer.class);
        assertNull(result);
        assertVectorMetadata(resultSet, 1, Types.INTEGER, 1);
      }
    }
  }

  @Test
  public void testGetNullAsFloatVector() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      enforceQueryResultFormat(stmt);
      try (ResultSet resultSet = stmt.executeQuery("select null::vector(float, 2)")) {
        assertTrue(resultSet.next());
        Integer[] result =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Integer.class);
        assertNull(result);
        assertVectorMetadata(resultSet, 1, Types.FLOAT, 1);
      }
    }
  }

  @Test
  public void testGetIntVectorFromTable() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      enforceQueryResultFormat(stmt);
      stmt.execute("create or replace table test_vector_int(x vector(int, 2), y int)");
      stmt.execute("insert into test_vector_int select [3, 7]::vector(int, 2), 15");
      try (ResultSet resultSet = stmt.executeQuery("select x, y from test_vector_int")) {
        assertTrue(resultSet.next());
        Integer[] result =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Integer.class);
        assertEquals(new Integer[] {3, 7}, result);
        assertVectorMetadata(resultSet, 1, Types.INTEGER, 2);
      }
    }
  }

  @Test
  public void testGetFloatVectorFromTable() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      enforceQueryResultFormat(stmt);
      stmt.execute("create or replace table test_vector_float(x vector(float, 2), y float)");
      stmt.execute("insert into test_vector_float select [-3, 7.1]::vector(float, 2), 20.3");
      try (ResultSet resultSet = stmt.executeQuery("select x, y from test_vector_float")) {
        assertTrue(resultSet.next());
        Float[] result = resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Float.class);
        assertEquals(new Float[] {-3f, 7.1f}, result);
        assertVectorMetadata(resultSet, 1, Types.FLOAT, 2);
      }
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

  private void assertVectorMetadata(
      ResultSet resultSet, int vectorColumnIndex, int expectedVectorFieldType, int allColumns)
      throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    assertEquals(allColumns, metadata.getColumnCount());
    assertEquals(EXTRA_TYPES_VECTOR, metadata.getColumnType(vectorColumnIndex));
    assertEquals("VECTOR", metadata.getColumnTypeName(vectorColumnIndex));
    SnowflakeResultSetMetaDataV1 sfMetadata = (SnowflakeResultSetMetaDataV1) metadata;
    assertEquals(EXTRA_TYPES_VECTOR, sfMetadata.getInternalColumnType(vectorColumnIndex));
    List<FieldMetadata> columnFields = sfMetadata.getColumnFields(vectorColumnIndex);
    assertEquals(1, columnFields.size());
    assertEquals(expectedVectorFieldType, columnFields.get(0).getType());
  }
}
