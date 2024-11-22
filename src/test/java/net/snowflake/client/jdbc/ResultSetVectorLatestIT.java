package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeUtil.EXTRA_TYPES_VECTOR;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * ResultSet integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. It works for drivers with version bigger than 3.15.1. Revisit this tests
 * whenever bumping up the oldest supported driver to examine if the tests still is not applicable.
 * If it is applicable, move tests to ResultSetVectorIT so that both the latest and oldest supported
 * driver run the tests.
 */
@Tag(TestTags.RESULT_SET)
public class ResultSetVectorLatestIT extends ResultSet0IT {

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetIntVectorAsIntArray(String queryResultFormat) throws SQLException {
    try (Statement stmt = createStatement(queryResultFormat)) {
      enforceQueryResultFormat(stmt, queryResultFormat);
      Integer[] vector = {-1, 5};
      try (ResultSet resultSet = stmt.executeQuery("select " + vectorToString(vector, "int"))) {
        assertTrue(resultSet.next());
        Integer[] result =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Integer.class);
        assertArrayEquals(vector, result);
        assertVectorMetadata(resultSet, 1, Types.INTEGER, 1);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetIntVectorAsLongArray(String queryResultFormat) throws SQLException {
    try (Statement stmt = createStatement(queryResultFormat)) {
      enforceQueryResultFormat(stmt, queryResultFormat);
      Long[] vector = {-1L, 5L};
      try (ResultSet resultSet = stmt.executeQuery("select " + vectorToString(vector, "int"))) {
        assertTrue(resultSet.next());
        Long[] result = resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Long.class);
        assertArrayEquals(vector, result);
        assertVectorMetadata(resultSet, 1, Types.INTEGER, 1);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetFloatVectorAsFloatArray(String queryResultFormat) throws SQLException {
    try (Statement stmt = createStatement(queryResultFormat)) {
      enforceQueryResultFormat(stmt, queryResultFormat);
      Float[] vector = {-1.2f, 5.1f, 15.87f};
      try (ResultSet resultSet = stmt.executeQuery("select " + vectorToString(vector, "float"))) {
        assertTrue(resultSet.next());
        Float[] result = resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Float.class);
        assertArrayEquals(vector, result);
        assertVectorMetadata(resultSet, 1, Types.FLOAT, 1);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetNullAsIntVector(String queryResultFormat) throws SQLException {
    try (Statement stmt = createStatement(queryResultFormat)) {
      enforceQueryResultFormat(stmt, queryResultFormat);
      try (ResultSet resultSet = stmt.executeQuery("select null::vector(int, 2)")) {
        assertTrue(resultSet.next());
        Integer[] result =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Integer.class);
        assertNull(result);
        assertVectorMetadata(resultSet, 1, Types.INTEGER, 1);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetNullAsFloatVector(String queryResultFormat) throws SQLException {
    try (Statement stmt = createStatement(queryResultFormat)) {
      enforceQueryResultFormat(stmt, queryResultFormat);
      try (ResultSet resultSet = stmt.executeQuery("select null::vector(float, 2)")) {
        assertTrue(resultSet.next());
        Integer[] result =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Integer.class);
        assertNull(result);
        assertVectorMetadata(resultSet, 1, Types.FLOAT, 1);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetIntVectorFromTable(String queryResultFormat) throws SQLException {
    try (Statement stmt = createStatement(queryResultFormat)) {
      enforceQueryResultFormat(stmt, queryResultFormat);
      stmt.execute("create or replace table test_vector_int(x vector(int, 2), y int)");
      stmt.execute("insert into test_vector_int select [3, 7]::vector(int, 2), 15");
      try (ResultSet resultSet = stmt.executeQuery("select x, y from test_vector_int")) {
        assertTrue(resultSet.next());
        Integer[] result =
            resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Integer.class);
        assertArrayEquals(new Integer[] {3, 7}, result);
        assertVectorMetadata(resultSet, 1, Types.INTEGER, 2);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetFloatVectorFromTable(String queryResultFormat) throws SQLException {
    try (Statement stmt = createStatement(queryResultFormat)) {
      enforceQueryResultFormat(stmt, queryResultFormat);
      stmt.execute("create or replace table test_vector_float(x vector(float, 2), y float)");
      stmt.execute("insert into test_vector_float select [-3, 7.1]::vector(float, 2), 20.3");
      try (ResultSet resultSet = stmt.executeQuery("select x, y from test_vector_float")) {
        assertTrue(resultSet.next());
        Float[] result = resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1, Float.class);
        assertArrayEquals(new Float[] {-3f, 7.1f}, result);
        assertVectorMetadata(resultSet, 1, Types.FLOAT, 2);
      }
    }
  }

  /** Added in > 3.16.1 */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetVectorViaGetStringIsEqualToTheGetObject(String queryResultFormat)
      throws SQLException {
    try (Statement stmt = createStatement(queryResultFormat)) {
      enforceQueryResultFormat(stmt, queryResultFormat);
      Integer[] intVector = {-1, 5};
      Float[] floatVector = {-1.2f, 5.1f, 15.87f};
      try (ResultSet resultSet =
          stmt.executeQuery(
              "select "
                  + vectorToString(intVector, "int")
                  + ", "
                  + vectorToString(floatVector, "float")
                  + ", "
                  + nullVectorToString("int")
                  + ", "
                  + nullVectorToString("float"))) {

        assertTrue(resultSet.next());
        assertGetObjectAndGetStringBeTheSame(resultSet, "[-1,5]", 1);
        String floatArrayRepresentation =
            "json".equalsIgnoreCase(queryResultFormat)
                // in json we have slightly different format that we accept in the result
                ? "[-1.200000,5.100000,15.870000]"
                : "[-1.2,5.1,15.87]";
        assertGetObjectAndGetStringBeTheSame(resultSet, floatArrayRepresentation, 2);
        assertGetObjectAndGetStringAreNull(resultSet, 3);
        assertGetObjectAndGetStringAreNull(resultSet, 4);
      }
    }
  }

  private static void assertGetObjectAndGetStringBeTheSame(
      ResultSet resultSet, String intArrayRepresentation, int columnIndex) throws SQLException {
    assertEquals(intArrayRepresentation, resultSet.getString(columnIndex));
    assertEquals(intArrayRepresentation, resultSet.getObject(columnIndex));
  }

  private static void assertGetObjectAndGetStringAreNull(ResultSet resultSet, int columnIndex)
      throws SQLException {
    assertNull(resultSet.getString(columnIndex));
    assertNull(resultSet.getObject(columnIndex));
  }

  private <T extends Number> String vectorToString(T[] vector, String vectorType) {
    return Arrays.toString(vector) + "::vector(" + vectorType + ", " + vector.length + ")";
  }

  private <T extends Number> String nullVectorToString(String vectorType) {
    return "null::vector(" + vectorType + ", 2)";
  }

  private void enforceQueryResultFormat(Statement stmt, String queryResultFormat)
      throws SQLException {
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
    assertTrue(sfMetadata.isStructuredTypeColumn(vectorColumnIndex));
    assertEquals(EXTRA_TYPES_VECTOR, sfMetadata.getInternalColumnType(vectorColumnIndex));
    List<FieldMetadata> columnFields = sfMetadata.getColumnFields(vectorColumnIndex);
    assertEquals(1, columnFields.size());
    assertEquals(expectedVectorFieldType, columnFields.get(0).getType());
  }
}
