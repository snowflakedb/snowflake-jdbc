package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.DatabaseMetaDataInternalIT.endMetaData;
import static net.snowflake.client.jdbc.DatabaseMetaDataInternalIT.initMetaData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Database Metadata tests for the latest JDBC driver. This doesn't work for the oldest supported
 * driver. Revisit this tests whenever bumping up the oldest supported driver to examine if the
 * tests still is not applicable. If it is applicable, move tests to DatabaseMetaDataIT so that both
 * the latest and oldest supported driver run the tests.
 */
@Category(TestCategoryOthers.class)
public class DatabaseMetaDataInternalLatestIT extends BaseJDBCTest {

  @Before
  public void setUp() throws Exception {
    try (Connection con = getConnection()) {
      initMetaData(con);
    }
  }

  @After
  public void tearDown() throws Exception {
    try (Connection con = getConnection()) {
      endMetaData(con);
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetMetaDataUseConnectionCtx() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();

    // setup: reset session db and schema, enable the parameter
    statement.execute("use database JDBC_DB1");
    statement.execute("use schema JDBC_SCHEMA11");
    statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");

    DatabaseMetaData databaseMetaData = connection.getMetaData();

    // Searches for tables only in database JDBC_DB1 and schema JDBC_SCHEMA11
    ResultSet resultSet = databaseMetaData.getTables(null, null, null, null);
    // Assert the tables are retrieved at schema level
    resultSet.next();
    assertEquals("JDBC_DB1", resultSet.getString(1));
    assertEquals("JDBC_SCHEMA11", resultSet.getString(2));
    // Searches for tables only in database JDBC_DB1 and schema JDBC_SCHEMA11
    resultSet = databaseMetaData.getColumns(null, null, null, null);
    // Assert the columns are retrieved at schema level
    resultSet.next();
    assertEquals("JDBC_DB1", resultSet.getString(1));
    assertEquals("JDBC_SCHEMA11", resultSet.getString(2));
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetFunctionColumns() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute(
        "create or replace function JDBC_DB1.JDBC_SCHEMA11.FUNC111 "
            + "(a number, b number) RETURNS NUMBER COMMENT='multiply numbers' as 'a*b'");
    statement.execute(
        "create or replace table JDBC_DB1.JDBC_SCHEMA11.BIN_TABLE(bin1 binary, bin2 binary(100), "
            + "sharedCol decimal)");
    // statement.execute("create or replace table JDBC_TBL111(colA string, colB decimal, colC
    // timestamp)");
    statement.execute(
        "create or replace function JDBC_DB1.JDBC_SCHEMA11.FUNC112 "
            + "() RETURNS TABLE(colA string, colB decimal, bin2 binary, sharedCol decimal) COMMENT= 'returns "
            + "table of 4 columns'"
            + " as 'select JDBC_DB1.JDBC_SCHEMA11.JDBC_TBL111.colA, JDBC_DB1.JDBC_SCHEMA11.JDBC_TBL111.colB, "
            + "JDBC_DB1.JDBC_SCHEMA11.BIN_TABLE.bin2, JDBC_DB1.JDBC_SCHEMA11.BIN_TABLE.sharedCol from JDBC_DB1"
            + ".JDBC_SCHEMA11.JDBC_TBL111 inner join JDBC_DB1.JDBC_SCHEMA11.BIN_TABLE on JDBC_DB1.JDBC_SCHEMA11"
            + ".JDBC_TBL111.colB = JDBC_DB1.JDBC_SCHEMA11.BIN_TABLE.sharedCol'");
    DatabaseMetaData databaseMetaData = connection.getMetaData();
    // test each column return the right value
    ResultSet resultSet =
        databaseMetaData.getFunctionColumns("JDBC_DB1", "JDBC_SCHEMA11", "FUNC111", "%");
    resultSet.next();
    assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
    assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
    assertEquals("FUNC111", resultSet.getString("FUNCTION_NAME"));
    assertEquals("", resultSet.getString("COLUMN_NAME"));
    assertEquals(DatabaseMetaData.functionReturn, resultSet.getInt("COLUMN_TYPE"));
    assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
    assertEquals("NUMBER(38,0)", resultSet.getString("TYPE_NAME"));
    assertEquals(38, resultSet.getInt("PRECISION"));
    assertEquals(0, resultSet.getInt("LENGTH"));
    assertEquals(0, resultSet.getShort("SCALE"));
    assertEquals(10, resultSet.getInt("RADIX"));
    assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
    assertEquals("multiply numbers", resultSet.getString("REMARKS"));
    assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
    assertEquals(0, resultSet.getInt("ORDINAL_POSITION"));
    assertEquals("", resultSet.getString("IS_NULLABLE"));
    assertEquals("FUNC111(NUMBER, NUMBER) RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
    resultSet.next();
    assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
    assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
    assertEquals("FUNC111", resultSet.getString("FUNCTION_NAME"));
    assertEquals("A", resultSet.getString("COLUMN_NAME"));
    assertEquals(1, resultSet.getInt("COLUMN_TYPE"));
    assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
    assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
    assertEquals(38, resultSet.getInt("PRECISION"));
    assertEquals(0, resultSet.getInt("LENGTH"));
    assertEquals(0, resultSet.getShort("SCALE"));
    assertEquals(10, resultSet.getInt("RADIX"));
    assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
    assertEquals("multiply numbers", resultSet.getString("REMARKS"));
    assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
    assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
    assertEquals("", resultSet.getString("IS_NULLABLE"));
    assertEquals("FUNC111(NUMBER, NUMBER) RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
    resultSet.next();
    assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
    assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
    assertEquals("FUNC111", resultSet.getString("FUNCTION_NAME"));
    assertEquals("B", resultSet.getString("COLUMN_NAME"));
    assertEquals(1, resultSet.getInt("COLUMN_TYPE"));
    assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
    assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
    assertEquals(38, resultSet.getInt("PRECISION"));
    assertEquals(0, resultSet.getInt("LENGTH"));
    assertEquals(0, resultSet.getShort("SCALE"));
    assertEquals(10, resultSet.getInt("RADIX"));
    assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
    assertEquals("multiply numbers", resultSet.getString("REMARKS"));
    assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
    assertEquals(2, resultSet.getInt("ORDINAL_POSITION"));
    assertEquals("", resultSet.getString("IS_NULLABLE"));
    assertEquals("FUNC111(NUMBER, NUMBER) RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
    assertFalse(resultSet.next());
    resultSet = databaseMetaData.getFunctionColumns("JDBC_DB1", "JDBC_SCHEMA11", "FUNC112", "%");
    resultSet.next();
    assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
    assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
    assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
    assertEquals("COLA", resultSet.getString("COLUMN_NAME"));
    assertEquals(DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
    assertEquals(Types.VARCHAR, resultSet.getInt("DATA_TYPE"));
    assertEquals("VARCHAR", resultSet.getString("TYPE_NAME"));
    assertEquals(0, resultSet.getInt("PRECISION"));
    assertEquals(0, resultSet.getInt("LENGTH"));
    assertEquals(0, resultSet.getInt("SCALE"));
    assertEquals(10, resultSet.getInt("RADIX"));
    assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
    assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
    assertEquals(16777216, resultSet.getInt("CHAR_OCTET_LENGTH"));
    assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
    assertEquals("", resultSet.getString("IS_NULLABLE"));
    assertEquals(
        "FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
        resultSet.getString("SPECIFIC_NAME"));
    resultSet.next();
    assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
    assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
    assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
    assertEquals("COLB", resultSet.getString("COLUMN_NAME"));
    assertEquals(DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
    assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
    assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
    assertEquals(38, resultSet.getInt("PRECISION"));
    assertEquals(0, resultSet.getInt("LENGTH"));
    assertEquals(0, resultSet.getInt("SCALE"));
    assertEquals(10, resultSet.getInt("RADIX"));
    assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
    assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
    assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
    assertEquals(2, resultSet.getInt("ORDINAL_POSITION"));
    assertEquals("", resultSet.getString("IS_NULLABLE"));
    assertEquals(
        "FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
        resultSet.getString("SPECIFIC_NAME"));
    resultSet.next();
    assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
    assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
    assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
    assertEquals("BIN2", resultSet.getString("COLUMN_NAME"));
    assertEquals(DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
    assertEquals(Types.BINARY, resultSet.getInt("DATA_TYPE"));
    assertEquals("BINARY", resultSet.getString("TYPE_NAME"));
    assertEquals(38, resultSet.getInt("PRECISION"));
    assertEquals(0, resultSet.getInt("LENGTH"));
    assertEquals(0, resultSet.getInt("SCALE"));
    assertEquals(10, resultSet.getInt("RADIX"));
    assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
    assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
    assertEquals(8388608, resultSet.getInt("CHAR_OCTET_LENGTH"));
    assertEquals(3, resultSet.getInt("ORDINAL_POSITION"));
    assertEquals("", resultSet.getString("IS_NULLABLE"));
    assertEquals(
        "FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
        resultSet.getString("SPECIFIC_NAME"));
    resultSet.next();
    assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
    assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
    assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
    assertEquals("SHAREDCOL", resultSet.getString("COLUMN_NAME"));
    assertEquals(DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
    assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
    assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
    assertEquals(38, resultSet.getInt("PRECISION"));
    assertEquals(0, resultSet.getInt("LENGTH"));
    assertEquals(0, resultSet.getInt("SCALE"));
    assertEquals(10, resultSet.getInt("RADIX"));
    assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
    assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
    assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
    assertEquals(4, resultSet.getInt("ORDINAL_POSITION"));
    assertEquals("", resultSet.getString("IS_NULLABLE"));
    assertEquals(
        "FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
        resultSet.getString("SPECIFIC_NAME"));
    resultSet = databaseMetaData.getFunctionColumns(null, "%", "%", "%");
    // we have 81 columns returned
    assertEquals(4, getSizeOfResultSet(resultSet));
    // setting catalog to % will result in 0 columns. % does not apply for catalog, only for other
    // params
    resultSet = databaseMetaData.getFunctionColumns("%", "%", "%", "%");
    assertEquals(0, getSizeOfResultSet(resultSet));
  }

  /** Tests that calling getTables() concurrently doesn't cause data race condition. */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetTablesRaceCondition()
      throws SQLException, ExecutionException, InterruptedException {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      DatabaseMetaData databaseMetaData = connection.getMetaData();

      // Create 10 threads, each calls getTables() concurrently
      ExecutorService executorService = Executors.newFixedThreadPool(10);
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        futures.add(
            executorService.submit(
                () -> {
                  try {
                    databaseMetaData.getTables(database, schema, null, null);
                  } catch (SQLException e) {
                    throw new RuntimeException(e);
                  }
                }));
      }
      executorService.shutdown();
      for (int i = 0; i < 10; i++) futures.get(i).get();
    }
  }
}
