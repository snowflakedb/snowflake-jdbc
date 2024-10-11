package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.DatabaseMetaDataInternalIT.endMetaData;
import static net.snowflake.client.jdbc.DatabaseMetaDataInternalIT.initMetaData;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Database Metadata tests for the latest JDBC driver. This doesn't work for the oldest supported
 * driver. Revisit this tests whenever bumping up the oldest supported driver to examine if the
 * tests still is not applicable. If it is applicable, move tests to DatabaseMetaDataIT so that both
 * the latest and oldest supported driver run the tests.
 */
// @Category(TestCategoryOthers.class)
public class DatabaseMetaDataInternalLatestIT extends BaseJDBCTest {

  @BeforeEach
  public void setUp() throws Exception {
    try (Connection con = getConnection()) {
      initMetaData(con);
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    try (Connection con = getConnection()) {
      endMetaData(con);
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGetMetaDataUseConnectionCtx() throws SQLException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {

      // setup: reset session db and schema, enable the parameter
      statement.execute("use database JDBC_DB1");
      statement.execute("use schema JDBC_SCHEMA11");
      statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");

      DatabaseMetaData databaseMetaData = connection.getMetaData();

      // Searches for tables only in database JDBC_DB1 and schema JDBC_SCHEMA11
      try (ResultSet resultSet = databaseMetaData.getTables(null, null, null, null)) {
        // Assert the tables are retrieved at schema level
        resultSet.next();
        Assertions.assertEquals("JDBC_DB1", resultSet.getString(1));
        Assertions.assertEquals("JDBC_SCHEMA11", resultSet.getString(2));
      }
      // Searches for tables only in database JDBC_DB1 and schema JDBC_SCHEMA11
      try (ResultSet resultSet = databaseMetaData.getColumns(null, null, null, null); ) {
        // Assert the columns are retrieved at schema level
        resultSet.next();
        Assertions.assertEquals("JDBC_DB1", resultSet.getString(1));
        Assertions.assertEquals("JDBC_SCHEMA11", resultSet.getString(2));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGetFunctionColumns() throws SQLException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create or replace function JDBC_DB1.JDBC_SCHEMA11.FUNC111 "
              + "(a number, b number) RETURNS NUMBER COMMENT='multiply numbers' as 'a*b'");
      statement.execute(
          "create or replace table JDBC_DB1.JDBC_SCHEMA11.BIN_TABLE(bin1 binary, bin2 binary(100), "
              + "sharedCol decimal)");
      statement.execute(
          "create or replace function JDBC_DB1.JDBC_SCHEMA11.FUNC112 "
              + "() RETURNS TABLE(colA string(16777216), colB decimal, bin2 binary(8388608), sharedCol decimal) COMMENT= 'returns "
              + "table of 4 columns'"
              + " as 'select JDBC_DB1.JDBC_SCHEMA11.JDBC_TBL111.colA, JDBC_DB1.JDBC_SCHEMA11.JDBC_TBL111.colB, "
              + "JDBC_DB1.JDBC_SCHEMA11.BIN_TABLE.bin2, JDBC_DB1.JDBC_SCHEMA11.BIN_TABLE.sharedCol from JDBC_DB1"
              + ".JDBC_SCHEMA11.JDBC_TBL111 inner join JDBC_DB1.JDBC_SCHEMA11.BIN_TABLE on JDBC_DB1.JDBC_SCHEMA11"
              + ".JDBC_TBL111.colB = JDBC_DB1.JDBC_SCHEMA11.BIN_TABLE.sharedCol'");
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      // test each column return the right value
      try (ResultSet resultSet =
          databaseMetaData.getFunctionColumns("JDBC_DB1", "JDBC_SCHEMA11", "FUNC111", "%")) {
        resultSet.next();
        Assertions.assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC111", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(DatabaseMetaData.functionReturn, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("NUMBER(38,0)", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getShort("SCALE"));
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        Assertions.assertEquals(
            DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("multiply numbers", resultSet.getString("REMARKS"));
        Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(0, resultSet.getInt("ORDINAL_POSITION"));
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals(
            "FUNC111(NUMBER, NUMBER) RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
        resultSet.next();
        Assertions.assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC111", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("A", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(1, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getShort("SCALE"));
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        Assertions.assertEquals(
            DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("multiply numbers", resultSet.getString("REMARKS"));
        Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals(
            "FUNC111(NUMBER, NUMBER) RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
        resultSet.next();
        Assertions.assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC111", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("B", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(1, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getShort("SCALE"));
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        Assertions.assertEquals(
            DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("multiply numbers", resultSet.getString("REMARKS"));
        Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(2, resultSet.getInt("ORDINAL_POSITION"));
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals(
            "FUNC111(NUMBER, NUMBER) RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
        Assertions.assertFalse(resultSet.next());
      }
      try (ResultSet resultSet =
          databaseMetaData.getFunctionColumns("JDBC_DB1", "JDBC_SCHEMA11", "FUNC112", "%")) {
        resultSet.next();
        Assertions.assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("COLA", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(
            DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.VARCHAR, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("VARCHAR", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(0, resultSet.getInt("PRECISION"));
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getInt("SCALE"));
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        Assertions.assertEquals(
            DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
        Assertions.assertEquals(
            databaseMetaData.getMaxCharLiteralLength(), resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals(
            "FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
            resultSet.getString("SPECIFIC_NAME"));
        resultSet.next();
        Assertions.assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("COLB", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(
            DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getInt("SCALE"));
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        Assertions.assertEquals(
            DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
        Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(2, resultSet.getInt("ORDINAL_POSITION"));
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals(
            "FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
            resultSet.getString("SPECIFIC_NAME"));
        resultSet.next();
        Assertions.assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("BIN2", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(
            DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.BINARY, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("BINARY", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getInt("SCALE"));
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        Assertions.assertEquals(
            DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
        Assertions.assertEquals(
            databaseMetaData.getMaxBinaryLiteralLength(), resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(3, resultSet.getInt("ORDINAL_POSITION"));
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals(
            "FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
            resultSet.getString("SPECIFIC_NAME"));
        resultSet.next();
        Assertions.assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("SHAREDCOL", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(
            DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getInt("SCALE"));
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        Assertions.assertEquals(
            DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
        Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(4, resultSet.getInt("ORDINAL_POSITION"));
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals(
            "FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
            resultSet.getString("SPECIFIC_NAME"));
        // setting catalog to % will result in 0 columns. % does not apply for catalog, only for
        // other
        // params
      }
      try (ResultSet resultSet = databaseMetaData.getFunctionColumns("%", "%", "%", "%")) {
        Assertions.assertEquals(0, getSizeOfResultSet(resultSet));
      }
    }
  }

  /** Tests that calling getTables() concurrently doesn't cause data race condition. */
  @Test
  @DontRunOnGithubActions
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
      for (int i = 0; i < 10; i++) {
        futures.get(i).get();
      }
    }
  }
}
