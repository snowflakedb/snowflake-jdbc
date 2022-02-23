/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.DatabaseMetaDataIT.verifyResultSetMetaDataColumns;
import static net.snowflake.client.jdbc.SnowflakeDatabaseMetaData.*;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * DatabaseMetaData test for the latest JDBC driver. This doesn't work for the oldest supported
 * driver. Revisit this tests whenever bumping up the oldest supported driver to examine if the
 * tests still is not applicable. If it is applicable, move tests to DatabaseMetaDataIT so that both
 * the latest and oldest supported driver run the tests.
 */
@Category(TestCategoryOthers.class)
public class DatabaseMetaDataLatestIT extends BaseJDBCTest {
  private static final String TEST_PROC =
      "create or replace procedure testproc(param1 float, param2 string)\n"
          + "    returns varchar\n"
          + "    language javascript\n"
          + "    as\n"
          + "    $$\n"
          + "    var sql_command = \"Hello, world!\"\n"
          + "    $$\n"
          + "    ;";

  /**
   * Tests for getFunctions
   *
   * @throws SQLException arises if any error occurs
   */
  @Test
  public void testUseConnectionCtx() throws SQLException {
    try (Connection connection = getConnection()) {
      connection
          .createStatement()
          .execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");
      String schema = connection.getSchema();
      DatabaseMetaData databaseMetaData = connection.getMetaData();

      // create tables within current schema.
      connection.createStatement().execute("create or replace schema TEST_CTX");
      connection
          .createStatement()
          .execute(
              "create or replace table CTX_TBL_A (colA string, colB decimal, "
                  + "colC number PRIMARY KEY);");
      connection
          .createStatement()
          .execute(
              "create or replace table CTX_TBL_B (colA string, colB decimal, "
                  + "colC number FOREIGN KEY REFERENCES CTX_TBL_A (colC));");
      connection
          .createStatement()
          .execute(
              "create or replace table CTX_TBL_C (colA string, colB decimal, "
                  + "colC number, colD int, colE timestamp, colF string, colG number);");
      // now create more tables under current schema
      connection.createStatement().execute("use schema " + schema);
      connection
          .createStatement()
          .execute(
              "create or replace table CTX_TBL_D (colA string, colB decimal, "
                  + "colC number PRIMARY KEY);");
      connection
          .createStatement()
          .execute(
              "create or replace table CTX_TBL_E (colA string, colB decimal, "
                  + "colC number FOREIGN KEY REFERENCES CTX_TBL_D (colC));");
      connection
          .createStatement()
          .execute(
              "create or replace table CTX_TBL_F (colA string, colB decimal, "
                  + "colC number, colD int, colE timestamp, colF string, colG number);");

      // this should only return TEST_CTX schema and tables
      connection.createStatement().execute("use schema TEST_CTX");

      ResultSet resultSet = databaseMetaData.getSchemas(null, null);
      assertEquals(1, getSizeOfResultSet(resultSet));

      resultSet = databaseMetaData.getTables(null, null, null, null);
      assertEquals(3, getSizeOfResultSet(resultSet));

      resultSet = databaseMetaData.getColumns(null, null, null, null);
      assertEquals(13, getSizeOfResultSet(resultSet));

      resultSet = databaseMetaData.getPrimaryKeys(null, null, null);
      assertEquals(1, getSizeOfResultSet(resultSet));

      resultSet = databaseMetaData.getImportedKeys(null, null, null);
      assertEquals(1, getSizeOfResultSet(resultSet));

      resultSet = databaseMetaData.getExportedKeys(null, null, null);
      assertEquals(1, getSizeOfResultSet(resultSet));

      resultSet = databaseMetaData.getCrossReference(null, null, null, null, null, null);
      assertEquals(1, getSizeOfResultSet(resultSet));

      // Now compare results to setting client metadata to false.
      connection
          .createStatement()
          .execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=false");
      databaseMetaData = connection.getMetaData();

      resultSet = databaseMetaData.getSchemas(null, null);
      assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));

      resultSet = databaseMetaData.getTables(null, null, null, null);
      assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(6));

      resultSet = databaseMetaData.getColumns(null, null, null, null);
      assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(26));

      resultSet = databaseMetaData.getPrimaryKeys(null, null, null);
      assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));

      resultSet = databaseMetaData.getImportedKeys(null, null, null);
      assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));

      resultSet = databaseMetaData.getExportedKeys(null, null, null);
      assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));

      resultSet = databaseMetaData.getCrossReference(null, null, null, null, null, null);
      assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));
    }
  }

  /**
   * This tests the ability to have quotes inside a schema or database. This fixes a bug where
   * double-quoted function arguments like schemas, databases, etc were returning empty resultsets.
   *
   * @throws SQLException
   */
  @Test
  public void testDoubleQuotedDatabaseAndSchema() throws SQLException {
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      String database = con.getCatalog();
      // To query the schema and table, we can use a normal java escaped quote. Wildcards are also
      // escaped here
      String querySchema = "TEST\\_SCHEMA\\_\"WITH\\_QUOTES\"";
      String queryTable = "TESTTABLE\\_\"WITH\\_QUOTES\"";
      // Create the schema and table. With SQL commands, double quotes must be escaped with another
      // quote
      statement.execute("create or replace schema \"TEST_SCHEMA_\"\"WITH_QUOTES\"\"\"");
      statement.execute(
          "create or replace table \"TESTTABLE_\"\"WITH_QUOTES\"\"\" (AMOUNT number,"
              + " \"COL_\"\"QUOTED\"\"\" string)");
      DatabaseMetaData metaData = con.getMetaData();
      ResultSet rs = metaData.getTables(database, querySchema, queryTable, null);
      // Assert 1 row returned for the testtable_"with_quotes"
      assertEquals(1, getSizeOfResultSet(rs));
      rs = metaData.getColumns(database, querySchema, queryTable, null);
      // Assert 2 rows returned for the 2 rows in testtable_"with_quotes"
      assertEquals(2, getSizeOfResultSet(rs));
      rs = metaData.getColumns(database, querySchema, queryTable, "COL\\_\"QUOTED\"");
      // Assert 1 row returned for the column col_"quoted"
      assertEquals(1, getSizeOfResultSet(rs));
      rs = metaData.getSchemas(database, querySchema);
      // Assert 1 row returned for the schema test_schema_"with_quotes"
      assertEquals(1, getSizeOfResultSet(rs));
      rs.close();
      statement.close();
    }
  }

  /**
   * This tests that wildcards can be used for the schema name for getProcedureColumns().
   * Previously, only empty resultsets were returned.
   *
   * @throws SQLException
   */
  @Test
  public void testGetProcedureColumnsWildcards() throws SQLException {
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      String database = con.getCatalog();
      String schema1 = "SCH1";
      String schema2 = "SCH2";
      // Create 2 schemas, each with the same stored procedure declared in them
      statement.execute("create or replace schema " + schema1);
      statement.execute(TEST_PROC);
      statement.execute("create or replace schema " + schema2);
      statement.execute(TEST_PROC);
      DatabaseMetaData metaData = con.getMetaData();
      ResultSet rs = metaData.getProcedureColumns(database, "SCH_", "TESTPROC", "PARAM1");
      // Assert 4 rows returned for the param PARAM1 that's present in each of the 2 identical
      // stored procs in different schemas. A result row is returned for each procedure, making the
      // total rowcount 4
      assertEquals(4, getSizeOfResultSet(rs));
      rs.close();
      statement.close();
    }
  }

  @Test
  public void testGetFunctions() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metadata = connection.getMetaData();
      String supportedStringFuncs = metadata.getStringFunctions();
      assertEquals(StringFunctionsSupported, supportedStringFuncs);

      String supportedNumberFuncs = metadata.getNumericFunctions();
      assertEquals(NumericFunctionsSupported, supportedNumberFuncs);

      String supportedSystemFuncs = metadata.getSystemFunctions();
      assertEquals(SystemFunctionsSupported, supportedSystemFuncs);
    }
  }

  @Test
  public void testGetStringValueFromColumnDef() throws SQLException {
    Map<String, String> params = getConnectionParameters();
    Properties properties = new Properties();
    for (Map.Entry<?, ?> entry : params.entrySet()) {
      if (entry.getValue() != null) {
        properties.put(entry.getKey(), entry.getValue());
      }
    }
    // test out connection parameter stringsQuoted to remove strings from quotes
    properties.put("stringsQuotedForColumnDef", "true");
    Connection connection = DriverManager.getConnection(params.get("uri"), properties);
    String database = connection.getCatalog();
    String schema = connection.getSchema();
    final String targetTable = "T0";

    connection
        .createStatement()
        .execute(
            "create or replace table "
                + targetTable
                + "(C1 string, C2 string default '', C3 string default 'apples', C4 string default"
                + " '\"apples\"', C5 int, C6 int default 5, C7 string default '''', C8 string"
                + " default '''apples''''', C9  string default '%')");

    DatabaseMetaData metaData = connection.getMetaData();

    ResultSet resultSet = metaData.getColumns(database, schema, targetTable, "%");
    assertTrue(resultSet.next());
    assertNull(resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("''", resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("'apples'", resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("'\"apples\"'", resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertNull(resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("5", resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("''''", resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("'''apples'''''", resultSet.getString("COLUMN_DEF"));
    assertTrue(resultSet.next());
    assertEquals("'%'", resultSet.getString("COLUMN_DEF"));
  }

  @Test
  public void testGetColumnsNullable() throws Throwable {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";

      connection
          .createStatement()
          .execute(
              "create or replace table "
                  + targetTable
                  + "(C1 int, C2 varchar(100), C3 string default '', C4 number(18,4), C5 double,"
                  + " C6 boolean, C7 date not null, C8 time, C9 timestamp_ntz(7), C10 binary,C11"
                  + " variant, C12 timestamp_ltz(8), C13 timestamp_tz(3))");

      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet = metaData.getColumns(database, schema, targetTable, "%");
      verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_COLUMNS);

      // C1 metadata
      assertTrue(resultSet.next());
      assertTrue(resultSet.getBoolean("NULLABLE"));
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testSessionDatabaseParameter() throws Throwable {
    String altdb = "ALTERNATEDB";
    String altschema1 = "ALTERNATESCHEMA1";
    String altschema2 = "ALTERNATESCHEMA2";
    try (Connection connection = getConnection()) {
      Statement statement = connection.createStatement();
      String catalog = connection.getCatalog();
      String schema = connection.getSchema();
      statement.execute("create or replace database " + altdb);
      statement.execute("create or replace schema " + altschema1);
      statement.execute("create or replace schema " + altschema2);
      statement.execute(
          "create or replace table "
              + altdb
              + "."
              + altschema1
              + ".testtable1 (colA string, colB number)");
      statement.execute(
          "create or replace table "
              + altdb
              + "."
              + altschema2
              + ".testtable2 (colA string, colB number)");
      statement.execute(
          "create or replace table "
              + catalog
              + "."
              + schema
              + ".testtable3 (colA string, colB number)");
      statement.execute("use database " + altdb);
      statement.execute("use schema " + altschema1);

      statement.execute("ALTER SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");
      statement.execute("ALTER SESSION set CLIENT_METADATA_USE_SESSION_DATABASE=true");

      DatabaseMetaData metadata = connection.getMetaData();
      ResultSet resultSet = metadata.getColumns(null, null, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      resultSet = metadata.getColumns(null, altschema2, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      resultSet = metadata.getColumns(altdb, null, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      resultSet = metadata.getColumns(altdb, altschema2, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      statement.execute("ALTER SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=false");

      metadata = connection.getMetaData();
      resultSet = metadata.getColumns(null, null, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
      assertTrue(resultSet.next());
      assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      resultSet = metadata.getColumns(null, altschema2, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      resultSet = metadata.getColumns(altdb, null, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
      assertTrue(resultSet.next());
      assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      resultSet = metadata.getColumns(altdb, altschema2, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      statement.execute("ALTER SESSION set CLIENT_METADATA_USE_SESSION_DATABASE=false");

      metadata = connection.getMetaData();
      resultSet = metadata.getColumns(null, null, "TESTTABLE_", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
      assertTrue(resultSet.next());
      assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
      assertTrue(resultSet.next());
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      resultSet = metadata.getColumns(null, altschema2, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      resultSet = metadata.getColumns(altdb, null, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
      assertTrue(resultSet.next());
      assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      resultSet = metadata.getColumns(altdb, altschema2, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      statement.execute("ALTER SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");

      metadata = connection.getMetaData();
      resultSet = metadata.getColumns(null, null, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      resultSet = metadata.getColumns(null, altschema2, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      resultSet = metadata.getColumns(altdb, null, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      resultSet = metadata.getColumns(altdb, altschema2, "%", "COLA");
      assertTrue(resultSet.next());
      assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
      assertFalse(resultSet.next());

      // clean up after creating extra database and schema
      statement.execute("use database " + catalog);
      statement.execute("drop schema " + altdb + "." + altschema1);
      statement.execute("drop schema " + altdb + "." + altschema2);
      statement.execute("drop database " + altdb);
    }
  }

  /**
   * Test function getFunctionColumns. This function has input parameters of database, schema, UDF
   * name, and optional UDF parameter names. It returns rows of information about the UDF, and
   * returns 1 row per return value and 1 row per input parameter.
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetFunctionColumns() throws Exception {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();

      /* Create a table and put values into it */
      connection
          .createStatement()
          .execute(
              "create or replace table FuncColTest (colA int, colB string, colC " + "number);");
      connection.createStatement().execute("INSERT INTO FuncColTest VALUES (4, 'Hello', 6);");
      connection.createStatement().execute("INSERT INTO FuncColTest VALUES (8, 'World', 10);");
      /* Create a UDF that counts up the total rows in the table just created */
      connection
          .createStatement()
          .execute(
              "create or replace function total_rows_in_table() returns number as "
                  + "'select count(*) from FuncColTest';");
      /* Create another UDF that takes in 2 numbers and multiplies them together */
      connection
          .createStatement()
          .execute(
              "create or replace function FUNC111 "
                  + "(a number, b number) RETURNS NUMBER COMMENT='multiply numbers' as 'a*b'");
      /* Create another 2 tables to be used for another UDF */
      connection
          .createStatement()
          .execute(
              "create or replace table BIN_TABLE(bin1 binary, bin2 binary(100), "
                  + "sharedCol decimal)");
      connection
          .createStatement()
          .execute(
              "create or replace table JDBC_TBL111(colA string, colB decimal, colC "
                  + "timestamp)");
      /* Create a UDF that returns a table made up of 4 columns from 2 different tables, joined together */
      connection
          .createStatement()
          .execute(
              "create or replace function FUNC112 () RETURNS TABLE(colA string, colB decimal, bin2"
                  + " binary, sharedCol decimal) COMMENT= 'returns table of 4 columns' as 'select"
                  + " JDBC_TBL111.colA, JDBC_TBL111.colB, BIN_TABLE.bin2, BIN_TABLE.sharedCol from"
                  + " JDBC_TBL111 inner join BIN_TABLE on JDBC_TBL111.colB =BIN_TABLE.sharedCol'");
      DatabaseMetaData metaData = connection.getMetaData();
      /* Call getFunctionColumns on FUNC111 and since there's no parameter name, get all rows back */
      ResultSet resultSet = metaData.getFunctionColumns(database, schema, "FUNC111", "%");
      verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_FUNCTION_COLUMNS);
      resultSet.next();
      assertEquals(database, resultSet.getString("FUNCTION_CAT"));
      assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
      assertEquals("FUNC111", resultSet.getString("FUNCTION_NAME"));
      assertEquals("", resultSet.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.functionReturn, resultSet.getInt("COLUMN_TYPE"));
      assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
      assertEquals("NUMBER(38,0)", resultSet.getString("TYPE_NAME"));
      assertEquals(38, resultSet.getInt("PRECISION"));
      // length column is not supported and will always be 0
      assertEquals(0, resultSet.getInt("LENGTH"));
      assertEquals(0, resultSet.getShort("SCALE"));
      // radix column is not supported and will always be default of 10 (assumes base 10 system)
      assertEquals(10, resultSet.getInt("RADIX"));
      // nullable column is not supported and always returns NullableUnknown
      assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
      assertEquals("multiply numbers", resultSet.getString("REMARKS"));
      // char octet length column is not supported and always returns 0
      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(0, resultSet.getInt("ORDINAL_POSITION"));
      // is_nullable column is not supported and always returns empty string
      assertEquals("", resultSet.getString("IS_NULLABLE"));
      assertEquals("FUNC111(NUMBER, NUMBER) RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
      /* Call next function to get next row in resultSet, which contains row with info about first parameter of FUNC111
      function */
      resultSet.next();
      assertEquals(database, resultSet.getString("FUNCTION_CAT"));
      assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
      assertEquals("FUNC111", resultSet.getString("FUNCTION_NAME"));
      assertEquals("A", resultSet.getString("COLUMN_NAME"));
      assertEquals(1, resultSet.getInt("COLUMN_TYPE"));
      assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
      assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
      assertEquals(38, resultSet.getInt("PRECISION"));
      // length column is not supported and will always be 0
      assertEquals(0, resultSet.getInt("LENGTH"));
      assertEquals(0, resultSet.getShort("SCALE"));
      // radix column is not supported and will always be default of 10 (assumes base 10 system)
      assertEquals(10, resultSet.getInt("RADIX"));
      // nullable column is not supported and always returns NullableUnknown
      assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
      assertEquals("multiply numbers", resultSet.getString("REMARKS"));
      // char octet length column is not supported and always returns 0
      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
      // is_nullable column is not supported and always returns empty string
      assertEquals("", resultSet.getString("IS_NULLABLE"));
      assertEquals("FUNC111(NUMBER, NUMBER) RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
      /* Call next to get next row with info about second parameter of FUNC111 function */
      resultSet.next();
      assertEquals(database, resultSet.getString("FUNCTION_CAT"));
      assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
      assertEquals("FUNC111", resultSet.getString("FUNCTION_NAME"));
      assertEquals("B", resultSet.getString("COLUMN_NAME"));
      assertEquals(1, resultSet.getInt("COLUMN_TYPE"));
      assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
      assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
      assertEquals(38, resultSet.getInt("PRECISION"));
      // length column is not supported and will always be 0
      assertEquals(0, resultSet.getInt("LENGTH"));
      assertEquals(0, resultSet.getShort("SCALE"));
      // radix column is not supported and will always be default of 10 (assumes base 10 system)
      assertEquals(10, resultSet.getInt("RADIX"));
      // nullable column is not supported and always returns NullableUnknown
      assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
      assertEquals("multiply numbers", resultSet.getString("REMARKS"));
      // char octet length column is not supported and always returns 0
      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(2, resultSet.getInt("ORDINAL_POSITION"));
      // is_nullable column is not supported and always returns empty string
      assertEquals("", resultSet.getString("IS_NULLABLE"));
      assertEquals("FUNC111(NUMBER, NUMBER) RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
      /* Assert that there are no more rows left in resultSet */
      assertFalse(resultSet.next());
      /* Look at resultSet from calling getFunctionColumns on FUNC112 */
      resultSet = metaData.getFunctionColumns(database, schema, "FUNC112", "%");
      resultSet.next();
      assertEquals(database, resultSet.getString("FUNCTION_CAT"));
      assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
      assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
      assertEquals("", resultSet.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.functionColumnOut, resultSet.getInt("COLUMN_TYPE"));
      assertEquals(Types.OTHER, resultSet.getInt("DATA_TYPE"));
      assertEquals(
          "TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
          resultSet.getString("TYPE_NAME"));
      assertEquals(0, resultSet.getInt("PRECISION"));
      // length column is not supported and will always be 0
      assertEquals(0, resultSet.getInt("LENGTH"));
      assertEquals(0, resultSet.getInt("SCALE"));
      // radix column is not supported and will always be default of 10 (assumes base 10 system)
      assertEquals(10, resultSet.getInt("RADIX"));
      // nullable column is not supported and always returns NullableUnknown
      assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
      assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
      // char octet length column is not supported and always returns 0
      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(0, resultSet.getInt("ORDINAL_POSITION"));
      // is_nullable column is not supported and always returns empty string
      assertEquals("", resultSet.getString("IS_NULLABLE"));
      assertEquals(
          "FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
          resultSet.getString("SPECIFIC_NAME"));
      /* There are no input parameters so only 1 row is returned, describing return value of function */
      assertFalse(resultSet.next());
      /* Assert that calling getFunctionColumns with no parameters returns empty result set */
      resultSet = metaData.getFunctionColumns("%", "%", "%", "%");
      assertFalse(resultSet.next());
      /* Look at result set from calling getFunctionColumns on total_rows_in_table */
      resultSet = metaData.getFunctionColumns(database, schema, "total_rows_in_table%", "%");
      /* Assert there are 17 columns in result set and 1 row */
      assertEquals(17, resultSet.getMetaData().getColumnCount());
      assertEquals(1, getSizeOfResultSet(resultSet));
      // getSizeofResultSet will mess up the row index of resultSet
      resultSet = metaData.getFunctionColumns(database, schema, "total_rows_in_table%", "%");
      resultSet.next();
      assertEquals(database, resultSet.getString("FUNCTION_CAT"));
      assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
      assertEquals("TOTAL_ROWS_IN_TABLE", resultSet.getString("FUNCTION_NAME"));
      assertEquals("", resultSet.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.functionReturn, resultSet.getInt("COLUMN_TYPE"));
      assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
      assertEquals("NUMBER(38,0)", resultSet.getString("TYPE_NAME"));
      assertEquals(38, resultSet.getInt("PRECISION"));
      // length column is not supported and will always be 0
      assertEquals(0, resultSet.getInt("LENGTH"));
      assertEquals(0, resultSet.getShort("SCALE"));
      // radix column is not supported and will always be default of 10 (assumes base 10 system)
      assertEquals(10, resultSet.getInt("RADIX"));
      // nullable column is not supported and always returns NullableUnknown
      assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
      assertEquals("user-defined function", resultSet.getString("REMARKS"));
      // char octet length column is not supported and always returns 0
      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(0, resultSet.getInt("ORDINAL_POSITION"));
      // is_nullable column is not supported and always returns empty string
      assertEquals("", resultSet.getString("IS_NULLABLE"));
      assertEquals("TOTAL_ROWS_IN_TABLE() RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testHandlingSpecialChars() throws SQLException {
    Connection connection = getConnection();
    String database = connection.getCatalog();
    String schema = connection.getSchema();
    DatabaseMetaData metaData = connection.getMetaData();
    Statement statement = connection.createStatement();
    String escapeChar = metaData.getSearchStringEscape();
    // test getColumns with escaped special characters in table name
    statement.execute(
        "create or replace table \"TEST\\1\\_1\" (\"C%1\" integer,\"C\\1\\\\11\" integer)");
    statement.execute("INSERT INTO \"TEST\\1\\_1\" (\"C%1\",\"C\\1\\\\11\") VALUES (0,0)");
    // test getColumns with escaped special characters in schema and table name
    statement.execute("create or replace schema \"SPECIAL%_\\SCHEMA\"");
    statement.execute(
        "create or replace table \"SPECIAL%_\\SCHEMA\".\"TEST_1_1\" ( \"RNUM\" integer not null, "
            + "\"C21\" integer,"
            + "\"C11\" integer,\"C%1\" integer,\"C\\1\\\\11\" integer , primary key (\"RNUM\"))");
    statement.execute(
        "INSERT INTO \"TEST_1_1\" (RNUM,C21,C11,\"C%1\",\"C\\1\\\\11\") VALUES (0,0,0,0,0)");
    String escapedTable1 = "TEST" + escapeChar + "\\1" + escapeChar + "\\" + escapeChar + "_1";
    ResultSet resultSet = metaData.getColumns(database, schema, escapedTable1, null);
    assertTrue(resultSet.next());
    assertEquals("C%1", resultSet.getString("COLUMN_NAME"));
    assertTrue(resultSet.next());
    assertEquals("C\\1\\\\11", resultSet.getString("COLUMN_NAME"));
    assertFalse(resultSet.next());

    // Underscore can match to any character, so check that table comes back when underscore is not
    // escaped.
    String partiallyEscapedTable1 = "TEST" + escapeChar + "\\1" + escapeChar + "\\_1";
    resultSet = metaData.getColumns(database, schema, partiallyEscapedTable1, null);
    assertTrue(resultSet.next());
    assertEquals("C%1", resultSet.getString("COLUMN_NAME"));
    assertTrue(resultSet.next());
    assertEquals("C\\1\\\\11", resultSet.getString("COLUMN_NAME"));
    assertFalse(resultSet.next());

    String escapedTable2 = "TEST" + escapeChar + "_1" + escapeChar + "_1";
    String escapedSchema = "SPECIAL%" + escapeChar + "_" + escapeChar + "\\SCHEMA";
    resultSet = metaData.getColumns(database, escapedSchema, escapedTable2, null);
    assertTrue(resultSet.next());
    assertEquals("RNUM", resultSet.getString("COLUMN_NAME"));
    assertTrue(resultSet.next());
    assertEquals("C21", resultSet.getString("COLUMN_NAME"));
    assertTrue(resultSet.next());
    assertEquals("C11", resultSet.getString("COLUMN_NAME"));
    assertTrue(resultSet.next());
    assertEquals("C%1", resultSet.getString("COLUMN_NAME"));
    assertTrue(resultSet.next());
    assertEquals("C\\1\\\\11", resultSet.getString("COLUMN_NAME"));
    assertFalse(resultSet.next());

    // test getTables with real special characters and escaped special characters. Unescaped _
    // should allow both
    // tables to be returned, while escaped _ should match up to the _ in both table names.
    statement.execute("create or replace table " + schema + ".\"TABLE_A\" (colA string)");
    statement.execute("create or replace table " + schema + ".\"TABLE_B\" (colB number)");
    String escapedTable = "TABLE" + escapeChar + "__";
    resultSet = metaData.getColumns(database, schema, escapedTable, null);
    assertTrue(resultSet.next());
    assertEquals("COLA", resultSet.getString("COLUMN_NAME"));
    assertTrue(resultSet.next());
    assertEquals("COLB", resultSet.getString("COLUMN_NAME"));
    assertFalse(resultSet.next());

    resultSet = metaData.getColumns(database, schema, escapedTable, "COLB");
    assertTrue(resultSet.next());
    assertEquals("COLB", resultSet.getString("COLUMN_NAME"));
    assertFalse(resultSet.next());

    statement.execute("create or replace table " + schema + ".\"special%table\" (colA string)");
    resultSet = metaData.getColumns(database, schema, "special" + escapeChar + "%table", null);
    assertTrue(resultSet.next());
    assertEquals("COLA", resultSet.getString("COLUMN_NAME"));
  }
}
