/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.DatabaseMetaDataIT.verifyResultSetMetaDataColumns;
import static net.snowflake.client.jdbc.SnowflakeDatabaseMetaData.*;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.TestUtil;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.client.core.SFBaseSession;
import org.junit.Ignore;
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

  private static final String PI_PROCEDURE =
      "create or replace procedure GETPI()\n"
          + "    returns float not null\n"
          + "    language javascript\n"
          + "    as\n"
          + "    $$\n"
          + "    return 3.1415926;\n"
          + "    $$\n"
          + "    ;";

  private static final String MESSAGE_PROCEDURE =
      "create or replace procedure MESSAGE_PROC(message varchar)\n"
          + "    returns varchar not null\n"
          + "    language javascript\n"
          + "    as\n"
          + "    $$\n"
          + "    return message;\n"
          + "    $$\n"
          + "    ;";

  /** Create catalog and schema for tests with double quotes */
  public void createDoubleQuotedSchemaAndCatalog(Statement statement) throws SQLException {
    statement.execute("create or replace database \"dbwith\"\"quotes\"");
    statement.execute("create or replace schema \"dbwith\"\"quotes\".\"schemawith\"\"quotes\"");
  }

  /**
   * Tests for getFunctions
   *
   * @throws Exception arises if any error occurs
   */
  @Test
  public void testUseConnectionCtx() throws Exception {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");
      String schema = connection.getSchema();

      TestUtil.withRandomSchema(
          statement,
          customSchema -> {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            // create tables within current schema.
            statement.execute(
                "create or replace table CTX_TBL_A (colA string, colB decimal, "
                    + "colC number PRIMARY KEY);");
            statement.execute(
                "create or replace table CTX_TBL_B (colA string, colB decimal, "
                    + "colC number FOREIGN KEY REFERENCES CTX_TBL_A (colC));");
            statement.execute(
                "create or replace table CTX_TBL_C (colA string, colB decimal, "
                    + "colC number, colD int, colE timestamp, colF string, colG number);");
            // now create more tables under current schema
            statement.execute("use schema " + schema);
            statement.execute(
                "create or replace table CTX_TBL_D (colA string, colB decimal, "
                    + "colC number PRIMARY KEY);");
            statement.execute(
                "create or replace table CTX_TBL_E (colA string, colB decimal, "
                    + "colC number FOREIGN KEY REFERENCES CTX_TBL_D (colC));");
            statement.execute(
                "create or replace table CTX_TBL_F (colA string, colB decimal, "
                    + "colC number, colD int, colE timestamp, colF string, colG number);");

            // this should only return `customSchema` schema and tables
            statement.execute("use schema " + customSchema);

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
            statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=false");
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
          });
    }
  }

  /**
   * This tests the ability to have quotes inside a schema or database. This fixes a bug where
   * double-quoted function arguments like schemas, databases, etc were returning empty resultsets.
   *
   * @throws Exception
   */
  @Test
  public void testDoubleQuotedDatabaseAndSchema() throws Exception {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      String database = con.getCatalog();
      // To query the schema and table, we can use a normal java escaped quote. Wildcards are also
      // escaped here
      String schemaRandomPart = SnowflakeUtil.randomAlphaNumeric(5);
      String querySchema = "TEST\\_SCHEMA\\_\"WITH\\_QUOTES" + schemaRandomPart + "\"";
      String queryTable = "TESTTABLE\\_\"WITH\\_QUOTES\"";
      // Create the schema and table. With SQL commands, double quotes must be escaped with another
      // quote
      String schemaName = "\"TEST_SCHEMA_\"\"WITH_QUOTES" + schemaRandomPart + "\"\"\"";
      TestUtil.withSchema(
          statement,
          schemaName,
          () -> {
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
          });
    }
  }

  /**
   * This tests the ability to have quotes inside a database or schema within getSchemas() function.
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testDoubleQuotedDatabaseInGetSchemas() throws SQLException {
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      // Create a database with double quotes inside the database name
      statement.execute("create or replace database \"\"\"quoteddb\"\"\"");
      // Create a database, lowercase, with no double quotes inside the database name
      statement.execute("create or replace database \"unquoteddb\"");
      DatabaseMetaData metaData = con.getMetaData();
      // Assert 2 rows returned for the PUBLIC and INFORMATION_SCHEMA schemas inside database
      ResultSet rs = metaData.getSchemas("\"quoteddb\"", null);
      assertEquals(2, getSizeOfResultSet(rs));
      // Assert no results are returned when failing to put quotes around quoted database
      rs = metaData.getSchemas("quoteddb", null);
      assertEquals(0, getSizeOfResultSet(rs));
      // Assert 2 rows returned for the PUBLIC and INFORMATION_SCHEMA schemas inside database
      rs = metaData.getSchemas("unquoteddb", null);
      assertEquals(2, getSizeOfResultSet(rs));
      // Assert no rows are returned when erroneously quoting unquoted database
      rs = metaData.getSchemas("\"unquoteddb\"", null);
      assertEquals(0, getSizeOfResultSet(rs));
      rs.close();
      statement.close();
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testDoubleQuotedDatabaseInGetTables() throws SQLException {
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      // Create a database with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a table with two columns
      statement.execute(
          "create or replace table \"dbwith\"\"quotes\".\"schemawith\"\"quotes\".\"testtable\" (col1 string, col2 string)");
      DatabaseMetaData metaData = con.getMetaData();
      ResultSet rs = metaData.getTables("dbwith\"quotes", "schemawith\"quotes", null, null);
      assertEquals(1, getSizeOfResultSet(rs));
      rs.close();
      statement.close();
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testDoubleQuotedDatabaseInGetColumns() throws SQLException {
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      // Create a database and schema with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a table with two columns
      statement.execute(
          "create or replace table \"dbwith\"\"quotes\".\"schemawith\"\"quotes\".\"testtable\"  (col1 string, col2 string)");
      DatabaseMetaData metaData = con.getMetaData();
      ResultSet rs = metaData.getColumns("dbwith\"quotes", "schemawith\"quotes", null, null);
      assertEquals(2, getSizeOfResultSet(rs));
      rs.close();
      statement.close();
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testDoubleQuotedDatabaseforGetPrimaryKeysAndForeignKeys() throws SQLException {
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      // Create a database and schema with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a table with a primary key constraint
      statement.execute(
          "create or replace table \"dbwith\"\"quotes\".\"schemawith\"\"quotes\".\"test1\"  (col1 integer not null, col2 integer not null, constraint pkey_1 primary key (col1, col2) not enforced)");
      // Create a table with a foreign key constraint that points to same columns as test1's primary
      // key constraint
      statement.execute(
          "create or replace table \"dbwith\"\"quotes\".\"schemawith\"\"quotes\".\"test2\" (col_a integer not null, col_b integer not null, constraint fkey_1 foreign key (col_a, col_b) references \"test1\" (col1, col2) not enforced)");
      DatabaseMetaData metaData = con.getMetaData();
      ResultSet rs = metaData.getPrimaryKeys("dbwith\"quotes", "schemawith\"quotes", null);
      // Assert 2 rows are returned for primary key constraint for table and schema with quotes
      assertEquals(2, getSizeOfResultSet(rs));
      rs = metaData.getImportedKeys("dbwith\"quotes", "schemawith\"quotes", null);
      // Assert 2 rows are returned for foreign key constraint
      assertEquals(2, getSizeOfResultSet(rs));
      rs.close();
      statement.close();
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testDoubleQuotedDatabaseInGetProcedures() throws SQLException {
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      // Create a database and schema with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a procedure
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
      statement.execute(
          "USE DATABASE \"dbwith\"\"quotes\"; USE SCHEMA \"schemawith\"\"quotes\"; " + TEST_PROC);
      DatabaseMetaData metaData = con.getMetaData();
      ResultSet rs = metaData.getProcedures("dbwith\"quotes", null, "TESTPROC");
      assertEquals(1, getSizeOfResultSet(rs));
      rs.close();
      statement.close();
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testDoubleQuotedDatabaseInGetTablePrivileges() throws SQLException {
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      // Create a database and schema with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a table under the current user and role
      statement.execute(
          "create or replace table \"dbwith\"\"quotes\".\"schemawith\"\"quotes\".\"testtable\" (col1 string, col2 string)");
      DatabaseMetaData metaData = con.getMetaData();
      ResultSet rs = metaData.getTablePrivileges("dbwith\"quotes", null, "%");
      assertEquals(1, getSizeOfResultSet(rs));
      rs.close();
      statement.close();
    }
  }

  /**
   * Test that SQL injection with multistatements into DatabaseMetaData get functions are no longer
   * possible.
   *
   * @throws Throwable
   */
  @Test
  public void testGetFunctionSqlInjectionProtection() throws Throwable {
    try (Connection connection = getConnection()) {
      // Enable multistatements since this is the only way a sql injection could occur
      connection.createStatement().execute("alter session set MULTI_STATEMENT_COUNT=0");
      DatabaseMetaData metaData = connection.getMetaData();
      String schemaSqlInection = "%' in database testwh; select 11 as bar; show databases like '%";
      ResultSet resultSet = metaData.getSchemas(null, schemaSqlInection);
      // assert result set is empty
      assertFalse(resultSet.next());
      String columnSqlInjection = "%' in schema testschema; show columns like '%";
      resultSet = metaData.getColumns(null, null, null, columnSqlInjection);
      // assert result set is empty
      assertFalse(resultSet.next());
      String functionSqlInjection = "%' in account snowflake; show functions like '%";
      resultSet = metaData.getColumns(null, null, null, functionSqlInjection);
      // assert result set is empty
      assertFalse(resultSet.next());
      // Clean up by unsetting multistatement
      connection.createStatement().execute("alter session unset MULTI_STATEMENT_COUNT");
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
      assertEquals("COLA", resultSet.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
      assertEquals(Types.VARCHAR, resultSet.getInt("DATA_TYPE"));
      assertEquals("VARCHAR", resultSet.getString("TYPE_NAME"));
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
      assertEquals(16777216, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
      // is_nullable column is not supported and always returns empty string
      assertEquals("", resultSet.getString("IS_NULLABLE"));
      assertEquals(
          "FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
          resultSet.getString("SPECIFIC_NAME"));
      resultSet.next();
      assertEquals(database, resultSet.getString("FUNCTION_CAT"));
      assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
      assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
      assertEquals("COLB", resultSet.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
      assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
      assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
      assertEquals(38, resultSet.getInt("PRECISION"));
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
      assertEquals(2, resultSet.getInt("ORDINAL_POSITION"));
      // is_nullable column is not supported and always returns empty string
      assertEquals("", resultSet.getString("IS_NULLABLE"));
      assertEquals(
          "FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
          resultSet.getString("SPECIFIC_NAME"));
      resultSet.next();
      assertEquals(database, resultSet.getString("FUNCTION_CAT"));
      assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
      assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
      assertEquals("BIN2", resultSet.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
      assertEquals(Types.BINARY, resultSet.getInt("DATA_TYPE"));
      assertEquals("BINARY", resultSet.getString("TYPE_NAME"));
      assertEquals(38, resultSet.getInt("PRECISION"));
      // length column is not supported and will always be 0
      assertEquals(0, resultSet.getInt("LENGTH"));
      assertEquals(0, resultSet.getInt("SCALE"));
      // radix column is not supported and will always be default of 10 (assumes base 10 system)
      assertEquals(10, resultSet.getInt("RADIX"));
      // nullable column is not supported and always returns NullableUnknown
      assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
      assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
      // char octet length column is not supported and always returns 0
      assertEquals(8388608, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(3, resultSet.getInt("ORDINAL_POSITION"));
      // is_nullable column is not supported and always returns empty string
      assertEquals("", resultSet.getString("IS_NULLABLE"));
      assertEquals(
          "FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
          resultSet.getString("SPECIFIC_NAME"));
      resultSet.next();
      assertEquals(database, resultSet.getString("FUNCTION_CAT"));
      assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
      assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
      assertEquals("SHAREDCOL", resultSet.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
      assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
      assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
      assertEquals(38, resultSet.getInt("PRECISION"));
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
      assertEquals(4, resultSet.getInt("ORDINAL_POSITION"));
      // is_nullable column is not supported and always returns empty string
      assertEquals("", resultSet.getString("IS_NULLABLE"));
      assertEquals(
          "FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)",
          resultSet.getString("SPECIFIC_NAME"));
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
  public void testHandlingSpecialChars() throws Exception {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      DatabaseMetaData metaData = connection.getMetaData();
      String escapeChar = metaData.getSearchStringEscape();
      // test getColumns with escaped special characters in table name
      statement.execute(
          "create or replace table \"TEST\\1\\_1\" (\"C%1\" integer,\"C\\1\\\\11\" integer)");
      statement.execute("INSERT INTO \"TEST\\1\\_1\" (\"C%1\",\"C\\1\\\\11\") VALUES (0,0)");
      // test getColumns with escaped special characters in schema and table name
      String specialSchemaSuffix = SnowflakeUtil.randomAlphaNumeric(5);
      String specialSchema = "\"SPECIAL%_\\SCHEMA" + specialSchemaSuffix + "\"";
      TestUtil.withSchema(
          statement,
          specialSchema,
          () -> {
            statement.execute(
                "create or replace table "
                    + specialSchema
                    + ".\"TEST_1_1\" ( \"RNUM\" integer not null, "
                    + "\"C21\" integer,"
                    + "\"C11\" integer,\"C%1\" integer,\"C\\1\\\\11\" integer , primary key (\"RNUM\"))");
            statement.execute(
                "INSERT INTO \"TEST_1_1\" (RNUM,C21,C11,\"C%1\",\"C\\1\\\\11\") VALUES (0,0,0,0,0)");
            String escapedTable1 =
                "TEST" + escapeChar + "\\1" + escapeChar + "\\" + escapeChar + "_1";
            ResultSet resultSet = metaData.getColumns(database, schema, escapedTable1, null);
            assertTrue(resultSet.next());
            assertEquals("C%1", resultSet.getString("COLUMN_NAME"));
            assertTrue(resultSet.next());
            assertEquals("C\\1\\\\11", resultSet.getString("COLUMN_NAME"));
            assertFalse(resultSet.next());

            // Underscore can match to any character, so check that table comes back when underscore
            // is not escaped.
            String partiallyEscapedTable1 = "TEST" + escapeChar + "\\1" + escapeChar + "\\_1";
            resultSet = metaData.getColumns(database, schema, partiallyEscapedTable1, null);
            assertTrue(resultSet.next());
            assertEquals("C%1", resultSet.getString("COLUMN_NAME"));
            assertTrue(resultSet.next());
            assertEquals("C\\1\\\\11", resultSet.getString("COLUMN_NAME"));
            assertFalse(resultSet.next());

            String escapedTable2 = "TEST" + escapeChar + "_1" + escapeChar + "_1";
            String escapedSchema =
                "SPECIAL%" + escapeChar + "_" + escapeChar + "\\SCHEMA" + specialSchemaSuffix;
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
          });

      // test getTables with real special characters and escaped special characters. Unescaped
      // _
      // should allow both
      // tables to be returned, while escaped _ should match up to the _ in both table names.
      statement.execute("create or replace table " + schema + ".\"TABLE_A\" (colA string)");
      statement.execute("create or replace table " + schema + ".\"TABLE_B\" (colB number)");
      String escapedTable = "TABLE" + escapeChar + "__";
      ResultSet resultSet = metaData.getColumns(database, schema, escapedTable, null);
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

  @Test
  public void testUnderscoreInSchemaNamePatternForPrimaryAndForeignKeys() throws Exception {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      String database = con.getCatalog();
      TestUtil.withRandomSchema(
          statement,
          customSchema -> {
            String escapedSchema = customSchema.replace("_", "\\_");
            statement.execute("use schema " + customSchema);
            statement.execute(
                "create or replace table PK_TEST (c1 int PRIMARY KEY, c2 VARCHAR(10))");
            statement.execute(
                "create or replace table FK_TEST (c1 int REFERENCES PK_TEST(c1), c2 VARCHAR(10))");
            DatabaseMetaData metaData = con.getMetaData();
            try (ResultSet rs = metaData.getPrimaryKeys(database, escapedSchema, null)) {
              assertEquals(1, getSizeOfResultSet(rs));
            }
            try (ResultSet rs = metaData.getImportedKeys(database, escapedSchema, null)) {
              assertEquals(1, getSizeOfResultSet(rs));
            }
          });
    }
  }

  @Test
  public void testTimestampWithTimezoneDataType() throws Exception {
    try (Connection connection = getConnection()) {
      Statement statement = connection.createStatement();
      statement.executeQuery("create or replace table ts_test(ts timestamp_tz)");
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet resultSet = metaData.getColumns(database, schema, "TS_TEST", "TS");
      resultSet.next();
      // Assert that TIMESTAMP_TZ type matches java.sql.TIMESTAMP_WITH_TIMEZONE
      assertEquals(resultSet.getObject("DATA_TYPE"), 2014);

      SFBaseSession baseSession = connection.unwrap(SnowflakeConnectionV1.class).getSFBaseSession();
      Field field = SFBaseSession.class.getDeclaredField("enableReturnTimestampWithTimeZone");
      field.setAccessible(true);
      field.set(baseSession, false);

      metaData = connection.getMetaData();
      resultSet = metaData.getColumns(database, schema, "TS_TEST", "TS");
      resultSet.next();
      // Assert that TIMESTAMP_TZ type matches java.sql.TIMESTAMP when
      // enableReturnTimestampWithTimeZone is false.
      assertEquals(resultSet.getObject("DATA_TYPE"), Types.TIMESTAMP);
    }
  }

  @Test
  public void testGetColumns() throws Throwable {
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
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
      assertEquals("C1", resultSet.getString("COLUMN_NAME"));
      assertEquals(Types.BIGINT, resultSet.getInt("DATA_TYPE"));
      assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
      assertEquals(38, resultSet.getInt("COLUMN_SIZE"));
      assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
      assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
      assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));

      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      // C2 metadata
      assertTrue(resultSet.next());
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
      assertEquals("C2", resultSet.getString("COLUMN_NAME"));
      assertEquals(Types.VARCHAR, resultSet.getInt("DATA_TYPE"));
      assertEquals("VARCHAR", resultSet.getString("TYPE_NAME"));
      assertEquals(100, resultSet.getInt("COLUMN_SIZE"));
      assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
      assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
      assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));

      assertEquals(100, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(2, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      // C3 metadata
      assertTrue(resultSet.next());
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
      assertEquals("C3", resultSet.getString("COLUMN_NAME"));
      assertEquals(Types.VARCHAR, resultSet.getInt("DATA_TYPE"));
      assertEquals("VARCHAR", resultSet.getString("TYPE_NAME"));
      assertEquals(16777216, resultSet.getInt("COLUMN_SIZE"));
      assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
      assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
      assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertEquals("", resultSet.getString("COLUMN_DEF"));

      assertEquals(16777216, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(3, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      // C4 metadata
      assertTrue(resultSet.next());
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
      assertEquals("C4", resultSet.getString("COLUMN_NAME"));
      assertEquals(Types.DECIMAL, resultSet.getInt("DATA_TYPE"));
      assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
      assertEquals(18, resultSet.getInt("COLUMN_SIZE"));
      assertEquals(4, resultSet.getInt("DECIMAL_DIGITS"));
      assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
      assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));

      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(4, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      // C5 metadata
      assertTrue(resultSet.next());
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
      assertEquals("C5", resultSet.getString("COLUMN_NAME"));
      assertEquals(Types.DOUBLE, resultSet.getInt("DATA_TYPE"));
      assertEquals("DOUBLE", resultSet.getString("TYPE_NAME"));
      assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
      assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
      assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
      assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));

      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(5, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      // C6 metadata
      assertTrue(resultSet.next());
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
      assertEquals("C6", resultSet.getString("COLUMN_NAME"));
      assertEquals(Types.BOOLEAN, resultSet.getInt("DATA_TYPE"));
      assertEquals("BOOLEAN", resultSet.getString("TYPE_NAME"));
      assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
      assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
      assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
      assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));

      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(6, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      // C7 metadata
      assertTrue(resultSet.next());
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
      assertEquals("C7", resultSet.getString("COLUMN_NAME"));
      assertEquals(Types.DATE, resultSet.getInt("DATA_TYPE"));
      assertEquals("DATE", resultSet.getString("TYPE_NAME"));
      assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
      assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
      assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
      assertEquals(ResultSetMetaData.columnNoNulls, resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));

      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(7, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("NO", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      // C8 metadata
      assertTrue(resultSet.next());
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
      assertEquals("C8", resultSet.getString("COLUMN_NAME"));
      assertEquals(Types.TIME, resultSet.getInt("DATA_TYPE"));
      assertEquals("TIME", resultSet.getString("TYPE_NAME"));
      assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
      assertEquals(9, resultSet.getInt("DECIMAL_DIGITS"));
      assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
      assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));

      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(8, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      // C9 metadata
      assertTrue(resultSet.next());
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
      assertEquals("C9", resultSet.getString("COLUMN_NAME"));
      assertEquals(Types.TIMESTAMP, resultSet.getInt("DATA_TYPE"));
      assertEquals("TIMESTAMPNTZ", resultSet.getString("TYPE_NAME"));
      assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
      assertEquals(7, resultSet.getInt("DECIMAL_DIGITS"));
      assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
      assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));

      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(9, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      // C10 metadata
      assertTrue(resultSet.next());
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
      assertEquals("C10", resultSet.getString("COLUMN_NAME"));
      assertEquals(Types.BINARY, resultSet.getInt("DATA_TYPE"));
      assertEquals("BINARY", resultSet.getString("TYPE_NAME"));
      assertEquals(8388608, resultSet.getInt("COLUMN_SIZE"));
      assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
      assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
      assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));

      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(10, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      // C11 metadata
      assertTrue(resultSet.next());
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
      assertEquals("C11", resultSet.getString("COLUMN_NAME"));
      assertEquals(Types.VARCHAR, resultSet.getInt("DATA_TYPE"));
      assertEquals("VARIANT", resultSet.getString("TYPE_NAME"));
      assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
      assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
      assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
      assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));

      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(11, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      // C12 metadata
      assertTrue(resultSet.next());
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
      assertEquals("C12", resultSet.getString("COLUMN_NAME"));
      assertEquals(Types.TIMESTAMP, resultSet.getInt("DATA_TYPE"));
      assertEquals("TIMESTAMPLTZ", resultSet.getString("TYPE_NAME"));
      assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
      assertEquals(8, resultSet.getInt("DECIMAL_DIGITS"));
      assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
      assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));

      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(12, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      // C13 metadata
      assertTrue(resultSet.next());
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
      assertEquals("C13", resultSet.getString("COLUMN_NAME"));
      assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, resultSet.getInt("DATA_TYPE"));
      assertEquals("TIMESTAMPTZ", resultSet.getString("TYPE_NAME"));
      assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
      assertEquals(3, resultSet.getInt("DECIMAL_DIGITS"));
      assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
      assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));

      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(13, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      connection
          .createStatement()
          .execute(
              "create or replace table "
                  + targetTable
                  + "(C1 string, C2 string default '', C3 string default 'apples', C4 string"
                  + " default '\"apples\"', C5 int, C6 int default 5, C7 string default '''', C8"
                  + " string default '''apples''''', C9  string default '%')");

      metaData = connection.getMetaData();

      resultSet = metaData.getColumns(database, schema, targetTable, "%");
      assertTrue(resultSet.next());
      assertNull(resultSet.getString("COLUMN_DEF"));
      assertTrue(resultSet.next());
      assertEquals("", resultSet.getString("COLUMN_DEF"));
      assertTrue(resultSet.next());
      assertEquals("apples", resultSet.getString("COLUMN_DEF"));
      assertTrue(resultSet.next());
      assertEquals("\"apples\"", resultSet.getString("COLUMN_DEF"));
      assertTrue(resultSet.next());
      assertNull(resultSet.getString("COLUMN_DEF"));
      assertTrue(resultSet.next());
      assertEquals("5", resultSet.getString("COLUMN_DEF"));
      assertTrue(resultSet.next());
      assertEquals("'", resultSet.getString("COLUMN_DEF"));
      assertTrue(resultSet.next());
      assertEquals("'apples''", resultSet.getString("COLUMN_DEF"));
      assertTrue(resultSet.next());
      assertEquals("%", resultSet.getString("COLUMN_DEF"));

      try {
        resultSet.getString("INVALID_COLUMN");
        fail("must fail");
      } catch (SQLException ex) {
        // nop
      }

      // no column privilege is supported.
      resultSet = metaData.getColumnPrivileges(database, schema, targetTable, "C1");
      assertEquals(0, super.getSizeOfResultSet(resultSet));

      connection.createStatement().execute("drop table if exists T0");
    }
  }

  @Test
  public void testGetStreams() throws SQLException {
    try (Connection con = getConnection()) {
      String database = con.getCatalog();
      String schema = con.getSchema();
      String owner = con.unwrap(SnowflakeConnectionV1.class).getSFBaseSession().getRole();
      final String targetStream = "S0";
      final String targetTable = "T0";
      String tableName = database + "." + schema + "." + targetTable;

      Statement statement = con.createStatement();
      statement.execute("create or replace table " + targetTable + "(C1 int)");
      statement.execute("create or replace stream " + targetStream + " on table " + targetTable);

      DatabaseMetaData metaData = con.getMetaData();

      // match stream
      ResultSet resultSet =
          metaData.unwrap(SnowflakeDatabaseMetaData.class).getStreams(database, schema, "%");
      verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_STREAMS);
      Set<String> streams = new HashSet<>();
      while (resultSet.next()) {
        streams.add(resultSet.getString(1));
      }
      assertTrue(streams.contains("S0"));

      // match exact stream
      resultSet =
          metaData
              .unwrap(SnowflakeDatabaseMetaData.class)
              .getStreams(database, schema, targetStream);
      resultSet.next();
      assertEquals(targetStream, resultSet.getString(1));
      assertEquals(database, resultSet.getString(2));
      assertEquals(schema, resultSet.getString(3));
      assertEquals(owner, resultSet.getString(4));
      assertEquals("", resultSet.getString(5));
      assertEquals(tableName, resultSet.getString(6));
      assertEquals("Table", resultSet.getString(7));
      assertEquals(tableName, resultSet.getString(8));
      assertEquals("DELTA", resultSet.getString(9));
      assertEquals("false", resultSet.getString(10));
      assertEquals("DEFAULT", resultSet.getString(11));

      con.createStatement().execute("drop table if exists " + targetTable);
      con.createStatement().execute("drop stream if exists " + targetStream);
      resultSet.close();
      statement.close();
    }
  }

  /*
   * This tests that an empty resultset will be returned for getProcedures when using a reader account.
   */
  @Test
  @Ignore
  public void testGetProceduresWithReaderAccount() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metadata = connection.getMetaData();
      ResultSet rs = metadata.getProcedures(null, null, null);
      assertEquals(0, getSizeOfResultSet(rs));
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetProcedureColumns() throws Exception {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      connection.createStatement().execute(PI_PROCEDURE);
      DatabaseMetaData metaData = connection.getMetaData();
      /* Call getProcedureColumns with no parameters for procedure name or column. This should return  all procedures
      in the current database and schema. It will return all rows as well (1 row per result and 1 row per parameter
      for each procedure) */
      ResultSet resultSet = metaData.getProcedureColumns(database, schema, "GETPI", "%");
      verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_PROCEDURE_COLUMNS);
      resultSet.next();
      assertEquals(database, resultSet.getString("PROCEDURE_CAT"));
      assertEquals(schema, resultSet.getString("PROCEDURE_SCHEM"));
      assertEquals("GETPI", resultSet.getString("PROCEDURE_NAME"));
      assertEquals("", resultSet.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.procedureColumnReturn, resultSet.getInt("COLUMN_TYPE"));
      assertEquals(Types.FLOAT, resultSet.getInt("DATA_TYPE"));
      assertEquals("FLOAT", resultSet.getString("TYPE_NAME"));
      assertEquals(38, resultSet.getInt("PRECISION"));
      // length column is not supported and will always be 0
      assertEquals(0, resultSet.getInt("LENGTH"));
      assertEquals(0, resultSet.getShort("SCALE"));
      // radix column is not supported and will always be default of 10 (assumes base 10 system)
      assertEquals(10, resultSet.getInt("RADIX"));
      // nullable column is not supported and always returns NullableUnknown
      assertEquals(DatabaseMetaData.procedureNoNulls, resultSet.getInt("NULLABLE"));
      assertEquals("user-defined procedure", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));
      assertEquals(0, resultSet.getInt("SQL_DATA_TYPE"));
      assertEquals(0, resultSet.getInt("SQL_DATETIME_SUB"));
      // char octet length column is not supported and always returns 0
      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(0, resultSet.getInt("ORDINAL_POSITION"));
      // is_nullable column is not supported and always returns empty string
      assertEquals("NO", resultSet.getString("IS_NULLABLE"));
      assertEquals("GETPI() RETURN FLOAT", resultSet.getString("SPECIFIC_NAME"));
      connection.createStatement().execute("drop procedure if exists GETPI()");
    }
  }

  @Test
  public void testGetProcedureColumnsReturnsResultSet() throws SQLException {
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      statement.execute(
          "create or replace table testtable (id int, name varchar(20), address varchar(20));");
      statement.execute(
          "create or replace procedure PROCTEST()\n"
              + "returns table (\"id\" number(38,0), \"name\" varchar(20), \"address\" varchar(20))\n"
              + "language sql\n"
              + "execute as owner\n"
              + "as 'declare\n"
              + "    res resultset default (select * from testtable);\n"
              + "  begin\n"
              + "    return table(res);\n"
              + "  end';");
      DatabaseMetaData metaData = con.getMetaData();
      ResultSet res = metaData.getProcedureColumns(con.getCatalog(), null, "PROCTEST", "%");
      res.next();
      assertEquals("PROCTEST", res.getString("PROCEDURE_NAME"));
      assertEquals("id", res.getString("COLUMN_NAME"));
      assertEquals(
          DatabaseMetaData.procedureColumnResult,
          res.getInt("COLUMN_TYPE")); // procedureColumnResult
      assertEquals(Types.NUMERIC, res.getInt("DATA_TYPE"));
      assertEquals("NUMBER", res.getString("TYPE_NAME"));
      assertEquals(1, res.getInt("ORDINAL_POSITION")); // result set column 1
      res.next();
      assertEquals("name", res.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.procedureColumnResult, res.getInt("COLUMN_TYPE"));
      assertEquals(Types.VARCHAR, res.getInt("DATA_TYPE"));
      assertEquals("VARCHAR", res.getString("TYPE_NAME"));
      assertEquals(2, res.getInt("ORDINAL_POSITION")); // result set column 2
      res.next();
      assertEquals("address", res.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.procedureColumnResult, res.getInt("COLUMN_TYPE"));
      assertEquals(Types.VARCHAR, res.getInt("DATA_TYPE"));
      assertEquals("VARCHAR", res.getString("TYPE_NAME"));
      assertEquals(3, res.getInt("ORDINAL_POSITION")); // result set column 3

      res.close();
      statement.execute("drop table if exists testtable");
      statement.close();
    }
  }

  @Test
  public void testGetProcedureColumnsReturnsValue() throws SQLException {
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      DatabaseMetaData metaData = con.getMetaData();
      // create a procedure with no parameters that has a return value
      statement.execute(PI_PROCEDURE);
      ResultSet res = metaData.getProcedureColumns(con.getCatalog(), null, "GETPI", "%");
      res.next();
      assertEquals("GETPI", res.getString("PROCEDURE_NAME"));
      assertEquals("", res.getString("COLUMN_NAME"));
      assertEquals(5, res.getInt("COLUMN_TYPE")); // procedureColumnReturn
      assertEquals(Types.FLOAT, res.getInt("DATA_TYPE"));
      assertEquals("FLOAT", res.getString("TYPE_NAME"));
      assertEquals(0, res.getInt("ORDINAL_POSITION"));

      // create a procedure that returns the value of the argument that is passed in
      statement.execute(MESSAGE_PROCEDURE);
      res = metaData.getProcedureColumns(con.getCatalog(), null, "MESSAGE_PROC", "%");
      res.next();
      assertEquals("MESSAGE_PROC", res.getString("PROCEDURE_NAME"));
      assertEquals("", res.getString("COLUMN_NAME"));
      assertEquals(
          DatabaseMetaData.procedureColumnReturn,
          res.getInt("COLUMN_TYPE")); // procedureColumnReturn
      assertEquals(Types.VARCHAR, res.getInt("DATA_TYPE"));
      assertEquals("VARCHAR", res.getString("TYPE_NAME"));
      assertEquals(0, res.getInt("ORDINAL_POSITION"));
      res.next();
      assertEquals("MESSAGE", res.getString("COLUMN_NAME"));
      assertEquals(
          DatabaseMetaData.procedureColumnIn, res.getInt("COLUMN_TYPE")); // procedureColumnIn
      assertEquals(Types.VARCHAR, res.getInt("DATA_TYPE"));
      assertEquals("VARCHAR", res.getString("TYPE_NAME"));
      assertEquals(1, res.getInt("ORDINAL_POSITION"));

      res.close();
      statement.close();
    }
  }

  @Test
  public void testGetProcedureColumnsReturnsNull() throws SQLException {
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      DatabaseMetaData metaData = con.getMetaData();
      // The CREATE PROCEDURE statement must include a RETURNS clause that defines a return type,
      // even
      // if the procedure does not explicitly return anything.
      statement.execute(
          "create or replace table testtable (id int, name varchar(20), address varchar(20));");
      statement.execute(
          "create or replace procedure insertproc() \n"
              + "returns varchar \n"
              + "language javascript as \n"
              + "'var sqlcommand = \n"
              + "`insert into testtable (id, name, address) values (1, \\'Tom\\', \\'Pacific Avenue\\');` \n"
              + "snowflake.execute({sqlText: sqlcommand}); \n"
              + "';");
      ResultSet res = metaData.getProcedureColumns(con.getCatalog(), null, "INSERTPROC", "%");
      res.next();
      // the procedure will return null as the value but column type will be varchar.
      assertEquals("INSERTPROC", res.getString("PROCEDURE_NAME"));
      assertEquals("", res.getString("COLUMN_NAME"));
      assertEquals(
          DatabaseMetaData.procedureColumnReturn,
          res.getInt("COLUMN_TYPE")); // procedureColumnReturn
      assertEquals(Types.VARCHAR, res.getInt("DATA_TYPE"));
      assertEquals("VARCHAR", res.getString("TYPE_NAME"));
      assertEquals(0, res.getInt("ORDINAL_POSITION"));
    }
  }

  @Test
  public void testUpdateLocatorsCopyUnsupported() throws SQLException {
    try (Connection con = getConnection()) {
      DatabaseMetaData metaData = con.getMetaData();
      assertFalse(metaData.locatorsUpdateCopy());
    }
  }
}
