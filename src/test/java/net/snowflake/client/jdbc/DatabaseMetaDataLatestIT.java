/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.DatabaseMetaDataIT.EXPECTED_MAX_BINARY_LENGTH;
import static net.snowflake.client.jdbc.DatabaseMetaDataIT.EXPECTED_MAX_CHAR_LENGTH;
import static net.snowflake.client.jdbc.DatabaseMetaDataIT.verifyResultSetMetaDataColumns;
import static net.snowflake.client.jdbc.SnowflakeDatabaseMetaData.NumericFunctionsSupported;
import static net.snowflake.client.jdbc.SnowflakeDatabaseMetaData.StringFunctionsSupported;
import static net.snowflake.client.jdbc.SnowflakeDatabaseMetaData.SystemFunctionsSupported;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import net.snowflake.client.TestUtil;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFSessionProperty;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * DatabaseMetaData test for the latest JDBC driver. This doesn't work for the oldest supported
 * driver. Revisit this tests whenever bumping up the oldest supported driver to examine if the
 * tests still is not applicable. If it is applicable, move tests to DatabaseMetaDataIT so that both
 * the latest and oldest supported driver run the tests.
 */
//@Category(TestCategoryOthers.class)
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
  private static final String ENABLE_PATTERN_SEARCH =
      SFSessionProperty.ENABLE_PATTERN_SEARCH.getPropertyKey();

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

            try (ResultSet resultSet = databaseMetaData.getSchemas(null, null)) {
              Assertions.assertEquals(1, getSizeOfResultSet(resultSet));
            }

            try (ResultSet resultSet = databaseMetaData.getTables(null, null, null, null)) {
              Assertions.assertEquals(3, getSizeOfResultSet(resultSet));
            }

            try (ResultSet resultSet = databaseMetaData.getColumns(null, null, null, null)) {
              Assertions.assertEquals(13, getSizeOfResultSet(resultSet));
            }

            try (ResultSet resultSet = databaseMetaData.getPrimaryKeys(null, null, null)) {
              Assertions.assertEquals(1, getSizeOfResultSet(resultSet));
            }

            try (ResultSet resultSet = databaseMetaData.getImportedKeys(null, null, null)) {
              Assertions.assertEquals(1, getSizeOfResultSet(resultSet));
            }

            try (ResultSet resultSet = databaseMetaData.getExportedKeys(null, null, null)) {
              Assertions.assertEquals(1, getSizeOfResultSet(resultSet));
            }

            try (ResultSet resultSet =
                databaseMetaData.getCrossReference(null, null, null, null, null, null)) {
              Assertions.assertEquals(1, getSizeOfResultSet(resultSet));
            }
            // Now compare results to setting client metadata to false.
            statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=false");
            databaseMetaData = connection.getMetaData();

            try (ResultSet resultSet = databaseMetaData.getSchemas(null, null)) {
              MatcherAssert.assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));
            }

            try (ResultSet resultSet = databaseMetaData.getTables(null, null, null, null)) {
              MatcherAssert.assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(6));
            }

            try (ResultSet resultSet = databaseMetaData.getColumns(null, null, null, null)) {
              MatcherAssert.assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(26));
            }

            try (ResultSet resultSet = databaseMetaData.getPrimaryKeys(null, null, null)) {
              MatcherAssert.assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));
            }

            try (ResultSet resultSet = databaseMetaData.getImportedKeys(null, null, null)) {
              MatcherAssert.assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));
            }

            try (ResultSet resultSet = databaseMetaData.getExportedKeys(null, null, null)) {
              MatcherAssert.assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));
            }

            try (ResultSet resultSet =
                databaseMetaData.getCrossReference(null, null, null, null, null, null)) {
              MatcherAssert.assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));
            }
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
      String querySchema =
          TestUtil.ESCAPED_GENERATED_SCHEMA_PREFIX
              + "TEST\\_SCHEMA\\_\"WITH\\_QUOTES"
              + schemaRandomPart
              + "\"";
      String queryTable = "TESTTABLE\\_\"WITH\\_QUOTES\"";
      // Create the schema and table. With SQL commands, double quotes must be escaped with another
      // quote
      String schemaName =
          "\""
              + TestUtil.GENERATED_SCHEMA_PREFIX
              + "TEST_SCHEMA_\"\"WITH_QUOTES"
              + schemaRandomPart
              + "\"\"\"";
      TestUtil.withSchema(
          statement,
          schemaName,
          () -> {
            statement.execute(
                "create or replace table \"TESTTABLE_\"\"WITH_QUOTES\"\"\" (AMOUNT number,"
                    + " \"COL_\"\"QUOTED\"\"\" string)");
            DatabaseMetaData metaData = con.getMetaData();
            try (ResultSet rs = metaData.getTables(database, querySchema, queryTable, null)) {
              // Assert 1 row returned for the testtable_"with_quotes"
              Assertions.assertEquals(1, getSizeOfResultSet(rs));
            }
            try (ResultSet rs = metaData.getColumns(database, querySchema, queryTable, null)) {
              // Assert 2 rows returned for the 2 rows in testtable_"with_quotes"
              Assertions.assertEquals(2, getSizeOfResultSet(rs));
            }
            try (ResultSet rs =
                metaData.getColumns(database, querySchema, queryTable, "COL\\_\"QUOTED\"")) {
              // Assert 1 row returned for the column col_"quoted"
              Assertions.assertEquals(1, getSizeOfResultSet(rs));
            }
            try (ResultSet rs = metaData.getSchemas(database, querySchema)) {
              // Assert 1 row returned for the schema test_schema_"with_quotes"
              Assertions.assertEquals(1, getSizeOfResultSet(rs));
            }
          });
    }
  }

  /**
   * This tests the ability to have quotes inside a database or schema within getSchemas() function.
   */
  @Test
  @DontRunOnGithubActions
  public void testDoubleQuotedDatabaseInGetSchemas() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      // Create a database with double quotes inside the database name
      statement.execute("create or replace database \"\"\"quoteddb\"\"\"");
      // Create a database, lowercase, with no double quotes inside the database name
      statement.execute("create or replace database \"unquoteddb\"");
      DatabaseMetaData metaData = con.getMetaData();
      // Assert 2 rows returned for the PUBLIC and INFORMATION_SCHEMA schemas inside database
      try (ResultSet rs = metaData.getSchemas("\"quoteddb\"", null)) {
        Assertions.assertEquals(2, getSizeOfResultSet(rs));
      }
      // Assert no results are returned when failing to put quotes around quoted database
      try (ResultSet rs = metaData.getSchemas("quoteddb", null)) {
        Assertions.assertEquals(0, getSizeOfResultSet(rs));
      }
      // Assert 2 rows returned for the PUBLIC and INFORMATION_SCHEMA schemas inside database
      try (ResultSet rs = metaData.getSchemas("unquoteddb", null)) {
        Assertions.assertEquals(2, getSizeOfResultSet(rs));
      }
      // Assert no rows are returned when erroneously quoting unquoted database
      try (ResultSet rs = metaData.getSchemas("\"unquoteddb\"", null)) {
        Assertions.assertEquals(0, getSizeOfResultSet(rs));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testDoubleQuotedDatabaseInGetTables() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      // Create a database with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a table with two columns
      statement.execute(
          "create or replace table \"dbwith\"\"quotes\".\"schemawith\"\"quotes\".\"testtable\" (col1 string, col2 string)");
      DatabaseMetaData metaData = con.getMetaData();
      try (ResultSet rs = metaData.getTables("dbwith\"quotes", "schemawith\"quotes", null, null)) {
        Assertions.assertEquals(1, getSizeOfResultSet(rs));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testDoubleQuotedDatabaseInGetColumns() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      // Create a database and schema with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a table with two columns
      statement.execute(
          "create or replace table \"dbwith\"\"quotes\".\"schemawith\"\"quotes\".\"testtable\"  (col1 string, col2 string)");
      DatabaseMetaData metaData = con.getMetaData();
      try (ResultSet rs = metaData.getColumns("dbwith\"quotes", "schemawith\"quotes", null, null)) {
        Assertions.assertEquals(2, getSizeOfResultSet(rs));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testDoubleQuotedDatabaseforGetPrimaryKeysAndForeignKeys() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
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
      try (ResultSet rs = metaData.getPrimaryKeys("dbwith\"quotes", "schemawith\"quotes", null)) {
        // Assert 2 rows are returned for primary key constraint for table and schema with quotes
        Assertions.assertEquals(2, getSizeOfResultSet(rs));
      }
      try (ResultSet rs = metaData.getImportedKeys("dbwith\"quotes", "schemawith\"quotes", null)) {
        // Assert 2 rows are returned for foreign key constraint
        Assertions.assertEquals(2, getSizeOfResultSet(rs));
      }
    }
  }

  /**
   * For driver versions higher than 3.14.5 we can disable the abilty to use pattern searches for
   * getPrimaryKeys and getImportedKeys functions by setting enablePatternSearch = false.
   */
  @Test
  @DontRunOnGithubActions
  public void testDoubleQuotedDatabaseforGetPrimaryKeysAndForeignKeysWithPatternSearchDisabled()
      throws SQLException {
    Properties properties = new Properties();
    properties.put(ENABLE_PATTERN_SEARCH, false);
    try (Connection con = getConnection(properties);
        Statement statement = con.createStatement()) {
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
      try (ResultSet rs = metaData.getPrimaryKeys("dbwith\"quotes", "schemawith\"quotes", null)) {
        // Assert 2 rows are returned for primary key constraint for table and schema with quotes
        Assertions.assertEquals(2, getSizeOfResultSet(rs));
      }
      try (ResultSet rs = metaData.getImportedKeys("dbwith\"quotes", "schemawith\"quotes", null)) {
        // Assert 2 rows are returned for foreign key constraint
        Assertions.assertEquals(2, getSizeOfResultSet(rs));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testDoubleQuotedDatabaseInGetProcedures() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      // Create a database and schema with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a procedure
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
      statement.execute(
          "USE DATABASE \"dbwith\"\"quotes\"; USE SCHEMA \"schemawith\"\"quotes\"; " + TEST_PROC);
      DatabaseMetaData metaData = con.getMetaData();
      try (ResultSet rs = metaData.getProcedures("dbwith\"quotes", null, "TESTPROC")) {
        Assertions.assertEquals(1, getSizeOfResultSet(rs));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testDoubleQuotedDatabaseInGetTablePrivileges() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      // Create a database and schema with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a table under the current user and role
      statement.execute(
          "create or replace table \"dbwith\"\"quotes\".\"schemawith\"\"quotes\".\"testtable\" (col1 string, col2 string)");
      DatabaseMetaData metaData = con.getMetaData();
      try (ResultSet rs = metaData.getTablePrivileges("dbwith\"quotes", null, "%")) {
        Assertions.assertEquals(1, getSizeOfResultSet(rs));
      }
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
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        // Enable multistatements since this is the only way a sql injection could occur
        statement.execute("alter session set MULTI_STATEMENT_COUNT=0");
        DatabaseMetaData metaData = connection.getMetaData();
        String schemaSqlInection =
            "%' in database testwh; select 11 as bar; show databases like '%";
        try (ResultSet resultSet = metaData.getSchemas(null, schemaSqlInection)) {
          // assert result set is empty
          Assertions.assertFalse(resultSet.next());
        }

        String columnSqlInjection = "%' in schema testschema; show columns like '%";
        try (ResultSet resultSet = metaData.getColumns(null, null, null, columnSqlInjection)) {
          // assert result set is empty
          Assertions.assertFalse(resultSet.next());
        }

        String functionSqlInjection = "%' in account snowflake; show functions like '%";
        try (ResultSet resultSet = metaData.getColumns(null, null, null, functionSqlInjection)) {
          // assert result set is empty
          Assertions.assertFalse(resultSet.next());
        }
      } finally {
        // Clean up by unsetting multistatement
        statement.execute("alter session unset MULTI_STATEMENT_COUNT");
      }
    }
  }

  /**
   * This tests that wildcards can be used for the schema name for getProcedureColumns().
   * Previously, only empty resultsets were returned.
   *
   * @throws SQLException
   */
  @Test
  public void testGetProcedureColumnsWildcards() throws Exception {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      String database = con.getCatalog();
      String schemaPrefix =
          TestUtil.GENERATED_SCHEMA_PREFIX + SnowflakeUtil.randomAlphaNumeric(5).toUpperCase();
      String schema1 = schemaPrefix + "SCH1";
      String schema2 = schemaPrefix + "SCH2";
      TestUtil.withSchema(
          statement,
          schema1,
          () -> {
            statement.execute(TEST_PROC);
            TestUtil.withSchema(
                statement,
                schema2,
                () -> {
                  statement.execute(TEST_PROC);
                  // Create 2 schemas, each with the same stored procedure declared in them
                  DatabaseMetaData metaData = con.getMetaData();
                  try (ResultSet rs =
                      metaData.getProcedureColumns(
                          database, schemaPrefix + "SCH_", "TESTPROC", "PARAM1")) {
                    // Assert 4 rows returned for the param PARAM1 that's present in each of the 2
                    // identical stored procs in different schemas. A result row is returned for
                    // each procedure, making the total rowcount 4
                    Assertions.assertEquals(4, getSizeOfResultSet(rs));
                  }
                });
          });
    }
  }

  @Test
  public void testGetFunctions() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metadata = connection.getMetaData();
      String supportedStringFuncs = metadata.getStringFunctions();
      Assertions.assertEquals(StringFunctionsSupported, supportedStringFuncs);

      String supportedNumberFuncs = metadata.getNumericFunctions();
      Assertions.assertEquals(NumericFunctionsSupported, supportedNumberFuncs);

      String supportedSystemFuncs = metadata.getSystemFunctions();
      Assertions.assertEquals(SystemFunctionsSupported, supportedSystemFuncs);
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
    try (Connection connection = DriverManager.getConnection(params.get("uri"), properties);
        Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";

      statement.execute(
          "create or replace table "
              + targetTable
              + "(C1 string, C2 string default '', C3 string default 'apples', C4 string default"
              + " '\"apples\"', C5 int, C6 int default 5, C7 string default '''', C8 string"
              + " default '''apples''''', C9  string default '%')");

      DatabaseMetaData metaData = connection.getMetaData();

      try (ResultSet resultSet = metaData.getColumns(database, schema, targetTable, "%")) {
        Assertions.assertTrue(resultSet.next());
        Assertions.assertNull(resultSet.getString("COLUMN_DEF"));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("''", resultSet.getString("COLUMN_DEF"));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("'apples'", resultSet.getString("COLUMN_DEF"));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("'\"apples\"'", resultSet.getString("COLUMN_DEF"));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertNull(resultSet.getString("COLUMN_DEF"));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("5", resultSet.getString("COLUMN_DEF"));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("''''", resultSet.getString("COLUMN_DEF"));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("'''apples'''''", resultSet.getString("COLUMN_DEF"));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("'%'", resultSet.getString("COLUMN_DEF"));
      }
    }
  }

  @Test
  public void testGetColumnsNullable() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";

      statement.execute(
          "create or replace table "
              + targetTable
              + "(C1 int, C2 varchar(100), C3 string default '', C4 number(18,4), C5 double,"
              + " C6 boolean, C7 date not null, C8 time, C9 timestamp_ntz(7), C10 binary,C11"
              + " variant, C12 timestamp_ltz(8), C13 timestamp_tz(3))");

      DatabaseMetaData metaData = connection.getMetaData();

      try (ResultSet resultSet = metaData.getColumns(database, schema, targetTable, "%")) {
        verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_COLUMNS);

        // C1 metadata
        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.getBoolean("NULLABLE"));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testSessionDatabaseParameter() throws Throwable {
    String altdb = "ALTERNATEDB";
    String altschema1 = "ALTERNATESCHEMA1";
    String altschema2 = "ALTERNATESCHEMA2";
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      String catalog = connection.getCatalog();
      String schema = connection.getSchema();
      try {
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
        try (ResultSet resultSet = metadata.getColumns(null, null, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(null, altschema2, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, null, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, altschema2, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        statement.execute("ALTER SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=false");

        metadata = connection.getMetaData();
        try (ResultSet resultSet = metadata.getColumns(null, null, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(null, altschema2, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, null, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, altschema2, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }
        statement.execute("ALTER SESSION set CLIENT_METADATA_USE_SESSION_DATABASE=false");

        metadata = connection.getMetaData();
        try (ResultSet resultSet = metadata.getColumns(null, null, "TESTTABLE_", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(null, altschema2, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, null, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, altschema2, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        statement.execute("ALTER SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");

        metadata = connection.getMetaData();
        try (ResultSet resultSet = metadata.getColumns(null, null, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(null, altschema2, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, null, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, altschema2, "%", "COLA")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertFalse(resultSet.next());
        }
      } finally {
        // clean up after creating extra database and schema
        statement.execute("use database " + catalog);
        statement.execute("drop schema " + altdb + "." + altschema1);
        statement.execute("drop schema " + altdb + "." + altschema2);
        statement.execute("drop database " + altdb);
      }
    }
  }

  /**
   * Test function getFunctionColumns. This function has input parameters of database, schema, UDF
   * name, and optional UDF parameter names. It returns rows of information about the UDF, and
   * returns 1 row per return value and 1 row per input parameter.
   */
  @Test
  @DontRunOnGithubActions
  public void testGetFunctionColumns() throws Exception {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();

      /* Create a table and put values into it */
      statement.execute(
          "create or replace table FuncColTest (colA int, colB string, colC " + "number);");
      statement.execute("INSERT INTO FuncColTest VALUES (4, 'Hello', 6);");
      statement.execute("INSERT INTO FuncColTest VALUES (8, 'World', 10);");
      /* Create a UDF that counts up the total rows in the table just created */
      statement.execute(
          "create or replace function total_rows_in_table() returns number as "
              + "'select count(*) from FuncColTest';");
      /* Create another UDF that takes in 2 numbers and multiplies them together */
      statement.execute(
          "create or replace function FUNC111 "
              + "(a number, b number) RETURNS NUMBER COMMENT='multiply numbers' as 'a*b'");
      /* Create another 2 tables to be used for another UDF */
      statement.execute(
          "create or replace table BIN_TABLE(bin1 binary, bin2 binary(100), "
              + "sharedCol decimal)");
      statement.execute(
          "create or replace table JDBC_TBL111(colA string, colB decimal, colC " + "timestamp)");
      /* Create a UDF that returns a table made up of 4 columns from 2 different tables, joined together */
      statement.execute(
          "create or replace function FUNC112 () RETURNS TABLE(colA string(16777216), colB decimal, bin2 "
              + "binary(8388608) , sharedCol decimal) COMMENT= 'returns table of 4 columns' as 'select"
              + " JDBC_TBL111.colA, JDBC_TBL111.colB, BIN_TABLE.bin2, BIN_TABLE.sharedCol from"
              + " JDBC_TBL111 inner join BIN_TABLE on JDBC_TBL111.colB =BIN_TABLE.sharedCol'");
      DatabaseMetaData metaData = connection.getMetaData();

      /* Call getFunctionColumns on FUNC111 and since there's no parameter name, get all rows back */
      try (ResultSet resultSet = metaData.getFunctionColumns(database, schema, "FUNC111", "%")) {
        verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_FUNCTION_COLUMNS);
        resultSet.next();
        Assertions.assertEquals(database, resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC111", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(DatabaseMetaData.functionReturn, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("NUMBER(38,0)", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
        // length column is not supported and will always be 0
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getShort("SCALE"));
        // radix column is not supported and will always be default of 10 (assumes base 10 system)
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        // nullable column is not supported and always returns NullableUnknown
        Assertions.assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("multiply numbers", resultSet.getString("REMARKS"));
        // char octet length column is not supported and always returns 0
        Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(0, resultSet.getInt("ORDINAL_POSITION"));
        // is_nullable column is not supported and always returns empty string
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals("FUNC111(NUMBER, NUMBER) RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
        /* Call next function to get next row in resultSet, which contains row with info about first parameter of FUNC111
        function */
        resultSet.next();
        Assertions.assertEquals(database, resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC111", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("A", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(1, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
        // length column is not supported and will always be 0
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getShort("SCALE"));
        // radix column is not supported and will always be default of 10 (assumes base 10 system)
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        // nullable column is not supported and always returns NullableUnknown
        Assertions.assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("multiply numbers", resultSet.getString("REMARKS"));
        // char octet length column is not supported and always returns 0
        Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
        // is_nullable column is not supported and always returns empty string
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals("FUNC111(NUMBER, NUMBER) RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
        /* Call next to get next row with info about second parameter of FUNC111 function */
        resultSet.next();
        Assertions.assertEquals(database, resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC111", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("B", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(1, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
        // length column is not supported and will always be 0
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getShort("SCALE"));
        // radix column is not supported and will always be default of 10 (assumes base 10 system)
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        // nullable column is not supported and always returns NullableUnknown
        Assertions.assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("multiply numbers", resultSet.getString("REMARKS"));
        // char octet length column is not supported and always returns 0
        Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(2, resultSet.getInt("ORDINAL_POSITION"));
        // is_nullable column is not supported and always returns empty string
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals("FUNC111(NUMBER, NUMBER) RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
        /* Assert that there are no more rows left in resultSet */
        Assertions.assertFalse(resultSet.next());
      }

      /* Look at resultSet from calling getFunctionColumns on FUNC112 */
      try (ResultSet resultSet = metaData.getFunctionColumns(database, schema, "FUNC112", "%")) {
        resultSet.next();
        Assertions.assertEquals(database, resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("COLA", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.VARCHAR, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("VARCHAR", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(0, resultSet.getInt("PRECISION"));
        // length column is not supported and will always be 0
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getInt("SCALE"));
        // radix column is not supported and will always be default of 10 (assumes base 10 system)
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        // nullable column is not supported and always returns NullableUnknown
        Assertions.assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
        // char octet length column is not supported and always returns 0
        Assertions.assertEquals(EXPECTED_MAX_CHAR_LENGTH, resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
        // is_nullable column is not supported and always returns empty string
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals("FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)", resultSet.getString("SPECIFIC_NAME"));
        resultSet.next();
        Assertions.assertEquals(database, resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("COLB", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
        // length column is not supported and will always be 0
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getInt("SCALE"));
        // radix column is not supported and will always be default of 10 (assumes base 10 system)
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        // nullable column is not supported and always returns NullableUnknown
        Assertions.assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
        // char octet length column is not supported and always returns 0
        Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(2, resultSet.getInt("ORDINAL_POSITION"));
        // is_nullable column is not supported and always returns empty string
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals("FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)", resultSet.getString("SPECIFIC_NAME"));
        resultSet.next();
        Assertions.assertEquals(database, resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("BIN2", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.BINARY, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("BINARY", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
        // length column is not supported and will always be 0
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getInt("SCALE"));
        // radix column is not supported and will always be default of 10 (assumes base 10 system)
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        // nullable column is not supported and always returns NullableUnknown
        Assertions.assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
        // char octet length column is not supported and always returns 0
        Assertions.assertEquals(EXPECTED_MAX_BINARY_LENGTH, resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(3, resultSet.getInt("ORDINAL_POSITION"));
        // is_nullable column is not supported and always returns empty string
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals("FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)", resultSet.getString("SPECIFIC_NAME"));
        resultSet.next();
        Assertions.assertEquals(database, resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("FUNC112", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("SHAREDCOL", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(DatabaseMetaData.functionColumnResult, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
        // length column is not supported and will always be 0
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getInt("SCALE"));
        // radix column is not supported and will always be default of 10 (assumes base 10 system)
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        // nullable column is not supported and always returns NullableUnknown
        Assertions.assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("returns table of 4 columns", resultSet.getString("REMARKS"));
        // char octet length column is not supported and always returns 0
        Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(4, resultSet.getInt("ORDINAL_POSITION"));
        // is_nullable column is not supported and always returns empty string
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals("FUNC112() RETURN TABLE (COLA VARCHAR, COLB NUMBER, BIN2 BINARY, SHAREDCOL NUMBER)", resultSet.getString("SPECIFIC_NAME"));
        Assertions.assertFalse(resultSet.next());
      }

      /* Assert that calling getFunctionColumns with no parameters returns empty result set */
      try (ResultSet resultSet = metaData.getFunctionColumns("%", "%", "%", "%")) {
        Assertions.assertFalse(resultSet.next());
      }

      /* Look at result set from calling getFunctionColumns on total_rows_in_table */
      try (ResultSet resultSet =
          metaData.getFunctionColumns(database, schema, "total_rows_in_table%", "%")) {
        /* Assert there are 17 columns in result set and 1 row */
        Assertions.assertEquals(17, resultSet.getMetaData().getColumnCount());
        Assertions.assertEquals(1, getSizeOfResultSet(resultSet));
      }

      // getSizeofResultSet will mess up the row index of resultSet
      try (ResultSet resultSet =
          metaData.getFunctionColumns(database, schema, "total_rows_in_table%", "%")) {
        resultSet.next();
        Assertions.assertEquals(database, resultSet.getString("FUNCTION_CAT"));
        Assertions.assertEquals(schema, resultSet.getString("FUNCTION_SCHEM"));
        Assertions.assertEquals("TOTAL_ROWS_IN_TABLE", resultSet.getString("FUNCTION_NAME"));
        Assertions.assertEquals("", resultSet.getString("COLUMN_NAME"));
        Assertions.assertEquals(DatabaseMetaData.functionReturn, resultSet.getInt("COLUMN_TYPE"));
        Assertions.assertEquals(Types.NUMERIC, resultSet.getInt("DATA_TYPE"));
        Assertions.assertEquals("NUMBER(38,0)", resultSet.getString("TYPE_NAME"));
        Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
        // length column is not supported and will always be 0
        Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
        Assertions.assertEquals(0, resultSet.getShort("SCALE"));
        // radix column is not supported and will always be default of 10 (assumes base 10 system)
        Assertions.assertEquals(10, resultSet.getInt("RADIX"));
        // nullable column is not supported and always returns NullableUnknown
        Assertions.assertEquals(DatabaseMetaData.functionNullableUnknown, resultSet.getInt("NULLABLE"));
        Assertions.assertEquals("user-defined function", resultSet.getString("REMARKS"));
        // char octet length column is not supported and always returns 0
        Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
        Assertions.assertEquals(0, resultSet.getInt("ORDINAL_POSITION"));
        // is_nullable column is not supported and always returns empty string
        Assertions.assertEquals("", resultSet.getString("IS_NULLABLE"));
        Assertions.assertEquals("TOTAL_ROWS_IN_TABLE() RETURN NUMBER", resultSet.getString("SPECIFIC_NAME"));
        Assertions.assertFalse(resultSet.next());
      }
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
      String specialSchema =
          "\""
              + TestUtil.GENERATED_SCHEMA_PREFIX
              + "SPECIAL%_\\SCHEMA"
              + specialSchemaSuffix
              + "\"";
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
            try (ResultSet resultSet = metaData.getColumns(database, schema, escapedTable1, null)) {
              Assertions.assertTrue(resultSet.next());
              Assertions.assertEquals("C%1", resultSet.getString("COLUMN_NAME"));
              Assertions.assertTrue(resultSet.next());
              Assertions.assertEquals("C\\1\\\\11", resultSet.getString("COLUMN_NAME"));
              Assertions.assertFalse(resultSet.next());
            }

            // Underscore can match to any character, so check that table comes back when underscore
            // is not escaped.
            String partiallyEscapedTable1 = "TEST" + escapeChar + "\\1" + escapeChar + "\\_1";
            try (ResultSet resultSet =
                metaData.getColumns(database, schema, partiallyEscapedTable1, null)) {
              Assertions.assertTrue(resultSet.next());
              Assertions.assertEquals("C%1", resultSet.getString("COLUMN_NAME"));
              Assertions.assertTrue(resultSet.next());
              Assertions.assertEquals("C\\1\\\\11", resultSet.getString("COLUMN_NAME"));
              Assertions.assertFalse(resultSet.next());
            }

            String escapedTable2 = "TEST" + escapeChar + "_1" + escapeChar + "_1";
            String escapedSchema =
                TestUtil.ESCAPED_GENERATED_SCHEMA_PREFIX
                    + "SPECIAL%"
                    + escapeChar
                    + "_"
                    + escapeChar
                    + "\\SCHEMA"
                    + specialSchemaSuffix;

            try (ResultSet resultSet =
                metaData.getColumns(database, escapedSchema, escapedTable2, null)) {
              Assertions.assertTrue(resultSet.next());
              Assertions.assertEquals("RNUM", resultSet.getString("COLUMN_NAME"));
              Assertions.assertTrue(resultSet.next());
              Assertions.assertEquals("C21", resultSet.getString("COLUMN_NAME"));
              Assertions.assertTrue(resultSet.next());
              Assertions.assertEquals("C11", resultSet.getString("COLUMN_NAME"));
              Assertions.assertTrue(resultSet.next());
              Assertions.assertEquals("C%1", resultSet.getString("COLUMN_NAME"));
              Assertions.assertTrue(resultSet.next());
              Assertions.assertEquals("C\\1\\\\11", resultSet.getString("COLUMN_NAME"));
              Assertions.assertFalse(resultSet.next());
            }
          });

      // test getTables with real special characters and escaped special characters. Unescaped
      // _
      // should allow both
      // tables to be returned, while escaped _ should match up to the _ in both table names.
      statement.execute("create or replace table " + schema + ".\"TABLE_A\" (colA string)");
      statement.execute("create or replace table " + schema + ".\"TABLE_B\" (colB number)");
      String escapedTable = "TABLE" + escapeChar + "__";
      try (ResultSet resultSet = metaData.getColumns(database, schema, escapedTable, null)) {
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("COLA", resultSet.getString("COLUMN_NAME"));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("COLB", resultSet.getString("COLUMN_NAME"));
        Assertions.assertFalse(resultSet.next());
      }

      try (ResultSet resultSet = metaData.getColumns(database, schema, escapedTable, "COLB")) {
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("COLB", resultSet.getString("COLUMN_NAME"));
        Assertions.assertFalse(resultSet.next());
      }

      statement.execute("create or replace table " + schema + ".\"special%table\" (colA string)");
      try (ResultSet resultSet =
          metaData.getColumns(database, schema, "special" + escapeChar + "%table", null)) {
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("COLA", resultSet.getString("COLUMN_NAME"));
      }
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
              Assertions.assertEquals(1, getSizeOfResultSet(rs));
            }
            try (ResultSet rs = metaData.getImportedKeys(database, escapedSchema, null)) {
              Assertions.assertEquals(1, getSizeOfResultSet(rs));
            }
          });
    }
  }

  /**
   * For driver versions higher than 3.14.5 we can disable the abilty to use pattern searches for
   * getPrimaryKeys and getImportedKeys functions by setting enablePatternSearch = false.
   */
  @Test
  public void testUnderscoreInSchemaNamePatternForPrimaryAndForeignKeysWithPatternSearchDisabled()
      throws Exception {
    Properties properties = new Properties();
    properties.put(ENABLE_PATTERN_SEARCH, false);

    try (Connection con = getConnection(properties);
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
            // We have disabled the pattern search so we should get no results.
            try (ResultSet rs = metaData.getPrimaryKeys(database, escapedSchema, null)) {
              Assertions.assertEquals(0, getSizeOfResultSet(rs));
            }
            try (ResultSet rs = metaData.getImportedKeys(database, escapedSchema, null)) {
              Assertions.assertEquals(0, getSizeOfResultSet(rs));
            }
            // We expect the results to be returned if we use the actual schema name
            try (ResultSet rs = metaData.getPrimaryKeys(database, customSchema, null)) {
              Assertions.assertEquals(1, getSizeOfResultSet(rs));
            }
            try (ResultSet rs = metaData.getImportedKeys(database, customSchema, null)) {
              Assertions.assertEquals(1, getSizeOfResultSet(rs));
            }
          });
    }
  }

  @Test
  public void testTimestampWithTimezoneDataType() throws Exception {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery("create or replace table ts_test(ts timestamp_tz)");
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet resultSet = metaData.getColumns(database, schema, "TS_TEST", "TS")) {
        resultSet.next();
        // Assert that TIMESTAMP_TZ type matches java.sql.TIMESTAMP_WITH_TIMEZONE
        Assertions.assertEquals(resultSet.getObject("DATA_TYPE"), 2014);
      }

      SFBaseSession baseSession = connection.unwrap(SnowflakeConnectionV1.class).getSFBaseSession();
      Field field = SFBaseSession.class.getDeclaredField("enableReturnTimestampWithTimeZone");
      field.setAccessible(true);
      field.set(baseSession, false);

      metaData = connection.getMetaData();
      try (ResultSet resultSet = metaData.getColumns(database, schema, "TS_TEST", "TS")) {
        resultSet.next();
        // Assert that TIMESTAMP_TZ type matches java.sql.TIMESTAMP when
        // enableReturnTimestampWithTimeZone is false.
        Assertions.assertEquals(resultSet.getObject("DATA_TYPE"), Types.TIMESTAMP);
      }
    }
  }

  @Test
  public void testGetColumns() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";
      try {
        statement.execute(
            "create or replace table "
                + targetTable
                + "(C1 int, C2 varchar(100), C3 string(16777216) default '', C4 number(18,4), C5 double,"
                + " C6 boolean, C7 date not null, C8 time, C9 timestamp_ntz(7), C10 binary(8388608),C11"
                + " variant, C12 timestamp_ltz(8), C13 timestamp_tz(3))");

        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet resultSet = metaData.getColumns(database, schema, targetTable, "%")) {
          verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_COLUMNS);

          // C1 metadata

          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
          Assertions.assertEquals("C1", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(Types.BIGINT, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(38, resultSet.getInt("COLUMN_SIZE"));
          Assertions.assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
          Assertions.assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("", resultSet.getString("REMARKS"));
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));

          Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
          Assertions.assertEquals("YES", resultSet.getString("IS_NULLABLE"));
          Assertions.assertNull(resultSet.getString("SCOPE_CATALOG"));
          Assertions.assertNull(resultSet.getString("SCOPE_SCHEMA"));
          Assertions.assertNull(resultSet.getString("SCOPE_TABLE"));
          Assertions.assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

          // C2 metadata
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
          Assertions.assertEquals("C2", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(Types.VARCHAR, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("VARCHAR", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(100, resultSet.getInt("COLUMN_SIZE"));
          Assertions.assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
          Assertions.assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("", resultSet.getString("REMARKS"));
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));

          Assertions.assertEquals(100, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(2, resultSet.getInt("ORDINAL_POSITION"));
          Assertions.assertEquals("YES", resultSet.getString("IS_NULLABLE"));
          Assertions.assertNull(resultSet.getString("SCOPE_CATALOG"));
          Assertions.assertNull(resultSet.getString("SCOPE_SCHEMA"));
          Assertions.assertNull(resultSet.getString("SCOPE_TABLE"));
          Assertions.assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

          // C3 metadata
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
          Assertions.assertEquals("C3", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(Types.VARCHAR, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("VARCHAR", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(EXPECTED_MAX_CHAR_LENGTH, resultSet.getInt("COLUMN_SIZE"));
          Assertions.assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
          Assertions.assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("", resultSet.getString("REMARKS"));
          Assertions.assertEquals("", resultSet.getString("COLUMN_DEF"));

          Assertions.assertEquals(EXPECTED_MAX_CHAR_LENGTH, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(3, resultSet.getInt("ORDINAL_POSITION"));
          Assertions.assertEquals("YES", resultSet.getString("IS_NULLABLE"));
          Assertions.assertNull(resultSet.getString("SCOPE_CATALOG"));
          Assertions.assertNull(resultSet.getString("SCOPE_SCHEMA"));
          Assertions.assertNull(resultSet.getString("SCOPE_TABLE"));
          Assertions.assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

          // C4 metadata
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
          Assertions.assertEquals("C4", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(Types.DECIMAL, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(18, resultSet.getInt("COLUMN_SIZE"));
          Assertions.assertEquals(4, resultSet.getInt("DECIMAL_DIGITS"));
          Assertions.assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("", resultSet.getString("REMARKS"));
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));

          Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(4, resultSet.getInt("ORDINAL_POSITION"));
          Assertions.assertEquals("YES", resultSet.getString("IS_NULLABLE"));
          Assertions.assertNull(resultSet.getString("SCOPE_CATALOG"));
          Assertions.assertNull(resultSet.getString("SCOPE_SCHEMA"));
          Assertions.assertNull(resultSet.getString("SCOPE_TABLE"));
          Assertions.assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

          // C5 metadata
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
          Assertions.assertEquals("C5", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(Types.DOUBLE, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("DOUBLE", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
          Assertions.assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
          Assertions.assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("", resultSet.getString("REMARKS"));
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));

          Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(5, resultSet.getInt("ORDINAL_POSITION"));
          Assertions.assertEquals("YES", resultSet.getString("IS_NULLABLE"));
          Assertions.assertNull(resultSet.getString("SCOPE_CATALOG"));
          Assertions.assertNull(resultSet.getString("SCOPE_SCHEMA"));
          Assertions.assertNull(resultSet.getString("SCOPE_TABLE"));
          Assertions.assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

          // C6 metadata
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
          Assertions.assertEquals("C6", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(Types.BOOLEAN, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("BOOLEAN", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
          Assertions.assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
          Assertions.assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("", resultSet.getString("REMARKS"));
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));

          Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(6, resultSet.getInt("ORDINAL_POSITION"));
          Assertions.assertEquals("YES", resultSet.getString("IS_NULLABLE"));
          Assertions.assertNull(resultSet.getString("SCOPE_CATALOG"));
          Assertions.assertNull(resultSet.getString("SCOPE_SCHEMA"));
          Assertions.assertNull(resultSet.getString("SCOPE_TABLE"));
          Assertions.assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

          // C7 metadata
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
          Assertions.assertEquals("C7", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(Types.DATE, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("DATE", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
          Assertions.assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
          Assertions.assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          Assertions.assertEquals(ResultSetMetaData.columnNoNulls, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("", resultSet.getString("REMARKS"));
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));

          Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(7, resultSet.getInt("ORDINAL_POSITION"));
          Assertions.assertEquals("NO", resultSet.getString("IS_NULLABLE"));
          Assertions.assertNull(resultSet.getString("SCOPE_CATALOG"));
          Assertions.assertNull(resultSet.getString("SCOPE_SCHEMA"));
          Assertions.assertNull(resultSet.getString("SCOPE_TABLE"));
          Assertions.assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

          // C8 metadata
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
          Assertions.assertEquals("C8", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(Types.TIME, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("TIME", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
          Assertions.assertEquals(9, resultSet.getInt("DECIMAL_DIGITS"));
          Assertions.assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("", resultSet.getString("REMARKS"));
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));

          Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(8, resultSet.getInt("ORDINAL_POSITION"));
          Assertions.assertEquals("YES", resultSet.getString("IS_NULLABLE"));
          Assertions.assertNull(resultSet.getString("SCOPE_CATALOG"));
          Assertions.assertNull(resultSet.getString("SCOPE_SCHEMA"));
          Assertions.assertNull(resultSet.getString("SCOPE_TABLE"));
          Assertions.assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

          // C9 metadata
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
          Assertions.assertEquals("C9", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(Types.TIMESTAMP, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("TIMESTAMPNTZ", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
          Assertions.assertEquals(7, resultSet.getInt("DECIMAL_DIGITS"));
          Assertions.assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("", resultSet.getString("REMARKS"));
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));

          Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(9, resultSet.getInt("ORDINAL_POSITION"));
          Assertions.assertEquals("YES", resultSet.getString("IS_NULLABLE"));
          Assertions.assertNull(resultSet.getString("SCOPE_CATALOG"));
          Assertions.assertNull(resultSet.getString("SCOPE_SCHEMA"));
          Assertions.assertNull(resultSet.getString("SCOPE_TABLE"));
          Assertions.assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

          // C10 metadata
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
          Assertions.assertEquals("C10", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(Types.BINARY, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("BINARY", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(EXPECTED_MAX_BINARY_LENGTH, resultSet.getInt("COLUMN_SIZE"));
          Assertions.assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
          Assertions.assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("", resultSet.getString("REMARKS"));
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));

          Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(10, resultSet.getInt("ORDINAL_POSITION"));
          Assertions.assertEquals("YES", resultSet.getString("IS_NULLABLE"));
          Assertions.assertNull(resultSet.getString("SCOPE_CATALOG"));
          Assertions.assertNull(resultSet.getString("SCOPE_SCHEMA"));
          Assertions.assertNull(resultSet.getString("SCOPE_TABLE"));
          Assertions.assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

          // C11 metadata
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
          Assertions.assertEquals("C11", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(Types.VARCHAR, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("VARIANT", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
          Assertions.assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
          Assertions.assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("", resultSet.getString("REMARKS"));
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));

          Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(11, resultSet.getInt("ORDINAL_POSITION"));
          Assertions.assertEquals("YES", resultSet.getString("IS_NULLABLE"));
          Assertions.assertNull(resultSet.getString("SCOPE_CATALOG"));
          Assertions.assertNull(resultSet.getString("SCOPE_SCHEMA"));
          Assertions.assertNull(resultSet.getString("SCOPE_TABLE"));
          Assertions.assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

          // C12 metadata
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
          Assertions.assertEquals("C12", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(Types.TIMESTAMP, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("TIMESTAMPLTZ", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
          Assertions.assertEquals(8, resultSet.getInt("DECIMAL_DIGITS"));
          Assertions.assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("", resultSet.getString("REMARKS"));
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));

          Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(12, resultSet.getInt("ORDINAL_POSITION"));
          Assertions.assertEquals("YES", resultSet.getString("IS_NULLABLE"));
          Assertions.assertNull(resultSet.getString("SCOPE_CATALOG"));
          Assertions.assertNull(resultSet.getString("SCOPE_SCHEMA"));
          Assertions.assertNull(resultSet.getString("SCOPE_TABLE"));
          Assertions.assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

          // C13 metadata
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString(3)); // table name (using index)
          Assertions.assertEquals("C13", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("TIMESTAMPTZ", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(0, resultSet.getInt("COLUMN_SIZE"));
          Assertions.assertEquals(3, resultSet.getInt("DECIMAL_DIGITS"));
          Assertions.assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          Assertions.assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("", resultSet.getString("REMARKS"));
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));

          Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(13, resultSet.getInt("ORDINAL_POSITION"));
          Assertions.assertEquals("YES", resultSet.getString("IS_NULLABLE"));
          Assertions.assertNull(resultSet.getString("SCOPE_CATALOG"));
          Assertions.assertNull(resultSet.getString("SCOPE_SCHEMA"));
          Assertions.assertNull(resultSet.getString("SCOPE_TABLE"));
          Assertions.assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));
        }
        statement.execute(
            "create or replace table "
                + targetTable
                + "(C1 string, C2 string default '', C3 string default 'apples', C4 string"
                + " default '\"apples\"', C5 int, C6 int default 5, C7 string default '''', C8"
                + " string default '''apples''''', C9  string default '%')");

        metaData = connection.getMetaData();

        try (ResultSet resultSet = metaData.getColumns(database, schema, targetTable, "%")) {
          Assertions.assertTrue(resultSet.next());
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals("", resultSet.getString("COLUMN_DEF"));
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals("apples", resultSet.getString("COLUMN_DEF"));
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals("\"apples\"", resultSet.getString("COLUMN_DEF"));
          Assertions.assertTrue(resultSet.next());
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals("5", resultSet.getString("COLUMN_DEF"));
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals("'", resultSet.getString("COLUMN_DEF"));
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals("'apples''", resultSet.getString("COLUMN_DEF"));
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals("%", resultSet.getString("COLUMN_DEF"));
          try {
            resultSet.getString("INVALID_COLUMN");
            Assertions.fail("must fail");
          } catch (SQLException ex) {
            // nop
          }
        }

        // no column privilege is supported.
        try (ResultSet resultSet =
            metaData.getColumnPrivileges(database, schema, targetTable, "C1")) {
          Assertions.assertEquals(0, super.getSizeOfResultSet(resultSet));
        }
      } finally {
        statement.execute("drop table if exists T0");
      }
    }
  }

  @Test
  public void testGetStreams() throws SQLException {
    final String targetStream = "S0";
    final String targetTable = "T0";
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      String database = con.getCatalog();
      String schema = con.getSchema();
      String owner = con.unwrap(SnowflakeConnectionV1.class).getSFBaseSession().getRole();
      String tableName = database + "." + schema + "." + targetTable;

      try {
        statement.execute("create or replace table " + targetTable + "(C1 int)");
        statement.execute("create or replace stream " + targetStream + " on table " + targetTable);

        DatabaseMetaData metaData = con.getMetaData();

        // match stream
        try (ResultSet resultSet =
            metaData.unwrap(SnowflakeDatabaseMetaData.class).getStreams(database, schema, "%")) {
          verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_STREAMS);
          Set<String> streams = new HashSet<>();
          while (resultSet.next()) {
            streams.add(resultSet.getString(1));
          }
          Assertions.assertTrue(streams.contains("S0"));
        }
        // match exact stream
        try (ResultSet resultSet =
            metaData
                .unwrap(SnowflakeDatabaseMetaData.class)
                .getStreams(database, schema, targetStream)) {
          resultSet.next();
          Assertions.assertEquals(targetStream, resultSet.getString(1));
          Assertions.assertEquals(database, resultSet.getString(2));
          Assertions.assertEquals(schema, resultSet.getString(3));
          Assertions.assertEquals(owner, resultSet.getString(4));
          Assertions.assertEquals("", resultSet.getString(5));
          Assertions.assertEquals(tableName, resultSet.getString(6));
          Assertions.assertEquals("Table", resultSet.getString(7));
          Assertions.assertEquals(tableName, resultSet.getString(8));
          Assertions.assertEquals("DELTA", resultSet.getString(9));
          Assertions.assertEquals("false", resultSet.getString(10));
          Assertions.assertEquals("DEFAULT", resultSet.getString(11));
        }
      } finally {
        statement.execute("drop table if exists " + targetTable);
        statement.execute("drop stream if exists " + targetStream);
      }
    }
  }

  /*
   * This tests that an empty resultset will be returned for getProcedures when using a reader account.
   */
  @Test
  @Disabled
  public void testGetProceduresWithReaderAccount() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metadata = connection.getMetaData();
      try (ResultSet rs = metadata.getProcedures(null, null, null)) {
        Assertions.assertEquals(0, getSizeOfResultSet(rs));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGetProcedureColumns() throws Exception {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      try {
        statement.execute(PI_PROCEDURE);
        DatabaseMetaData metaData = connection.getMetaData();
        /* Call getProcedureColumns with no parameters for procedure name or column. This should return  all procedures
        in the current database and schema. It will return all rows as well (1 row per result and 1 row per parameter
        for each procedure) */
        try (ResultSet resultSet = metaData.getProcedureColumns(database, schema, "GETPI", "%")) {
          verifyResultSetMetaDataColumns(
              resultSet, DBMetadataResultSetMetadata.GET_PROCEDURE_COLUMNS);
          resultSet.next();
          Assertions.assertEquals(database, resultSet.getString("PROCEDURE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("PROCEDURE_SCHEM"));
          Assertions.assertEquals("GETPI", resultSet.getString("PROCEDURE_NAME"));
          Assertions.assertEquals("", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(DatabaseMetaData.procedureColumnReturn, resultSet.getInt("COLUMN_TYPE"));
          Assertions.assertEquals(Types.FLOAT, resultSet.getInt("DATA_TYPE"));
          Assertions.assertEquals("FLOAT", resultSet.getString("TYPE_NAME"));
          Assertions.assertEquals(38, resultSet.getInt("PRECISION"));
          // length column is not supported and will always be 0
          Assertions.assertEquals(0, resultSet.getInt("LENGTH"));
          Assertions.assertEquals(0, resultSet.getShort("SCALE"));
          // radix column is not supported and will always be default of 10 (assumes base 10 system)
          Assertions.assertEquals(10, resultSet.getInt("RADIX"));
          // nullable column is not supported and always returns NullableUnknown
          Assertions.assertEquals(DatabaseMetaData.procedureNoNulls, resultSet.getInt("NULLABLE"));
          Assertions.assertEquals("user-defined procedure", resultSet.getString("REMARKS"));
          Assertions.assertNull(resultSet.getString("COLUMN_DEF"));
          Assertions.assertEquals(0, resultSet.getInt("SQL_DATA_TYPE"));
          Assertions.assertEquals(0, resultSet.getInt("SQL_DATETIME_SUB"));
          // char octet length column is not supported and always returns 0
          Assertions.assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
          Assertions.assertEquals(0, resultSet.getInt("ORDINAL_POSITION"));
          // is_nullable column is not supported and always returns empty string
          Assertions.assertEquals("NO", resultSet.getString("IS_NULLABLE"));
          Assertions.assertEquals("GETPI() RETURN FLOAT", resultSet.getString("SPECIFIC_NAME"));
        }

      } finally {
        statement.execute("drop procedure if exists GETPI()");
      }
    }
  }

  @Test
  public void testGetProcedureColumnsReturnsResultSet() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
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
        try (ResultSet res =
            metaData.getProcedureColumns(con.getCatalog(), null, "PROCTEST", "%")) {
          res.next();
          Assertions.assertEquals("PROCTEST", res.getString("PROCEDURE_NAME"));
          Assertions.assertEquals("id", res.getString("COLUMN_NAME"));
          Assertions.assertEquals(DatabaseMetaData.procedureColumnResult, res.getInt("COLUMN_TYPE")); // procedureColumnResult
          Assertions.assertEquals(Types.NUMERIC, res.getInt("DATA_TYPE"));
          Assertions.assertEquals("NUMBER", res.getString("TYPE_NAME"));
          Assertions.assertEquals(1, res.getInt("ORDINAL_POSITION")); // result set column 1
          res.next();
          Assertions.assertEquals("name", res.getString("COLUMN_NAME"));
          Assertions.assertEquals(DatabaseMetaData.procedureColumnResult, res.getInt("COLUMN_TYPE"));
          Assertions.assertEquals(Types.VARCHAR, res.getInt("DATA_TYPE"));
          Assertions.assertEquals("VARCHAR", res.getString("TYPE_NAME"));
          Assertions.assertEquals(2, res.getInt("ORDINAL_POSITION")); // result set column 2
          res.next();
          Assertions.assertEquals("address", res.getString("COLUMN_NAME"));
          Assertions.assertEquals(DatabaseMetaData.procedureColumnResult, res.getInt("COLUMN_TYPE"));
          Assertions.assertEquals(Types.VARCHAR, res.getInt("DATA_TYPE"));
          Assertions.assertEquals("VARCHAR", res.getString("TYPE_NAME"));
          Assertions.assertEquals(3, res.getInt("ORDINAL_POSITION")); // result set column 3
        }
      } finally {
        statement.execute("drop table if exists testtable");
      }
    }
  }

  @Test
  public void testGetProcedureColumnsReturnsValue() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement(); ) {
      DatabaseMetaData metaData = con.getMetaData();
      // create a procedure with no parameters that has a return value
      statement.execute(PI_PROCEDURE);
      try (ResultSet res = metaData.getProcedureColumns(con.getCatalog(), null, "GETPI", "%")) {
        res.next();
        Assertions.assertEquals("GETPI", res.getString("PROCEDURE_NAME"));
        Assertions.assertEquals("", res.getString("COLUMN_NAME"));
        Assertions.assertEquals(5, res.getInt("COLUMN_TYPE")); // procedureColumnReturn
        Assertions.assertEquals(Types.FLOAT, res.getInt("DATA_TYPE"));
        Assertions.assertEquals("FLOAT", res.getString("TYPE_NAME"));
        Assertions.assertEquals(0, res.getInt("ORDINAL_POSITION"));
      }

      // create a procedure that returns the value of the argument that is passed in
      statement.execute(MESSAGE_PROCEDURE);
      try (ResultSet res =
          metaData.getProcedureColumns(con.getCatalog(), null, "MESSAGE_PROC", "%")) {
        res.next();
        Assertions.assertEquals("MESSAGE_PROC", res.getString("PROCEDURE_NAME"));
        Assertions.assertEquals("", res.getString("COLUMN_NAME"));
        Assertions.assertEquals(DatabaseMetaData.procedureColumnReturn, res.getInt("COLUMN_TYPE")); // procedureColumnReturn
        Assertions.assertEquals(Types.VARCHAR, res.getInt("DATA_TYPE"));
        Assertions.assertEquals("VARCHAR", res.getString("TYPE_NAME"));
        Assertions.assertEquals(0, res.getInt("ORDINAL_POSITION"));
        res.next();
        Assertions.assertEquals("MESSAGE", res.getString("COLUMN_NAME"));
        Assertions.assertEquals(DatabaseMetaData.procedureColumnIn, res.getInt("COLUMN_TYPE")); // procedureColumnIn
        Assertions.assertEquals(Types.VARCHAR, res.getInt("DATA_TYPE"));
        Assertions.assertEquals("VARCHAR", res.getString("TYPE_NAME"));
        Assertions.assertEquals(1, res.getInt("ORDINAL_POSITION"));
      }
    }
  }

  @Test
  public void testGetProcedureColumnsReturnsNull() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement(); ) {
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
      try (ResultSet res =
          metaData.getProcedureColumns(con.getCatalog(), null, "INSERTPROC", "%")) {
        res.next();
        // the procedure will return null as the value but column type will be varchar.
        Assertions.assertEquals("INSERTPROC", res.getString("PROCEDURE_NAME"));
        Assertions.assertEquals("", res.getString("COLUMN_NAME"));
        Assertions.assertEquals(DatabaseMetaData.procedureColumnReturn, res.getInt("COLUMN_TYPE")); // procedureColumnReturn
        Assertions.assertEquals(Types.VARCHAR, res.getInt("DATA_TYPE"));
        Assertions.assertEquals("VARCHAR", res.getString("TYPE_NAME"));
        Assertions.assertEquals(0, res.getInt("ORDINAL_POSITION"));
      }
    }
  }

  @Test
  public void testUpdateLocatorsCopyUnsupported() throws SQLException {
    try (Connection con = getConnection()) {
      DatabaseMetaData metaData = con.getMetaData();
      Assertions.assertFalse(metaData.locatorsUpdateCopy());
    }
  }

  /**
   * For driver versions higher than 3.14.5 we have the enablePatternSearch connection property
   * which sets whether pattern searches are allowed for certain DatabaseMetaData queries. This test
   * sets that value to false meaning pattern searches are not allowed for getPrimaryKeys,
   * getImportedKeys, getExportedKeys, and getCrossReference.
   */
  @Test
  public void testNoPatternSearchAllowedForPrimaryAndForeignKeys() throws Exception {
    Properties properties = new Properties();
    properties.put(ENABLE_PATTERN_SEARCH, "false");
    final String table1 = "PATTERN_SEARCH_TABLE1";
    final String table2 = "PATTERN_SEARCH_TABLE2";

    try (Connection connection = getConnection(properties);
        Statement statement = connection.createStatement()) {
      String schemaRandomPart = SnowflakeUtil.randomAlphaNumeric(5);
      String schemaName =
          "\""
              + TestUtil.GENERATED_SCHEMA_PREFIX
              + "TEST_PATTERNS_SCHEMA_"
              + schemaRandomPart
              + "\"";

      TestUtil.withSchema(
          statement,
          schemaName,
          () -> {
            String schema = connection.getSchema();
            statement.execute(
                "create or replace table " + table1 + "(C1 int primary key, C2 string)");
            statement.execute(
                "create or replace table "
                    + table2
                    + "(C1 int primary key, C2 string, C3 int references "
                    + table1
                    + ")");

            DatabaseMetaData dbmd = connection.getMetaData();

            String schemaPattern1 = schema.substring(0, schema.length() - 1).concat("%");
            String schemaPattern2 = schema.substring(0, schema.length() - 1).concat("_");
            String tablePattern1 = "PATTERN_SEARCH_TABLE%";
            String tablePattern2 = "PATTERN_SEARCH_TABLE_";
            String database = connection.getCatalog();

            // Should return result for matching schema and table name
            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schema, table1)));

            // Should return an empty result if we try a pattern match on the schema
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern1, table1)));

            // Should return an empty result if we try a pattern match on the schema
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern2, table1)));

            // Should return an empty result if we try a pattern match on the schema
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern1, null)));

            // Should return an empty result if we try a pattern match on the schema
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern2, null)));

            // Should return an empty result if we try a pattern match on the table name
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schema, tablePattern1)));

            // Should return an empty result if we try a pattern match on the table name
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schema, tablePattern2)));

            // Should return an empty result if we try a pattern match on the table name
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, null, tablePattern1)));

            // Should return an empty result if we try a pattern match on the table name
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, null, tablePattern2)));

            // Should return result for matching schema and table name
            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getImportedKeys(database, schema, table2)));

            // Should return an empty result if we try a pattern match on the schema
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern1, table2)));

            // Should return an empty result if we try a pattern match on the schema
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern1, null)));

            // Should return an empty result if we try a pattern match on the schema
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern2, table2)));

            // Should return an empty result if we try a pattern match on the schema
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern2, null)));

            // Should return an empty result if we try a pattern match on the table name
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getImportedKeys(database, null, tablePattern1)));

            // Should return an empty result if we try a pattern match on the table name
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getImportedKeys(database, null, tablePattern2)));

            // Should return an empty result if we try a pattern match on the table name
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getImportedKeys(database, schema, tablePattern1)));

            // Should return an empty result if we try a pattern match on the table name
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getImportedKeys(database, schema, tablePattern2)));

            // Should return result for matching schema and table name
            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getExportedKeys(database, schema, table1)));

            // Should return an empty result if we try a pattern match on the schema
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getExportedKeys(database, schemaPattern1, table1)));

            // Should return an empty result if we try a pattern match on the table name
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getExportedKeys(database, null, tablePattern1)));

            // Should return an empty result if we try a pattern match on the table name
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getExportedKeys(database, null, tablePattern2)));

            // Should return an empty result if we try a pattern match on the table name
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getExportedKeys(database, schema, tablePattern1)));

            // Should return an empty result if we try a pattern match on the table name
            Assertions.assertEquals(0, getSizeOfResultSet(dbmd.getExportedKeys(database, schema, tablePattern2)));

            // Should return result for matching schema and table name
            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(database, schema, table1, database, schema, table2)));

            // Should return an empty result if we try a pattern match on any of the table or schema
            // names
            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schemaPattern1, table1, database, schema, table2)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schemaPattern2, table1, database, schema, table2)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schemaPattern1, null, database, schema, table2)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schemaPattern2, null, database, schema, table2)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, schemaPattern1, table2)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, schemaPattern2, table2)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, schemaPattern1, null)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, schemaPattern2, null)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, null, tablePattern1, database, schema, table2)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, null, tablePattern2, database, schema, table2)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, tablePattern1, database, schema, table2)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, tablePattern2, database, schema, table2)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, null, tablePattern1)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, null, tablePattern2)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, schema, tablePattern1)));

            Assertions.assertEquals(0, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, schema, tablePattern2)));
          });
    }
  }

  /**
   * For driver versions higher than 3.14.5 we have the enablePatternSearch connection property
   * which sets whether pattern searches are allowed for certain DatabaseMetaData queries. This test
   * uses the default setting for this property which is true.
   */
  @Test
  public void testPatternSearchAllowedForPrimaryAndForeignKeys() throws Exception {
    final String table1 = "PATTERN_SEARCH_TABLE1";
    final String table2 = "PATTERN_SEARCH_TABLE2";

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      String schemaRandomPart = SnowflakeUtil.randomAlphaNumeric(5);
      String schemaName =
          "\""
              + TestUtil.GENERATED_SCHEMA_PREFIX
              + "TEST_PATTERNS_SCHEMA_"
              + schemaRandomPart
              + "\"";

      TestUtil.withSchema(
          statement,
          schemaName,
          () -> {
            String schema = connection.getSchema();
            statement.execute(
                "create or replace table " + table1 + "(C1 int primary key, C2 string)");
            statement.execute(
                "create or replace table "
                    + table2
                    + "(C1 int primary key, C2 string, C3 int references "
                    + table1
                    + ")");

            DatabaseMetaData dbmd = connection.getMetaData();

            String schemaPattern1 = schema.substring(0, schema.length() - 1).concat("%");
            String schemaPattern2 = schema.substring(0, schema.length() - 1).concat("_");
            String tablePattern1 = "PATTERN_SEARCH_TABLE%";
            String tablePattern2 = "PATTERN_SEARCH_TABLE_";
            String database = connection.getCatalog();

            // Should return result for matching on either an exact schema and table name or a
            // pattern
            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schema, table1)));

            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern1, table1)));

            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern2, table1)));

            Assertions.assertEquals(2, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern1, null)));

            Assertions.assertEquals(2, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern2, null)));

            Assertions.assertEquals(2, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schema, tablePattern1)));

            Assertions.assertEquals(2, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schema, tablePattern2)));

            MatcherAssert.assertThat(getSizeOfResultSet(dbmd.getPrimaryKeys(database, null, tablePattern1)), greaterThanOrEqualTo(1));

            MatcherAssert.assertThat(getSizeOfResultSet(dbmd.getPrimaryKeys(database, null, tablePattern2)), greaterThanOrEqualTo(1));

            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getImportedKeys(database, schema, table2)));

            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern1, table2)));

            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern1, null)));

            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern2, table2)));

            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern2, null)));

            MatcherAssert.assertThat(getSizeOfResultSet(dbmd.getImportedKeys(database, null, tablePattern1)), greaterThanOrEqualTo(1));

            MatcherAssert.assertThat(getSizeOfResultSet(dbmd.getImportedKeys(database, null, tablePattern2)), greaterThanOrEqualTo(1));

            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getImportedKeys(database, schema, tablePattern1)));

            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getImportedKeys(database, schema, tablePattern2)));

            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getExportedKeys(database, schema, table1)));

            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getExportedKeys(database, schemaPattern1, table1)));

            MatcherAssert.assertThat(getSizeOfResultSet(dbmd.getExportedKeys(database, null, tablePattern1)), greaterThanOrEqualTo(1));

            MatcherAssert.assertThat(getSizeOfResultSet(dbmd.getExportedKeys(database, null, tablePattern2)), greaterThanOrEqualTo(1));

            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getExportedKeys(database, schema, tablePattern1)));

            Assertions.assertEquals(1, getSizeOfResultSet(dbmd.getExportedKeys(database, schema, tablePattern2)));

            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(database, schema, table1, database, schema, table2)));

            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schemaPattern1, table1, database, schema, table2)));

            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schemaPattern2, table1, database, schema, table2)));

            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schemaPattern1, null, database, schema, table2)));

            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schemaPattern2, null, database, schema, table2)));

            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, schemaPattern1, table2)));

            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, schemaPattern2, table2)));

            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, schemaPattern1, null)));

            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, schemaPattern2, null)));

            MatcherAssert.assertThat(getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, null, tablePattern1, database, schema, table2)), greaterThanOrEqualTo(1));

            MatcherAssert.assertThat(getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, null, tablePattern2, database, schema, table2)), greaterThanOrEqualTo(1));

            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, tablePattern1, database, schema, table2)));

            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, tablePattern2, database, schema, table2)));

            MatcherAssert.assertThat(getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, null, tablePattern1)), greaterThanOrEqualTo(1));

            MatcherAssert.assertThat(getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, null, tablePattern2)), greaterThanOrEqualTo(1));

            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, schema, tablePattern1)));

            Assertions.assertEquals(1, getSizeOfResultSet(
                dbmd.getCrossReference(
                    database, schema, table1, database, schema, tablePattern2)));
          });
    }
  }

  /**
   * For driver versions higher than 3.14.5 the driver reports support for JDBC 4.2. For driver
   * version 3.14.5 and earlier, the driver reports support for JDBC 1.0.
   *
   * @throws SQLException
   */
  @Test
  public void testGetJDBCVersion() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();

      // JDBC x.x compatible
      Assertions.assertEquals(4, metaData.getJDBCMajorVersion());
      Assertions.assertEquals(2, metaData.getJDBCMinorVersion());
    }
  }

  /** Added in > 3.15.1 */
  @Test
  public void testKeywordsCount() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      Assertions.assertEquals(43, metaData.getSQLKeywords().split(",").length);
    }
  }
  /** Added in > 3.16.1 */
  @Test
  public void testVectorDimension() throws SQLException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create or replace table JDBC_VECTOR(text_col varchar(32), float_vec VECTOR(FLOAT, 256), int_vec VECTOR(INT, 16))");
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet resultSet =
          metaData.getColumns(
              connection.getCatalog(),
              connection.getSchema().replaceAll("_", "\\\\_"),
              "JDBC\\_VECTOR",
              null)) {
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(32, resultSet.getObject("COLUMN_SIZE"));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(256, resultSet.getObject("COLUMN_SIZE"));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(16, resultSet.getObject("COLUMN_SIZE"));
        Assertions.assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("Select text_col, float_vec, int_vec from JDBC_VECTOR")) {
        SnowflakeResultSetMetaData unwrapResultSetMetadata =
            resultSet.getMetaData().unwrap(SnowflakeResultSetMetaData.class);
        Assertions.assertEquals(0, unwrapResultSetMetadata.getDimension("TEXT_COL"));
        Assertions.assertEquals(0, unwrapResultSetMetadata.getDimension(1));
        Assertions.assertEquals(256, unwrapResultSetMetadata.getDimension("FLOAT_VEC"));
        Assertions.assertEquals(256, unwrapResultSetMetadata.getDimension(2));
        Assertions.assertEquals(16, unwrapResultSetMetadata.getDimension("INT_VEC"));
        Assertions.assertEquals(16, unwrapResultSetMetadata.getDimension(3));
      }
    }
  }
}
