package net.snowflake.client.jdbc;

import static net.snowflake.client.TestUtil.GENERATED_SCHEMA_PREFIX;
import static net.snowflake.client.jdbc.DatabaseMetaDataIT.EXPECTED_MAX_BINARY_LENGTH;
import static net.snowflake.client.jdbc.DatabaseMetaDataIT.EXPECTED_MAX_CHAR_LENGTH;
import static net.snowflake.client.jdbc.DatabaseMetaDataIT.verifyResultSetMetaDataColumns;
import static net.snowflake.client.jdbc.SnowflakeDatabaseMetaData.NumericFunctionsSupported;
import static net.snowflake.client.jdbc.SnowflakeDatabaseMetaData.StringFunctionsSupported;
import static net.snowflake.client.jdbc.SnowflakeDatabaseMetaData.SystemFunctionsSupported;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import net.snowflake.client.TestUtil;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFSessionProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * DatabaseMetaData test for the latest JDBC driver. This doesn't work for the oldest supported
 * driver. Revisit this tests whenever bumping up the oldest supported driver to examine if the
 * tests still is not applicable. If it is applicable, move tests to DatabaseMetaDataIT so that both
 * the latest and oldest supported driver run the tests.
 */
@Tag(TestTags.OTHERS)
public class DatabaseMetaDataLatestIT extends BaseJDBCWithSharedConnectionIT {
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

  private static String startingSchema;
  private static String startingDatabase;

  @BeforeAll
  public static void prepare() {
    try {
      startingSchema = connection.getSchema();
      startingDatabase = connection.getCatalog();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** Create catalog and schema for tests with double quotes */
  public void createDoubleQuotedSchemaAndCatalog(Statement statement) throws SQLException {
    statement.execute("create or replace database \"dbwith\"\"quotes\"");
    statement.execute("create or replace schema \"dbwith\"\"quotes\".\"schemawith\"\"quotes\"");
  }

  @BeforeEach
  public void setUp() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("USE DATABASE " + startingDatabase);
      stmt.execute("USE SCHEMA " + startingSchema);
    }
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
              assertEquals(1, getSizeOfResultSet(resultSet));
            }

            try (ResultSet resultSet = databaseMetaData.getTables(null, null, null, null)) {
              assertEquals(3, getSizeOfResultSet(resultSet));
            }

            try (ResultSet resultSet = databaseMetaData.getColumns(null, null, null, null)) {
              assertEquals(13, getSizeOfResultSet(resultSet));
            }

            try (ResultSet resultSet = databaseMetaData.getPrimaryKeys(null, null, null)) {
              assertEquals(1, getSizeOfResultSet(resultSet));
            }

            try (ResultSet resultSet = databaseMetaData.getImportedKeys(null, null, null)) {
              assertEquals(1, getSizeOfResultSet(resultSet));
            }

            try (ResultSet resultSet = databaseMetaData.getExportedKeys(null, null, null)) {
              assertEquals(1, getSizeOfResultSet(resultSet));
            }

            try (ResultSet resultSet =
                databaseMetaData.getCrossReference(null, null, null, null, null, null)) {
              assertEquals(1, getSizeOfResultSet(resultSet));
            }
            // Now compare results to setting client metadata to false.
            statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=false");
            databaseMetaData = connection.getMetaData();

            try (ResultSet resultSet = databaseMetaData.getSchemas(null, null)) {
              assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));
            }

            try (ResultSet resultSet = databaseMetaData.getTables(null, null, null, null)) {
              assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(6));
            }

            try (ResultSet resultSet = databaseMetaData.getColumns(null, null, null, null)) {
              assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(26));
            }

            try (ResultSet resultSet = databaseMetaData.getPrimaryKeys(null, null, null)) {
              assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));
            }

            try (ResultSet resultSet = databaseMetaData.getImportedKeys(null, null, null)) {
              assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));
            }

            try (ResultSet resultSet = databaseMetaData.getExportedKeys(null, null, null)) {
              assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));
            }

            try (ResultSet resultSet =
                databaseMetaData.getCrossReference(null, null, null, null, null, null)) {
              assertThat(getSizeOfResultSet(resultSet), greaterThanOrEqualTo(2));
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
    try (Statement statement = connection.createStatement()) {
      String database = startingDatabase;
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
              + GENERATED_SCHEMA_PREFIX
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
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = metaData.getTables(database, querySchema, queryTable, null)) {
              // Assert 1 row returned for the testtable_"with_quotes"
              assertEquals(1, getSizeOfResultSet(rs));
            }
            try (ResultSet rs = metaData.getColumns(database, querySchema, queryTable, null)) {
              // Assert 2 rows returned for the 2 rows in testtable_"with_quotes"
              assertEquals(2, getSizeOfResultSet(rs));
            }
            try (ResultSet rs =
                metaData.getColumns(database, querySchema, queryTable, "COL\\_\"QUOTED\"")) {
              // Assert 1 row returned for the column col_"quoted"
              assertEquals(1, getSizeOfResultSet(rs));
            }
            try (ResultSet rs = metaData.getSchemas(database, querySchema)) {
              // Assert 1 row returned for the schema test_schema_"with_quotes"
              assertEquals(1, getSizeOfResultSet(rs));
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
    try (Statement statement = connection.createStatement()) {
      // Create a database with double quotes inside the database name
      statement.execute("create or replace database \"\"\"quoteddb\"\"\"");
      // Create a database, lowercase, with no double quotes inside the database name
      statement.execute("create or replace database \"unquoteddb\"");
      DatabaseMetaData metaData = connection.getMetaData();
      // Assert 2 rows returned for the PUBLIC and INFORMATION_SCHEMA schemas inside database
      try (ResultSet rs = metaData.getSchemas("\"quoteddb\"", null)) {
        assertEquals(2, getSizeOfResultSet(rs));
      }
      // Assert no results are returned when failing to put quotes around quoted database
      try (ResultSet rs = metaData.getSchemas("quoteddb", null)) {
        assertEquals(0, getSizeOfResultSet(rs));
      }
      // Assert 2 rows returned for the PUBLIC and INFORMATION_SCHEMA schemas inside database
      try (ResultSet rs = metaData.getSchemas("unquoteddb", null)) {
        assertEquals(2, getSizeOfResultSet(rs));
      }
      // Assert no rows are returned when erroneously quoting unquoted database
      try (ResultSet rs = metaData.getSchemas("\"unquoteddb\"", null)) {
        assertEquals(0, getSizeOfResultSet(rs));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testDoubleQuotedDatabaseInGetTables() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Create a database with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a table with two columns
      statement.execute(
          "create or replace table \"dbwith\"\"quotes\".\"schemawith\"\"quotes\".\"testtable\" (col1 string, col2 string)");
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet rs = metaData.getTables("dbwith\"quotes", "schemawith\"quotes", null, null)) {
        assertEquals(1, getSizeOfResultSet(rs));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testDoubleQuotedDatabaseInGetColumns() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Create a database and schema with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a table with two columns
      statement.execute(
          "create or replace table \"dbwith\"\"quotes\".\"schemawith\"\"quotes\".\"testtable\"  (col1 string, col2 string)");
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet rs = metaData.getColumns("dbwith\"quotes", "schemawith\"quotes", null, null)) {
        assertEquals(2, getSizeOfResultSet(rs));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testDoubleQuotedDatabaseforGetPrimaryKeysAndForeignKeys() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Create a database and schema with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a table with a primary key constraint
      statement.execute(
          "create or replace table \"dbwith\"\"quotes\".\"schemawith\"\"quotes\".\"test1\"  (col1 integer not null, col2 integer not null, constraint pkey_1 primary key (col1, col2) not enforced)");
      // Create a table with a foreign key constraint that points to same columns as test1's primary
      // key constraint
      statement.execute(
          "create or replace table \"dbwith\"\"quotes\".\"schemawith\"\"quotes\".\"test2\" (col_a integer not null, col_b integer not null, constraint fkey_1 foreign key (col_a, col_b) references \"test1\" (col1, col2) not enforced)");
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet rs = metaData.getPrimaryKeys("dbwith\"quotes", "schemawith\"quotes", null)) {
        // Assert 2 rows are returned for primary key constraint for table and schema with quotes
        assertEquals(2, getSizeOfResultSet(rs));
      }
      try (ResultSet rs = metaData.getImportedKeys("dbwith\"quotes", "schemawith\"quotes", null)) {
        // Assert 2 rows are returned for foreign key constraint
        assertEquals(2, getSizeOfResultSet(rs));
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
        assertEquals(2, getSizeOfResultSet(rs));
      }
      try (ResultSet rs = metaData.getImportedKeys("dbwith\"quotes", "schemawith\"quotes", null)) {
        // Assert 2 rows are returned for foreign key constraint
        assertEquals(2, getSizeOfResultSet(rs));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testDoubleQuotedDatabaseInGetProcedures() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Create a database and schema with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a procedure
      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
      statement.execute(
          "USE DATABASE \"dbwith\"\"quotes\"; USE SCHEMA \"schemawith\"\"quotes\"; " + TEST_PROC);
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet rs = metaData.getProcedures("dbwith\"quotes", null, "TESTPROC")) {
        assertEquals(1, getSizeOfResultSet(rs));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testDoubleQuotedDatabaseInGetTablePrivileges() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Create a database and schema with double quotes inside the database name
      createDoubleQuotedSchemaAndCatalog(statement);
      // Create a table under the current user and role
      statement.execute(
          "create or replace table \"dbwith\"\"quotes\".\"schemawith\"\"quotes\".\"testtable\" (col1 string, col2 string)");
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet rs = metaData.getTablePrivileges("dbwith\"quotes", null, "%")) {
        assertEquals(1, getSizeOfResultSet(rs));
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
          assertFalse(resultSet.next());
        }

        String columnSqlInjection = "%' in schema testschema; show columns like '%";
        try (ResultSet resultSet = metaData.getColumns(null, null, null, columnSqlInjection)) {
          // assert result set is empty
          assertFalse(resultSet.next());
        }

        String functionSqlInjection = "%' in account snowflake; show functions like '%";
        try (ResultSet resultSet = metaData.getColumns(null, null, null, functionSqlInjection)) {
          // assert result set is empty
          assertFalse(resultSet.next());
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
    try (Statement statement = connection.createStatement()) {
      String database = startingDatabase;
      String schemaPrefix =
          GENERATED_SCHEMA_PREFIX + SnowflakeUtil.randomAlphaNumeric(5).toUpperCase();
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
                  DatabaseMetaData metaData = connection.getMetaData();
                  try (ResultSet rs =
                      metaData.getProcedureColumns(
                          database, schemaPrefix + "SCH_", "TESTPROC", "PARAM1")) {
                    // Assert 4 rows returned for the param PARAM1 that's present in each of the 2
                    // identical stored procs in different schemas. A result row is returned for
                    // each procedure, making the total rowcount 4
                    assertEquals(4, getSizeOfResultSet(rs));
                  }
                });
          });
    }
  }

  @Test
  public void testGetFunctions() throws SQLException {
    DatabaseMetaData metadata = connection.getMetaData();
    String supportedStringFuncs = metadata.getStringFunctions();
    assertEquals(StringFunctionsSupported, supportedStringFuncs);

    String supportedNumberFuncs = metadata.getNumericFunctions();
    assertEquals(NumericFunctionsSupported, supportedNumberFuncs);

    String supportedSystemFuncs = metadata.getSystemFunctions();
    assertEquals(SystemFunctionsSupported, supportedSystemFuncs);
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
    }
  }

  @Test
  public void testGetColumnsNullable() throws Throwable {
    try (Statement statement = connection.createStatement()) {
      String database = startingDatabase;
      String schema = startingSchema;
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
        assertTrue(resultSet.next());
        assertTrue(resultSet.getBoolean("NULLABLE"));
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
          assertTrue(resultSet.next());
          assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(null, altschema2, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, null, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, altschema2, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        statement.execute("ALTER SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=false");

        metadata = connection.getMetaData();
        try (ResultSet resultSet = metadata.getColumns(null, null, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          assertTrue(resultSet.next());
          assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(null, altschema2, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, null, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          assertTrue(resultSet.next());
          assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, altschema2, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }
        statement.execute("ALTER SESSION set CLIENT_METADATA_USE_SESSION_DATABASE=false");

        metadata = connection.getMetaData();
        try (ResultSet resultSet = metadata.getColumns(null, null, "TESTTABLE_", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          assertTrue(resultSet.next());
          assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          assertTrue(resultSet.next());
          assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(null, altschema2, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, null, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          assertTrue(resultSet.next());
          assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, altschema2, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        statement.execute("ALTER SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");

        metadata = connection.getMetaData();
        try (ResultSet resultSet = metadata.getColumns(null, null, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(null, altschema2, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, null, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema1, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet = metadata.getColumns(altdb, altschema2, "%", "COLA")) {
          assertTrue(resultSet.next());
          assertEquals(altschema2, resultSet.getString("TABLE_SCHEM"));
          assertFalse(resultSet.next());
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
    try (Statement statement = connection.createStatement()) {
      String database = startingDatabase;
      String schema = startingSchema;

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
      }

      /* Look at resultSet from calling getFunctionColumns on FUNC112 */
      try (ResultSet resultSet = metaData.getFunctionColumns(database, schema, "FUNC112", "%")) {
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
        assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
        // is_nullable column is not supported and always returns empty string
        assertEquals("", resultSet.getString("IS_NULLABLE"));
        assertThat(
            "Columns metadata SPECIFIC_NAME should contains expected columns ",
            resultSet
                .getString("SPECIFIC_NAME")
                .replaceAll("\\s", "")
                .matches(
                    "^FUNC112.*RETURNTABLE.*COLAVARCHAR.*,COLBNUMBER,BIN2BINARY.*,SHAREDCOLNUMBER.?$"));
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
        assertThat(
            "Columns metadata SPECIFIC_NAME should contains expected columns ",
            resultSet
                .getString("SPECIFIC_NAME")
                .replaceAll("\\s", "")
                .matches(
                    "^FUNC112.*RETURNTABLE.*COLAVARCHAR.*,COLBNUMBER,BIN2BINARY.*,SHAREDCOLNUMBER.?$"));
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
        assertEquals(3, resultSet.getInt("ORDINAL_POSITION"));
        // is_nullable column is not supported and always returns empty string
        assertEquals("", resultSet.getString("IS_NULLABLE"));
        assertThat(
            "Columns metadata SPECIFIC_NAME should contains expected columns ",
            resultSet
                .getString("SPECIFIC_NAME")
                .replaceAll("\\s", "")
                .matches(
                    "^FUNC112.*RETURNTABLE.*COLAVARCHAR.*,COLBNUMBER,BIN2BINARY.*,SHAREDCOLNUMBER.?$"));
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
        assertThat(
            "Columns metadata SPECIFIC_NAME should contains expected columns ",
            resultSet
                .getString("SPECIFIC_NAME")
                .replaceAll("\\s", "")
                .matches(
                    "^FUNC112.*RETURNTABLE.*COLAVARCHAR.*,COLBNUMBER,BIN2BINARY.*,SHAREDCOLNUMBER.?$"));
        assertFalse(resultSet.next());
      }

      /* Assert that calling getFunctionColumns with no parameters returns empty result set */
      try (ResultSet resultSet = metaData.getFunctionColumns("%", "%", "%", "%")) {
        assertFalse(resultSet.next());
      }

      /* Look at result set from calling getFunctionColumns on total_rows_in_table */
      try (ResultSet resultSet =
          metaData.getFunctionColumns(database, schema, "total_rows_in_table%", "%")) {
        /* Assert there are 17 columns in result set and 1 row */
        assertEquals(17, resultSet.getMetaData().getColumnCount());
        assertEquals(1, getSizeOfResultSet(resultSet));
      }

      // getSizeofResultSet will mess up the row index of resultSet
      try (ResultSet resultSet =
          metaData.getFunctionColumns(database, schema, "total_rows_in_table%", "%")) {
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
  }

  @Test
  public void testHandlingSpecialChars() throws Exception {
    try (Statement statement = connection.createStatement()) {
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
          "\"" + GENERATED_SCHEMA_PREFIX + "SPECIAL%_\\SCHEMA" + specialSchemaSuffix + "\"";
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
              assertTrue(resultSet.next());
              assertEquals("C%1", resultSet.getString("COLUMN_NAME"));
              assertTrue(resultSet.next());
              assertEquals("C\\1\\\\11", resultSet.getString("COLUMN_NAME"));
              assertFalse(resultSet.next());
            }

            // Underscore can match to any character, so check that table comes back when underscore
            // is not escaped.
            String partiallyEscapedTable1 = "TEST" + escapeChar + "\\1" + escapeChar + "\\_1";
            try (ResultSet resultSet =
                metaData.getColumns(database, schema, partiallyEscapedTable1, null)) {
              assertTrue(resultSet.next());
              assertEquals("C%1", resultSet.getString("COLUMN_NAME"));
              assertTrue(resultSet.next());
              assertEquals("C\\1\\\\11", resultSet.getString("COLUMN_NAME"));
              assertFalse(resultSet.next());
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
        assertTrue(resultSet.next());
        assertEquals("COLA", resultSet.getString("COLUMN_NAME"));
        assertTrue(resultSet.next());
        assertEquals("COLB", resultSet.getString("COLUMN_NAME"));
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet = metaData.getColumns(database, schema, escapedTable, "COLB")) {
        assertTrue(resultSet.next());
        assertEquals("COLB", resultSet.getString("COLUMN_NAME"));
        assertFalse(resultSet.next());
      }

      statement.execute("create or replace table " + schema + ".\"special%table\" (colA string)");
      try (ResultSet resultSet =
          metaData.getColumns(database, schema, "special" + escapeChar + "%table", null)) {
        assertTrue(resultSet.next());
        assertEquals("COLA", resultSet.getString("COLUMN_NAME"));
      }
    }
  }

  @Test
  public void testUnderscoreInSchemaNamePatternForPrimaryAndForeignKeys() throws Exception {
    try (Statement statement = connection.createStatement()) {
      String database = startingDatabase;
      TestUtil.withRandomSchema(
          statement,
          customSchema -> {
            String escapedSchema = customSchema.replace("_", "\\_");
            statement.execute("use schema " + customSchema);
            statement.execute(
                "create or replace table PK_TEST (c1 int PRIMARY KEY, c2 VARCHAR(10))");
            statement.execute(
                "create or replace table FK_TEST (c1 int REFERENCES PK_TEST(c1), c2 VARCHAR(10))");
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = metaData.getPrimaryKeys(database, escapedSchema, null)) {
              assertEquals(1, getSizeOfResultSet(rs));
            }
            try (ResultSet rs = metaData.getImportedKeys(database, escapedSchema, null)) {
              assertEquals(1, getSizeOfResultSet(rs));
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
              assertEquals(0, getSizeOfResultSet(rs));
            }
            try (ResultSet rs = metaData.getImportedKeys(database, escapedSchema, null)) {
              assertEquals(0, getSizeOfResultSet(rs));
            }
            // We expect the results to be returned if we use the actual schema name
            try (ResultSet rs = metaData.getPrimaryKeys(database, customSchema, null)) {
              assertEquals(1, getSizeOfResultSet(rs));
            }
            try (ResultSet rs = metaData.getImportedKeys(database, customSchema, null)) {
              assertEquals(1, getSizeOfResultSet(rs));
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
        assertEquals(resultSet.getObject("DATA_TYPE"), 2014);
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
        assertEquals(resultSet.getObject("DATA_TYPE"), Types.TIMESTAMP);
      }
    }
  }

  @Test
  public void testGetColumns() throws Throwable {
    try (Statement statement = connection.createStatement()) {
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
          assertEquals(EXPECTED_MAX_CHAR_LENGTH, resultSet.getInt("COLUMN_SIZE"));
          assertEquals(0, resultSet.getInt("DECIMAL_DIGITS"));
          assertEquals(0, resultSet.getInt("NUM_PREC_RADIX"));
          assertEquals(ResultSetMetaData.columnNullable, resultSet.getInt("NULLABLE"));
          assertEquals("", resultSet.getString("REMARKS"));
          assertEquals("", resultSet.getString("COLUMN_DEF"));

          assertEquals(EXPECTED_MAX_CHAR_LENGTH, resultSet.getInt("CHAR_OCTET_LENGTH"));
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
          assertEquals(EXPECTED_MAX_BINARY_LENGTH, resultSet.getInt("COLUMN_SIZE"));
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
        }
        statement.execute(
            "create or replace table "
                + targetTable
                + "(C1 string, C2 string default '', C3 string default 'apples', C4 string"
                + " default '\"apples\"', C5 int, C6 int default 5, C7 string default '''', C8"
                + " string default '''apples''''', C9  string default '%')");

        metaData = connection.getMetaData();

        try (ResultSet resultSet = metaData.getColumns(database, schema, targetTable, "%")) {
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
          assertThrows(SQLException.class, () -> resultSet.getString("INVALID_COLUMN"));
        }

        // no column privilege is supported.
        try (ResultSet resultSet =
            metaData.getColumnPrivileges(database, schema, targetTable, "C1")) {
          assertEquals(0, super.getSizeOfResultSet(resultSet));
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
    try (Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      String owner = connection.unwrap(SnowflakeConnectionV1.class).getSFBaseSession().getRole();
      String tableName = database + "." + schema + "." + targetTable;

      try {
        statement.execute("create or replace table " + targetTable + "(C1 int)");
        statement.execute("create or replace stream " + targetStream + " on table " + targetTable);

        DatabaseMetaData metaData = connection.getMetaData();

        // match stream
        try (ResultSet resultSet =
            metaData.unwrap(SnowflakeDatabaseMetaData.class).getStreams(database, schema, "%")) {
          verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_STREAMS);
          Set<String> streams = new HashSet<>();
          while (resultSet.next()) {
            streams.add(resultSet.getString(1));
          }
          assertTrue(streams.contains("S0"));
        }
        // match exact stream
        try (ResultSet resultSet =
            metaData
                .unwrap(SnowflakeDatabaseMetaData.class)
                .getStreams(database, schema, targetStream)) {
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
    DatabaseMetaData metadata = connection.getMetaData();
    try (ResultSet rs = metadata.getProcedures(null, null, null)) {
      assertEquals(0, getSizeOfResultSet(rs));
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGetProcedureColumns() throws Exception {
    try (Statement statement = connection.createStatement()) {
      String database = startingDatabase;
      String schema = startingSchema;
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
        }

      } finally {
        statement.execute("drop procedure if exists GETPI()");
      }
    }
  }

  @Test
  public void testGetProcedureColumnsReturnsResultSet() throws SQLException {
    try (Statement statement = connection.createStatement()) {
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
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet res =
            metaData.getProcedureColumns(connection.getCatalog(), null, "PROCTEST", "%")) {
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
        }
      } finally {
        statement.execute("drop table if exists testtable");
      }
    }
  }

  @Test
  public void testGetProcedureColumnsReturnsValue() throws SQLException {
    try (Statement statement = connection.createStatement(); ) {
      DatabaseMetaData metaData = connection.getMetaData();
      // create a procedure with no parameters that has a return value
      statement.execute(PI_PROCEDURE);
      try (ResultSet res =
          metaData.getProcedureColumns(connection.getCatalog(), null, "GETPI", "%")) {
        res.next();
        assertEquals("GETPI", res.getString("PROCEDURE_NAME"));
        assertEquals("", res.getString("COLUMN_NAME"));
        assertEquals(5, res.getInt("COLUMN_TYPE")); // procedureColumnReturn
        assertEquals(Types.FLOAT, res.getInt("DATA_TYPE"));
        assertEquals("FLOAT", res.getString("TYPE_NAME"));
        assertEquals(0, res.getInt("ORDINAL_POSITION"));
      }

      // create a procedure that returns the value of the argument that is passed in
      statement.execute(MESSAGE_PROCEDURE);
      try (ResultSet res =
          metaData.getProcedureColumns(connection.getCatalog(), null, "MESSAGE_PROC", "%")) {
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
      }
    }
  }

  @Test
  public void testGetProcedureColumnsReturnsNull() throws SQLException {
    try (Statement statement = connection.createStatement(); ) {
      DatabaseMetaData metaData = connection.getMetaData();
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
          metaData.getProcedureColumns(connection.getCatalog(), null, "INSERTPROC", "%")) {
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
  }

  @Test
  public void testUpdateLocatorsCopyUnsupported() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    assertFalse(metaData.locatorsUpdateCopy());
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
          "\"" + GENERATED_SCHEMA_PREFIX + "TEST_PATTERNS_SCHEMA_" + schemaRandomPart + "\"";

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
            assertEquals(1, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schema, table1)));

            // Should return an empty result if we try a pattern match on the schema
            assertEquals(
                0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern1, table1)));

            // Should return an empty result if we try a pattern match on the schema
            assertEquals(
                0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern2, table1)));

            // Should return an empty result if we try a pattern match on the schema
            assertEquals(
                0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern1, null)));

            // Should return an empty result if we try a pattern match on the schema
            assertEquals(
                0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern2, null)));

            // Should return an empty result if we try a pattern match on the table name
            assertEquals(
                0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schema, tablePattern1)));

            // Should return an empty result if we try a pattern match on the table name
            assertEquals(
                0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schema, tablePattern2)));

            // Should return an empty result if we try a pattern match on the table name
            assertEquals(0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, null, tablePattern1)));

            // Should return an empty result if we try a pattern match on the table name
            assertEquals(0, getSizeOfResultSet(dbmd.getPrimaryKeys(database, null, tablePattern2)));

            // Should return result for matching schema and table name
            assertEquals(1, getSizeOfResultSet(dbmd.getImportedKeys(database, schema, table2)));

            // Should return an empty result if we try a pattern match on the schema
            assertEquals(
                0, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern1, table2)));

            // Should return an empty result if we try a pattern match on the schema
            assertEquals(
                0, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern1, null)));

            // Should return an empty result if we try a pattern match on the schema
            assertEquals(
                0, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern2, table2)));

            // Should return an empty result if we try a pattern match on the schema
            assertEquals(
                0, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern2, null)));

            // Should return an empty result if we try a pattern match on the table name
            assertEquals(
                0, getSizeOfResultSet(dbmd.getImportedKeys(database, null, tablePattern1)));

            // Should return an empty result if we try a pattern match on the table name
            assertEquals(
                0, getSizeOfResultSet(dbmd.getImportedKeys(database, null, tablePattern2)));

            // Should return an empty result if we try a pattern match on the table name
            assertEquals(
                0, getSizeOfResultSet(dbmd.getImportedKeys(database, schema, tablePattern1)));

            // Should return an empty result if we try a pattern match on the table name
            assertEquals(
                0, getSizeOfResultSet(dbmd.getImportedKeys(database, schema, tablePattern2)));

            // Should return result for matching schema and table name
            assertEquals(1, getSizeOfResultSet(dbmd.getExportedKeys(database, schema, table1)));

            // Should return an empty result if we try a pattern match on the schema
            assertEquals(
                0, getSizeOfResultSet(dbmd.getExportedKeys(database, schemaPattern1, table1)));

            // Should return an empty result if we try a pattern match on the table name
            assertEquals(
                0, getSizeOfResultSet(dbmd.getExportedKeys(database, null, tablePattern1)));

            // Should return an empty result if we try a pattern match on the table name
            assertEquals(
                0, getSizeOfResultSet(dbmd.getExportedKeys(database, null, tablePattern2)));

            // Should return an empty result if we try a pattern match on the table name
            assertEquals(
                0, getSizeOfResultSet(dbmd.getExportedKeys(database, schema, tablePattern1)));

            // Should return an empty result if we try a pattern match on the table name
            assertEquals(
                0, getSizeOfResultSet(dbmd.getExportedKeys(database, schema, tablePattern2)));

            // Should return result for matching schema and table name
            assertEquals(
                1,
                getSizeOfResultSet(
                    dbmd.getCrossReference(database, schema, table1, database, schema, table2)));

            // Should return an empty result if we try a pattern match on any of the table or schema
            // names
            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schemaPattern1, table1, database, schema, table2)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schemaPattern2, table1, database, schema, table2)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schemaPattern1, null, database, schema, table2)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schemaPattern2, null, database, schema, table2)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, schemaPattern1, table2)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, schemaPattern2, table2)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, schemaPattern1, null)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, schemaPattern2, null)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, null, tablePattern1, database, schema, table2)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, null, tablePattern2, database, schema, table2)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, tablePattern1, database, schema, table2)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, tablePattern2, database, schema, table2)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, null, tablePattern1)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, null, tablePattern2)));

            assertEquals(
                0,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, schema, tablePattern1)));

            assertEquals(
                0,
                getSizeOfResultSet(
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

    try (Statement statement = connection.createStatement()) {
      String schemaRandomPart = SnowflakeUtil.randomAlphaNumeric(5);
      String schemaName =
          "\"" + GENERATED_SCHEMA_PREFIX + "TEST_PATTERNS_SCHEMA_" + schemaRandomPart + "\"";

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
            assertEquals(1, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schema, table1)));

            assertEquals(
                1, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern1, table1)));

            assertEquals(
                1, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern2, table1)));

            assertEquals(
                2, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern1, null)));

            assertEquals(
                2, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schemaPattern2, null)));

            assertEquals(
                2, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schema, tablePattern1)));

            assertEquals(
                2, getSizeOfResultSet(dbmd.getPrimaryKeys(database, schema, tablePattern2)));

            assertThat(
                getSizeOfResultSet(dbmd.getPrimaryKeys(database, null, tablePattern1)),
                greaterThanOrEqualTo(1));

            assertThat(
                getSizeOfResultSet(dbmd.getPrimaryKeys(database, null, tablePattern2)),
                greaterThanOrEqualTo(1));

            assertEquals(1, getSizeOfResultSet(dbmd.getImportedKeys(database, schema, table2)));

            assertEquals(
                1, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern1, table2)));

            assertEquals(
                1, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern1, null)));

            assertEquals(
                1, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern2, table2)));

            assertEquals(
                1, getSizeOfResultSet(dbmd.getImportedKeys(database, schemaPattern2, null)));

            assertThat(
                getSizeOfResultSet(dbmd.getImportedKeys(database, null, tablePattern1)),
                greaterThanOrEqualTo(1));

            assertThat(
                getSizeOfResultSet(dbmd.getImportedKeys(database, null, tablePattern2)),
                greaterThanOrEqualTo(1));

            assertEquals(
                1, getSizeOfResultSet(dbmd.getImportedKeys(database, schema, tablePattern1)));

            assertEquals(
                1, getSizeOfResultSet(dbmd.getImportedKeys(database, schema, tablePattern2)));

            assertEquals(1, getSizeOfResultSet(dbmd.getExportedKeys(database, schema, table1)));

            assertEquals(
                1, getSizeOfResultSet(dbmd.getExportedKeys(database, schemaPattern1, table1)));

            assertThat(
                getSizeOfResultSet(dbmd.getExportedKeys(database, null, tablePattern1)),
                greaterThanOrEqualTo(1));

            assertThat(
                getSizeOfResultSet(dbmd.getExportedKeys(database, null, tablePattern2)),
                greaterThanOrEqualTo(1));

            assertEquals(
                1, getSizeOfResultSet(dbmd.getExportedKeys(database, schema, tablePattern1)));

            assertEquals(
                1, getSizeOfResultSet(dbmd.getExportedKeys(database, schema, tablePattern2)));

            assertEquals(
                1,
                getSizeOfResultSet(
                    dbmd.getCrossReference(database, schema, table1, database, schema, table2)));

            assertEquals(
                1,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schemaPattern1, table1, database, schema, table2)));

            assertEquals(
                1,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schemaPattern2, table1, database, schema, table2)));

            assertEquals(
                1,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schemaPattern1, null, database, schema, table2)));

            assertEquals(
                1,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schemaPattern2, null, database, schema, table2)));

            assertEquals(
                1,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, schemaPattern1, table2)));

            assertEquals(
                1,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, schemaPattern2, table2)));

            assertEquals(
                1,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, schemaPattern1, null)));

            assertEquals(
                1,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, schemaPattern2, null)));

            assertThat(
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, null, tablePattern1, database, schema, table2)),
                greaterThanOrEqualTo(1));

            assertThat(
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, null, tablePattern2, database, schema, table2)),
                greaterThanOrEqualTo(1));

            assertEquals(
                1,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, tablePattern1, database, schema, table2)));

            assertEquals(
                1,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, tablePattern2, database, schema, table2)));

            assertThat(
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, null, tablePattern1)),
                greaterThanOrEqualTo(1));

            assertThat(
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, null, tablePattern2)),
                greaterThanOrEqualTo(1));

            assertEquals(
                1,
                getSizeOfResultSet(
                    dbmd.getCrossReference(
                        database, schema, table1, database, schema, tablePattern1)));

            assertEquals(
                1,
                getSizeOfResultSet(
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
    DatabaseMetaData metaData = connection.getMetaData();

    // JDBC x.x compatible
    assertEquals(4, metaData.getJDBCMajorVersion());
    assertEquals(2, metaData.getJDBCMinorVersion());
  }

  /** Added in > 3.15.1 */
  @Test
  public void testKeywordsCount() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    assertEquals(43, metaData.getSQLKeywords().split(",").length);
  }

  /** Added in > 3.16.1 */
  @Test
  public void testVectorDimension() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "create or replace table JDBC_VECTOR(text_col varchar(32), float_vec VECTOR(FLOAT, 256), int_vec VECTOR(INT, 16))");
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet resultSet =
          metaData.getColumns(
              connection.getCatalog(),
              connection.getSchema().replaceAll("_", "\\\\_"),
              "JDBC\\_VECTOR",
              null)) {
        assertTrue(resultSet.next());
        assertEquals(32, resultSet.getObject("COLUMN_SIZE"));
        assertTrue(resultSet.next());
        assertEquals(256, resultSet.getObject("COLUMN_SIZE"));
        assertTrue(resultSet.next());
        assertEquals(16, resultSet.getObject("COLUMN_SIZE"));
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("Select text_col, float_vec, int_vec from JDBC_VECTOR")) {
        SnowflakeResultSetMetaData unwrapResultSetMetadata =
            resultSet.getMetaData().unwrap(SnowflakeResultSetMetaData.class);
        assertEquals(0, unwrapResultSetMetadata.getDimension("TEXT_COL"));
        assertEquals(0, unwrapResultSetMetadata.getDimension(1));
        assertEquals(256, unwrapResultSetMetadata.getDimension("FLOAT_VEC"));
        assertEquals(256, unwrapResultSetMetadata.getDimension(2));
        assertEquals(16, unwrapResultSetMetadata.getDimension("INT_VEC"));
        assertEquals(16, unwrapResultSetMetadata.getDimension(3));
      }
    }
  }

  /**
   * Added in > 3.21.0 for SNOW-1619625 - we need to use exact schema when
   * CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = true
   */
  @Test
  public void testExactSchemaSearching() throws SQLException {
    Random random = new Random();
    int suffix = random.nextInt(Integer.MAX_VALUE);
    // schemas created in tests must start with GENERATED_SCHEMA_PREFIX value
    String coreSchemaName = "ESCAPED_UNDERSCORE%" + suffix;
    String schemaName = GENERATED_SCHEMA_PREFIX + coreSchemaName;
    String alternativeSchemaName =
        GENERATED_SCHEMA_PREFIX + coreSchemaName.replaceAll("_", "x").replaceAll("%", "y");
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      for (String scm : Arrays.asList(schemaName, alternativeSchemaName)) {
        statement.execute("CREATE SCHEMA \"" + scm + "\"");
        statement.execute(
            "CREATE TABLE \""
                + connection.getCatalog()
                + "\".\""
                + scm
                + "\".\"mytable1\" (a text)");
        statement.execute(
            "CREATE TABLE \""
                + connection.getCatalog()
                + "\".\""
                + scm
                + "\".\"mytable2\" (a text)");

        statement.execute(
            "ALTER TABLE \""
                + connection.getCatalog()
                + "\".\""
                + scm
                + "\".\"mytable2\""
                + " ADD CONSTRAINT pk_1 PRIMARY KEY (a);");
        statement.execute(
            "ALTER TABLE \""
                + connection.getCatalog()
                + "\".\""
                + scm
                + "\".\"mytable1\""
                + " ADD CONSTRAINT fk_1 FOREIGN KEY (a) REFERENCES \""
                + connection.getCatalog()
                + "\".\""
                + scm
                + "\".\"mytable2\""
                + " (a);");
        statement.execute(
            "create or replace function \""
                + connection.getCatalog()
                + "\".\""
                + scm
                + "\".\"myfun1\""
                + "(a number, b number) RETURNS NUMBER as 'a*b'");
        statement.execute(
            "create or replace procedure \""
                + connection.getCatalog()
                + "\".\""
                + scm
                + "\".\"myproc1\""
                + "(a varchar, b varchar)\n"
                + "    returns varchar not null\n"
                + "    language javascript\n"
                + "    as\n"
                + "    $$\n"
                + "    return a +b;\n"
                + "    $$\n"
                + "    ;");
        statement.execute(
            "create or replace stream \""
                + connection.getCatalog()
                + "\".\""
                + scm
                + "\".\"stream1\""
                + " on table \""
                + connection.getCatalog()
                + "\".\""
                + scm
                + "\".\"mytable1\"");
      }

      String schemaNameColumnInMetadata = "TABLE_SCHEM"; // it's not typo

      try {
        Properties props = new Properties();
        props.put("CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX", true);
        props.put("ENABLE_EXACT_SCHEMA_SEARCH_ENABLED", true);
        props.put("schema", schemaName); // we should not escape schema here
        try (Connection connectionWithContext = getConnection(props)) {
          try (ResultSet schemas = connectionWithContext.getMetaData().getSchemas()) {
            assertEquals(schemaName, connectionWithContext.getSchema());
            assertTrue(schemas.next());
            assertEquals(schemaName, schemas.getString(schemaNameColumnInMetadata));
            assertFalse(schemas.next());
          }

          try (ResultSet schemasWithPattern =
              connectionWithContext.getMetaData().getSchemas(null, null)) {
            assertTrue(schemasWithPattern.next());
            assertEquals(schemaName, schemasWithPattern.getString(schemaNameColumnInMetadata));
            assertFalse(schemasWithPattern.next());
          }

          try (ResultSet tables =
              connectionWithContext.getMetaData().getTables(null, null, null, null)) {
            assertTrue(tables.next());
            assertEquals("mytable1", tables.getString("TABLE_NAME"));
            assertEquals(schemaName, tables.getString(schemaNameColumnInMetadata));
            assertTrue(tables.next());
            assertEquals("mytable2", tables.getString("TABLE_NAME"));
            assertEquals(schemaName, tables.getString(schemaNameColumnInMetadata));
            assertFalse(tables.next());
          }

          try (ResultSet tablePrivileges =
              connectionWithContext.getMetaData().getTablePrivileges(null, null, "mytable1")) {
            assertTrue(tablePrivileges.next());
            assertEquals("mytable1", tablePrivileges.getString("TABLE_NAME"));
            assertEquals(schemaName, tablePrivileges.getString(schemaNameColumnInMetadata));
            assertFalse(tablePrivileges.next());
          }

          try (ResultSet columns =
              connectionWithContext.getMetaData().getColumns(null, null, null, null)) {
            assertTrue(columns.next());
            assertEquals("mytable1", columns.getString("TABLE_NAME"));
            assertEquals(schemaName, columns.getString(schemaNameColumnInMetadata));
            assertEquals("A", columns.getString("COLUMN_NAME"));
            assertTrue(columns.next());
            assertEquals("mytable2", columns.getString("TABLE_NAME"));
            assertEquals(schemaName, columns.getString(schemaNameColumnInMetadata));
            assertEquals("A", columns.getString("COLUMN_NAME"));
            assertFalse(columns.next());
          }

          try (ResultSet primaryKeys =
              connectionWithContext.getMetaData().getPrimaryKeys(null, null, null)) {
            assertTrue(primaryKeys.next());
            assertEquals("mytable2", primaryKeys.getString("TABLE_NAME"));
            assertEquals(schemaName, primaryKeys.getString(schemaNameColumnInMetadata));
            assertEquals("A", primaryKeys.getString("COLUMN_NAME"));
            assertFalse(primaryKeys.next());
          }

          try (ResultSet importedKeys =
              connectionWithContext.getMetaData().getImportedKeys(null, null, null)) {
            assertTrue(importedKeys.next());
            assertEquals("mytable2", importedKeys.getString("PKTABLE_NAME"));
            assertEquals(schemaName, importedKeys.getString("PKTABLE_SCHEM"));
            assertEquals("A", importedKeys.getString("PKCOLUMN_NAME"));
            assertEquals("mytable1", importedKeys.getString("FKTABLE_NAME"));
            assertEquals(schemaName, importedKeys.getString("FKTABLE_SCHEM"));
            assertEquals("A", importedKeys.getString("FKCOLUMN_NAME"));
            assertFalse(importedKeys.next());
          }

          try (ResultSet exportedKeys =
              connectionWithContext.getMetaData().getExportedKeys(null, null, null)) {
            assertTrue(exportedKeys.next());
            assertEquals("mytable2", exportedKeys.getString("PKTABLE_NAME"));
            assertEquals(schemaName, exportedKeys.getString("PKTABLE_SCHEM"));
            assertEquals("A", exportedKeys.getString("PKCOLUMN_NAME"));
            assertEquals("mytable1", exportedKeys.getString("FKTABLE_NAME"));
            assertEquals(schemaName, exportedKeys.getString("FKTABLE_SCHEM"));
            assertEquals("A", exportedKeys.getString("FKCOLUMN_NAME"));
            assertFalse(exportedKeys.next());
          }

          try (ResultSet crossReferences =
              connectionWithContext
                  .getMetaData()
                  .getCrossReference(null, null, null, null, null, null)) {
            assertTrue(crossReferences.next());
            assertEquals("mytable2", crossReferences.getString("PKTABLE_NAME"));
            assertEquals(schemaName, crossReferences.getString("PKTABLE_SCHEM"));
            assertEquals("A", crossReferences.getString("PKCOLUMN_NAME"));
            assertEquals("mytable1", crossReferences.getString("FKTABLE_NAME"));
            assertEquals(schemaName, crossReferences.getString("FKTABLE_SCHEM"));
            assertEquals("A", crossReferences.getString("FKCOLUMN_NAME"));
            assertFalse(crossReferences.next());
          }

          try (ResultSet functions =
              connectionWithContext.getMetaData().getFunctions(null, null, "myfun%")) {
            assertTrue(functions.next());
            assertEquals("myfun1", functions.getString("FUNCTION_NAME"));
            // schema name contains % so it is returned in ""
            assertEquals('"' + schemaName + '"', functions.getString("FUNCTION_SCHEM"));
            assertFalse(functions.next());
          }

          try (ResultSet functionColumns =
              connectionWithContext.getMetaData().getFunctionColumns(null, null, "myfun%", null)) {
            assertTrue(functionColumns.next());
            assertEquals("myfun1", functionColumns.getString("FUNCTION_NAME"));
            // schema name is empty when getFunctionColumns schema is empty
            assertEquals(null, functionColumns.getString("FUNCTION_SCHEM"));
            // first parameter is an empty string
            assertEquals("", functionColumns.getString("COLUMN_NAME"));
            assertTrue(functionColumns.next());
            // schema name is empty when getFunctionColumns schema is empty
            assertEquals(null, functionColumns.getString("FUNCTION_SCHEM"));
            assertEquals("A", functionColumns.getString("COLUMN_NAME"));
            assertTrue(functionColumns.next());
            assertEquals("myfun1", functionColumns.getString("FUNCTION_NAME"));
            // schema name is empty when getFunctionColumns schema is empty
            assertEquals(null, functionColumns.getString("FUNCTION_SCHEM"));
            assertEquals("B", functionColumns.getString("COLUMN_NAME"));
            assertFalse(functionColumns.next());
          }

          try (ResultSet procedures =
              connectionWithContext.getMetaData().getProcedures(null, null, "myproc%")) {
            assertTrue(procedures.next());
            // schema name contains % so it is returned in ""
            assertEquals('"' + schemaName + '"', procedures.getString("PROCEDURE_SCHEM"));
            assertEquals("myproc1", procedures.getString("PROCEDURE_NAME"));
            assertFalse(procedures.next());
          }

          try (ResultSet procedureColumns =
              connectionWithContext
                  .getMetaData()
                  .getProcedureColumns(null, null, "myproc%", null)) {
            assertTrue(procedureColumns.next());
            assertEquals("myproc1", procedureColumns.getString("PROCEDURE_NAME"));
            // schema name contains % so it is returned in ""
            assertEquals('"' + schemaName + '"', procedureColumns.getString("PROCEDURE_SCHEM"));
            // first parameter is an empty string
            assertEquals("", procedureColumns.getString("COLUMN_NAME"));
            assertTrue(procedureColumns.next());
            assertEquals('"' + schemaName + '"', procedureColumns.getString("PROCEDURE_SCHEM"));
            assertEquals("A", procedureColumns.getString("COLUMN_NAME"));
            assertTrue(procedureColumns.next());
            assertEquals("myproc1", procedureColumns.getString("PROCEDURE_NAME"));
            assertEquals('"' + schemaName + '"', procedureColumns.getString("PROCEDURE_SCHEM"));
            assertEquals("B", procedureColumns.getString("COLUMN_NAME"));
            assertFalse(procedureColumns.next());
          }

          try (ResultSet streams =
              connectionWithContext
                  .getMetaData()
                  .unwrap(SnowflakeDatabaseMetaData.class)
                  .getStreams(null, null, null)) {
            assertTrue(streams.next());
            // here table name is full qualified name, schema name contains % so it is returned in
            // "", table name is always in ""
            assertEquals(
                connection.getCatalog() + ".\"" + schemaName + "\".\"mytable1\"",
                streams.getString("TABLE_NAME"));
            // here schema name does not contain "" but maybe should
            assertEquals(schemaName, streams.getString("SCHEMA_NAME"));
            assertEquals("stream1", streams.getString("STREAM_NAME"));
            assertFalse(streams.next());
          }

          // not supported:
          // getAttributes
          // getBestRowIdentifier
          // getColumnPrivileges
          // getIndexInfo
          // getPseudoColumns
          // getSuperTypes
          // getSuperTables
          // getVersionColumns
          // getUDTs
        }
      } finally {
        for (String scm : Arrays.asList(schemaName, alternativeSchemaName)) {
          statement.execute("DROP SCHEMA \"" + scm + "\"");
        }
      }
    }
  }
}
