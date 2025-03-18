package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.DatabaseMetaDataIT.EXPECTED_MAX_BINARY_LENGTH;
import static net.snowflake.client.jdbc.DatabaseMetaDataIT.verifyResultSetMetaDataColumns;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Database Metadata IT */
@Tag(TestTags.OTHERS)
public class DatabaseMetaDataInternalIT extends BaseJDBCTest {
  private Connection connection;
  private Statement statement;
  private DatabaseMetaData databaseMetaData;
  private ResultSet resultSet;

  @BeforeEach
  public void setUp() throws SQLException {
    try (Connection con = getConnection()) {
      initMetaData(con);
    }
  }

  static void initMetaData(Connection con) throws SQLException {
    try (Statement st = con.createStatement()) {

      st.execute("create or replace database JDBC_DB1");
      st.execute("create or replace schema JDBC_SCHEMA11");
      st.execute("create or replace table JDBC_TBL111(colA string, colB decimal, colC timestamp)");
      st.execute("create or replace schema TEST_CTX");
      st.execute(
          "create or replace table JDBC_A (colA string, colB decimal, "
              + "colC number PRIMARY KEY);");
      st.execute(
          "create or replace table JDBC_B (colA string, colB decimal, "
              + "colC number FOREIGN KEY REFERENCES JDBC_A(colC));");
      st.execute("create or replace schema JDBC_SCHEMA12");
      st.execute("create or replace table JDBC_TBL121(colA varchar)");
      st.execute(
          "create or replace table JDBC_TBL122(colA NUMBER(20, 2) AUTOINCREMENT comment 'cmt"
              + " colA', colB NUMBER(20, 2) DEFAULT(3) NOT NULL, colC NUMBER(20,2) IDENTITY(20,"
              + " 2))");
      st.execute("create or replace database JDBC_DB2");
      st.execute("create or replace schema JDBC_SCHEMA21");
      st.execute("create or replace table JDBC_TBL211(colA string)");
      st.execute("create or replace table JDBC_BIN(bin1 binary(8388608), bin2 binary(100))");

      //    st.execute("create or replace table JDBC_TBL211(colA string(25) NOT NULL DEFAULT
      // 'defstring')");
    }
  }

  @AfterEach
  public void tearDown() throws SQLException {
    try (Connection con = getConnection()) {
      endMetaData(con);
    }
  }

  static void endMetaData(Connection con) throws SQLException {
    try (Statement st = con.createStatement()) {
      st.execute("drop database if exists JDBC_DB1");
      st.execute("drop database if exists JDBC_DB2");
    }
  }

  @Test
  @Disabled // TODO: SNOW-1805299
  @DontRunOnGithubActions
  public void testGetColumn() throws SQLException {
    String getAllColumnsCount = "select count(*) from db.information_schema.columns";
    connection = getConnection();
    statement = connection.createStatement();
    databaseMetaData = connection.getMetaData();

    // Exclude SNOWFLAKE system database from DatabaseMetadata
    ResultSet snowflakeResultSet = databaseMetaData.getColumns("SNOWFLAKE", null, null, null);
    int numSnowflakeColumns = getSizeOfResultSet(snowflakeResultSet);

    resultSet = databaseMetaData.getColumns(null, null, null, null);
    assertEquals(
        getAllObjectCountInDBViaInforSchema(getAllColumnsCount),
        getSizeOfResultSet(resultSet) - numSnowflakeColumns);

    resultSet = databaseMetaData.getColumns(null, "JDBC_SCHEMA11", null, null);
    assertEquals(3, getSizeOfResultSet(resultSet));

    resultSet = databaseMetaData.getColumns(null, "JDBC_SCH_MA11", null, null);
    assertEquals(3, getSizeOfResultSet(resultSet));

    resultSet = databaseMetaData.getColumns(null, "JDBC%", null, null);
    assertEquals(10, getSizeOfResultSet(resultSet));

    resultSet = databaseMetaData.getColumns(null, "JDBC_SCHEMA1_", null, null);
    assertEquals(7, getSizeOfResultSet(resultSet));

    resultSet = databaseMetaData.getColumns(null, "JDBC_SCHEMA21", "JDBC_BIN", "BIN1");
    resultSet.next();
    assertEquals(EXPECTED_MAX_BINARY_LENGTH, resultSet.getInt("COLUMN_SIZE"));
    assertEquals(1, getSizeOfResultSet(resultSet) + 1);

    resultSet = databaseMetaData.getColumns(null, "JDBC_SCHEMA21", "JDBC_BIN", "BIN2");
    resultSet.next();
    assertEquals(100, resultSet.getInt("COLUMN_SIZE"));
    assertEquals(1, getSizeOfResultSet(resultSet) + 1);

    // test if return the correct info
    resultSet = databaseMetaData.getColumns("JDBC_DB1", "JDBC_SCHEMA12", "JDBC_TBL122", "COLA");
    resultSet.next();
    // fetchResultSet(resultSet, true);
    assertEquals("JDBC_DB1", resultSet.getString("TABLE_CAT"));
    assertEquals("JDBC_SCHEMA12", resultSet.getString("TABLE_SCHEM"));
    assertEquals("JDBC_TBL122", resultSet.getString("TABLE_NAME"));
    assertEquals("COLA", resultSet.getString("COLUMN_NAME"));
    assertEquals(Types.DECIMAL, resultSet.getInt("DATA_TYPE"));
    assertEquals("NUMBER", resultSet.getString("TYPE_NAME"));
    assertEquals("20", resultSet.getString("COLUMN_SIZE"));
    assertEquals("2", resultSet.getString("DECIMAL_DIGITS"));
    assertEquals(DatabaseMetaData.columnNullable, resultSet.getInt("NULLABLE"));
    assertEquals("cmt colA", resultSet.getString("REMARKS"));
    assertEquals(null, resultSet.getString("COLUMN_DEF"));
    assertEquals("YES", resultSet.getString("IS_NULLABLE"));
    assertEquals("YES", resultSet.getString("IS_AUTOINCREMENT"));
    resultSet.close();

    // more on default and autoincrement
    resultSet = databaseMetaData.getColumns("JDBC_DB1", "JDBC_SCHEMA12", "JDBC_TBL122", "COLB");
    resultSet.next();
    assertEquals(DatabaseMetaData.columnNoNulls, resultSet.getInt(11));
    assertEquals("3", resultSet.getString(13));
    assertEquals("NO", resultSet.getString(23));
    resultSet.close();

    resultSet = databaseMetaData.getColumns("JDBC_DB1", "JDBC_SCHEMA1_", null, "COL_");

    resultSet = databaseMetaData.getColumns("JDBC_DB1", "JDBC_SCHEMA12", "JDBC_TBL122", "COLC");
    resultSet.next();
    assertEquals(null, resultSet.getString(13));
    assertEquals("YES", resultSet.getString(23));

    // SNOW-24558 Metadata request with special characters in table name
    statement.execute("create or replace table \"@@specialchartable$1234\"(colA int)");
    resultSet = databaseMetaData.getColumns(null, null, "@@specialchartable$%", null);
    assertEquals(1, getSizeOfResultSet(resultSet));

    resultSet.close();
    resultSet.next();
    connection.close();
  }

  @Test
  @DontRunOnGithubActions
  public void testGetFunctions() throws SQLException {
    connection = getConnection();
    statement = connection.createStatement();
    statement.execute(
        "create or replace function JDBC_DB1.JDBC_SCHEMA11.JDBCFUNCTEST111 "
            + "(a number, b number) RETURNS NUMBER COMMENT='multiply numbers' as 'a*b'");
    statement.execute(
        "create or replace function JDBC_DB1.JDBC_SCHEMA12.JDBCFUNCTEST121 "
            + "(a number, b number) RETURNS NUMBER COMMENT='multiply numbers' as 'a*b'");
    statement.execute(
        "create or replace function JDBC_DB1.JDBC_SCHEMA12.JDBCFUNCTEST122 "
            + "(a number, b number) RETURNS NUMBER COMMENT='multiply numbers' as 'a*b'");
    statement.execute(
        "create or replace function JDBC_DB2.JDBC_SCHEMA21.JDBCFUNCTEST211 "
            + "(a number, b number) RETURNS NUMBER COMMENT='multiply numbers' as 'a*b'");
    statement.execute(
        "create or replace function JDBC_DB2.JDBC_SCHEMA21.JDBCFUNCTEST212 () RETURNS TABLE(colA"
            + " varchar) as 'select COLA from JDBC_DB2.JDBC_SCHEMA21.JDBC_TBL211'");
    databaseMetaData = connection.getMetaData();

    // test each column return the right value
    resultSet = databaseMetaData.getFunctions("JDBC_DB1", "JDBC_SCHEMA11", "JDBCFUNCTEST111");
    verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_FUNCTIONS);
    resultSet.next();
    assertEquals("JDBC_DB1", resultSet.getString("FUNCTION_CAT"));
    assertEquals("JDBC_SCHEMA11", resultSet.getString("FUNCTION_SCHEM"));
    assertEquals("JDBCFUNCTEST111", resultSet.getString("FUNCTION_NAME"));
    assertEquals("multiply numbers", resultSet.getString("REMARKS"));
    assertEquals(DatabaseMetaData.functionNoTable, resultSet.getInt("FUNCTION_TYPE"));
    assertEquals("JDBCFUNCTEST111", resultSet.getString("SPECIFIC_NAME"));
    assertFalse(resultSet.next());

    // test a table function
    resultSet = databaseMetaData.getFunctions("JDBC_DB2", "JDBC_SCHEMA21", "JDBCFUNCTEST212");
    resultSet.next();
    assertEquals(DatabaseMetaData.functionReturnsTable, resultSet.getInt("FUNCTION_TYPE"));
    assertFalse(resultSet.next());

    // test a builtin function
    resultSet = databaseMetaData.getFunctions(null, null, "AND");
    resultSet.next();
    assertEquals("", resultSet.getString("FUNCTION_CAT"));
    assertEquals("", resultSet.getString("FUNCTION_SCHEM"));
    assertEquals("AND", resultSet.getString("FUNCTION_NAME"));
    assertEquals(DatabaseMetaData.functionNoTable, resultSet.getInt("FUNCTION_TYPE"));
    assertEquals("AND", resultSet.getString("SPECIFIC_NAME"));
    assertFalse(resultSet.next());

    // test pattern
    resultSet = databaseMetaData.getFunctions(null, null, "JDBCFUNCTEST%");
    assertEquals(5, getSizeOfResultSet(resultSet));
    resultSet = databaseMetaData.getFunctions(null, "JDBC_SCHEMA1_", "_DBCFUNCTEST%");
    assertEquals(3, getSizeOfResultSet(resultSet));
    // resultSet = databaseMetaData.getFunctions("JDBC_DB1", "AAAAAAAAAAA", "AAAAAAA");

    resultSet = databaseMetaData.getFunctions("JDBC_DB3", "JDBC_SCHEMA1_", "_DBCFUNCTEST%");
    assertEquals(0, getSizeOfResultSet(resultSet));

    resultSet = databaseMetaData.getFunctions("JDBC_DB1", "JDBC_SCHEMA__", "_DBCFUNCTEST%");
    assertEquals(3, getSizeOfResultSet(resultSet));
    resultSet = databaseMetaData.getFunctions("JDBC_DB1", "JDBC_SCHEMA1_", "_DBCFUNCTEST11_");
    assertEquals(1, getSizeOfResultSet(resultSet));
    resultSet = databaseMetaData.getFunctions("JDBC_DB1", null, "_DBCFUNCTEST11_");
    assertEquals(1, getSizeOfResultSet(resultSet));

    resultSet.close();
    resultSet.next();

    statement.close();
    connection.close();
  }

  @Test
  @Disabled // TODO: SNOW-1805299
  @DontRunOnGithubActions
  public void testGetSchema() throws SQLException {
    String getSchemaCount = "select count(*) from db.information_schema.schemata";
    connection = getConnection();
    databaseMetaData = connection.getMetaData();
    assertEquals("schema", databaseMetaData.getSchemaTerm());

    // Exclude SNOWFLAKE system database from DatabaseMetadata
    ResultSet snowflakeResultSet = databaseMetaData.getSchemas("SNOWFLAKE", null);
    int numSnowflakeSchemas = getSizeOfResultSet(snowflakeResultSet);

    resultSet = databaseMetaData.getSchemas();
    assertEquals(
        getAllObjectCountInDBViaInforSchema(getSchemaCount),
        getSizeOfResultSet(resultSet) - numSnowflakeSchemas);

    resultSet = databaseMetaData.getSchemas(null, null);
    assertEquals(
        getAllObjectCountInDBViaInforSchema(getSchemaCount),
        getSizeOfResultSet(resultSet) - numSnowflakeSchemas);

    resultSet = databaseMetaData.getSchemas("JDBC_DB1", "%");
    resultSet.next();
    assertEquals("INFORMATION_SCHEMA", resultSet.getString(1));
    assertEquals("JDBC_DB1", resultSet.getString(2));
    resultSet.next();
    assertEquals("JDBC_SCHEMA11", resultSet.getString(1));
    assertEquals("JDBC_DB1", resultSet.getString(2));
    resultSet.next();
    assertEquals("JDBC_SCHEMA12", resultSet.getString(1));
    assertEquals("JDBC_DB1", resultSet.getString(2));
    resultSet.next();
    assertEquals("PUBLIC", resultSet.getString(1));
    assertEquals("JDBC_DB1", resultSet.getString(2));

    resultSet = databaseMetaData.getSchemas("JDBC_DB1", "JDBC%");
    assertEquals(2, getSizeOfResultSet(resultSet));
    resultSet.close();
    resultSet.next();

    connection.close();
  }

  /**
   * SNOW-51427 SNOW-54196 Enable persisted cache results for show objects This test ensures that
   * cached show results are correctly created, used, and invalidated when it is called from JDBC's
   * getTables() function Author: Andong Zhan Created on 09/28/2018
   */
  @Test
  @Disabled // SNOW-85084 detected this is a flaky test, so ignore it here.
  // We have other regression tests to cover it
  @DontRunOnGithubActions
  public void testGetTablesReusingCachedResults() throws SQLException {
    Connection snowflakeConnection = getSnowflakeAdminConnection();
    Statement snowflake = snowflakeConnection.createStatement();
    snowflake.execute("alter system set OVERRIDE_USE_CACHED_RESULT = true;");

    connection = getConnection();
    databaseMetaData = connection.getMetaData();
    Statement stmt = connection.createStatement();

    // Clean existing cache
    stmt.execute("select system$drop_result_reuse_cache();");

    // Setup parameters
    stmt.execute("alter session set USE_CACHED_RESULT = true;");
    stmt.execute("alter session set USE_CACHED_SHOW_RESULT = true;");
    String accountName = getAccountName(stmt);
    // Setup test database
    long accountId = getAccountId(stmt, accountName);
    String dbname = "JDBC_DSHOW";
    String schemaname = "SSHOW_" + randomAlphaNumeric(6);
    stmt.execute("create or replace database " + dbname);
    stmt.execute("create or replace schema " + schemaname);
    stmt.execute("use schema " + dbname + "." + schemaname);
    stmt.execute("create table show1(c1 number);");

    // run show objects
    long oldNumCacheRes = getNumCachedResults(stmt, accountId);
    resultSet = databaseMetaData.getTables(dbname, null, null, null);
    long newNumCacheRes = getNumCachedResults(stmt, accountId);
    // the number of cached results should increase by one
    assertEquals(1, newNumCacheRes - oldNumCacheRes);
    // run show objects in database again and the #cache should be the same
    oldNumCacheRes = newNumCacheRes;
    resultSet = databaseMetaData.getTables(dbname, null, null, null);
    newNumCacheRes = getNumCachedResults(stmt, accountId);
    // the number of cached results should not increase
    assertEquals(0, newNumCacheRes - oldNumCacheRes);

    // create new table, then the first getTables should create a new cache
    // and the 2nd getTables should reuse the cache
    stmt.execute("create table show2(c2 number);");
    resultSet = databaseMetaData.getTables(dbname, null, null, null);
    newNumCacheRes = getNumCachedResults(stmt, accountId);
    assertEquals(1, newNumCacheRes - oldNumCacheRes);
    oldNumCacheRes = newNumCacheRes;
    resultSet = databaseMetaData.getTables(dbname, null, null, null);
    newNumCacheRes = getNumCachedResults(stmt, accountId);
    assertEquals(0, newNumCacheRes - oldNumCacheRes);
    oldNumCacheRes = newNumCacheRes;

    // rename table, then the first getTables should create a new cache
    // and the 2nd getTables should reuse the cache
    stmt.execute("alter table show2 rename to show3");
    resultSet = databaseMetaData.getTables(dbname, null, null, null);
    newNumCacheRes = getNumCachedResults(stmt, accountId);
    assertEquals(1, newNumCacheRes - oldNumCacheRes);
    oldNumCacheRes = newNumCacheRes;
    resultSet = databaseMetaData.getTables(dbname, null, null, null);
    newNumCacheRes = getNumCachedResults(stmt, accountId);
    assertEquals(0, newNumCacheRes - oldNumCacheRes);
    oldNumCacheRes = newNumCacheRes;

    // comment table, then the first getTables should create a new cache
    // and the 2nd getTables should reuse the cache
    stmt.execute("alter table show3 set comment = 'show3'");
    resultSet = databaseMetaData.getTables(dbname, null, null, null);
    newNumCacheRes = getNumCachedResults(stmt, accountId);
    assertEquals(1, newNumCacheRes - oldNumCacheRes);
    oldNumCacheRes = newNumCacheRes;
    resultSet = databaseMetaData.getTables(dbname, null, null, null);
    newNumCacheRes = getNumCachedResults(stmt, accountId);
    assertEquals(0, newNumCacheRes - oldNumCacheRes);
    oldNumCacheRes = newNumCacheRes;

    // insert table, then getTables should reuse last cache
    stmt.execute("insert into show3 values (3),(4)");
    resultSet = databaseMetaData.getTables(dbname, null, null, null);
    newNumCacheRes = getNumCachedResults(stmt, accountId);
    assertEquals(0, newNumCacheRes - oldNumCacheRes);

    // drop table, then the first getTables should create a new cache
    // and the 2nd getTables should reuse the cache
    stmt.execute("drop table show1");
    resultSet = databaseMetaData.getTables(dbname, null, null, null);
    newNumCacheRes = getNumCachedResults(stmt, accountId);
    assertEquals(1, newNumCacheRes - oldNumCacheRes);
    oldNumCacheRes = newNumCacheRes;
    resultSet = databaseMetaData.getTables(dbname, null, null, null);
    newNumCacheRes = getNumCachedResults(stmt, accountId);
    assertEquals(0, newNumCacheRes - oldNumCacheRes);

    // clean up
    stmt.execute("drop database if exists " + dbname);
    // Setup parameters
    stmt.execute("alter session set USE_CACHED_RESULT = default;");
    snowflake.execute("alter system set OVERRIDE_USE_CACHED_RESULT = true;");
    stmt.execute("alter session set USE_CACHED_SHOW_RESULT = default;");

    stmt.close();
    snowflake.close();
    snowflakeConnection.close();
    connection.close();
  }

  private long getNumCachedResults(Statement stmt, long accountId) throws SQLException {
    String query =
        "select count($1:\"JobResultDPO:share\")\n"
            + "from table(dposcan('\n"
            + "  {\n"
            + "    \"slices\" : [{\"name\" : \"JobResultDPO:share\"}],\n"
            + "    \"ranges\" : [\n"
            + "            {\"name\" : \"accountId\", \"value\" : %d}\n"
            + "          ]\n"
            + "   }'));";
    stmt.execute(String.format(query, accountId));
    resultSet = stmt.getResultSet();
    assertTrue(resultSet.next());
    return resultSet.getLong(1);
  }

  private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  public static String randomAlphaNumeric(int count) {
    StringBuilder builder = new StringBuilder();
    while (count-- != 0) {
      int character = (int) (Math.random() * ALPHA_NUMERIC_STRING.length());
      builder.append(ALPHA_NUMERIC_STRING.charAt(character));
    }
    return builder.toString();
  }

  private String getAccountName(Statement stmt) throws SQLException {
    stmt.execute("select current_account_locator()");
    resultSet = stmt.getResultSet();
    assertTrue(resultSet.next());
    return resultSet.getString(1);
  }

  private long getAccountId(Statement stmt, String accountName) throws SQLException {
    stmt.execute(
        "select to_number($1:\"AccountDPO:active_by_name\":id) as id\n"
            + "from table(dposcan('\n"
            + "  {\n"
            + "    \"slices\" : [{\"name\" : \"AccountDPO:active_by_name\"}],\n"
            + "    \"ranges\" : [\n"
            + "            {\"name\": \"name\", \"value\": \""
            + accountName
            + "\"}\n"
            + "          ]\n"
            + "   }'));");
    resultSet = stmt.getResultSet();
    assertTrue(resultSet.next());
    return resultSet.getLong(1);
  }

  @Test
  @Disabled // TODO: SNOW-1805299
  @DontRunOnGithubActions
  public void testGetTables() throws SQLException {
    String getAllTable = "select count(*) from db.information_schema.tables";
    String getAllBaseTable =
        "select count(*) from db.information_schema." + "tables where table_type = 'BASE TABLE'";
    String getAllView =
        "select count(*) from db.information_schema." + "tables where table_type = 'VIEW'";
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement()) {

      // set parameter
      stmt.execute("alter session set ENABLE_DRIVER_TERSE_SHOW = true;");
      stmt.execute("alter session set qa_mode = false;");

      databaseMetaData = connection.getMetaData();

      SQLException e =
          assertThrows(
              SQLException.class,
              () -> databaseMetaData.getTables(null, null, null, new String[] {"ALIAS"}));
      assertEquals(ErrorCode.FEATURE_UNSUPPORTED.getSqlState(), e.getSQLState());
      assertEquals(ErrorCode.FEATURE_UNSUPPORTED.getMessageCode().intValue(), e.getErrorCode());

      try (ResultSet resultSet =
          databaseMetaData.getTables(null, null, null, new String[] {"SYSTEM_TABLE"})) {
        assertEquals(0, getSizeOfResultSet(resultSet));
      }

      // Get the count of tables in the SNOWFLAKE system database, so we can exclude them from
      // subsequent assertions
      int numSnowflakeTables = 0;
      try (ResultSet snowflakeResultSet =
          databaseMetaData.getTables("SNOWFLAKE", null, null, null)) {
        numSnowflakeTables = getSizeOfResultSet(snowflakeResultSet);
      }

      try (ResultSet resultSet = databaseMetaData.getTables(null, null, null, null)) {
        assertEquals(
            getAllObjectCountInDBViaInforSchema(getAllTable),
            getSizeOfResultSet(resultSet) - numSnowflakeTables);
      }

      try (ResultSet resultSet =
          databaseMetaData.getTables(null, null, null, new String[] {"VIEW", "SYSTEM_TABLE"})) {
        assertEquals(
            getAllObjectCountInDBViaInforSchema(getAllView),
            getSizeOfResultSet(resultSet) - numSnowflakeTables);
      }

      try (ResultSet resultSet =
          databaseMetaData.getTables(null, null, null, new String[] {"TABLE", "SYSTEM_TABLE"})) {
        assertEquals(
            getAllObjectCountInDBViaInforSchema(getAllBaseTable), getSizeOfResultSet(resultSet));
      }

      try (ResultSet resultSet =
          databaseMetaData.getTables(
              null, null, null, new String[] {"TABLE", "VIEW", "SYSTEM_TABLE"})) {
        assertEquals(
            getAllObjectCountInDBViaInforSchema(getAllTable),
            getSizeOfResultSet(resultSet) - numSnowflakeTables);
      }

      try (ResultSet resultSet =
          databaseMetaData.getTables(null, null, null, new String[] {"TABLE", "VIEW"})) {
        assertEquals(
            getAllObjectCountInDBViaInforSchema(getAllTable),
            getSizeOfResultSet(resultSet) - numSnowflakeTables);
      }

      try (ResultSet resultSet =
          databaseMetaData.getTables(null, null, null, new String[] {"TABLE"})) {
        assertEquals(
            getAllObjectCountInDBViaInforSchema(getAllBaseTable), getSizeOfResultSet(resultSet));
      }

      try (ResultSet resultSet =
          databaseMetaData.getTables(null, null, null, new String[] {"VIEW"})) {
        assertEquals(
            getAllObjectCountInDBViaInforSchema(getAllView),
            getSizeOfResultSet(resultSet) - numSnowflakeTables);
      }

      try (ResultSet resultSet =
          databaseMetaData.getTables("JDBC_DB1", "JDBC_SCHEMA11", null, new String[] {"TABLE"})) {
        assertEquals(1, getSizeOfResultSet(resultSet));
      }

      // snow-26032. JDBC should strip backslash before sending the show functions to server
      try (ResultSet resultSet =
          databaseMetaData.getTables("JDBC_DB1", "JDBC\\_SCHEMA11", "%", new String[] {"TABLE"})) {
        assertEquals(1, getSizeOfResultSet(resultSet));
      }

      try (ResultSet resultSet =
          databaseMetaData.getTables("JDBC_DB1", "JDBC%", null, new String[] {"TABLE"})) {
        assertEquals(3, getSizeOfResultSet(resultSet));
      }

      // SNOW-487548: disable the key-value feature to hide is_hybrid column
      // in show tables command. The column is controlled by two parameters:
      // enable_key_value_table and qa_mode.
      stmt.execute("alter session set ENABLE_KEY_VALUE_TABLE = false;");
      try (ResultSet resultSet =
          databaseMetaData.getTables(
              "JDBC_DB1", "JDBC_SCH%", "J_BC_TBL122", new String[] {"TABLE"})) {
        resultSet.next();
        assertEquals("JDBC_DB1", resultSet.getString(1));
        assertEquals("JDBC_SCHEMA12", resultSet.getString(2));
        assertEquals("JDBC_TBL122", resultSet.getString(3));
        assertEquals("TABLE", resultSet.getString(4));
        assertEquals("", resultSet.getString(5));
        stmt.execute("alter session unset ENABLE_KEY_VALUE_TABLE;");
      }
      try (ResultSet resultSet =
          databaseMetaData.getTables("JDBC_DB1", null, "JDBC_TBL211", new String[] {"TABLE"})) {
        assertEquals(0, getSizeOfResultSet(resultSet));
      }

      try (ResultSet resultSet = databaseMetaData.getTableTypes()) {
        resultSet.next();
        assertEquals("TABLE", resultSet.getString(1));
        resultSet.next();
        assertEquals("VIEW", resultSet.getString(1));
      }
      stmt.execute("alter session set ENABLE_DRIVER_TERSE_SHOW = default;");
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

      // this should only return JDBC_SCHEMA11
      try (ResultSet resultSet = databaseMetaData.getSchemas(null, null)) {
        assertEquals(1, getSizeOfResultSet(resultSet));
      }
      // only returns tables in JDBC_DB1.JDBC_SCHEMA11
      try (ResultSet resultSet = databaseMetaData.getTables(null, null, null, null)) {
        assertEquals(1, getSizeOfResultSet(resultSet));
      }

      statement.execute("use schema JDBC_SCHEMA12");
      try (ResultSet resultSet = databaseMetaData.getTables(null, null, null, null)) {
        assertEquals(2, getSizeOfResultSet(resultSet));
      }

      try (ResultSet resultSet = databaseMetaData.getColumns(null, null, null, null)) {
        assertEquals(4, getSizeOfResultSet(resultSet));
      }

      statement.execute("use schema TEST_CTX");
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
    }
  }

  private int getAllObjectCountInDBViaInforSchema(String SQLCmdTemplate) throws SQLException {
    int objectCount = 0;
    try (Connection con = getConnection();
        Statement st = con.createStatement()) {
      st.execute("alter session set ENABLE_BUILTIN_SCHEMAS = true");
      try (ResultSet dbNameRS =
          st.executeQuery("select database_name from information_schema.databases")) {
        while (dbNameRS.next()) {
          String databaseName = dbNameRS.getString(1);
          String execSQLCmd = SQLCmdTemplate.replaceAll("db", databaseName);
          ResultSet object = st.executeQuery(execSQLCmd);
          object.next();
          objectCount += object.getInt(1);
        }
      }
    }
    return objectCount;
  }
}
