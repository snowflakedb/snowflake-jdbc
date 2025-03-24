package net.snowflake.client.jdbc;

import static java.sql.DatabaseMetaData.procedureReturnsResult;
import static java.sql.ResultSetMetaData.columnNullableUnknown;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Strings;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.snowflake.client.TestUtil;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Database Metadata IT */
@Tag(TestTags.OTHERS)
public class DatabaseMetaDataIT extends BaseJDBCWithSharedConnectionIT {
  private static final Pattern VERSION_PATTERN =
      Pattern.compile("^(\\d+)\\.(\\d+)(?:\\.\\d+)+\\s*.*");
  private static final String PI_PROCEDURE =
      "create or replace procedure GETPI()\n"
          + "    returns float not null\n"
          + "    language javascript\n"
          + "    as\n"
          + "    $$\n"
          + "    return 3.1415926;\n"
          + "    $$\n"
          + "    ;";
  private static final String STPROC1_PROCEDURE =
      "create or replace procedure stproc1(param1 float, param2 string)\n"
          + "    returns table(retval varchar)\n"
          + "    language javascript\n"
          + "    as\n"
          + "    $$\n"
          + "    var sql_command = \"Hello, world!\"\n"
          + "    $$\n"
          + "    ;";

  public static final int EXPECTED_MAX_CHAR_LENGTH = 16777216;

  public static final int EXPECTED_MAX_BINARY_LENGTH = 8388608;

  @Test
  public void testGetConnection() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    assertEquals(connection, metaData.getConnection());
  }

  @Test
  public void testDatabaseAndDriverInfo() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();

    // identifiers
    assertEquals("Snowflake", metaData.getDatabaseProductName());
    assertEquals("Snowflake", metaData.getDriverName());

    // Snowflake JDBC driver version
    String driverVersion = metaData.getDriverVersion();
    Matcher m = VERSION_PATTERN.matcher(driverVersion);
    assertTrue(m.matches());
    int majorVersion = metaData.getDriverMajorVersion();
    int minorVersion = metaData.getDriverMinorVersion();
    assertEquals(m.group(1), String.valueOf(majorVersion));
    assertEquals(m.group(2), String.valueOf(minorVersion));
  }

  @Test
  public void testGetCatalogs() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    assertEquals(".", metaData.getCatalogSeparator());
    assertEquals("database", metaData.getCatalogTerm());

    ResultSet resultSet = metaData.getCatalogs();
    verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_CATALOGS);
    assertTrue(resultSet.isBeforeFirst());

    int cnt = 0;
    Set<String> allVisibleDatabases = new HashSet<>();
    while (resultSet.next()) {
      allVisibleDatabases.add(resultSet.getString(1));
      if (cnt == 0) {
        assertTrue(resultSet.isFirst());
      }
      ++cnt;
      assertThrows(SQLFeatureNotSupportedException.class, resultSet::isLast);
      assertThrows(SQLFeatureNotSupportedException.class, resultSet::isAfterLast);
    }
    assertThat(cnt, greaterThanOrEqualTo(1));
    SQLException ex = assertThrows(SQLException.class, resultSet::isAfterLast);
    assertEquals((int) ErrorCode.RESULTSET_ALREADY_CLOSED.getMessageCode(), ex.getErrorCode());
    resultSet.close(); // double closing does nothing.
    resultSet.next(); // no exception

    List<String> allAccessibleDatabases =
        getInfoBySQL("select database_name from information_schema.databases");

    assertTrue(allVisibleDatabases.containsAll(allAccessibleDatabases));
  }

  @Test
  public void testGetSchemas() throws Throwable {
    // CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = false
    DatabaseMetaData metaData = connection.getMetaData();
    String currentSchema = connection.getSchema();
    assertEquals("schema", metaData.getSchemaTerm());
    Set<String> schemas = new HashSet<>();
    try (ResultSet resultSet = metaData.getSchemas()) {
      verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_SCHEMAS);
      while (resultSet.next()) {
        String schema = resultSet.getString(1);
        if (currentSchema.equals(schema) || !TestUtil.isSchemaGeneratedInTests(schema)) {
          schemas.add(schema);
        }
      }
    }
    assertThat(schemas.size(), greaterThanOrEqualTo(1));

    Set<String> schemasInDb = new HashSet<>();
    try (ResultSet resultSet = metaData.getSchemas(connection.getCatalog(), "%")) {
      while (resultSet.next()) {
        String schema = resultSet.getString(1);
        if (currentSchema.equals(schema) || !TestUtil.isSchemaGeneratedInTests(schema)) {
          schemasInDb.add(schema);
        }
      }
    }
    assertThat(schemasInDb.size(), greaterThanOrEqualTo(1));
    assertThat(schemas.size(), greaterThanOrEqualTo(schemasInDb.size()));
    schemasInDb.forEach(schemaInDb -> assertThat(schemas, hasItem(schemaInDb)));
    assertTrue(schemas.contains(currentSchema));
    assertTrue(schemasInDb.contains(currentSchema));

    // CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = true
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");

      DatabaseMetaData metaData2 = connection.getMetaData();
      assertEquals("schema", metaData2.getSchemaTerm());
      try (ResultSet resultSet = metaData2.getSchemas()) {
        Set<String> schemas2 = new HashSet<>();
        while (resultSet.next()) {
          schemas2.add(resultSet.getString(1));
        }
        assertThat(schemas2.size(), equalTo(1));
      }
    }
  }

  @Test
  public void testGetTableTypes() throws Throwable {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet resultSet = metaData.getTableTypes()) {
      Set<String> types = new HashSet<>();
      while (resultSet.next()) {
        types.add(resultSet.getString(1));
      }
      assertEquals(2, types.size());
      assertTrue(types.contains("TABLE"));
      assertTrue(types.contains("VIEW"));
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGetTables() throws Throwable {
    Set<String> tables = null;
    try (Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";
      final String targetView = "V0";
      try {
        statement.execute("create or replace table " + targetTable + "(C1 int)");
        statement.execute("create or replace view " + targetView + " as select 1 as C");

        DatabaseMetaData metaData = connection.getMetaData();

        // match table
        try (ResultSet resultSet =
            metaData.getTables(database, schema, "%", new String[] {"TABLE"})) {
          verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_TABLES);
          tables = new HashSet<>();
          while (resultSet.next()) {
            tables.add(resultSet.getString(3));
          }
          assertTrue(tables.contains("T0"));
        }
        // exact match table
        try (ResultSet resultSet =
            metaData.getTables(database, schema, targetTable, new String[] {"TABLE"})) {
          tables = new HashSet<>();
          while (resultSet.next()) {
            tables.add(resultSet.getString(3));
          }
          assertEquals(targetTable, tables.iterator().next());
        }
        // match view
        try (ResultSet resultSet =
            metaData.getTables(database, schema, "%", new String[] {"VIEW"})) {
          Set<String> views = new HashSet<>();
          while (resultSet.next()) {
            views.add(resultSet.getString(3));
          }
          assertTrue(views.contains(targetView));
        }

        try (ResultSet resultSet = metaData.getTablePrivileges(database, schema, targetTable)) {
          assertEquals(1, getSizeOfResultSet(resultSet));
        }
      } finally {
        statement.execute("drop table if exists " + targetTable);
        statement.execute("drop view if exists " + targetView);
      }
    }
  }

  @Test
  public void testGetPrimarykeys() throws Throwable {
    try (Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";
      try {
        statement.execute(
            "create or replace table " + targetTable + "(C1 int primary key, C2 string)");

        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet resultSet = metaData.getPrimaryKeys(database, schema, targetTable)) {
          verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_PRIMARY_KEYS);
          assertTrue(resultSet.next());
          assertEquals(database, resultSet.getString("TABLE_CAT"));
          assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          assertEquals(targetTable, resultSet.getString("TABLE_NAME"));
          assertEquals("C1", resultSet.getString("COLUMN_NAME"));
          assertEquals(1, resultSet.getInt("KEY_SEQ"));
          assertNotEquals("", resultSet.getString("PK_NAME"));
        }
      } finally {
        statement.execute("drop table if exists " + targetTable);
      }
    }
  }

  static void verifyResultSetMetaDataColumns(
      ResultSet resultSet, DBMetadataResultSetMetadata metadata) throws SQLException {
    final int numCol = metadata.getColumnNames().size();
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertEquals(numCol, resultSetMetaData.getColumnCount());

    for (int col = 1; col <= numCol; ++col) {
      List<String> colNames = metadata.getColumnNames();
      List<String> colTypeNames = metadata.getColumnTypeNames();
      List<Integer> colTypes = metadata.getColumnTypes();

      assertEquals("", resultSetMetaData.getCatalogName(col));
      assertEquals("", resultSetMetaData.getSchemaName(col));
      assertEquals("T", resultSetMetaData.getTableName(col));
      assertEquals(colNames.get(col - 1), resultSetMetaData.getColumnName(col));

      assertEquals(colNames.get(col - 1), resultSetMetaData.getColumnLabel(col));
      assertEquals(
          SnowflakeType.javaTypeToClassName(resultSetMetaData.getColumnType(col)),
          resultSetMetaData.getColumnClassName(col));
      assertEquals(25, resultSetMetaData.getColumnDisplaySize(col));
      assertEquals((int) colTypes.get(col - 1), resultSetMetaData.getColumnType(col));
      assertEquals(colTypeNames.get(col - 1), resultSetMetaData.getColumnTypeName(col));
      assertEquals(9, resultSetMetaData.getPrecision(col));
      assertEquals(9, resultSetMetaData.getScale(col));

      assertEquals(
          SnowflakeType.isJavaTypeSigned(resultSetMetaData.getColumnType(col)),
          resultSetMetaData.isSigned(col));
      assertFalse(resultSetMetaData.isAutoIncrement(col));
      assertFalse(resultSetMetaData.isCurrency(col));
      assertTrue(resultSetMetaData.isReadOnly(col));
      assertTrue(resultSetMetaData.isSearchable(col));
      assertFalse(resultSetMetaData.isWritable(col));
      assertFalse(resultSetMetaData.isDefinitelyWritable(col));

      assertEquals(columnNullableUnknown, resultSetMetaData.isNullable(col));
    }
  }

  @Test
  public void testGetImportedKeys() throws Throwable {
    try (Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable1 = "T0";
      final String targetTable2 = "T1";
      try {
        statement.execute(
            "create or replace table " + targetTable1 + "(C1 int primary key, C2 string)");
        statement.execute(
            "create or replace table "
                + targetTable2
                + "(C1 int primary key, C2 string, C3 int references "
                + targetTable1
                + ")");

        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet resultSet = metaData.getImportedKeys(database, schema, targetTable2)) {
          verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_FOREIGN_KEYS);
          assertTrue(resultSet.next());
          assertEquals(database, resultSet.getString("PKTABLE_CAT"));
          assertEquals(schema, resultSet.getString("PKTABLE_SCHEM"));
          assertEquals(targetTable1, resultSet.getString("PKTABLE_NAME"));
          assertEquals("C1", resultSet.getString("PKCOLUMN_NAME"));
          assertEquals(database, resultSet.getString("FKTABLE_CAT"));
          assertEquals(schema, resultSet.getString("FKTABLE_SCHEM"));
          assertEquals(targetTable2, resultSet.getString("FKTABLE_NAME"));
          assertEquals("C3", resultSet.getString("FKCOLUMN_NAME"));
          assertEquals(1, resultSet.getInt("KEY_SEQ"));
          assertNotEquals("", resultSet.getString("PK_NAME"));
          assertNotEquals("", resultSet.getString("FK_NAME"));
          assertEquals(DatabaseMetaData.importedKeyNoAction, resultSet.getShort("UPDATE_RULE"));
          assertEquals(DatabaseMetaData.importedKeyNoAction, resultSet.getShort("DELETE_RULE"));
          assertEquals(
              DatabaseMetaData.importedKeyNotDeferrable, resultSet.getShort("DEFERRABILITY"));
        }
      } finally {
        statement.execute("drop table if exists " + targetTable1);
        statement.execute("drop table if exists " + targetTable2);
      }
    }
  }

  @Test
  public void testGetExportedKeys() throws Throwable {
    try (Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable1 = "T0";
      final String targetTable2 = "T1";
      try {
        statement.execute(
            "create or replace table " + targetTable1 + "(C1 int primary key, C2 string)");
        statement.execute(
            "create or replace table "
                + targetTable2
                + "(C1 int primary key, C2 string, C3 int references "
                + targetTable1
                + ")");

        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet resultSet = metaData.getExportedKeys(database, schema, targetTable1)) {
          verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_FOREIGN_KEYS);

          assertTrue(resultSet.next());
          assertEquals(database, resultSet.getString("PKTABLE_CAT"));
          assertEquals(schema, resultSet.getString("PKTABLE_SCHEM"));
          assertEquals(targetTable1, resultSet.getString("PKTABLE_NAME"));
          assertEquals("C1", resultSet.getString("PKCOLUMN_NAME"));
          assertEquals(database, resultSet.getString("FKTABLE_CAT"));
          assertEquals(schema, resultSet.getString("FKTABLE_SCHEM"));
          assertEquals(targetTable2, resultSet.getString("FKTABLE_NAME"));
          assertEquals("C3", resultSet.getString("FKCOLUMN_NAME"));
          assertEquals(1, resultSet.getInt("KEY_SEQ"));
          assertNotEquals("", resultSet.getString("PK_NAME"));
          assertNotEquals("", resultSet.getString("FK_NAME"));
          assertEquals(DatabaseMetaData.importedKeyNoAction, resultSet.getShort("UPDATE_RULE"));
          assertEquals(DatabaseMetaData.importedKeyNoAction, resultSet.getShort("DELETE_RULE"));
          assertEquals(
              DatabaseMetaData.importedKeyNotDeferrable, resultSet.getShort("DEFERRABILITY"));
        }
      } finally {
        statement.execute("drop table if exists " + targetTable1);
        statement.execute("drop table if exists " + targetTable2);
      }
    }
  }

  @Test
  public void testGetCrossReferences() throws Throwable {
    try (Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable1 = "T0";
      final String targetTable2 = "T1";
      try {
        statement.execute(
            "create or replace table " + targetTable1 + "(C1 int primary key, C2 string)");
        statement.execute(
            "create or replace table "
                + targetTable2
                + "(C1 int primary key, C2 string, C3 int references "
                + targetTable1
                + ")");

        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet resultSet =
            metaData.getCrossReference(
                database, schema, targetTable1, database, schema, targetTable2)) {
          verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_FOREIGN_KEYS);

          assertTrue(resultSet.next());
          assertEquals(database, resultSet.getString("PKTABLE_CAT"));
          assertEquals(schema, resultSet.getString("PKTABLE_SCHEM"));
          assertEquals(targetTable1, resultSet.getString("PKTABLE_NAME"));
          assertEquals("C1", resultSet.getString("PKCOLUMN_NAME"));
          assertEquals(database, resultSet.getString("FKTABLE_CAT"));
          assertEquals(schema, resultSet.getString("FKTABLE_SCHEM"));
          assertEquals(targetTable2, resultSet.getString("FKTABLE_NAME"));
          assertEquals("C3", resultSet.getString("FKCOLUMN_NAME"));
          assertEquals(1, resultSet.getInt("KEY_SEQ"));
          assertNotEquals("", resultSet.getString("PK_NAME"));
          assertNotEquals("", resultSet.getString("FK_NAME"));
          assertEquals(DatabaseMetaData.importedKeyNoAction, resultSet.getShort("UPDATE_RULE"));
          assertEquals(DatabaseMetaData.importedKeyNoAction, resultSet.getShort("DELETE_RULE"));
          assertEquals(
              DatabaseMetaData.importedKeyNotDeferrable, resultSet.getShort("DEFERRABILITY"));
        }
      } finally {
        statement.execute("drop table if exists " + targetTable1);
        statement.execute("drop table if exists " + targetTable2);
      }
    }
  }

  @Test
  public void testGetObjectsDoesNotExists() throws Throwable {
    try (Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";
      final String targetView = "V0";
      try {
        statement.execute("create or replace table " + targetTable + "(C1 int)");
        statement.execute("create or replace view " + targetView + " as select 1 as C");

        DatabaseMetaData metaData = connection.getMetaData();

        // sanity check if getTables really works.
        try (ResultSet resultSet = metaData.getTables(database, schema, "%", null)) {
          assertTrue(getSizeOfResultSet(resultSet) > 0);
        }

        // invalid object type. empty result is expected.
        try (ResultSet resultSet =
            metaData.getTables(database, schema, "%", new String[] {"INVALID_TYPE"})) {
          assertEquals(0, getSizeOfResultSet(resultSet));
        }

        // rest of the cases should return empty results.
        try (ResultSet resultSet = metaData.getSchemas("DB_NOT_EXIST", "SCHEMA_NOT_EXIST")) {
          assertFalse(resultSet.next());
          assertTrue(resultSet.isClosed());
        }

        try (ResultSet resultSet =
            metaData.getTables("DB_NOT_EXIST", "SCHEMA_NOT_EXIST", "%", null)) {
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            metaData.getTables(database, "SCHEMA\\_NOT\\_EXIST", "%", null)) {
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            metaData.getColumns("DB_NOT_EXIST", "SCHEMA_NOT_EXIST", "%", "%")) {
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            metaData.getColumns(database, "SCHEMA\\_NOT\\_EXIST", "%", "%")) {
          assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            metaData.getColumns(database, schema, "TBL\\_NOT\\_EXIST", "%")) {
          assertFalse(resultSet.next());
        }
      } finally {
        statement.execute("drop table if exists " + targetTable);
        statement.execute("drop view if exists " + targetView);
      }
    }
  }

  @Test
  public void testTypeInfo() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getTypeInfo();
    resultSet.next();
    assertEquals("NUMBER", resultSet.getString(1));
    resultSet.next();
    assertEquals("INTEGER", resultSet.getString(1));
    resultSet.next();
    assertEquals("DOUBLE", resultSet.getString(1));
    resultSet.next();
    assertEquals("VARCHAR", resultSet.getString(1));
    resultSet.next();
    assertEquals("DATE", resultSet.getString(1));
    resultSet.next();
    assertEquals("TIME", resultSet.getString(1));
    resultSet.next();
    assertEquals("TIMESTAMP", resultSet.getString(1));
    resultSet.next();
    assertEquals("BOOLEAN", resultSet.getString(1));
    assertFalse(resultSet.next());
  }

  @Test
  public void testProcedure() throws Throwable {
    DatabaseMetaData metaData = connection.getMetaData();
    assertEquals("procedure", metaData.getProcedureTerm());
    // no stored procedure support
    assertTrue(metaData.supportsStoredProcedures());
    try (ResultSet resultSet = metaData.getProcedureColumns("%", "%", "%", "%")) {
      assertEquals(0, getSizeOfResultSet(resultSet));
    }
    try (ResultSet resultSet = metaData.getProcedures("%", "%", "%")) {
      assertEquals(0, getSizeOfResultSet(resultSet));
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGetTablePrivileges() throws Exception {
    try (Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      try {
        statement.execute(
            "create or replace table PRIVTEST(colA string, colB number, colC " + "timestamp)");
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet resultSet = metaData.getTablePrivileges(database, schema, "PRIVTEST")) {
          verifyResultSetMetaDataColumns(
              resultSet, DBMetadataResultSetMetadata.GET_TABLE_PRIVILEGES);
          resultSet.next();
          assertEquals(database, resultSet.getString("TABLE_CAT"));
          assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          assertEquals("PRIVTEST", resultSet.getString("TABLE_NAME"));
          assertEquals("SYSADMIN", resultSet.getString("GRANTOR"));
          assertEquals("SYSADMIN", resultSet.getString("GRANTEE"));
          assertEquals("OWNERSHIP", resultSet.getString("PRIVILEGE"));
          assertEquals("YES", resultSet.getString("IS_GRANTABLE"));
        }
        // grant select privileges to table for role security admin and test that a new row of table
        // privileges is added
        statement.execute("grant select on table PRIVTEST to role securityadmin");
        try (ResultSet resultSet = metaData.getTablePrivileges(database, schema, "PRIVTEST")) {
          resultSet.next();
          assertEquals(database, resultSet.getString("TABLE_CAT"));
          assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          assertEquals("PRIVTEST", resultSet.getString("TABLE_NAME"));
          assertEquals("SYSADMIN", resultSet.getString("GRANTOR"));
          assertEquals("SYSADMIN", resultSet.getString("GRANTEE"));
          assertEquals("OWNERSHIP", resultSet.getString("PRIVILEGE"));
          assertEquals("YES", resultSet.getString("IS_GRANTABLE"));
          resultSet.next();
          assertEquals(database, resultSet.getString("TABLE_CAT"));
          assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          assertEquals("PRIVTEST", resultSet.getString("TABLE_NAME"));
          assertEquals("SYSADMIN", resultSet.getString("GRANTOR"));
          assertEquals("SECURITYADMIN", resultSet.getString("GRANTEE"));
          assertEquals("SELECT", resultSet.getString("PRIVILEGE"));
          assertEquals("NO", resultSet.getString("IS_GRANTABLE"));
        }

        // if tableNamePattern is null, empty resultSet is returned.
        try (ResultSet resultSet = metaData.getTablePrivileges(null, null, null)) {
          assertEquals(7, resultSet.getMetaData().getColumnCount());
          assertEquals(0, getSizeOfResultSet(resultSet));
        }
      } finally {
        statement.execute("drop table if exists PRIVTEST");
      }
    }
  }

  @Test
  public void testGetProcedures() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try {
        String database = connection.getCatalog();
        String schema = connection.getSchema();

        /* Create a procedure and put values into it */
        statement.execute(PI_PROCEDURE);
        DatabaseMetaData metaData = connection.getMetaData();
        /* Call getFunctionColumns on FUNC111 and since there's no parameter name, get all rows back */
        try (ResultSet resultSet = metaData.getProcedures(database, schema, "GETPI")) {
          verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_PROCEDURES);
          resultSet.next();
          assertEquals("GETPI", resultSet.getString("PROCEDURE_NAME"));
          assertEquals(database, resultSet.getString("PROCEDURE_CAT"));
          assertEquals(schema, resultSet.getString("PROCEDURE_SCHEM"));
          assertEquals("GETPI", resultSet.getString("PROCEDURE_NAME"));
          assertEquals("user-defined procedure", resultSet.getString("REMARKS"));
          assertEquals(procedureReturnsResult, resultSet.getShort("PROCEDURE_TYPE"));
          assertEquals("GETPI() RETURN FLOAT", resultSet.getString("SPECIFIC_NAME"));
        }
      } finally {
        statement.execute("drop procedure if exists GETPI()");
      }
    }
  }

  @Test
  public void testDatabaseMetadata() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();

    String dbVersion = metaData.getDatabaseProductVersion();
    Matcher m = VERSION_PATTERN.matcher(dbVersion);
    assertTrue(m.matches());
    int majorVersion = metaData.getDatabaseMajorVersion();
    int minorVersion = metaData.getDatabaseMinorVersion();
    assertEquals(m.group(1), String.valueOf(majorVersion));
    assertEquals(m.group(2), String.valueOf(minorVersion));

    assertFalse(Strings.isNullOrEmpty(metaData.getSQLKeywords()));
    assertFalse(Strings.isNullOrEmpty(metaData.getNumericFunctions()));
    assertFalse(Strings.isNullOrEmpty(metaData.getStringFunctions()));
    assertFalse(Strings.isNullOrEmpty(metaData.getSystemFunctions()));
    assertFalse(Strings.isNullOrEmpty(metaData.getTimeDateFunctions()));

    assertEquals("\\", metaData.getSearchStringEscape());

    assertTrue(metaData.getURL().startsWith("jdbc:snowflake://"));
    assertFalse(metaData.allProceduresAreCallable());
    assertTrue(metaData.allTablesAreSelectable());
    assertTrue(metaData.dataDefinitionCausesTransactionCommit());
    assertFalse(metaData.dataDefinitionIgnoredInTransactions());
    assertFalse(metaData.deletesAreDetected(1));
    assertTrue(metaData.doesMaxRowSizeIncludeBlobs());
    assertTrue(metaData.supportsTransactions());
    assertEquals(Connection.TRANSACTION_READ_COMMITTED, metaData.getDefaultTransactionIsolation());
    assertEquals("$", metaData.getExtraNameCharacters());
    assertEquals("\"", metaData.getIdentifierQuoteString());
    assertEquals(0, getSizeOfResultSet(metaData.getIndexInfo(null, null, null, true, true)));
    assertThat(
        metaData.getMaxBinaryLiteralLength(), greaterThanOrEqualTo(EXPECTED_MAX_BINARY_LENGTH));
    assertEquals(255, metaData.getMaxCatalogNameLength());
    assertThat(metaData.getMaxCharLiteralLength(), greaterThanOrEqualTo(EXPECTED_MAX_CHAR_LENGTH));
    assertEquals(255, metaData.getMaxColumnNameLength());
    assertEquals(0, metaData.getMaxColumnsInGroupBy());
    assertEquals(0, metaData.getMaxColumnsInIndex());
    assertEquals(0, metaData.getMaxColumnsInOrderBy());
    assertEquals(0, metaData.getMaxColumnsInSelect());
    assertEquals(0, metaData.getMaxColumnsInTable());
    assertEquals(0, metaData.getMaxConnections());
    assertEquals(0, metaData.getMaxCursorNameLength());
    assertEquals(0, metaData.getMaxIndexLength());
    assertEquals(0, metaData.getMaxProcedureNameLength());
    assertEquals(0, metaData.getMaxRowSize());
    assertEquals(255, metaData.getMaxSchemaNameLength());
    assertEquals(0, metaData.getMaxStatementLength());
    assertEquals(0, metaData.getMaxStatements());
    assertEquals(255, metaData.getMaxTableNameLength());
    assertEquals(0, metaData.getMaxTablesInSelect());
    assertEquals(255, metaData.getMaxUserNameLength());
    assertEquals(0, getSizeOfResultSet(metaData.getTablePrivileges(null, null, null)));
    // assertEquals("", metaData.getTimeDateFunctions());
    assertEquals(TestUtil.systemGetEnv("SNOWFLAKE_TEST_USER"), metaData.getUserName());
    assertFalse(metaData.insertsAreDetected(1));
    assertTrue(metaData.isCatalogAtStart());
    assertFalse(metaData.isReadOnly());
    assertTrue(metaData.nullPlusNonNullIsNull());
    assertFalse(metaData.nullsAreSortedAtEnd());
    assertFalse(metaData.nullsAreSortedAtStart());
    assertTrue(metaData.nullsAreSortedHigh());
    assertFalse(metaData.nullsAreSortedLow());
    assertFalse(metaData.othersDeletesAreVisible(1));
    assertFalse(metaData.othersInsertsAreVisible(1));
    assertFalse(metaData.othersUpdatesAreVisible(1));
    assertFalse(metaData.ownDeletesAreVisible(1));
    assertFalse(metaData.ownInsertsAreVisible(1));
    assertFalse(metaData.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
    assertFalse(metaData.storesLowerCaseIdentifiers());
    assertFalse(metaData.storesLowerCaseQuotedIdentifiers());
    assertFalse(metaData.storesMixedCaseIdentifiers());
    assertTrue(metaData.storesMixedCaseQuotedIdentifiers());
    assertTrue(metaData.storesUpperCaseIdentifiers());
    assertFalse(metaData.storesUpperCaseQuotedIdentifiers());
    assertTrue(metaData.supportsAlterTableWithAddColumn());
    assertTrue(metaData.supportsAlterTableWithDropColumn());
    assertTrue(metaData.supportsANSI92EntryLevelSQL());
    assertFalse(metaData.supportsANSI92FullSQL());
    assertFalse(metaData.supportsANSI92IntermediateSQL());
    assertTrue(metaData.supportsBatchUpdates());
    assertTrue(metaData.supportsCatalogsInDataManipulation());
    assertFalse(metaData.supportsCatalogsInIndexDefinitions());
    assertFalse(metaData.supportsCatalogsInPrivilegeDefinitions());
    assertFalse(metaData.supportsCatalogsInProcedureCalls());
    assertTrue(metaData.supportsCatalogsInTableDefinitions());
    assertTrue(metaData.supportsColumnAliasing());
    assertFalse(metaData.supportsConvert());
    assertFalse(metaData.supportsConvert(1, 2));
    assertFalse(metaData.supportsCoreSQLGrammar());
    assertTrue(metaData.supportsCorrelatedSubqueries());
    assertTrue(metaData.supportsDataDefinitionAndDataManipulationTransactions());
    assertFalse(metaData.supportsDataManipulationTransactionsOnly());
    assertFalse(metaData.supportsDifferentTableCorrelationNames());
    assertTrue(metaData.supportsExpressionsInOrderBy());
    assertFalse(metaData.supportsExtendedSQLGrammar());
    assertTrue(metaData.supportsFullOuterJoins());
    assertFalse(metaData.supportsGetGeneratedKeys());
    assertTrue(metaData.supportsGroupBy());
    assertTrue(metaData.supportsGroupByBeyondSelect());
    assertFalse(metaData.supportsGroupByUnrelated());
    assertFalse(metaData.supportsIntegrityEnhancementFacility());
    assertFalse(metaData.supportsLikeEscapeClause());
    assertTrue(metaData.supportsLimitedOuterJoins());
    assertFalse(metaData.supportsMinimumSQLGrammar());
    assertFalse(metaData.supportsMixedCaseIdentifiers());
    assertTrue(metaData.supportsMixedCaseQuotedIdentifiers());
    assertFalse(metaData.supportsMultipleOpenResults());
    assertFalse(metaData.supportsMultipleResultSets());
    assertTrue(metaData.supportsMultipleTransactions());
    assertFalse(metaData.supportsNamedParameters());
    assertTrue(metaData.supportsNonNullableColumns());
    assertFalse(metaData.supportsOpenCursorsAcrossCommit());
    assertFalse(metaData.supportsOpenCursorsAcrossRollback());
    assertFalse(metaData.supportsOpenStatementsAcrossCommit());
    assertFalse(metaData.supportsOpenStatementsAcrossRollback());
    assertTrue(metaData.supportsOrderByUnrelated());
    assertTrue(metaData.supportsOuterJoins());
    assertFalse(metaData.supportsPositionedDelete());
    assertFalse(metaData.supportsPositionedUpdate());
    assertTrue(
        metaData.supportsResultSetConcurrency(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    assertFalse(
        metaData.supportsResultSetConcurrency(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
    assertTrue(metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
    assertTrue(metaData.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
    assertFalse(metaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, metaData.getResultSetHoldability());
    assertFalse(metaData.supportsSavepoints());
    assertTrue(metaData.supportsSchemasInDataManipulation());
    assertFalse(metaData.supportsSchemasInIndexDefinitions());
    assertFalse(metaData.supportsSchemasInPrivilegeDefinitions());
    assertFalse(metaData.supportsSchemasInProcedureCalls());
    assertTrue(metaData.supportsSchemasInTableDefinitions());
    assertFalse(metaData.supportsSelectForUpdate());
    assertFalse(metaData.supportsStatementPooling());
    assertTrue(metaData.supportsStoredFunctionsUsingCallSyntax());
    assertTrue(metaData.supportsSubqueriesInComparisons());
    assertTrue(metaData.supportsSubqueriesInExists());
    assertTrue(metaData.supportsSubqueriesInIns());
    assertFalse(metaData.supportsSubqueriesInQuantifieds());
    assertTrue(metaData.supportsTableCorrelationNames());
    assertTrue(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
    assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
    assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
    assertFalse(
        metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
    assertTrue(metaData.supportsUnion());
    assertTrue(metaData.supportsUnionAll());
    assertFalse(metaData.updatesAreDetected(1));
    assertFalse(metaData.usesLocalFilePerTable());
    assertFalse(metaData.usesLocalFiles());
  }

  @Test
  public void testOtherEmptyTables() throws Throwable {
    DatabaseMetaData metaData = connection.getMetaData();

    // index is not supported.
    try (ResultSet resultSet = metaData.getIndexInfo(null, null, null, true, true)) {
      assertEquals(0, getSizeOfResultSet(resultSet));
    }
    // UDT is not supported.
    try (ResultSet resultSet = metaData.getUDTs(null, null, null, new int[] {})) {
      assertEquals(0, getSizeOfResultSet(resultSet));
    }
  }

  @Test
  public void testFeatureNotSupportedException() throws Throwable {
    DatabaseMetaData metaData = connection.getMetaData();
    expectFeatureNotSupportedException(
        () -> metaData.getBestRowIdentifier(null, null, null, 0, true));
    expectFeatureNotSupportedException(() -> metaData.getVersionColumns(null, null, null));
    expectFeatureNotSupportedException(() -> metaData.getSuperTypes(null, null, null));
    expectFeatureNotSupportedException(() -> metaData.getSuperTables(null, null, null));
    expectFeatureNotSupportedException(() -> metaData.getAttributes(null, null, null, null));
    expectFeatureNotSupportedException(metaData::getRowIdLifetime);
    expectFeatureNotSupportedException(metaData::autoCommitFailureClosesAllResultSets);
    expectFeatureNotSupportedException(metaData::getClientInfoProperties);
    expectFeatureNotSupportedException(() -> metaData.getPseudoColumns(null, null, null, null));
    expectFeatureNotSupportedException(metaData::generatedKeyAlwaysReturned);
    expectFeatureNotSupportedException(
        () -> metaData.isWrapperFor(SnowflakeDatabaseMetaData.class));
  }
}
