/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static java.sql.DatabaseMetaData.procedureReturnsResult;
import static java.sql.ResultSetMetaData.columnNullableUnknown;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;

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
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Database Metadata IT */
// @Category(TestCategoryOthers.class)
public class DatabaseMetaDataIT extends BaseJDBCTest {
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
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      Assertions.assertEquals(connection, metaData.getConnection());
    }
  }

  @Test
  public void testDatabaseAndDriverInfo() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();

      // identifiers
      Assertions.assertEquals("Snowflake", metaData.getDatabaseProductName());
      Assertions.assertEquals("Snowflake", metaData.getDriverName());

      // Snowflake JDBC driver version
      String driverVersion = metaData.getDriverVersion();
      Matcher m = VERSION_PATTERN.matcher(driverVersion);
      Assertions.assertTrue(m.matches());
      int majorVersion = metaData.getDriverMajorVersion();
      int minorVersion = metaData.getDriverMinorVersion();
      Assertions.assertEquals(m.group(1), String.valueOf(majorVersion));
      Assertions.assertEquals(m.group(2), String.valueOf(minorVersion));
    }
  }

  @Test
  public void testGetCatalogs() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      Assertions.assertEquals(".", metaData.getCatalogSeparator());
      Assertions.assertEquals("database", metaData.getCatalogTerm());

      ResultSet resultSet = metaData.getCatalogs();
      verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_CATALOGS);
      Assertions.assertTrue(resultSet.isBeforeFirst());

      int cnt = 0;
      Set<String> allVisibleDatabases = new HashSet<>();
      while (resultSet.next()) {
        allVisibleDatabases.add(resultSet.getString(1));
        if (cnt == 0) {
          Assertions.assertTrue(resultSet.isFirst());
        }
        ++cnt;
        try {
          resultSet.isLast();
          Assertions.fail("No isLast support for query based metadata");
        } catch (SQLFeatureNotSupportedException ex) {
          // nop
        }
        try {
          resultSet.isAfterLast();
          Assertions.fail("No isAfterLast support for query based metadata");
        } catch (SQLFeatureNotSupportedException ex) {
          // nop
        }
      }
      MatcherAssert.assertThat(cnt, greaterThanOrEqualTo(1));
      try {
        Assertions.assertTrue(resultSet.isAfterLast());
        Assertions.fail("The result set is automatically closed when all rows are fetched.");
      } catch (SQLException ex) {
        Assertions.assertEquals(
            (int) ErrorCode.RESULTSET_ALREADY_CLOSED.getMessageCode(), ex.getErrorCode());
      }
      try {
        resultSet.isAfterLast();
        Assertions.fail("No isAfterLast support for query based metadata");
      } catch (SQLException ex) {
        Assertions.assertEquals(
            (int) ErrorCode.RESULTSET_ALREADY_CLOSED.getMessageCode(), ex.getErrorCode());
      }
      resultSet.close(); // double closing does nothing.
      resultSet.next(); // no exception

      List<String> allAccessibleDatabases =
          getInfoBySQL("select database_name from information_schema.databases");

      Assertions.assertTrue(allVisibleDatabases.containsAll(allAccessibleDatabases));
    }
  }

  @Test
  public void testGetSchemas() throws Throwable {
    // CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = false
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      String currentSchema = connection.getSchema();
      Assertions.assertEquals("schema", metaData.getSchemaTerm());
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
      MatcherAssert.assertThat(schemas.size(), greaterThanOrEqualTo(1));

      Set<String> schemasInDb = new HashSet<>();
      try (ResultSet resultSet = metaData.getSchemas(connection.getCatalog(), "%")) {
        while (resultSet.next()) {
          String schema = resultSet.getString(1);
          if (currentSchema.equals(schema) || !TestUtil.isSchemaGeneratedInTests(schema)) {
            schemasInDb.add(schema);
          }
        }
      }
      MatcherAssert.assertThat(schemasInDb.size(), greaterThanOrEqualTo(1));
      MatcherAssert.assertThat(schemas.size(), greaterThanOrEqualTo(schemasInDb.size()));
      schemasInDb.forEach(schemaInDb -> MatcherAssert.assertThat(schemas, hasItem(schemaInDb)));
      Assertions.assertTrue(schemas.contains(currentSchema));
      Assertions.assertTrue(schemasInDb.contains(currentSchema));
    }

    // CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = true
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");

      DatabaseMetaData metaData = connection.getMetaData();
      Assertions.assertEquals("schema", metaData.getSchemaTerm());
      try (ResultSet resultSet = metaData.getSchemas()) {
        Set<String> schemas = new HashSet<>();
        while (resultSet.next()) {
          schemas.add(resultSet.getString(1));
        }
        MatcherAssert.assertThat(schemas.size(), equalTo(1));
      }
    }
  }

  @Test
  public void testGetTableTypes() throws Throwable {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet resultSet = metaData.getTableTypes()) {
        Set<String> types = new HashSet<>();
        while (resultSet.next()) {
          types.add(resultSet.getString(1));
        }
        Assertions.assertEquals(2, types.size());
        Assertions.assertTrue(types.contains("TABLE"));
        Assertions.assertTrue(types.contains("VIEW"));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGetTables() throws Throwable {
    Set<String> tables = null;
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
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
          Assertions.assertTrue(tables.contains("T0"));
        }
        // exact match table
        try (ResultSet resultSet =
            metaData.getTables(database, schema, targetTable, new String[] {"TABLE"})) {
          tables = new HashSet<>();
          while (resultSet.next()) {
            tables.add(resultSet.getString(3));
          }
          Assertions.assertEquals(targetTable, tables.iterator().next());
        }
        // match view
        try (ResultSet resultSet =
            metaData.getTables(database, schema, "%", new String[] {"VIEW"})) {
          Set<String> views = new HashSet<>();
          while (resultSet.next()) {
            views.add(resultSet.getString(3));
          }
          Assertions.assertTrue(views.contains(targetView));
        }

        try (ResultSet resultSet = metaData.getTablePrivileges(database, schema, targetTable)) {
          Assertions.assertEquals(1, getSizeOfResultSet(resultSet));
        }
      } finally {
        statement.execute("drop table if exists " + targetTable);
        statement.execute("drop view if exists " + targetView);
      }
    }
  }

  @Test
  public void testGetPrimarykeys() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";
      try {
        statement.execute(
            "create or replace table " + targetTable + "(C1 int primary key, C2 string)");

        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet resultSet = metaData.getPrimaryKeys(database, schema, targetTable)) {
          verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_PRIMARY_KEYS);
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals(targetTable, resultSet.getString("TABLE_NAME"));
          Assertions.assertEquals("C1", resultSet.getString("COLUMN_NAME"));
          Assertions.assertEquals(1, resultSet.getInt("KEY_SEQ"));
          Assertions.assertNotEquals("", resultSet.getString("PK_NAME"));
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
    Assertions.assertEquals(numCol, resultSetMetaData.getColumnCount());

    for (int col = 1; col <= numCol; ++col) {
      List<String> colNames = metadata.getColumnNames();
      List<String> colTypeNames = metadata.getColumnTypeNames();
      List<Integer> colTypes = metadata.getColumnTypes();

      Assertions.assertEquals("", resultSetMetaData.getCatalogName(col));
      Assertions.assertEquals("", resultSetMetaData.getSchemaName(col));
      Assertions.assertEquals("T", resultSetMetaData.getTableName(col));
      Assertions.assertEquals(colNames.get(col - 1), resultSetMetaData.getColumnName(col));

      Assertions.assertEquals(colNames.get(col - 1), resultSetMetaData.getColumnLabel(col));
      Assertions.assertEquals(
          SnowflakeType.javaTypeToClassName(resultSetMetaData.getColumnType(col)),
          resultSetMetaData.getColumnClassName(col));
      Assertions.assertEquals(25, resultSetMetaData.getColumnDisplaySize(col));
      Assertions.assertEquals((int) colTypes.get(col - 1), resultSetMetaData.getColumnType(col));
      Assertions.assertEquals(colTypeNames.get(col - 1), resultSetMetaData.getColumnTypeName(col));
      Assertions.assertEquals(9, resultSetMetaData.getPrecision(col));
      Assertions.assertEquals(9, resultSetMetaData.getScale(col));

      Assertions.assertEquals(
          SnowflakeType.isJavaTypeSigned(resultSetMetaData.getColumnType(col)),
          resultSetMetaData.isSigned(col));
      Assertions.assertFalse(resultSetMetaData.isAutoIncrement(col));
      Assertions.assertFalse(resultSetMetaData.isCurrency(col));
      Assertions.assertTrue(resultSetMetaData.isReadOnly(col));
      Assertions.assertTrue(resultSetMetaData.isSearchable(col));
      Assertions.assertFalse(resultSetMetaData.isWritable(col));
      Assertions.assertFalse(resultSetMetaData.isDefinitelyWritable(col));

      Assertions.assertEquals(columnNullableUnknown, resultSetMetaData.isNullable(col));
    }
  }

  @Test
  public void testGetImportedKeys() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
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
          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("PKTABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("PKTABLE_SCHEM"));
          Assertions.assertEquals(targetTable1, resultSet.getString("PKTABLE_NAME"));
          Assertions.assertEquals("C1", resultSet.getString("PKCOLUMN_NAME"));
          Assertions.assertEquals(database, resultSet.getString("FKTABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("FKTABLE_SCHEM"));
          Assertions.assertEquals(targetTable2, resultSet.getString("FKTABLE_NAME"));
          Assertions.assertEquals("C3", resultSet.getString("FKCOLUMN_NAME"));
          Assertions.assertEquals(1, resultSet.getInt("KEY_SEQ"));
          Assertions.assertNotEquals("", resultSet.getString("PK_NAME"));
          Assertions.assertNotEquals("", resultSet.getString("FK_NAME"));
          Assertions.assertEquals(
              DatabaseMetaData.importedKeyNoAction, resultSet.getShort("UPDATE_RULE"));
          Assertions.assertEquals(
              DatabaseMetaData.importedKeyNoAction, resultSet.getShort("DELETE_RULE"));
          Assertions.assertEquals(
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
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement(); ) {
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

          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("PKTABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("PKTABLE_SCHEM"));
          Assertions.assertEquals(targetTable1, resultSet.getString("PKTABLE_NAME"));
          Assertions.assertEquals("C1", resultSet.getString("PKCOLUMN_NAME"));
          Assertions.assertEquals(database, resultSet.getString("FKTABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("FKTABLE_SCHEM"));
          Assertions.assertEquals(targetTable2, resultSet.getString("FKTABLE_NAME"));
          Assertions.assertEquals("C3", resultSet.getString("FKCOLUMN_NAME"));
          Assertions.assertEquals(1, resultSet.getInt("KEY_SEQ"));
          Assertions.assertNotEquals("", resultSet.getString("PK_NAME"));
          Assertions.assertNotEquals("", resultSet.getString("FK_NAME"));
          Assertions.assertEquals(
              DatabaseMetaData.importedKeyNoAction, resultSet.getShort("UPDATE_RULE"));
          Assertions.assertEquals(
              DatabaseMetaData.importedKeyNoAction, resultSet.getShort("DELETE_RULE"));
          Assertions.assertEquals(
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
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
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

          Assertions.assertTrue(resultSet.next());
          Assertions.assertEquals(database, resultSet.getString("PKTABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("PKTABLE_SCHEM"));
          Assertions.assertEquals(targetTable1, resultSet.getString("PKTABLE_NAME"));
          Assertions.assertEquals("C1", resultSet.getString("PKCOLUMN_NAME"));
          Assertions.assertEquals(database, resultSet.getString("FKTABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("FKTABLE_SCHEM"));
          Assertions.assertEquals(targetTable2, resultSet.getString("FKTABLE_NAME"));
          Assertions.assertEquals("C3", resultSet.getString("FKCOLUMN_NAME"));
          Assertions.assertEquals(1, resultSet.getInt("KEY_SEQ"));
          Assertions.assertNotEquals("", resultSet.getString("PK_NAME"));
          Assertions.assertNotEquals("", resultSet.getString("FK_NAME"));
          Assertions.assertEquals(
              DatabaseMetaData.importedKeyNoAction, resultSet.getShort("UPDATE_RULE"));
          Assertions.assertEquals(
              DatabaseMetaData.importedKeyNoAction, resultSet.getShort("DELETE_RULE"));
          Assertions.assertEquals(
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
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
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
          Assertions.assertTrue(getSizeOfResultSet(resultSet) > 0);
        }

        // invalid object type. empty result is expected.
        try (ResultSet resultSet =
            metaData.getTables(database, schema, "%", new String[] {"INVALID_TYPE"})) {
          Assertions.assertEquals(0, getSizeOfResultSet(resultSet));
        }

        // rest of the cases should return empty results.
        try (ResultSet resultSet = metaData.getSchemas("DB_NOT_EXIST", "SCHEMA_NOT_EXIST")) {
          Assertions.assertFalse(resultSet.next());
          Assertions.assertTrue(resultSet.isClosed());
        }

        try (ResultSet resultSet =
            metaData.getTables("DB_NOT_EXIST", "SCHEMA_NOT_EXIST", "%", null)) {
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            metaData.getTables(database, "SCHEMA\\_NOT\\_EXIST", "%", null)) {
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            metaData.getColumns("DB_NOT_EXIST", "SCHEMA_NOT_EXIST", "%", "%")) {
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            metaData.getColumns(database, "SCHEMA\\_NOT\\_EXIST", "%", "%")) {
          Assertions.assertFalse(resultSet.next());
        }

        try (ResultSet resultSet =
            metaData.getColumns(database, schema, "TBL\\_NOT\\_EXIST", "%")) {
          Assertions.assertFalse(resultSet.next());
        }
      } finally {
        statement.execute("drop table if exists " + targetTable);
        statement.execute("drop view if exists " + targetView);
      }
    }
  }

  @Test
  public void testTypeInfo() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet resultSet = metaData.getTypeInfo();
      resultSet.next();
      Assertions.assertEquals("NUMBER", resultSet.getString(1));
      resultSet.next();
      Assertions.assertEquals("INTEGER", resultSet.getString(1));
      resultSet.next();
      Assertions.assertEquals("DOUBLE", resultSet.getString(1));
      resultSet.next();
      Assertions.assertEquals("VARCHAR", resultSet.getString(1));
      resultSet.next();
      Assertions.assertEquals("DATE", resultSet.getString(1));
      resultSet.next();
      Assertions.assertEquals("TIME", resultSet.getString(1));
      resultSet.next();
      Assertions.assertEquals("TIMESTAMP", resultSet.getString(1));
      resultSet.next();
      Assertions.assertEquals("BOOLEAN", resultSet.getString(1));
      Assertions.assertFalse(resultSet.next());
    }
  }

  @Test
  public void testProcedure() throws Throwable {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      Assertions.assertEquals("procedure", metaData.getProcedureTerm());
      // no stored procedure support
      Assertions.assertTrue(metaData.supportsStoredProcedures());
      try (ResultSet resultSet = metaData.getProcedureColumns("%", "%", "%", "%")) {
        Assertions.assertEquals(0, getSizeOfResultSet(resultSet));
      }
      try (ResultSet resultSet = metaData.getProcedures("%", "%", "%")) {
        Assertions.assertEquals(0, getSizeOfResultSet(resultSet));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testGetTablePrivileges() throws Exception {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
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
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals("PRIVTEST", resultSet.getString("TABLE_NAME"));
          Assertions.assertEquals("SYSADMIN", resultSet.getString("GRANTOR"));
          Assertions.assertEquals("SYSADMIN", resultSet.getString("GRANTEE"));
          Assertions.assertEquals("OWNERSHIP", resultSet.getString("PRIVILEGE"));
          Assertions.assertEquals("YES", resultSet.getString("IS_GRANTABLE"));
        }
        // grant select privileges to table for role security admin and test that a new row of table
        // privileges is added
        statement.execute("grant select on table PRIVTEST to role securityadmin");
        try (ResultSet resultSet = metaData.getTablePrivileges(database, schema, "PRIVTEST")) {
          resultSet.next();
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals("PRIVTEST", resultSet.getString("TABLE_NAME"));
          Assertions.assertEquals("SYSADMIN", resultSet.getString("GRANTOR"));
          Assertions.assertEquals("SYSADMIN", resultSet.getString("GRANTEE"));
          Assertions.assertEquals("OWNERSHIP", resultSet.getString("PRIVILEGE"));
          Assertions.assertEquals("YES", resultSet.getString("IS_GRANTABLE"));
          resultSet.next();
          Assertions.assertEquals(database, resultSet.getString("TABLE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
          Assertions.assertEquals("PRIVTEST", resultSet.getString("TABLE_NAME"));
          Assertions.assertEquals("SYSADMIN", resultSet.getString("GRANTOR"));
          Assertions.assertEquals("SECURITYADMIN", resultSet.getString("GRANTEE"));
          Assertions.assertEquals("SELECT", resultSet.getString("PRIVILEGE"));
          Assertions.assertEquals("NO", resultSet.getString("IS_GRANTABLE"));
        }

        // if tableNamePattern is null, empty resultSet is returned.
        try (ResultSet resultSet = metaData.getTablePrivileges(null, null, null)) {
          Assertions.assertEquals(7, resultSet.getMetaData().getColumnCount());
          Assertions.assertEquals(0, getSizeOfResultSet(resultSet));
        }
      } finally {
        statement.execute("drop table if exists PRIVTEST");
      }
    }
  }

  @Test
  public void testGetProcedures() throws SQLException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
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
          Assertions.assertEquals("GETPI", resultSet.getString("PROCEDURE_NAME"));
          Assertions.assertEquals(database, resultSet.getString("PROCEDURE_CAT"));
          Assertions.assertEquals(schema, resultSet.getString("PROCEDURE_SCHEM"));
          Assertions.assertEquals("GETPI", resultSet.getString("PROCEDURE_NAME"));
          Assertions.assertEquals("user-defined procedure", resultSet.getString("REMARKS"));
          Assertions.assertEquals(procedureReturnsResult, resultSet.getShort("PROCEDURE_TYPE"));
          Assertions.assertEquals("GETPI() RETURN FLOAT", resultSet.getString("SPECIFIC_NAME"));
        }
      } finally {
        statement.execute("drop procedure if exists GETPI()");
      }
    }
  }

  @Test
  public void testDatabaseMetadata() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();

      String dbVersion = metaData.getDatabaseProductVersion();
      Matcher m = VERSION_PATTERN.matcher(dbVersion);
      Assertions.assertTrue(m.matches());
      int majorVersion = metaData.getDatabaseMajorVersion();
      int minorVersion = metaData.getDatabaseMinorVersion();
      Assertions.assertEquals(m.group(1), String.valueOf(majorVersion));
      Assertions.assertEquals(m.group(2), String.valueOf(minorVersion));

      Assertions.assertFalse(Strings.isNullOrEmpty(metaData.getSQLKeywords()));
      Assertions.assertFalse(Strings.isNullOrEmpty(metaData.getNumericFunctions()));
      Assertions.assertFalse(Strings.isNullOrEmpty(metaData.getStringFunctions()));
      Assertions.assertFalse(Strings.isNullOrEmpty(metaData.getSystemFunctions()));
      Assertions.assertFalse(Strings.isNullOrEmpty(metaData.getTimeDateFunctions()));

      Assertions.assertEquals("\\", metaData.getSearchStringEscape());

      Assertions.assertTrue(metaData.getURL().startsWith("jdbc:snowflake://"));
      Assertions.assertFalse(metaData.allProceduresAreCallable());
      Assertions.assertTrue(metaData.allTablesAreSelectable());
      Assertions.assertTrue(metaData.dataDefinitionCausesTransactionCommit());
      Assertions.assertFalse(metaData.dataDefinitionIgnoredInTransactions());
      Assertions.assertFalse(metaData.deletesAreDetected(1));
      Assertions.assertTrue(metaData.doesMaxRowSizeIncludeBlobs());
      Assertions.assertTrue(metaData.supportsTransactions());
      Assertions.assertEquals(
          Connection.TRANSACTION_READ_COMMITTED, metaData.getDefaultTransactionIsolation());
      Assertions.assertEquals("$", metaData.getExtraNameCharacters());
      Assertions.assertEquals("\"", metaData.getIdentifierQuoteString());
      Assertions.assertEquals(
          0, getSizeOfResultSet(metaData.getIndexInfo(null, null, null, true, true)));
      Assertions.assertEquals(EXPECTED_MAX_BINARY_LENGTH, metaData.getMaxBinaryLiteralLength());
      Assertions.assertEquals(255, metaData.getMaxCatalogNameLength());
      Assertions.assertEquals(EXPECTED_MAX_CHAR_LENGTH, metaData.getMaxCharLiteralLength());
      Assertions.assertEquals(255, metaData.getMaxColumnNameLength());
      Assertions.assertEquals(0, metaData.getMaxColumnsInGroupBy());
      Assertions.assertEquals(0, metaData.getMaxColumnsInIndex());
      Assertions.assertEquals(0, metaData.getMaxColumnsInOrderBy());
      Assertions.assertEquals(0, metaData.getMaxColumnsInSelect());
      Assertions.assertEquals(0, metaData.getMaxColumnsInTable());
      Assertions.assertEquals(0, metaData.getMaxConnections());
      Assertions.assertEquals(0, metaData.getMaxCursorNameLength());
      Assertions.assertEquals(0, metaData.getMaxIndexLength());
      Assertions.assertEquals(0, metaData.getMaxProcedureNameLength());
      Assertions.assertEquals(0, metaData.getMaxRowSize());
      Assertions.assertEquals(255, metaData.getMaxSchemaNameLength());
      Assertions.assertEquals(0, metaData.getMaxStatementLength());
      Assertions.assertEquals(0, metaData.getMaxStatements());
      Assertions.assertEquals(255, metaData.getMaxTableNameLength());
      Assertions.assertEquals(0, metaData.getMaxTablesInSelect());
      Assertions.assertEquals(255, metaData.getMaxUserNameLength());
      Assertions.assertEquals(0, getSizeOfResultSet(metaData.getTablePrivileges(null, null, null)));
      // assertEquals("", metaData.getTimeDateFunctions());
      Assertions.assertEquals(TestUtil.systemGetEnv("SNOWFLAKE_TEST_USER"), metaData.getUserName());
      Assertions.assertFalse(metaData.insertsAreDetected(1));
      Assertions.assertTrue(metaData.isCatalogAtStart());
      Assertions.assertFalse(metaData.isReadOnly());
      Assertions.assertTrue(metaData.nullPlusNonNullIsNull());
      Assertions.assertFalse(metaData.nullsAreSortedAtEnd());
      Assertions.assertFalse(metaData.nullsAreSortedAtStart());
      Assertions.assertTrue(metaData.nullsAreSortedHigh());
      Assertions.assertFalse(metaData.nullsAreSortedLow());
      Assertions.assertFalse(metaData.othersDeletesAreVisible(1));
      Assertions.assertFalse(metaData.othersInsertsAreVisible(1));
      Assertions.assertFalse(metaData.othersUpdatesAreVisible(1));
      Assertions.assertFalse(metaData.ownDeletesAreVisible(1));
      Assertions.assertFalse(metaData.ownInsertsAreVisible(1));
      Assertions.assertFalse(metaData.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
      Assertions.assertFalse(metaData.storesLowerCaseIdentifiers());
      Assertions.assertFalse(metaData.storesLowerCaseQuotedIdentifiers());
      Assertions.assertFalse(metaData.storesMixedCaseIdentifiers());
      Assertions.assertTrue(metaData.storesMixedCaseQuotedIdentifiers());
      Assertions.assertTrue(metaData.storesUpperCaseIdentifiers());
      Assertions.assertFalse(metaData.storesUpperCaseQuotedIdentifiers());
      Assertions.assertTrue(metaData.supportsAlterTableWithAddColumn());
      Assertions.assertTrue(metaData.supportsAlterTableWithDropColumn());
      Assertions.assertTrue(metaData.supportsANSI92EntryLevelSQL());
      Assertions.assertFalse(metaData.supportsANSI92FullSQL());
      Assertions.assertFalse(metaData.supportsANSI92IntermediateSQL());
      Assertions.assertTrue(metaData.supportsBatchUpdates());
      Assertions.assertTrue(metaData.supportsCatalogsInDataManipulation());
      Assertions.assertFalse(metaData.supportsCatalogsInIndexDefinitions());
      Assertions.assertFalse(metaData.supportsCatalogsInPrivilegeDefinitions());
      Assertions.assertFalse(metaData.supportsCatalogsInProcedureCalls());
      Assertions.assertTrue(metaData.supportsCatalogsInTableDefinitions());
      Assertions.assertTrue(metaData.supportsColumnAliasing());
      Assertions.assertFalse(metaData.supportsConvert());
      Assertions.assertFalse(metaData.supportsConvert(1, 2));
      Assertions.assertFalse(metaData.supportsCoreSQLGrammar());
      Assertions.assertTrue(metaData.supportsCorrelatedSubqueries());
      Assertions.assertTrue(metaData.supportsDataDefinitionAndDataManipulationTransactions());
      Assertions.assertFalse(metaData.supportsDataManipulationTransactionsOnly());
      Assertions.assertFalse(metaData.supportsDifferentTableCorrelationNames());
      Assertions.assertTrue(metaData.supportsExpressionsInOrderBy());
      Assertions.assertFalse(metaData.supportsExtendedSQLGrammar());
      Assertions.assertTrue(metaData.supportsFullOuterJoins());
      Assertions.assertFalse(metaData.supportsGetGeneratedKeys());
      Assertions.assertTrue(metaData.supportsGroupBy());
      Assertions.assertTrue(metaData.supportsGroupByBeyondSelect());
      Assertions.assertFalse(metaData.supportsGroupByUnrelated());
      Assertions.assertFalse(metaData.supportsIntegrityEnhancementFacility());
      Assertions.assertFalse(metaData.supportsLikeEscapeClause());
      Assertions.assertTrue(metaData.supportsLimitedOuterJoins());
      Assertions.assertFalse(metaData.supportsMinimumSQLGrammar());
      Assertions.assertFalse(metaData.supportsMixedCaseIdentifiers());
      Assertions.assertTrue(metaData.supportsMixedCaseQuotedIdentifiers());
      Assertions.assertFalse(metaData.supportsMultipleOpenResults());
      Assertions.assertFalse(metaData.supportsMultipleResultSets());
      Assertions.assertTrue(metaData.supportsMultipleTransactions());
      Assertions.assertFalse(metaData.supportsNamedParameters());
      Assertions.assertTrue(metaData.supportsNonNullableColumns());
      Assertions.assertFalse(metaData.supportsOpenCursorsAcrossCommit());
      Assertions.assertFalse(metaData.supportsOpenCursorsAcrossRollback());
      Assertions.assertFalse(metaData.supportsOpenStatementsAcrossCommit());
      Assertions.assertFalse(metaData.supportsOpenStatementsAcrossRollback());
      Assertions.assertTrue(metaData.supportsOrderByUnrelated());
      Assertions.assertTrue(metaData.supportsOuterJoins());
      Assertions.assertFalse(metaData.supportsPositionedDelete());
      Assertions.assertFalse(metaData.supportsPositionedUpdate());
      Assertions.assertTrue(
          metaData.supportsResultSetConcurrency(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
      Assertions.assertFalse(
          metaData.supportsResultSetConcurrency(
              ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
      Assertions.assertTrue(metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
      Assertions.assertTrue(
          metaData.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
      Assertions.assertFalse(
          metaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
      Assertions.assertEquals(
          ResultSet.CLOSE_CURSORS_AT_COMMIT, metaData.getResultSetHoldability());
      Assertions.assertFalse(metaData.supportsSavepoints());
      Assertions.assertTrue(metaData.supportsSchemasInDataManipulation());
      Assertions.assertFalse(metaData.supportsSchemasInIndexDefinitions());
      Assertions.assertFalse(metaData.supportsSchemasInPrivilegeDefinitions());
      Assertions.assertFalse(metaData.supportsSchemasInProcedureCalls());
      Assertions.assertTrue(metaData.supportsSchemasInTableDefinitions());
      Assertions.assertFalse(metaData.supportsSelectForUpdate());
      Assertions.assertFalse(metaData.supportsStatementPooling());
      Assertions.assertTrue(metaData.supportsStoredFunctionsUsingCallSyntax());
      Assertions.assertTrue(metaData.supportsSubqueriesInComparisons());
      Assertions.assertTrue(metaData.supportsSubqueriesInExists());
      Assertions.assertTrue(metaData.supportsSubqueriesInIns());
      Assertions.assertFalse(metaData.supportsSubqueriesInQuantifieds());
      Assertions.assertTrue(metaData.supportsTableCorrelationNames());
      Assertions.assertTrue(
          metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
      Assertions.assertFalse(
          metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
      Assertions.assertFalse(
          metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
      Assertions.assertFalse(
          metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
      Assertions.assertTrue(metaData.supportsUnion());
      Assertions.assertTrue(metaData.supportsUnionAll());
      Assertions.assertFalse(metaData.updatesAreDetected(1));
      Assertions.assertFalse(metaData.usesLocalFilePerTable());
      Assertions.assertFalse(metaData.usesLocalFiles());
    }
  }

  @Test
  public void testOtherEmptyTables() throws Throwable {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();

      // index is not supported.
      try (ResultSet resultSet = metaData.getIndexInfo(null, null, null, true, true)) {
        Assertions.assertEquals(0, getSizeOfResultSet(resultSet));
      }
      // UDT is not supported.
      try (ResultSet resultSet = metaData.getUDTs(null, null, null, new int[] {})) {
        Assertions.assertEquals(0, getSizeOfResultSet(resultSet));
      }
    }
  }

  @Test
  public void testFeatureNotSupportedException() throws Throwable {
    try (Connection connection = getConnection()) {
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
}
