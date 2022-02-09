/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static java.sql.DatabaseMetaData.procedureReturnsResult;
import static java.sql.ResultSetMetaData.columnNullableUnknown;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.*;

import com.google.common.base.Strings;
import java.sql.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.TestUtil;
import net.snowflake.client.category.TestCategoryOthers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Database Metadata IT */
@Category(TestCategoryOthers.class)
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

  @Test
  public void testGetConnection() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      assertEquals(connection, metaData.getConnection());
    }
  }

  @Test
  public void testDatabaseAndDriverInfo() throws SQLException {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();

      // JDBC x.x compatible
      assertEquals(1, metaData.getJDBCMajorVersion());
      assertEquals(0, metaData.getJDBCMinorVersion());

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
  }

  @Test
  public void testGetCatalogs() throws SQLException {
    try (Connection connection = getConnection()) {
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
        try {
          resultSet.isLast();
          fail("No isLast support for query based metadata");
        } catch (SQLFeatureNotSupportedException ex) {
          // nop
        }
        try {
          resultSet.isAfterLast();
          fail("No isAfterLast support for query based metadata");
        } catch (SQLFeatureNotSupportedException ex) {
          // nop
        }
      }
      assertThat(cnt, greaterThanOrEqualTo(1));
      try {
        assertTrue(resultSet.isAfterLast());
        fail("The result set is automatically closed when all rows are fetched.");
      } catch (SQLException ex) {
        assertEquals((int) ErrorCode.RESULTSET_ALREADY_CLOSED.getMessageCode(), ex.getErrorCode());
      }
      try {
        resultSet.isAfterLast();
        fail("No isAfterLast support for query based metadata");
      } catch (SQLException ex) {
        assertEquals((int) ErrorCode.RESULTSET_ALREADY_CLOSED.getMessageCode(), ex.getErrorCode());
      }
      resultSet.close(); // double closing does nothing.
      resultSet.next(); // no exception

      List<String> allAccessibleDatabases =
          getInfoBySQL("select database_name from information_schema.databases");

      assertTrue(allVisibleDatabases.containsAll(allAccessibleDatabases));
    }
  }

  @Test
  public void testGetSchemas() throws Throwable {
    // CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = false
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      assertEquals("schema", metaData.getSchemaTerm());
      ResultSet resultSet = metaData.getSchemas();
      verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_SCHEMAS);

      Set<String> schemas = new HashSet<>();
      while (resultSet.next()) {
        schemas.add(resultSet.getString(1));
      }
      resultSet.close();
      assertThat(schemas.size(), greaterThanOrEqualTo(1)); // one or more schemas

      Set<String> schemasInDb = new HashSet<>();
      resultSet = metaData.getSchemas(connection.getCatalog(), "%");
      while (resultSet.next()) {
        schemasInDb.add(resultSet.getString(1));
      }
      assertThat(schemasInDb.size(), greaterThanOrEqualTo(1)); // one or more schemas
      assertThat(schemas.size(), greaterThanOrEqualTo(schemasInDb.size()));
      assertTrue(schemas.containsAll(schemasInDb));
      assertTrue(schemas.contains(connection.getSchema()));
    }

    // CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = true
    try (Connection connection = getConnection()) {
      Statement statement = connection.createStatement();
      statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");

      DatabaseMetaData metaData = connection.getMetaData();
      assertEquals("schema", metaData.getSchemaTerm());
      ResultSet resultSet = metaData.getSchemas();
      Set<String> schemas = new HashSet<>();
      while (resultSet.next()) {
        schemas.add(resultSet.getString(1));
      }
      assertThat(schemas.size(), equalTo(1)); // more than 1 schema
    }
  }

  @Test
  public void testGetTableTypes() throws Throwable {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet resultSet = metaData.getTableTypes();
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
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetTables() throws Throwable {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";
      final String targetView = "V0";

      connection.createStatement().execute("create or replace table " + targetTable + "(C1 int)");
      connection
          .createStatement()
          .execute("create or replace view " + targetView + " as select 1 as C");

      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet;

      // match table
      resultSet = metaData.getTables(database, schema, "%", new String[] {"TABLE"});
      verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_TABLES);
      Set<String> tables = new HashSet<>();
      while (resultSet.next()) {
        tables.add(resultSet.getString(3));
      }
      assertTrue(tables.contains("T0"));

      // exact match table
      resultSet = metaData.getTables(database, schema, targetTable, new String[] {"TABLE"});
      tables = new HashSet<>();
      while (resultSet.next()) {
        tables.add(resultSet.getString(3));
      }
      assertEquals(targetTable, tables.iterator().next());

      // match view
      resultSet = metaData.getTables(database, schema, "%", new String[] {"VIEW"});
      Set<String> views = new HashSet<>();
      while (resultSet.next()) {
        views.add(resultSet.getString(3));
      }
      assertTrue(views.contains(targetView));

      resultSet = metaData.getTablePrivileges(database, schema, targetTable);
      assertEquals(1, getSizeOfResultSet(resultSet));

      connection.createStatement().execute("drop table if exists " + targetTable);
      connection.createStatement().execute("drop view if exists " + targetView);
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
      assertEquals(Types.TIMESTAMP, resultSet.getInt("DATA_TYPE"));
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
  public void testGetPrimarykeys() throws Throwable {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";

      connection
          .createStatement()
          .execute("create or replace table " + targetTable + "(C1 int primary key, C2 string)");

      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet;

      resultSet = metaData.getPrimaryKeys(database, schema, targetTable);
      verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_PRIMARY_KEYS);
      assertTrue(resultSet.next());
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals(targetTable, resultSet.getString("TABLE_NAME"));
      assertEquals("C1", resultSet.getString("COLUMN_NAME"));
      assertEquals(1, resultSet.getInt("KEY_SEQ"));
      assertNotEquals("", resultSet.getString("PK_NAME"));

      connection.createStatement().execute("drop table if exists " + targetTable);
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
      assertFalse(resultSetMetaData.isCaseSensitive(col));
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
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable1 = "T0";
      final String targetTable2 = "T1";

      connection
          .createStatement()
          .execute("create or replace table " + targetTable1 + "(C1 int primary key, C2 string)");
      connection
          .createStatement()
          .execute(
              "create or replace table "
                  + targetTable2
                  + "(C1 int primary key, C2 string, C3 int references "
                  + targetTable1
                  + ")");

      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet = metaData.getImportedKeys(database, schema, targetTable2);
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
      assertEquals(DatabaseMetaData.importedKeyNotDeferrable, resultSet.getShort("DEFERRABILITY"));

      connection.createStatement().execute("drop table if exists " + targetTable1);
      connection.createStatement().execute("drop table if exists " + targetTable2);
    }
  }

  @Test
  public void testGetExportedKeys() throws Throwable {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable1 = "T0";
      final String targetTable2 = "T1";

      connection
          .createStatement()
          .execute("create or replace table " + targetTable1 + "(C1 int primary key, C2 string)");
      connection
          .createStatement()
          .execute(
              "create or replace table "
                  + targetTable2
                  + "(C1 int primary key, C2 string, C3 int references "
                  + targetTable1
                  + ")");

      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet = metaData.getExportedKeys(database, schema, targetTable1);
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
      assertEquals(DatabaseMetaData.importedKeyNotDeferrable, resultSet.getShort("DEFERRABILITY"));

      connection.createStatement().execute("drop table if exists " + targetTable1);
      connection.createStatement().execute("drop table if exists " + targetTable2);
    }
  }

  @Test
  public void testGetCrossReferences() throws Throwable {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable1 = "T0";
      final String targetTable2 = "T1";

      connection
          .createStatement()
          .execute("create or replace table " + targetTable1 + "(C1 int primary key, C2 string)");
      connection
          .createStatement()
          .execute(
              "create or replace table "
                  + targetTable2
                  + "(C1 int primary key, C2 string, C3 int references "
                  + targetTable1
                  + ")");

      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet =
          metaData.getCrossReference(
              database, schema, targetTable1, database, schema, targetTable2);
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
      assertEquals(DatabaseMetaData.importedKeyNotDeferrable, resultSet.getShort("DEFERRABILITY"));

      connection.createStatement().execute("drop table if exists " + targetTable1);
      connection.createStatement().execute("drop table if exists " + targetTable2);
    }
  }

  @Test
  public void testGetObjectsDoesNotExists() throws Throwable {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";
      final String targetView = "V0";

      connection.createStatement().execute("create or replace table " + targetTable + "(C1 int)");
      connection
          .createStatement()
          .execute("create or replace view " + targetView + " as select 1 as C");

      DatabaseMetaData metaData = connection.getMetaData();

      // sanity check if getTables really works.
      ResultSet resultSet = metaData.getTables(database, schema, "%", null);
      assertTrue(getSizeOfResultSet(resultSet) > 0);

      // invalid object type. empty result is expected.
      resultSet = metaData.getTables(database, schema, "%", new String[] {"INVALID_TYPE"});
      assertEquals(0, getSizeOfResultSet(resultSet));

      // rest of the cases should return empty results.
      resultSet = metaData.getSchemas("DB_NOT_EXIST", "SCHEMA_NOT_EXIST");
      assertFalse(resultSet.next());
      assertTrue(resultSet.isClosed());

      resultSet = metaData.getTables("DB_NOT_EXIST", "SCHEMA_NOT_EXIST", "%", null);
      assertFalse(resultSet.next());

      resultSet = metaData.getTables(database, "SCHEMA\\_NOT\\_EXIST", "%", null);
      assertFalse(resultSet.next());

      resultSet = metaData.getColumns("DB_NOT_EXIST", "SCHEMA_NOT_EXIST", "%", "%");
      assertFalse(resultSet.next());

      resultSet = metaData.getColumns(database, "SCHEMA\\_NOT\\_EXIST", "%", "%");
      assertFalse(resultSet.next());

      resultSet = metaData.getColumns(database, schema, "TBL\\_NOT\\_EXIST", "%");
      assertFalse(resultSet.next());
      connection.createStatement().execute("drop table if exists " + targetTable);
      connection.createStatement().execute("drop view if exists " + targetView);
    }
  }

  @Test
  public void testTypeInfo() throws SQLException {

    try (Connection connection = getConnection()) {
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
  }

  @Test
  public void testProcedure() throws Throwable {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      assertEquals("procedure", metaData.getProcedureTerm());
      // no stored procedure support
      assertTrue(metaData.supportsStoredProcedures());
      ResultSet resultSet;
      resultSet = metaData.getProcedureColumns("%", "%", "%", "%");
      assertEquals(0, getSizeOfResultSet(resultSet));
      resultSet = metaData.getProcedures("%", "%", "%");
      assertEquals(0, getSizeOfResultSet(resultSet));
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetTablePrivileges() throws Exception {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      connection
          .createStatement()
          .execute(
              "create or replace table PRIVTEST(colA string, colB number, colC " + "timestamp)");
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet resultSet = metaData.getTablePrivileges(database, schema, "PRIVTEST");
      verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_TABLE_PRIVILEGES);
      resultSet.next();
      assertEquals(database, resultSet.getString("TABLE_CAT"));
      assertEquals(schema, resultSet.getString("TABLE_SCHEM"));
      assertEquals("PRIVTEST", resultSet.getString("TABLE_NAME"));
      assertEquals("SYSADMIN", resultSet.getString("GRANTOR"));
      assertEquals("SYSADMIN", resultSet.getString("GRANTEE"));
      assertEquals("OWNERSHIP", resultSet.getString("PRIVILEGE"));
      assertEquals("YES", resultSet.getString("IS_GRANTABLE"));
      // grant select privileges to table for role security admin and test that a new row of table
      // privileges is added
      connection.createStatement().execute("grant select on table PRIVTEST to role securityadmin");
      resultSet = metaData.getTablePrivileges(database, schema, "PRIVTEST");
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
      // if tableNamePattern is null, empty resultSet is returned.
      resultSet = metaData.getTablePrivileges(null, null, null);
      assertEquals(7, resultSet.getMetaData().getColumnCount());
      assertEquals(0, getSizeOfResultSet(resultSet));
      connection.createStatement().execute("drop table if exists PRIVTEST");
    }
  }

  @Test
  public void testGetProcedures() throws SQLException {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();

      /* Create a procedure and put values into it */
      connection.createStatement().execute(PI_PROCEDURE);
      DatabaseMetaData metaData = connection.getMetaData();
      /* Call getFunctionColumns on FUNC111 and since there's no parameter name, get all rows back */
      ResultSet resultSet = metaData.getProcedures(database, schema, "GETPI");
      verifyResultSetMetaDataColumns(resultSet, DBMetadataResultSetMetadata.GET_PROCEDURES);
      resultSet.next();
      assertEquals("GETPI", resultSet.getString("PROCEDURE_NAME"));
      assertEquals(database, resultSet.getString("PROCEDURE_CAT"));
      assertEquals(schema, resultSet.getString("PROCEDURE_SCHEM"));
      assertEquals("GETPI", resultSet.getString("PROCEDURE_NAME"));
      assertEquals("user-defined procedure", resultSet.getString("REMARKS"));
      assertEquals(procedureReturnsResult, resultSet.getShort("PROCEDURE_TYPE"));
      assertEquals("GETPI() RETURN FLOAT", resultSet.getString("SPECIFIC_NAME"));
      connection.createStatement().execute("drop procedure if exists GETPI()");
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testGetProcedureColumns() throws Exception {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      connection.createStatement().execute(PI_PROCEDURE);
      connection.createStatement().execute(STPROC1_PROCEDURE);
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
      resultSet = metaData.getProcedureColumns(database, schema, "STPROC1", "%");
      resultSet.next();
      assertEquals(database, resultSet.getString("PROCEDURE_CAT"));
      assertEquals(schema, resultSet.getString("PROCEDURE_SCHEM"));
      assertEquals("STPROC1", resultSet.getString("PROCEDURE_NAME"));
      assertEquals("", resultSet.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.procedureColumnOut, resultSet.getInt("COLUMN_TYPE"));
      assertEquals(Types.OTHER, resultSet.getInt("DATA_TYPE"));
      assertEquals("TABLE (RETVAL VARCHAR)", resultSet.getString("TYPE_NAME"));
      assertEquals(0, resultSet.getInt("PRECISION"));
      // length column is not supported and will always be 0
      assertEquals(0, resultSet.getInt("LENGTH"));
      assertEquals(0, resultSet.getShort("SCALE"));
      // radix column is not supported and will always be default of 10 (assumes base 10 system)
      assertEquals(10, resultSet.getInt("RADIX"));
      // nullable column is not supported and always returns NullableUnknown
      assertEquals(DatabaseMetaData.procedureNullable, resultSet.getInt("NULLABLE"));
      assertEquals("user-defined procedure", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));
      assertEquals(0, resultSet.getInt("SQL_DATA_TYPE"));
      assertEquals(0, resultSet.getInt("SQL_DATETIME_SUB"));
      // char octet length column is not supported and always returns 0
      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(0, resultSet.getInt("ORDINAL_POSITION"));
      // is_nullable column is not supported and always returns empty string
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertEquals(
          "STPROC1(FLOAT, VARCHAR) RETURN TABLE (RETVAL VARCHAR)",
          resultSet.getString("SPECIFIC_NAME"));
      resultSet.next();
      assertEquals(database, resultSet.getString("PROCEDURE_CAT"));
      assertEquals(schema, resultSet.getString("PROCEDURE_SCHEM"));
      assertEquals("STPROC1", resultSet.getString("PROCEDURE_NAME"));
      assertEquals("PARAM1", resultSet.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.procedureColumnIn, resultSet.getInt("COLUMN_TYPE"));
      assertEquals(Types.FLOAT, resultSet.getInt("DATA_TYPE"));
      assertEquals("FLOAT", resultSet.getString("TYPE_NAME"));
      assertEquals(38, resultSet.getInt("PRECISION"));
      // length column is not supported and will always be 0
      assertEquals(0, resultSet.getInt("LENGTH"));
      assertEquals(0, resultSet.getShort("SCALE"));
      // radix column is not supported and will always be default of 10 (assumes base 10 system)
      assertEquals(10, resultSet.getInt("RADIX"));
      // nullable column is not supported and always returns NullableUnknown
      assertEquals(DatabaseMetaData.procedureNullableUnknown, resultSet.getInt("NULLABLE"));
      assertEquals("user-defined procedure", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));
      assertEquals(0, resultSet.getInt("SQL_DATA_TYPE"));
      assertEquals(0, resultSet.getInt("SQL_DATETIME_SUB"));
      // char octet length column is not supported and always returns 0
      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(1, resultSet.getInt("ORDINAL_POSITION"));
      // is_nullable column is not supported and always returns empty string
      assertEquals("", resultSet.getString("IS_NULLABLE"));
      assertEquals(
          "STPROC1(FLOAT, VARCHAR) RETURN TABLE (RETVAL VARCHAR)",
          resultSet.getString("SPECIFIC_NAME"));
      resultSet.next();
      assertEquals(database, resultSet.getString("PROCEDURE_CAT"));
      assertEquals(schema, resultSet.getString("PROCEDURE_SCHEM"));
      assertEquals("STPROC1", resultSet.getString("PROCEDURE_NAME"));
      assertEquals("PARAM2", resultSet.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.procedureColumnIn, resultSet.getInt("COLUMN_TYPE"));
      assertEquals(Types.VARCHAR, resultSet.getInt("DATA_TYPE"));
      assertEquals("VARCHAR", resultSet.getString("TYPE_NAME"));
      assertEquals(0, resultSet.getInt("PRECISION"));
      // length column is not supported and will always be 0
      assertEquals(0, resultSet.getInt("LENGTH"));
      assertEquals(0, resultSet.getShort("SCALE"));
      // radix column is not supported and will always be default of 10 (assumes base 10 system)
      assertEquals(10, resultSet.getInt("RADIX"));
      // nullable column is not supported and always returns NullableUnknown
      assertEquals(DatabaseMetaData.procedureNullableUnknown, resultSet.getInt("NULLABLE"));
      assertEquals("user-defined procedure", resultSet.getString("REMARKS"));
      assertNull(resultSet.getString("COLUMN_DEF"));
      assertEquals(0, resultSet.getInt("SQL_DATA_TYPE"));
      assertEquals(0, resultSet.getInt("SQL_DATETIME_SUB"));
      // char octet length column is not supported and always returns 0
      assertEquals(16777216, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(2, resultSet.getInt("ORDINAL_POSITION"));
      // is_nullable column is not supported and always returns empty string
      assertEquals("", resultSet.getString("IS_NULLABLE"));
      assertEquals(
          "STPROC1(FLOAT, VARCHAR) RETURN TABLE (RETVAL VARCHAR)",
          resultSet.getString("SPECIFIC_NAME"));
      assertFalse(resultSet.next());
      /* Call getProcedureColumns with specified procedure name and column name. This will return 2 rows: 1 for the
      result values of the procedure, and 1 for the parameter values for the specified parameter. */
      resultSet = metaData.getProcedureColumns(null, null, "STPROC1", "PARAM2");
      assertEquals(20, resultSet.getMetaData().getColumnCount());
      assertEquals(2, getSizeOfResultSet(resultSet));
      connection.createStatement().execute("drop procedure if exists stproc1(float, string)");
      connection.createStatement().execute("drop procedure if exists GETPI()");
    }
  }

  @Test
  public void testDatabaseMetadata() throws SQLException {
    try (Connection connection = getConnection()) {
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
      assertEquals(
          Connection.TRANSACTION_READ_COMMITTED, metaData.getDefaultTransactionIsolation());
      assertEquals("$", metaData.getExtraNameCharacters());
      assertEquals("\"", metaData.getIdentifierQuoteString());
      assertEquals(0, getSizeOfResultSet(metaData.getIndexInfo(null, null, null, true, true)));
      assertEquals(8388608, metaData.getMaxBinaryLiteralLength());
      assertEquals(255, metaData.getMaxCatalogNameLength());
      assertEquals(16777216, metaData.getMaxCharLiteralLength());
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
      assertFalse(
          metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
      assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
      assertFalse(
          metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
      assertTrue(metaData.supportsUnion());
      assertTrue(metaData.supportsUnionAll());
      assertFalse(metaData.updatesAreDetected(1));
      assertFalse(metaData.usesLocalFilePerTable());
      assertFalse(metaData.usesLocalFiles());
    }
  }

  @Test
  public void testOtherEmptyTables() throws Throwable {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet;
      // index is not supported.
      resultSet = metaData.getIndexInfo(null, null, null, true, true);
      assertEquals(0, getSizeOfResultSet(resultSet));

      // UDT is not supported.
      resultSet = metaData.getUDTs(null, null, null, new int[] {});
      assertEquals(0, getSizeOfResultSet(resultSet));
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
      expectFeatureNotSupportedException(metaData::getSQLStateType);
      expectFeatureNotSupportedException(metaData::locatorsUpdateCopy);
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
