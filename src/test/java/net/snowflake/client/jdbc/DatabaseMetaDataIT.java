/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import com.google.common.base.Strings;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Database Metadata IT
 */
public class DatabaseMetaDataIT extends BaseJDBCTest
{
  private static Pattern VERSION_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)(?:\\.\\d+)+\\s*.*");

  @Test
  public void testGetConnection() throws SQLException
  {
    try (Connection connection = getConnection())
    {
      DatabaseMetaData metaData = connection.getMetaData();
      assertEquals(connection, metaData.getConnection());
    }
  }

  @Test
  public void testDatabaseAndDriverInfo() throws SQLException
  {
    try (Connection connection = getConnection())
    {
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
  public void testGetCatalogs() throws SQLException
  {
    try (Connection connection = getConnection())
    {
      DatabaseMetaData metaData = connection.getMetaData();
      assertEquals(".", metaData.getCatalogSeparator());
      assertEquals("database", metaData.getCatalogTerm());

      ResultSet resultSet = metaData.getCatalogs();
      assertTrue(resultSet.isBeforeFirst());

      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      assertEquals(1, resultSetMetaData.getColumnCount());

      assertFalse(resultSetMetaData.isAutoIncrement(1));
      assertFalse(resultSetMetaData.isCaseSensitive(1));
      assertTrue(resultSetMetaData.isSearchable(1));
      assertFalse(resultSetMetaData.isCurrency(1));
      assertTrue(resultSetMetaData.isReadOnly(1));
      assertEquals(ResultSetMetaData.columnNullableUnknown, resultSetMetaData.isNullable(1));
      assertFalse(resultSetMetaData.isSigned(1));
      assertFalse(resultSetMetaData.isWritable(1));
      assertFalse(resultSetMetaData.isDefinitelyWritable(1));

      assertEquals(25, resultSetMetaData.getColumnDisplaySize(1)); // fix this
      assertEquals("TABLE_CAT", resultSetMetaData.getColumnLabel(1));
      assertEquals("TABLE_CAT", resultSetMetaData.getColumnName(1));

      assertEquals(9, resultSetMetaData.getPrecision(1)); // fix this
      assertEquals(9, resultSetMetaData.getScale(1)); // fix this
      assertEquals("T", resultSetMetaData.getTableName(1)); // fix this
      assertEquals("", resultSetMetaData.getCatalogName(1));
      assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(1));
      assertEquals("TEXT", resultSetMetaData.getColumnTypeName(1));
      assertEquals("java.lang.String", resultSetMetaData.getColumnClassName(1));

      int cnt = 0;
      Set<String> allVisibleDatabases = new HashSet<>();
      while (resultSet.next())
      {
        allVisibleDatabases.add(resultSet.getString(1));
        if (cnt == 0)
        {
          assertTrue(resultSet.isFirst());
        }
        ++cnt;
        try
        {
          resultSet.isLast();
          fail("No isLast support for query based metadata");
        }
        catch (SQLFeatureNotSupportedException ex)
        {
          // nop
        }
        try
        {
          resultSet.isAfterLast();
          fail("No isAfterLast support for query based metadata");
        }
        catch (SQLFeatureNotSupportedException ex)
        {
          // nop
        }
      }
      assertThat(cnt, greaterThanOrEqualTo(1));
      try
      {
        assertTrue(resultSet.isAfterLast());
        fail("The result set is automatically closed when all rows are fetched.");
      }
      catch (SQLException ex)
      {
        assertEquals((int) ErrorCode.RESULTSET_ALREADY_CLOSED.getMessageCode(), ex.getErrorCode());
      }
      try
      {
        resultSet.isAfterLast();
        fail("No isAfterLast support for query based metadata");
      }
      catch (SQLException ex)
      {
        assertEquals((int) ErrorCode.RESULTSET_ALREADY_CLOSED.getMessageCode(), ex.getErrorCode());
      }
      resultSet.close(); // double closing does nothing.
      resultSet.next(); // no exception

      List<String> allAccessibleDatabases = getInfoBySQL(
          "select database_name from information_schema.databases");

      assertTrue(allVisibleDatabases.containsAll(allAccessibleDatabases));
    }
  }

  @Test
  public void testGetSchemas() throws Throwable
  {
    // CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = false
    try (Connection connection = getConnection())
    {
      DatabaseMetaData metaData = connection.getMetaData();
      assertEquals("schema", metaData.getSchemaTerm());
      ResultSet resultSet = metaData.getSchemas();

      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      assertEquals(2, resultSetMetaData.getColumnCount());

      // first column
      assertFalse(resultSetMetaData.isAutoIncrement(1));
      assertFalse(resultSetMetaData.isCaseSensitive(1));
      assertTrue(resultSetMetaData.isSearchable(1));
      assertFalse(resultSetMetaData.isCurrency(1));
      assertTrue(resultSetMetaData.isReadOnly(1));
      assertEquals(ResultSetMetaData.columnNullableUnknown, resultSetMetaData.isNullable(1));
      assertFalse(resultSetMetaData.isSigned(1));
      assertFalse(resultSetMetaData.isWritable(1));
      assertFalse(resultSetMetaData.isDefinitelyWritable(1));
      assertEquals(25, resultSetMetaData.getColumnDisplaySize(1)); // fix this
      assertEquals("TABLE_SCHEM", resultSetMetaData.getColumnLabel(1));
      assertEquals("TABLE_SCHEM", resultSetMetaData.getColumnName(1));
      assertEquals(9, resultSetMetaData.getPrecision(1)); // fix this
      assertEquals(9, resultSetMetaData.getScale(1)); // fix this
      assertEquals("T", resultSetMetaData.getTableName(1)); // fix this
      assertEquals("", resultSetMetaData.getCatalogName(1));
      assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(1));
      assertEquals("TEXT", resultSetMetaData.getColumnTypeName(1));
      assertEquals("java.lang.String", resultSetMetaData.getColumnClassName(1));

      // second column
      assertFalse(resultSetMetaData.isAutoIncrement(2));
      assertFalse(resultSetMetaData.isCaseSensitive(2));
      assertTrue(resultSetMetaData.isSearchable(2));
      assertFalse(resultSetMetaData.isCurrency(2));
      assertTrue(resultSetMetaData.isReadOnly(2));
      assertEquals(ResultSetMetaData.columnNullableUnknown, resultSetMetaData.isNullable(2));
      assertFalse(resultSetMetaData.isSigned(2));
      assertFalse(resultSetMetaData.isWritable(2));
      assertFalse(resultSetMetaData.isDefinitelyWritable(2));
      assertEquals(25, resultSetMetaData.getColumnDisplaySize(2)); // fix this
      assertEquals("TABLE_CATALOG", resultSetMetaData.getColumnLabel(2));
      assertEquals("TABLE_CATALOG", resultSetMetaData.getColumnName(2));
      assertEquals(9, resultSetMetaData.getPrecision(2)); // fix this
      assertEquals(9, resultSetMetaData.getScale(2)); // fix this
      assertEquals("T", resultSetMetaData.getTableName(2)); // fix this
      assertEquals("", resultSetMetaData.getCatalogName(2));
      assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));
      assertEquals("TEXT", resultSetMetaData.getColumnTypeName(2));
      assertEquals("java.lang.String", resultSetMetaData.getColumnClassName(2));

      Set<String> schemas = new HashSet<>();
      while (resultSet.next())
      {
        schemas.add(resultSet.getString(1));
      }
      resultSet.close();
      assertThat(schemas.size(), greaterThanOrEqualTo(1)); // one or more schemas

      Set<String> schemasInDb = new HashSet<>();
      resultSet = metaData.getSchemas(connection.getCatalog(), "%");
      while (resultSet.next())
      {
        schemasInDb.add(resultSet.getString(1));
      }
      assertThat(schemasInDb.size(), greaterThanOrEqualTo(1)); // one or more schemas
      assertThat(schemas.size(), greaterThanOrEqualTo(schemasInDb.size()));
      assertTrue(schemas.containsAll(schemasInDb));
      assertTrue(schemas.contains(connection.getSchema()));
    }

    // CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = true
    try (Connection connection = getConnection())
    {
      Statement statement = connection.createStatement();
      statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");

      DatabaseMetaData metaData = connection.getMetaData();
      assertEquals("schema", metaData.getSchemaTerm());
      ResultSet resultSet = metaData.getSchemas();
      Set<String> schemas = new HashSet<>();
      while (resultSet.next())
      {
        schemas.add(resultSet.getString(1));
      }
      assertThat(schemas.size(), equalTo(1)); // more than 1 schema
    }
  }

  @Test
  public void testGetTableTypes() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet resultSet = metaData.getTableTypes();
      Set<String> types = new HashSet<>();
      while (resultSet.next())
      {
        types.add(resultSet.getString(1));
      }
      assertEquals(2, types.size());
      assertTrue(types.contains("TABLE"));
      assertTrue(types.contains("VIEW"));
    }
  }

  @Test
  public void testGetTables() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";
      final String targetView = "V0";

      connection.createStatement().execute("create or replace table " + targetTable + "(C1 int)");
      connection.createStatement().execute("create or replace view " + targetView + " as select 1 as C");

      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet;

      // match table
      resultSet = metaData.getTables(
          database, schema, "%", new String[]{"TABLE"});
      Set<String> tables = new HashSet<>();
      while (resultSet.next())
      {
        tables.add(resultSet.getString(3));
      }
      assertTrue(tables.contains("T0"));

      // exact match table
      resultSet = metaData.getTables(
          database, schema, targetTable, new String[]{"TABLE"});
      tables = new HashSet<>();
      while (resultSet.next())
      {
        tables.add(resultSet.getString(3));
      }
      assertEquals(targetTable, tables.iterator().next());

      // match view
      resultSet = metaData.getTables(
          database, schema, "%", new String[]{"VIEW"}
      );
      Set<String> views = new HashSet<>();
      while (resultSet.next())
      {
        views.add(resultSet.getString(3));
      }
      assertTrue(views.contains(targetView));

      resultSet = metaData.getTablePrivileges(
          database, schema, targetTable
      );
      assertEquals(0, getSizeOfResultSet(resultSet));

      connection.createStatement().execute("drop table if exists " + targetTable);
      connection.createStatement().execute("drop view if exists " + targetView);
    }
  }

  @Test
  public void testGetColumns() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";

      connection.createStatement().execute(
          "create or replace table " + targetTable +
          "(C1 int, C2 varchar(100), C3 string, C4 number(18,4), C5 double, C6 boolean, " +
          "C7 date not null, C8 time, C9 timestamp_ntz(7), C10 binary," +
          "C11 variant, C12 timestamp_ltz(8), C13 timestamp_tz(3))");

      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet;

      resultSet = metaData.getColumns(database, schema, targetTable, "%");
      assertEquals(24, resultSet.getMetaData().getColumnCount());

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
      assertEquals(ResultSetMetaData.columnNullable,
                   resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertEquals("", resultSet.getString("COLUMN_DEF"));

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
      assertEquals(ResultSetMetaData.columnNullable,
                   resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertEquals("", resultSet.getString("COLUMN_DEF"));

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
      assertEquals(ResultSetMetaData.columnNullable,
                   resultSet.getInt("NULLABLE"));
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
      assertEquals(ResultSetMetaData.columnNullable,
                   resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertEquals("", resultSet.getString("COLUMN_DEF"));

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
      assertEquals(ResultSetMetaData.columnNullable,
                   resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertEquals("", resultSet.getString("COLUMN_DEF"));

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
      assertEquals(ResultSetMetaData.columnNullable,
                   resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertEquals("", resultSet.getString("COLUMN_DEF"));

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
      assertEquals(ResultSetMetaData.columnNoNulls,
                   resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertEquals("", resultSet.getString("COLUMN_DEF"));

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
      assertEquals(ResultSetMetaData.columnNullable,
                   resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertEquals("", resultSet.getString("COLUMN_DEF"));

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
      assertEquals(ResultSetMetaData.columnNullable,
                   resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertEquals("", resultSet.getString("COLUMN_DEF"));

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
      assertEquals(ResultSetMetaData.columnNullable,
                   resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertEquals("", resultSet.getString("COLUMN_DEF"));

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
      assertEquals(ResultSetMetaData.columnNullable,
                   resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertEquals("", resultSet.getString("COLUMN_DEF"));

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
      assertEquals(ResultSetMetaData.columnNullable,
                   resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertEquals("", resultSet.getString("COLUMN_DEF"));

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
      assertEquals(ResultSetMetaData.columnNullable,
                   resultSet.getInt("NULLABLE"));
      assertEquals("", resultSet.getString("REMARKS"));
      assertEquals("", resultSet.getString("COLUMN_DEF"));

      assertEquals(0, resultSet.getInt("CHAR_OCTET_LENGTH"));
      assertEquals(13, resultSet.getInt("ORDINAL_POSITION"));
      assertEquals("YES", resultSet.getString("IS_NULLABLE"));
      assertNull(resultSet.getString("SCOPE_CATALOG"));
      assertNull(resultSet.getString("SCOPE_SCHEMA"));
      assertNull(resultSet.getString("SCOPE_TABLE"));
      assertEquals((short) 0, resultSet.getShort("SOURCE_DATA_TYPE"));
      assertEquals("NO", resultSet.getString("IS_AUTOINCREMENT"));
      assertEquals("NO", resultSet.getString("IS_GENERATEDCOLUMN"));

      try
      {
        resultSet.getString("INVALID_COLUMN");
        fail("must fail");
      }
      catch (SQLException ex)
      {
        // nop
      }

      // no column privilege is supported.
      resultSet = metaData.getColumnPrivileges(database, schema, targetTable, "C1");
      assertEquals(0, super.getSizeOfResultSet(resultSet));

      connection.createStatement().execute("drop table if exists T0");
    }
  }

  @Test
  public void testGetPrimarykeys() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";

      connection.createStatement().execute(
          "create or replace table " + targetTable + "(C1 int primary key, C2 string)");

      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet;

      resultSet = metaData.getPrimaryKeys(database, schema, targetTable);
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

  @Test
  public void testGetImportedKeys() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable1 = "T0";
      final String targetTable2 = "T1";

      connection.createStatement().execute(
          "create or replace table " +
          targetTable1 + "(C1 int primary key, C2 string)");
      connection.createStatement().execute(
          "create or replace table " +
          targetTable2 + "(C1 int primary key, C2 string, C3 int references " + targetTable1 + ")");


      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet;

      resultSet = metaData.getImportedKeys(database, schema, targetTable2);
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
      assertEquals(DatabaseMetaData.importedKeyNoAction,
                   resultSet.getShort("UPDATE_RULE"));
      assertEquals(DatabaseMetaData.importedKeyNoAction,
                   resultSet.getShort("DELETE_RULE"));
      assertEquals(DatabaseMetaData.importedKeyNotDeferrable,
                   resultSet.getShort("DEFERRABILITY"));

      connection.createStatement().execute("drop table if exists " + targetTable1);
      connection.createStatement().execute("drop table if exists " + targetTable2);
    }
  }

  @Test
  public void testGetExportedKeys() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable1 = "T0";
      final String targetTable2 = "T1";

      connection.createStatement().execute(
          "create or replace table " +
          targetTable1 + "(C1 int primary key, C2 string)");
      connection.createStatement().execute(
          "create or replace table " +
          targetTable2 + "(C1 int primary key, C2 string, C3 int references " + targetTable1 + ")");


      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet;

      resultSet = metaData.getExportedKeys(database, schema, targetTable1);
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
      assertEquals(DatabaseMetaData.importedKeyNoAction,
                   resultSet.getShort("UPDATE_RULE"));
      assertEquals(DatabaseMetaData.importedKeyNoAction,
                   resultSet.getShort("DELETE_RULE"));
      assertEquals(DatabaseMetaData.importedKeyNotDeferrable,
                   resultSet.getShort("DEFERRABILITY"));

      connection.createStatement().execute("drop table if exists " + targetTable1);
      connection.createStatement().execute("drop table if exists " + targetTable2);
    }
  }

  @Test
  public void testGetCrossReferences() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable1 = "T0";
      final String targetTable2 = "T1";

      connection.createStatement().execute(
          "create or replace table " +
          targetTable1 + "(C1 int primary key, C2 string)");
      connection.createStatement().execute(
          "create or replace table " +
          targetTable2 + "(C1 int primary key, C2 string, C3 int references " + targetTable1 + ")");


      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet;

      resultSet = metaData.getCrossReference(
          database, schema, targetTable1, database, schema, targetTable2);
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
      assertEquals(DatabaseMetaData.importedKeyNoAction,
                   resultSet.getShort("UPDATE_RULE"));
      assertEquals(DatabaseMetaData.importedKeyNoAction,
                   resultSet.getShort("DELETE_RULE"));
      assertEquals(DatabaseMetaData.importedKeyNotDeferrable,
                   resultSet.getShort("DEFERRABILITY"));

      connection.createStatement().execute("drop table if exists " + targetTable1);
      connection.createStatement().execute("drop table if exists " + targetTable2);
    }
  }

  @Test
  public void testGetObjectsDoesNotExists() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      final String targetTable = "T0";
      final String targetView = "V0";

      connection.createStatement().execute("create or replace table " + targetTable + "(C1 int)");
      connection.createStatement().execute("create or replace view " + targetView + " as select 1 as C");

      DatabaseMetaData metaData = connection.getMetaData();

      // sanity check if getTables really works.
      ResultSet resultSet = metaData.getTables(
          database, schema, "%", null);
      assertTrue(getSizeOfResultSet(resultSet) > 0);

      // invalid object type. empty result is expected.
      resultSet = metaData.getTables(
          database, schema, "%", new String[]{"INVALID_TYPE"}
      );
      assertEquals(0, getSizeOfResultSet(resultSet));

      // rest of the cases should return empty results.
      resultSet = metaData.getSchemas(
          "DB_NOT_EXIST", "SCHEMA_NOT_EXIST");
      assertFalse(resultSet.next());
      assertTrue(resultSet.isClosed());


      resultSet = metaData.getTables(
          "DB_NOT_EXIST", "SCHEMA_NOT_EXIST", "%", null);
      assertFalse(resultSet.next());

      resultSet = metaData.getTables(
          database, "SCHEMA\\_NOT\\_EXIST", "%", null);
      assertFalse(resultSet.next());

      resultSet = metaData.getColumns(
          "DB_NOT_EXIST", "SCHEMA_NOT_EXIST", "%", "%");
      assertFalse(resultSet.next());

      resultSet = metaData.getColumns(
          database, "SCHEMA\\_NOT\\_EXIST", "%", "%");
      assertFalse(resultSet.next());

      resultSet = metaData.getColumns(
          database, schema, "TBL\\_NOT\\_EXIST", "%");
      assertFalse(resultSet.next());
      connection.createStatement().execute("drop table if exists " + targetTable);
      connection.createStatement().execute("drop view if exists " + targetView);
    }
  }

  @Test
  public void testTypeInfo() throws SQLException
  {
    try (Connection connection = getConnection())
    {
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
  public void testProcedure() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      DatabaseMetaData metaData = connection.getMetaData();
      assertEquals("procedure", metaData.getProcedureTerm());
      // no stored procedure support
      assertFalse(metaData.supportsStoredProcedures());
      ResultSet resultSet;
      resultSet = metaData.getProcedureColumns(
          "%", "%", "%", "%");
      assertEquals(0, getSizeOfResultSet(resultSet));
      resultSet = metaData.getProcedures(
          "%", "%", "%");
      assertEquals(0, getSizeOfResultSet(resultSet));
    }
  }

  /**
   * No function column support
   */
  @Test
  public void testGetFunctionColumns() throws Exception
  {
    try (Connection connection = getConnection())
    {
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet resultSet = metaData.getFunctionColumns("%", "%", "%", "%");
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testDatabaseMetadata() throws SQLException
  {
    try (Connection connection = getConnection())
    {
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
      //assertEquals("", metaData.getTimeDateFunctions());
      assertEquals(System.getenv("SNOWFLAKE_TEST_USER"), metaData.getUserName());
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
      assertTrue(metaData.supportsResultSetConcurrency(
          ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY));
      assertFalse(metaData.supportsResultSetConcurrency(
          ResultSet.TYPE_SCROLL_INSENSITIVE,
          ResultSet.CONCUR_READ_ONLY));
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
      assertFalse(metaData.supportsStoredFunctionsUsingCallSyntax());
      assertTrue(metaData.supportsSubqueriesInComparisons());
      assertTrue(metaData.supportsSubqueriesInExists());
      assertTrue(metaData.supportsSubqueriesInIns());
      assertFalse(metaData.supportsSubqueriesInQuantifieds());
      assertTrue(metaData.supportsTableCorrelationNames());
      assertTrue(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
      assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
      assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
      assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
      assertTrue(metaData.supportsUnion());
      assertTrue(metaData.supportsUnionAll());
      assertFalse(metaData.updatesAreDetected(1));
      assertFalse(metaData.usesLocalFilePerTable());
      assertFalse(metaData.usesLocalFiles());

    }
  }

  @Test
  public void testOtherEmptyTables() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      DatabaseMetaData metaData = connection.getMetaData();

      ResultSet resultSet;
      // index is not supported.
      resultSet = metaData.getIndexInfo(null, null, null, true, true);
      assertEquals(0, getSizeOfResultSet(resultSet));

      // UDT is not supported.
      resultSet = metaData.getUDTs(null, null, null, new int[]{});
      assertEquals(0, getSizeOfResultSet(resultSet));
    }
  }

  @Test
  public void testFeatureNotSupportedException() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      DatabaseMetaData metaData = connection.getMetaData();
      expectFeatureNotSupportedException(() -> metaData.getBestRowIdentifier(
          null, null, null, 0, true));
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
      expectFeatureNotSupportedException(() -> metaData.unwrap(SnowflakeDatabaseMetaData.class));
      expectFeatureNotSupportedException(() -> metaData.isWrapperFor(SnowflakeDatabaseMetaData.class));
    }
  }
}
