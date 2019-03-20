/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Database Metadata IT
 */
public class DatabaseMetaDataIT extends BaseJDBCTest
{
  @Test
  public void testCatalog() throws SQLException
  {
    try (Connection connection = getConnection())
    {
      Statement statement = connection.createStatement();
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet resultSet = metaData.getCatalogs();
      assertEquals(".", metaData.getCatalogSeparator());
      assertEquals("database", metaData.getCatalogTerm());

      Set<String> allVisibleTables = new HashSet<>();
      while (resultSet.next())
      {
        allVisibleTables.add(resultSet.getString(1));
      }
      resultSet.close();
      resultSet.next(); // no exception

      List<String> allAccessibleDatabases = getInfoViaSQLCmd(
          "select database_name from information_schema.databases");

      assertTrue(allVisibleTables.containsAll(allAccessibleDatabases));
      statement.close();
    }
  }

  @Test
  public void testGetConnection() throws SQLException
  {
    try (Connection connection = getConnection())
    {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      assertEquals(connection, databaseMetaData.getConnection());
    }
  }

  @Test
  public void testDatabaseAndDriverInfo() throws SQLException
  {
    try (Connection connection = getConnection())
    {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      assertEquals("Snowflake", databaseMetaData.getDatabaseProductName());
      assertEquals(1, databaseMetaData.getJDBCMajorVersion());
      assertEquals(0, databaseMetaData.getJDBCMinorVersion());
      assertEquals("Snowflake", databaseMetaData.getDriverName());
    }
  }

  @Test
  public void testGetObjectsDoesNotExists() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      String database = connection.getCatalog();
      String schema = connection.getSchema();

      connection.createStatement().execute("create or replace table t0(c1 int)");

      DatabaseMetaData databaseMetaData = connection.getMetaData();

      // sanity check if getTables really works.
      ResultSet resultSet = databaseMetaData.getTables(
          database, schema, "%", null);
      assertTrue(getSizeOfResultSet(resultSet) > 0);

      // rest of the cases should return empty results.
      resultSet = databaseMetaData.getSchemas(
          "DB_NOT_EXIST", "SCHEMA_NOT_EXIST");
      assertFalse(resultSet.next());
      assertTrue(resultSet.isClosed());


      resultSet = databaseMetaData.getTables(
          "DB_NOT_EXIST", "SCHEMA_NOT_EXIST", "%", null);
      assertFalse(resultSet.next());

      resultSet = databaseMetaData.getTables(
          database, "SCHEMA\\_NOT\\_EXIST", "%", null);
      assertFalse(resultSet.next());

      resultSet = databaseMetaData.getColumns(
          "DB_NOT_EXIST", "SCHEMA_NOT_EXIST", "%", "%");
      assertFalse(resultSet.next());

      resultSet = databaseMetaData.getColumns(
          database, "SCHEMA\\_NOT\\_EXIST", "%", "%");
      assertFalse(resultSet.next());

      resultSet = databaseMetaData.getColumns(
          database, schema, "TBL\\_NOT\\_EXIST", "%");
      assertFalse(resultSet.next());
      connection.createStatement().execute("drop table if exists t0");
    }
  }

  @Test
  public void testTypeInfo() throws SQLException
  {
    try (Connection connection = getConnection())
    {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      ResultSet resultSet = databaseMetaData.getTypeInfo();
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
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      ResultSet resultSet = databaseMetaData.getFunctionColumns("%", "%", "%", "%");
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testInfoThatAlwaysReturnSameValue() throws SQLException
  {
    try (Connection connection = getConnection())
    {
      DatabaseMetaData databaseMetaData = connection.getMetaData();

      assertTrue(databaseMetaData.getURL().startsWith("jdbc:snowflake://"));
      assertFalse(databaseMetaData.allProceduresAreCallable());
      assertTrue(databaseMetaData.allTablesAreSelectable());
      assertTrue(databaseMetaData.dataDefinitionCausesTransactionCommit());
      assertFalse(databaseMetaData.dataDefinitionIgnoredInTransactions());
      assertFalse(databaseMetaData.deletesAreDetected(1));
      assertTrue(databaseMetaData.doesMaxRowSizeIncludeBlobs());
      assertEquals(Connection.TRANSACTION_READ_COMMITTED, databaseMetaData.getDefaultTransactionIsolation());
      assertEquals("$", databaseMetaData.getExtraNameCharacters());
      assertEquals("\"", databaseMetaData.getIdentifierQuoteString());
      assertEquals(0, getSizeOfResultSet(databaseMetaData.getIndexInfo(null, null, null, true, true)));
      assertEquals(0, databaseMetaData.getMaxBinaryLiteralLength());
      assertEquals(255, databaseMetaData.getMaxCatalogNameLength());
      assertEquals(16777216, databaseMetaData.getMaxCharLiteralLength());
      assertEquals(255, databaseMetaData.getMaxColumnNameLength());
      assertEquals(0, databaseMetaData.getMaxColumnsInGroupBy());
      assertEquals(0, databaseMetaData.getMaxColumnsInIndex());
      assertEquals(0, databaseMetaData.getMaxColumnsInOrderBy());
      assertEquals(0, databaseMetaData.getMaxColumnsInSelect());
      assertEquals(0, databaseMetaData.getMaxColumnsInTable());
      assertEquals(0, databaseMetaData.getMaxConnections());
      assertEquals(0, databaseMetaData.getMaxCursorNameLength());
      assertEquals(0, databaseMetaData.getMaxIndexLength());
      assertEquals(0, databaseMetaData.getMaxProcedureNameLength());
      assertEquals(0, databaseMetaData.getMaxRowSize());
      assertEquals(255, databaseMetaData.getMaxSchemaNameLength());
      assertEquals(0, databaseMetaData.getMaxStatementLength());
      assertEquals(0, databaseMetaData.getMaxStatements());
      assertEquals(255, databaseMetaData.getMaxTableNameLength());
      assertEquals(0, databaseMetaData.getMaxTablesInSelect());
      assertEquals(255, databaseMetaData.getMaxUserNameLength());
      //assertEquals("", databaseMetaData.getNumericFunctions());
      assertEquals(0, getSizeOfResultSet(databaseMetaData.getProcedures(null, null, null)));
      assertEquals("procedure", databaseMetaData.getProcedureTerm());
      //assertEquals("", databaseMetaData.getStringFunctions());
      //assertEquals("", databaseMetaData.getSystemFunctions());
      assertEquals(0, getSizeOfResultSet(databaseMetaData.getTablePrivileges(null, null, null)));
      //assertEquals("", databaseMetaData.getTimeDateFunctions());
      assertEquals(System.getenv("SNOWFLAKE_TEST_USER"), databaseMetaData.getUserName());
      assertFalse(databaseMetaData.insertsAreDetected(1));
      assertTrue(databaseMetaData.isCatalogAtStart());
      assertFalse(databaseMetaData.isReadOnly());
      assertTrue(databaseMetaData.nullPlusNonNullIsNull());
      assertFalse(databaseMetaData.nullsAreSortedAtEnd());
      assertFalse(databaseMetaData.nullsAreSortedAtStart());
      assertTrue(databaseMetaData.nullsAreSortedHigh());
      assertFalse(databaseMetaData.nullsAreSortedLow());
      assertFalse(databaseMetaData.othersDeletesAreVisible(1));
      assertFalse(databaseMetaData.othersInsertsAreVisible(1));
      assertFalse(databaseMetaData.othersUpdatesAreVisible(1));
      assertFalse(databaseMetaData.ownDeletesAreVisible(1));
      assertFalse(databaseMetaData.ownInsertsAreVisible(1));
      assertFalse(databaseMetaData.ownUpdatesAreVisible(1));
      assertFalse(databaseMetaData.storesLowerCaseIdentifiers());
      assertFalse(databaseMetaData.storesLowerCaseQuotedIdentifiers());
      assertFalse(databaseMetaData.storesMixedCaseIdentifiers());
      assertTrue(databaseMetaData.storesMixedCaseQuotedIdentifiers());
      assertTrue(databaseMetaData.storesUpperCaseIdentifiers());
      assertFalse(databaseMetaData.storesUpperCaseQuotedIdentifiers());
      assertTrue(databaseMetaData.supportsAlterTableWithAddColumn());
      assertTrue(databaseMetaData.supportsAlterTableWithDropColumn());
      assertTrue(databaseMetaData.supportsANSI92EntryLevelSQL());
      assertFalse(databaseMetaData.supportsANSI92FullSQL());
      assertFalse(databaseMetaData.supportsANSI92IntermediateSQL());
      assertTrue(databaseMetaData.supportsBatchUpdates());
      assertTrue(databaseMetaData.supportsCatalogsInDataManipulation());
      assertFalse(databaseMetaData.supportsCatalogsInIndexDefinitions());
      assertFalse(databaseMetaData.supportsCatalogsInPrivilegeDefinitions());
      assertFalse(databaseMetaData.supportsCatalogsInProcedureCalls());
      assertTrue(databaseMetaData.supportsCatalogsInTableDefinitions());
      assertTrue(databaseMetaData.supportsColumnAliasing());
      assertFalse(databaseMetaData.supportsConvert());
      assertFalse(databaseMetaData.supportsConvert(1, 2));
      assertFalse(databaseMetaData.supportsCoreSQLGrammar());
      assertTrue(databaseMetaData.supportsCorrelatedSubqueries());
      assertTrue(databaseMetaData.supportsDataDefinitionAndDataManipulationTransactions());
      assertFalse(databaseMetaData.supportsDataManipulationTransactionsOnly());
      assertFalse(databaseMetaData.supportsDifferentTableCorrelationNames());
      assertTrue(databaseMetaData.supportsExpressionsInOrderBy());
      assertFalse(databaseMetaData.supportsExtendedSQLGrammar());
      assertTrue(databaseMetaData.supportsFullOuterJoins());
      assertFalse(databaseMetaData.supportsGetGeneratedKeys());
      assertTrue(databaseMetaData.supportsGroupBy());
      assertTrue(databaseMetaData.supportsGroupByBeyondSelect());
      assertFalse(databaseMetaData.supportsGroupByUnrelated());
      assertFalse(databaseMetaData.supportsIntegrityEnhancementFacility());
      assertFalse(databaseMetaData.supportsLikeEscapeClause());
      assertTrue(databaseMetaData.supportsLimitedOuterJoins());
      assertFalse(databaseMetaData.supportsMinimumSQLGrammar());
      assertFalse(databaseMetaData.supportsMixedCaseIdentifiers());
      assertTrue(databaseMetaData.supportsMixedCaseQuotedIdentifiers());
      assertFalse(databaseMetaData.supportsMultipleOpenResults());
      assertFalse(databaseMetaData.supportsMultipleResultSets());
      assertTrue(databaseMetaData.supportsMultipleTransactions());
      assertFalse(databaseMetaData.supportsNamedParameters());
      assertTrue(databaseMetaData.supportsNonNullableColumns());
      assertFalse(databaseMetaData.supportsOpenCursorsAcrossCommit());
      assertFalse(databaseMetaData.supportsOpenCursorsAcrossRollback());
      assertFalse(databaseMetaData.supportsOpenStatementsAcrossCommit());
      assertFalse(databaseMetaData.supportsOpenStatementsAcrossRollback());
      assertTrue(databaseMetaData.supportsOrderByUnrelated());
      assertTrue(databaseMetaData.supportsOuterJoins());
      assertFalse(databaseMetaData.supportsPositionedDelete());
      assertFalse(databaseMetaData.supportsPositionedUpdate());
      assertTrue(databaseMetaData.supportsResultSetConcurrency(
          ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY));
      assertTrue(databaseMetaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
      assertFalse(databaseMetaData.supportsResultSetHoldability(0));
      assertFalse(databaseMetaData.supportsSavepoints());
      assertTrue(databaseMetaData.supportsSchemasInDataManipulation());
      assertFalse(databaseMetaData.supportsSchemasInIndexDefinitions());
      assertFalse(databaseMetaData.supportsSchemasInPrivilegeDefinitions());
      assertFalse(databaseMetaData.supportsSchemasInProcedureCalls());
      assertTrue(databaseMetaData.supportsSchemasInTableDefinitions());
      assertFalse(databaseMetaData.supportsSelectForUpdate());
      assertFalse(databaseMetaData.supportsStatementPooling());
      assertFalse(databaseMetaData.supportsStoredFunctionsUsingCallSyntax());
      assertTrue(databaseMetaData.supportsSubqueriesInComparisons());
      assertTrue(databaseMetaData.supportsSubqueriesInExists());
      assertTrue(databaseMetaData.supportsSubqueriesInIns());
      assertFalse(databaseMetaData.supportsSubqueriesInQuantifieds());
      assertTrue(databaseMetaData.supportsTableCorrelationNames());
      assertTrue(databaseMetaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
      assertTrue(databaseMetaData.supportsUnion());
      assertTrue(databaseMetaData.supportsUnionAll());
      assertFalse(databaseMetaData.updatesAreDetected(1));
      assertFalse(databaseMetaData.usesLocalFilePerTable());
      assertFalse(databaseMetaData.usesLocalFiles());

    }
  }

}
