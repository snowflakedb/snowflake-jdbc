/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.common.core.SqlState;
import net.snowflake.client.core.SFSession;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import net.snowflake.common.util.Wildcard;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import static net.snowflake.client.jdbc.DBMetadataResultSetMetadata.*;

/**
 *
 * @author jhuang
 */
public class SnowflakeDatabaseMetaData implements DatabaseMetaData
{

  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeDatabaseMetaData.class);

  static final ObjectMapper mapper = new ObjectMapper();

  static final private String DatabaseProductName = "Snowflake";

  static final private String DriverName = "Snowflake";

  static final private char SEARCH_STRING_ESCAPE = '\\';

  static final private String JDBCVersion = "1.0";
  // Open Group CLI Functions
  // LOG10 is not supported
  static final private String NumericFunctionsSupported = "ABS,ACOS,ASIN,"
      + "CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,MOD,PI,POWER,RADIANS,RAND,"
      + "ROUND,SIGN,SQRT,TAN,TRUNCATE";
  // DIFFERENCE and SOUNDEX are not supported
  static final private String StringFunctionsSupported = "ASCII,CHAR,"
      + "CONCAT,INSERT,LCASE,LEFT,LENGTH,LOCATE,LTRIM,REPEAT,REPLACE,"
      + "RIGHT,RTRIM,SPACE,SUBSTRING,UCASE";
  static final private String DateAndTimeFunctionsSupported = "CURDATE," +
      "CURTIME,DAYNAME,DAYOFMONTH,DAYOFWEEK,DAYOFYEAR,HOUR,MINUTE,MONTH," +
      "MONTHNAME,NOW,QUARTER,SECOND,TIMESTAMPADD,TIMESTAMPDIFF,WEEK,YEAR";
  static final private String SystemFunctionsSupported = "DATABASE,IFNULL,USER";

  // These are keywords not in SQL2003 standard
  static final private String notSQL2003Keywords = "ACCOUNT,DATABASE,SCHEMA,VIEW,ISSUE,DATE_PART,EXTRACT," +
      "POSITION,TRY_CAST,BIT,DATETIME,NUMBERC,OBJECT,BYTEINT,STRING,TEXT," +
      "TIMESTAMPLTZ,TIMESTAMPNTZ,TIMESTAMPTZ,TIMESTAMP_LTZ,TIMESTAMP_NTZ,TIMESTAMP_TZ,TINYINT," +
      "VARBINARY,VARIANT,ACCOUNTS,ACTION,ACTIVATE,ASC,AUTOINCREMENT,BEFORE," +
      "BUILTIN,BYTE,CACHE,CHANGE,CLEAREPCACHE,CLONE,CLUSTER,CLUSTERS,COLUMNS,COMMENT," +
      "COMPRESSION,CONSTRAINTS,COPY,CP,CREDENTIALS,D,DATA,DATABASES,DEFERRABLE," +
      "DEFERRED,DELIMITED,DESC,DIRECTORY,DISABLE,DUAL,ENABLE,ENFORCED," +
      "EXCLUSIVE,EXPLAIN,EXPORTED,FAIL,FIELDS,FILE,FILES,FIRST,FN,FORCE,FORMAT," +
      "FORMATS,FUNCTIONS,GRANTS,GSINSTANCE,GSINSTANCES,HELP,HIBERNATE,HINTS," +
      "HISTORY,IDENTIFIED,IMMUTABLE,IMPORTED,INCIDENT,INCIDENTS,INFO,INITIALLY," +
      "ISSUES,KEEP,KEY,KEYS,LAST,LIMIT,LIST,LOAD,LOCATION,LOCK,LOCKS,LS,MANAGE,MAP,MATCHED," +
      "MATERIALIZED,MODIFY,MONITOR,MONITORS,NAME,NETWORK,NEXT,NORELY,NOTIFY,NOVALIDATE,NULLS,OBJECTS," +
      "OFFSET,OJ,OPERATE,OPERATION,OPTION,OWNERSHIP,PARAMETERS,PARTIAL," +
      "PERCENT,PLAN,PLUS,POLICIES,POLICY,POOL,PRESERVE,PRIVILEGES,PUBLIC,PURGE,PUT,QUIESCE," +
      "READ,RECLUSTER,REFERENCE,RELY,REMOVE,RENAME,REPLACE,REPLACE_FAIL,RESOURCE," +
      "RESTART,RESTORE,RESTRICT,RESUME,REWRITE,RM,ROLE,ROLES,RULE,SAMPLE,SCHEMAS,SEMI," +
      "SEQUENCE,SEQUENCES,SERVER,SERVERS,SESSION,SETLOGLEVEL,SETS,SFC,SHARE,SHARED,SHARES,SHOW,SHUTDOWN,SIMPLE,SORT," +
      "STAGE,STAGES,STATEMENT,STATISTICS,STOP,STORED,STRICT,STRUCT,SUSPEND,SUSPEND_IMMEDIATE,SWAP,SWITCH,T," +
      "TABLES,TEMP,TEMPORARY,TRANSACTION,TRANSACTIONS,TRANSIENT,TRIGGERS,TRUNCATE,TS,TYPE,UNDROP,UNLOCK,UNSET," +
      "UPGRADE,USAGE,USE,USERS,UTC,UTCTIMESTAMP,VALIDATE,VARIABLES,VERSION,VIEWS,VOLATILE,VOLUME," +
      "VOLUMES,WAREHOUSE,WAREHOUSES,WARN,WORK,WRITE,ZONE,INCREMENT,MINUS,REGEXP,RLIKE";

  private Connection connection;

  private SFSession session;

  private boolean metadataRequestUseConnectionCtx;

  public SnowflakeDatabaseMetaData(Connection connection)
  {
    logger.debug(
               "public SnowflakeDatabaseMetaData(SnowflakeConnection connection)");

    this.connection = connection;
    this.session = ((SnowflakeConnectionV1)connection).getSfSession();
    this.metadataRequestUseConnectionCtx = session.getMetadataRequestUseConnectionCtx();
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException
  {
    logger.debug(
               "public boolean allProceduresAreCallable()");

    return false;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException
  {
    logger.debug("public boolean allTablesAreSelectable()");

    return true;
  }

  @Override
  public String getURL() throws SQLException
  {
    logger.debug("public String getURL()");

    return session.getUrl();
  }

  @Override
  public String getUserName() throws SQLException
  {
    logger.debug("public String getUserName()");

    return session.getUser();
  }

  @Override
  public boolean isReadOnly() throws SQLException
  {
    logger.debug("public boolean isReadOnly()");

    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException
  {
    logger.debug("public boolean nullsAreSortedHigh()");

    return true;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException
  {
    logger.debug("public boolean nullsAreSortedLow()");

    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException
  {
    logger.debug("public boolean nullsAreSortedAtStart()");

    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException
  {
    logger.debug("public boolean nullsAreSortedAtEnd()");

    return false;
  }

  @Override
  public String getDatabaseProductName() throws SQLException
  {
    logger.debug("public String getDatabaseProductName()");

    return DatabaseProductName;
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException
  {
    logger.debug("public String getDatabaseProductVersion()");

    // We will use the same version numbers for client, GS and XP +

    return ((SnowflakeConnectionV1)connection).getDatabaseVersion() +
        " (" + getDriverVersion() + ")";
  }

  @Override
  public String getDriverName() throws SQLException
  {
    logger.debug("public String getDriverName()");

    return DriverName;
  }

  @Override
  public String getDriverVersion() throws SQLException
  {
    logger.debug("public String getDriverVersion()");

    StringBuilder versionBuilder = new StringBuilder();

    versionBuilder.append(("driver change version: "));
    versionBuilder.append(SnowflakeDriver.majorVersion);
    versionBuilder.append(".");
    versionBuilder.append(SnowflakeDriver.minorVersion);
    versionBuilder.append(".");
    versionBuilder.append(SnowflakeDriver.changeVersion);

    String newClientForUpdate =
        ((SnowflakeConnectionV1)connection).getNewClientForUpdate();

    // add new client version if current is older
    if (newClientForUpdate != null)
    {
      versionBuilder.append(", latest change version: ");
      versionBuilder.append(newClientForUpdate);
    }

    return versionBuilder.toString();
  }

  @Override
  public int getDriverMajorVersion()
  {
    logger.debug("public int getDriverMajorVersion()");

    return SnowflakeDriver.majorVersion;
  }

  @Override
  public int getDriverMinorVersion()
  {
    logger.debug("public int getDriverMinorVersion()");

    return SnowflakeDriver.minorVersion;
  }

  @Override
  public boolean usesLocalFiles() throws SQLException
  {
    logger.debug("public boolean usesLocalFiles()");

    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException
  {
    logger.debug("public boolean usesLocalFilePerTable()");

    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException
  {
    logger.debug(
               "public boolean supportsMixedCaseIdentifiers()");

    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException
  {
    logger.debug(
               "public boolean storesUpperCaseIdentifiers()");

    return true;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException
  {
    logger.debug(
               "public boolean storesLowerCaseIdentifiers()");

    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException
  {
    logger.debug(
               "public boolean storesMixedCaseIdentifiers()");

    return false;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
  {
    logger.debug(
               "public boolean supportsMixedCaseQuotedIdentifiers()");

    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
  {
    logger.debug(
               "public boolean storesUpperCaseQuotedIdentifiers()");

    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
  {
    logger.debug(
               "public boolean storesLowerCaseQuotedIdentifiers()");

    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
  {
    logger.debug(
               "public boolean storesMixedCaseQuotedIdentifiers()");

    return true;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException
  {
    logger.debug(
               "public String getIdentifierQuoteString()");

    return "\"";
  }

  @Override
  public String getSQLKeywords() throws SQLException
  {
    logger.debug("public String getSQLKeywords()");

    return notSQL2003Keywords;
  }

  @Override
  public String getNumericFunctions() throws SQLException
  {
    logger.debug("public String getNumericFunctions()");

    return NumericFunctionsSupported;
  }

  @Override
  public String getStringFunctions() throws SQLException
  {
    logger.debug("public String getStringFunctions()");

    return StringFunctionsSupported;
  }

  @Override
  public String getSystemFunctions() throws SQLException
  {
    logger.debug("public String getSystemFunctions()");

    return SystemFunctionsSupported;
  }

  @Override
  public String getTimeDateFunctions() throws SQLException
  {
    logger.debug("public String getTimeDateFunctions()");

    return DateAndTimeFunctionsSupported;
  }

  @Override
  public String getSearchStringEscape() throws SQLException
  {
    logger.debug("public String getSearchStringEscape()");

    return Character.toString(SEARCH_STRING_ESCAPE);
  }

  @Override
  public String getExtraNameCharacters() throws SQLException
  {
    logger.debug("public String getExtraNameCharacters()");

    return "$";
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException
  {
    logger.debug(
               "public boolean supportsAlterTableWithAddColumn()");

    return true;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException
  {
    logger.debug(
               "public boolean supportsAlterTableWithDropColumn()");

    return true;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException
  {
    logger.debug("public boolean supportsColumnAliasing()");

    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException
  {
    logger.debug("public boolean nullPlusNonNullIsNull()");

    return true;
  }

  @Override
  public boolean supportsConvert() throws SQLException
  {
    logger.debug("public boolean supportsConvert()");

    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException
  {
    logger.debug(
               "public boolean supportsConvert(int fromType, int toType)");

    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException
  {
    logger.debug(
               "public boolean supportsTableCorrelationNames()");

    return true;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException
  {
    logger.debug(
               "public boolean supportsDifferentTableCorrelationNames()");

    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException
  {
    logger.debug(
               "public boolean supportsExpressionsInOrderBy()");

    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException
  {
    logger.debug(
               "public boolean supportsOrderByUnrelated()");

    return true;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException
  {
    logger.debug("public boolean supportsGroupBy()");

    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException
  {
    logger.debug(
               "public boolean supportsGroupByUnrelated()");

    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException
  {
    logger.debug(
               "public boolean supportsGroupByBeyondSelect()");

    return true;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException
  {
    logger.debug(
               "public boolean supportsLikeEscapeClause()");

    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException
  {
    logger.debug(
               "public boolean supportsMultipleResultSets()");

    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException
  {
    logger.debug(
               "public boolean supportsMultipleTransactions()");

    return true;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException
  {
    logger.debug(
               "public boolean supportsNonNullableColumns()");

    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException
  {
    logger.debug(
               "public boolean supportsMinimumSQLGrammar()");

    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException
  {
    logger.debug("public boolean supportsCoreSQLGrammar()");

    return false;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException
  {
    logger.debug(
               "public boolean supportsExtendedSQLGrammar()");

    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException
  {
    logger.debug(
               "public boolean supportsANSI92EntryLevelSQL()");

    return true;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException
  {
    logger.debug(
               "public boolean supportsANSI92IntermediateSQL()");

    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException
  {
    logger.debug("public boolean supportsANSI92FullSQL()");

    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException
  {
    logger.debug(
               "public boolean supportsIntegrityEnhancementFacility()");

    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException
  {
    logger.debug("public boolean supportsOuterJoins()");

    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException
  {
    logger.debug("public boolean supportsFullOuterJoins()");

    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException
  {
    logger.debug(
               "public boolean supportsLimitedOuterJoins()");

    return true;
  }

  @Override
  public String getSchemaTerm() throws SQLException
  {
    logger.debug("public String getSchemaTerm()");

    return "schema";
  }

  @Override
  public String getProcedureTerm() throws SQLException
  {
    logger.debug("public String getProcedureTerm()");

    return "procedure";
  }

  @Override
  public String getCatalogTerm() throws SQLException
  {
    logger.debug("public String getCatalogTerm()");

    return "database";
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException
  {
    logger.debug("public boolean isCatalogAtStart()");

    return true;
  }

  @Override
  public String getCatalogSeparator() throws SQLException
  {
    logger.debug("public String getCatalogSeparator()");

    return ".";
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException
  {
    logger.debug(
               "public boolean supportsSchemasInDataManipulation()");

    return true;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException
  {
    logger.debug(
               "public boolean supportsSchemasInProcedureCalls()");

    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException
  {
    logger.debug(
               "public boolean supportsSchemasInTableDefinitions()");

    return true;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException
  {
    logger.debug(
               "public boolean supportsSchemasInIndexDefinitions()");

    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
  {
    logger.debug(
               "public boolean supportsSchemasInPrivilegeDefinitions()");

    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException
  {
    logger.debug(
               "public boolean supportsCatalogsInDataManipulation()");

    return true;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException
  {
    logger.debug(
               "public boolean supportsCatalogsInProcedureCalls()");

    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException
  {
    logger.debug(
               "public boolean supportsCatalogsInTableDefinitions()");

    return true;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException
  {
    logger.debug(
               "public boolean supportsCatalogsInIndexDefinitions()");

    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
  {
    logger.debug(
               "public boolean supportsCatalogsInPrivilegeDefinitions()");

    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException
  {
    logger.debug(
               "public boolean supportsPositionedDelete()");

    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException
  {
    logger.debug(
               "public boolean supportsPositionedUpdate()");

    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException
  {
    logger.debug(
               "public boolean supportsSelectForUpdate()");

    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException
  {
    logger.debug(
               "public boolean supportsStoredProcedures()");

    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException
  {
    logger.debug(
               "public boolean supportsSubqueriesInComparisons()");

    return true;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException
  {
    logger.debug(
               "public boolean supportsSubqueriesInExists()");

    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException
  {
    logger.debug(
               "public boolean supportsSubqueriesInIns()");

    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException
  {
    logger.debug(
               "public boolean supportsSubqueriesInQuantifieds()");

    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException
  {
    logger.debug(
               "public boolean supportsCorrelatedSubqueries()");

    return true;
  }

  @Override
  public boolean supportsUnion() throws SQLException
  {
    logger.debug("public boolean supportsUnion()");

    return true;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException
  {
    logger.debug("public boolean supportsUnionAll()");

    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException
  {
    logger.debug(
               "public boolean supportsOpenCursorsAcrossCommit()");

    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException
  {
    logger.debug(
               "public boolean supportsOpenCursorsAcrossRollback()");

    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException
  {
    logger.debug(
               "public boolean supportsOpenStatementsAcrossCommit()");

    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException
  {
    logger.debug(
               "public boolean supportsOpenStatementsAcrossRollback()");

    return false;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException
  {
    logger.debug("public int getMaxBinaryLiteralLength()");

    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException
  {
    logger.debug("public int getMaxCharLiteralLength()");

    return 16777216;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException
  {
    logger.debug("public int getMaxColumnNameLength()");

    return 255;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException
  {
    logger.debug("public int getMaxColumnsInGroupBy()");

    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException
  {
    logger.debug("public int getMaxColumnsInIndex()");

    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException
  {
    logger.debug("public int getMaxColumnsInOrderBy()");

    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException
  {
    logger.debug("public int getMaxColumnsInSelect()");

    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException
  {
    logger.debug("public int getMaxColumnsInTable()");

    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException
  {
    logger.debug("public int getMaxConnections()");

    return 0;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException
  {
    logger.debug("public int getMaxCursorNameLength()");

    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException
  {
    logger.debug("public int getMaxIndexLength()");

    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException
  {
    logger.debug("public int getMaxSchemaNameLength()");

    return 255;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException
  {
    logger.debug("public int getMaxProcedureNameLength()");

    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException
  {
    logger.debug("public int getMaxCatalogNameLength()");

    return 255;
  }

  @Override
  public int getMaxRowSize() throws SQLException
  {
    logger.debug("public int getMaxRowSize()");

    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
  {
    logger.debug(
               "public boolean doesMaxRowSizeIncludeBlobs()");

    return true;
  }

  @Override
  public int getMaxStatementLength() throws SQLException
  {
    logger.debug("public int getMaxStatementLength()");

    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException
  {
    logger.debug("public int getMaxStatements()");

    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException
  {
    logger.debug("public int getMaxTableNameLength()");

    return 255;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException
  {
    logger.debug("public int getMaxTablesInSelect()");

    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException
  {
    logger.debug("public int getMaxUserNameLength()");

    return 255;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException
  {
    logger.debug(
               "public int getDefaultTransactionIsolation()");

    return Connection.TRANSACTION_READ_COMMITTED;
  }

  @Override
  public boolean supportsTransactions() throws SQLException
  {
    logger.debug("public boolean supportsTransactions()");

    return true;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level)
          throws SQLException
  {
    logger.debug(
               "public boolean supportsTransactionIsolationLevel(int level)");

    return (level == Connection.TRANSACTION_NONE)
        || (level == Connection.TRANSACTION_READ_COMMITTED);
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions()
          throws SQLException
  {
    logger.debug(
               "public boolean "
               + "supportsDataDefinitionAndDataManipulationTransactions()");

    return true;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException
  {
    logger.debug(
               "public boolean supportsDataManipulationTransactionsOnly()");

    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException
  {
    logger.debug(
               "public boolean dataDefinitionCausesTransactionCommit()");

    return true;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException
  {
    logger.debug(
               "public boolean dataDefinitionIgnoredInTransactions()");

    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern,
                                 String procedureNamePattern)
          throws SQLException
  {
    logger.debug(
               "public ResultSet getProcedures(String catalog, "
               + "String schemaPattern,String procedureNamePattern)");

    Statement statement = connection.createStatement();

    // Return empty result set since we don't have primary keys yet
    return new SnowflakeDatabaseMetaDataResultSet(
            Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                          "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"),
            Arrays.asList("TEXT", "TEXT", "TEXT", "TEXT",
                          "SHORT", "TEXT"),
            Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                          Types.VARCHAR, Types.SMALLINT, Types.VARCHAR),
            new Object[][]
            {
            }, statement);
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                       String procedureNamePattern,
                                       String columnNamePattern)
          throws SQLException
  {
    logger.debug(
               "public ResultSet getProcedureColumns(String catalog, "
               + "String schemaPattern,String procedureNamePattern,"
               + "String columnNamePattern)");

    Statement statement = connection.createStatement();
    //throw new SQLFeatureNotSupportedException();
    return new SnowflakeDatabaseMetaDataResultSet(
        Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                      "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME",
                      "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE",
                      "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
                      "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
                      "SPECIFIC_NAME"),
        Arrays.asList("TEXT", "TEXT", "TEXT", "TEXT", "SHORT", "INTEGER", "TEXT",
                      "INTEGER", "INTEGER", "SHORT", "SHORT", "SHORT", "TEXT",
                      "TEXT", "INTEGER", "INTEGER", "INTEGER", "INTEGER", "TEXT", "TEXT"),
        Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                      Types.SMALLINT, Types.INTEGER, Types.VARCHAR, Types.INTEGER,
                      Types.INTEGER, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
                      Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                      Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR),
        new Object[][]{},
        statement
    );
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern,
                             String tableNamePattern, String[] types)
          throws SQLException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug(
          "public ResultSet getTables(String catalog={}, String "
              + "schemaPattern={}, String tableNamePattern={}, " +
              "String[] types={})",
          new Object[]{catalog, schemaPattern, tableNamePattern,
              Arrays.toString(types)});
    }

    Set<String> supportedTableTypes = new HashSet<>();
    ResultSet resultSet = getTableTypes();
    while(resultSet.next())
    {
      supportedTableTypes.add(resultSet.getString("TABLE_TYPE"));
    }
    resultSet.close();

    List<String> inputValidTableTypes = new ArrayList<>();
    // then filter on the input table types;
    if (types != null)
    {
      for (int i = 0; i < types.length; i++) {
        if (supportedTableTypes.contains(types[i])) {
          inputValidTableTypes.add(types[i]);
        }
      }
    }
    else
    {
      inputValidTableTypes = new ArrayList<String>(supportedTableTypes);
    }

    // if the input table types don't have types supported by Snowflake,
    // then return an empty result set directly
    Statement statement = connection.createStatement();
    if (inputValidTableTypes.size() == 0)
    {
      return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(GET_TABLES, statement);
    }

    if (catalog == null && schemaPattern == null && metadataRequestUseConnectionCtx)
    {
      catalog = session.getDatabase();
      schemaPattern = session.getSchema();
    }

    final Pattern compiledSchemaPattern = Wildcard.toRegexPattern(schemaPattern, true);
    final Pattern compiledTablePattern = Wildcard.toRegexPattern(tableNamePattern, true);

    String showCommand = null;
    final boolean viewOnly = inputValidTableTypes.size() == 1
                             && "VIEW".equalsIgnoreCase(inputValidTableTypes.get(0));
    final boolean tableOnly = inputValidTableTypes.size() == 1
                             && "TABLE".equalsIgnoreCase(inputValidTableTypes.get(0));
    if (viewOnly)
    {
      showCommand = "show /* JDBC:DatabaseMetaData.getTables() */ views";
    }
    else if (tableOnly)
    {
      showCommand = "show /* JDBC:DatabaseMetaData.getTables() */ tables";
    }
    else
    {
      showCommand = "show /* JDBC:DatabaseMetaData.getTables() */ objects";
    }

    // only add pattern if it is not empty and not matching all character.
    if (tableNamePattern != null && !tableNamePattern.isEmpty()
        && !tableNamePattern.trim().equals("%")
        && !tableNamePattern.trim().equals(".*"))
    {
      showCommand += " like '" + tableNamePattern + "'";
    }

    if (catalog == null)
    {
      showCommand += " in account";
    }
    else if (catalog.isEmpty())
    {
      return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(GET_TABLES, statement);
    }
    else
    {
      // if the schema pattern is a deterministic identifier, specify schema
      // in the show command. This is necessary for us to see any tables in
      // a schema if the current schema a user is connected to is different
      // given that we don't support show tables without a known schema.
      if (schemaPattern == null || Wildcard.isWildcardPatternStr(schemaPattern))
      {
        showCommand += " in database \"" + catalog + "\"";
      }
      else if (schemaPattern.isEmpty())
      {
        return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(GET_TABLES, statement);
      }
      else
      {
        schemaPattern = schemaPattern.replace("\\", "");
        showCommand += " in schema \"" + catalog + "\".\"" +
              schemaPattern + "\"";
      }
    }

    logger.debug("sql command to get table metadata: {}", showCommand);

    resultSet = executeAndReturnEmptyResultIfNotFound(statement, showCommand, GET_TABLES);

    return new SnowflakeDatabaseMetaDataResultSet(GET_TABLES, resultSet, statement)
            {
              public boolean next() throws SQLException
              {
                logger.debug("public boolean next()");

                // iterate throw the show table result until we find an entry
                // that matches the table name
                while (showObjectResultSet.next())
                {
                  String tableName = showObjectResultSet.getString(2);

                  String dbName;
                  String schemaName;
                  String kind;
                  String comment;

                  if (viewOnly)
                  {
                    dbName = showObjectResultSet.getString(4);
                    schemaName = showObjectResultSet.getString(5);
                    kind = "VIEW";
                    comment = showObjectResultSet.getString(7);
                  }
                  else
                  {
                    dbName = showObjectResultSet.getString(3);
                    schemaName = showObjectResultSet.getString(4);
                    kind = showObjectResultSet.getString(5);
                    comment = showObjectResultSet.getString(6);
                  }

                  if ((compiledTablePattern == null
                       || compiledTablePattern.matcher(tableName).matches())
                      && (compiledSchemaPattern == null
                          || compiledSchemaPattern.matcher(schemaName).matches()))
                  {
                    nextRow[0] = dbName;
                    nextRow[1] = schemaName;
                    nextRow[2] = tableName;
                    nextRow[3] = kind;
                    nextRow[4] = comment;
                    nextRow[5] = null;
                    nextRow[6] = null;
                    nextRow[7] = null;
                    nextRow[8] = null;
                    nextRow[9] = null;
                    return true;
                  }
                }

                statement.close();
                statement = null;

                return false;
              }
            };
  }

  @Override
  public ResultSet getSchemas() throws SQLException
  {
    logger.debug("public ResultSet getSchemas()");

    return getSchemas(null, null);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException
  {
    logger.debug("public ResultSet getCatalogs()");

    String showDB = "show /* JDBC:DatabaseMetaData.getCatalogs() */ databases in account";

    Statement statement = connection.createStatement();
    return new SnowflakeDatabaseMetaDataResultSet(GET_CATALOGS,
            statement.executeQuery(showDB), statement)
            {
              public boolean next() throws SQLException
              {
                logger.debug("public boolean next()");

                // iterate throw the show databases result
                while (showObjectResultSet.next())
                {
                  String dbName = showObjectResultSet.getString(2);

                  nextRow[0] = dbName;
                  return true;
                }

                statement.close();
                statement = null;

                return false;
              }
            };
  }

  @Override
  public ResultSet getTableTypes() throws SQLException
  {
    logger.debug("public ResultSet getTableTypes()");

    Statement statement = connection.createStatement();

    // TODO: We should really get the list of table types from GS
    return new SnowflakeDatabaseMetaDataResultSet(
            Arrays.asList("TABLE_TYPE"),
            Arrays.asList("TEXT"),
            Arrays.asList(Types.VARCHAR),
            new Object[][]
            {
              {
                "TABLE"
              },
              {
                "VIEW"
              }
            }, statement);
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern,
                              String tableNamePattern,
                              String columnNamePattern)
          throws SQLException
  {
    return getColumns(catalog, schemaPattern, tableNamePattern,
                      columnNamePattern, false);
  }

  public ResultSet getColumns(String catalog, String schemaPattern,
                              String tableNamePattern,
                              String columnNamePattern,
                              final boolean extendedSet)
          throws SQLException
  {
    Statement statement = connection.createStatement();
    logger.debug("public ResultSet getColumns(String catalog={}, String schemaPattern={}" +
            "String tableNamePattern={}, String columnNamePattern={}, boolean extendedSet={}",
        catalog, schemaPattern, tableNamePattern, columnNamePattern, extendedSet);

    if (catalog == null && schemaPattern == null && metadataRequestUseConnectionCtx)
    {
      catalog = session.getDatabase();
      schemaPattern = session.getSchema();
    }

    final Pattern compiledSchemaPattern = Wildcard.toRegexPattern(schemaPattern, true);
    final Pattern compiledTablePattern = Wildcard.toRegexPattern(tableNamePattern, true);
    final Pattern compiledColumnPattern = Wildcard.toRegexPattern(columnNamePattern, true);

    String showColumnCommand = "show /* JDBC:DatabaseMetaData.getColumns() */ columns";

    if (columnNamePattern != null && !columnNamePattern.isEmpty() &&
        !columnNamePattern.trim().equals("%") &&
        !columnNamePattern.trim().equals(".*"))
    {
      showColumnCommand += " like '" + columnNamePattern + "'";
    }

    if (catalog == null)
    {
      showColumnCommand += " in account";
    }
    else if (catalog.isEmpty())
    {
      return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(
          extendedSet ? GET_COLUMNS_EXTENDED_SET : GET_COLUMNS, statement);
    }
    else
    {
      if (schemaPattern == null || Wildcard.isWildcardPatternStr(schemaPattern))
      {
        showColumnCommand += " in database \"" + catalog + "\"";
      }
      else if (schemaPattern.isEmpty())
      {
        return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(
            extendedSet ? GET_COLUMNS_EXTENDED_SET : GET_COLUMNS, statement);
      }
      else
      {
        schemaPattern = schemaPattern.replace("\\", "");
        if (tableNamePattern == null || Wildcard.isWildcardPatternStr(tableNamePattern))
        {
          showColumnCommand += " in schema \"" + catalog + "\".\"" +
              schemaPattern + "\"";
        }
        else if (tableNamePattern.isEmpty())
        {
          return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(
              extendedSet ? GET_COLUMNS_EXTENDED_SET : GET_COLUMNS, statement);
        }
        else
        {
          tableNamePattern = tableNamePattern.replace("\\", "");
          showColumnCommand += " in table \"" + catalog + "\".\"" +
              schemaPattern + "\".\"" + tableNamePattern + "\"";
        }
      }
    }

    logger.debug("sql command to get column metadata: {}",
               showColumnCommand);

    ResultSet resultSet = executeAndReturnEmptyResultIfNotFound(statement, showColumnCommand,
        extendedSet ? GET_COLUMNS_EXTENDED_SET : GET_COLUMNS);

    return new SnowflakeDatabaseMetaDataResultSet(
            extendedSet ? GET_COLUMNS_EXTENDED_SET : GET_COLUMNS,
            resultSet, statement)
            {
              int ordinalPosition = 0;

              String currentTableName = null;

              public boolean next() throws SQLException
              {
                logger.debug("public boolean next()");

                // iterate throw the show table result until we find an entry
                // that matches the table name
                while (showObjectResultSet.next())
                {
                  String tableName = showObjectResultSet.getString(1);
                  String schemaName = showObjectResultSet.getString(2);
                  String columnName = showObjectResultSet.getString(3);
                  String dataTypeStr = showObjectResultSet.getString(4);
                  String defaultValue = showObjectResultSet.getString(6);
                  String comment = showObjectResultSet.getString(9);
                  String catalogName = showObjectResultSet.getString(10);
                  String autoIncrement = showObjectResultSet.getString(11);

                  if ((compiledTablePattern == null
                       || compiledTablePattern.matcher(tableName).matches())
                      && (compiledSchemaPattern == null
                          || compiledSchemaPattern.matcher(schemaName).matches())
                      && (compiledColumnPattern == null
                          || compiledColumnPattern.matcher(columnName).matches()))
                  {
                    logger.debug(
                               "Found a matched column:" + tableName
                               + "." + columnName);

                    // reset ordinal position for new table
                    if (!tableName.equals(currentTableName))
                    {
                      ordinalPosition = 1;
                      currentTableName = tableName;
                    }
                    else
                    {
                      ordinalPosition++;
                    }

                    JsonNode jsonNode;
                    try
                    {
                      jsonNode = mapper.readTree(dataTypeStr);
                    }
                    catch (Exception ex)
                    {
                      logger.error("Exeception when parsing column"
                              + " result", ex);

                      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
                                                      ErrorCode.INTERNAL_ERROR
                                                      .getMessageCode(),
                                                      "error parsing data type: "
                                                      + dataTypeStr);
                    }

                    logger.debug("data type string: {}",
                               dataTypeStr);

                    SnowflakeColumnMetadata columnMetadata = SnowflakeUtil
                                    .extractColumnMetadata(jsonNode,
                                            session.isJdbcTreatDecimalAsInt());

                    logger.debug("nullable: {}",
                               columnMetadata.isNullable());

                    // SNOW-16881: add catalog name
                    nextRow[0] = catalogName;
                    nextRow[1] = schemaName;
                    nextRow[2] = tableName;
                    nextRow[3] = columnName;

                    int internalColumnType = columnMetadata.getType();
                    int externalColumnType = internalColumnType;

                    if (internalColumnType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ ||
                        internalColumnType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ)
                      externalColumnType = Types.TIMESTAMP;

                    nextRow[4] = new Integer(externalColumnType);
                    nextRow[5] = columnMetadata.getTypeName();

                    int columnSize = 0;

                    if (columnMetadata.getType() == Types.VARCHAR
                        || columnMetadata.getType() == Types.CHAR)
                    {
                      columnSize = columnMetadata.getLength();
                    }
                    else if (columnMetadata.getType() == Types.DECIMAL
                             || columnMetadata.getType() == Types.BIGINT
                             || columnMetadata.getType() == Types.TIME
                             || columnMetadata.getType() == Types.TIMESTAMP)
                    {
                      columnSize = columnMetadata.getPrecision();
                    }

                    nextRow[6] = new Integer(columnSize);
                    nextRow[7] = null;
                    nextRow[8] = new Integer(columnMetadata.getScale());
                    nextRow[9] = null;
                    nextRow[10] = (columnMetadata.isNullable()
                                   ? columnNullable : columnNoNulls);

                    logger.debug("returning nullable: {}",
                               nextRow[10]);

                    nextRow[11] = comment;
                    nextRow[12] = defaultValue;
                    // snow-10597: sql data type is integer instead of string
                    nextRow[13] = externalColumnType;
                    nextRow[14] = null;
                    nextRow[15] = (columnMetadata.getType() == Types.VARCHAR
                                   || columnMetadata.getType() == Types.CHAR)
                                  ? new Integer(columnMetadata.getLength()) : null;
                    nextRow[16] = new Integer(ordinalPosition);

                    nextRow[17] = (columnMetadata.isNullable() ? "YES" : "NO");
                    nextRow[18] = null;
                    nextRow[19] = null;
                    nextRow[20] = null;
                    nextRow[21] = null;
                    nextRow[22] = "".equals(autoIncrement) ? "NO" : "YES";
                    nextRow[23] = "NO";
                    if (extendedSet)
                    {
                      nextRow[24] = columnMetadata.getBase().name();
                    }
                    return true;
                  }
                }

                statement.close();
                statement = null;
                return false;
              }
            };
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema,
                                       String table, String columnNamePattern)
          throws SQLException
  {
    logger.debug(
               "public ResultSet getColumnPrivileges(String catalog, "
               + "String schema,String table, String columnNamePattern)");

    Statement statement = connection.createStatement();
    return new SnowflakeDatabaseMetaDataResultSet(
        Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                      "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"),
        Arrays.asList("TEXT", "TEXT", "TEXT", "TEXT",
                      "TEXT", "TEXT", "TEXT", "TEXT"),
        Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                      Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR),
        new Object[][]
        {
        }, statement);
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                      String tableNamePattern)
          throws SQLException
  {
    logger.debug(
               "public ResultSet getTablePrivileges(String catalog, "
               + "String schemaPattern,String tableNamePattern)");

    Statement statement = connection.createStatement();

    // Return empty result set since we don't have primary keys yet
    return new SnowflakeDatabaseMetaDataResultSet(
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR",
                          "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"),
            Arrays.asList("TEXT", "TEXT", "TEXT", "TEXT", "TEXT",
                          "TEXT", "TEXT"),
            Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                          Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                          Types.VARCHAR),
            new Object[][]
            {
            }, statement);
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema,
                                        String table, int scope,
                                        boolean nullable) throws SQLException
  {
    logger.debug(
               "public ResultSet getBestRowIdentifier(String catalog, "
               + "String schema,String table, int scope,boolean nullable)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema,
                                     String table) throws SQLException
  {
    logger.debug(
               "public ResultSet getVersionColumns(String catalog, "
               + "String schema, String table)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, final String table)
          throws SQLException
  {
    Statement statement = connection.createStatement();
    logger.debug(
               "public ResultSet getPrimaryKeys(String catalog={}, "
               + "String schema={}, String table={})",
              new Object[]{catalog, schema, table});
    String showPKCommand = "show /* JDBC:DatabaseMetaData.getPrimaryKeys() */ primary keys in ";

    if (catalog == null && schema == null && metadataRequestUseConnectionCtx)
    {
      catalog = session.getDatabase();
      schema = session.getSchema();
    }

    if (catalog == null)
    {
      showPKCommand += "account";
    }
    else if (catalog.isEmpty())
    {
      return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(GET_PRIMARY_KEYS, statement);
    }
    else
    {
      if (schema == null)
      {
        showPKCommand += "database \"" + catalog + "\"";
      }
      else if (schema.isEmpty())
      {
        return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(GET_PRIMARY_KEYS, statement);
      }
      else
      {
        if (table == null)
        {
          showPKCommand += "schema \"" + catalog + "\".\"" + schema + "\"";
        }
        else if (table.isEmpty())
          return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(GET_PRIMARY_KEYS, statement);
        else
        {
          showPKCommand += "table \"" + catalog + "\".\"" + schema + "\".\"" +
                            table + "\"";
        }
      }
    }

    final String catalogIn = catalog;
    final String schemaIn = schema;
    final String tableIn = table;

    logger.debug("sql command to get primary key metadata: {}",
               showPKCommand);

    ResultSet resultSet = executeAndReturnEmptyResultIfNotFound(statement, showPKCommand, GET_PRIMARY_KEYS);
    // Return empty result set since we don't have primary keys yet
    return new SnowflakeDatabaseMetaDataResultSet(GET_PRIMARY_KEYS, resultSet, statement)
    {
      @Override
      public boolean next() throws SQLException
      {
        logger.debug("public boolean next()");

        while (showObjectResultSet.next())
        {
          // Get the values for each field to display
          String table_cat = showObjectResultSet.getString(2);
          String table_schem = showObjectResultSet.getString(3);
          String table_name = showObjectResultSet.getString(4);
          String column_name = showObjectResultSet.getString(5);
          int key_seq = showObjectResultSet.getInt(6);
          String pk_name = showObjectResultSet.getString(7);

          // Post filter based on the input
          if ((catalogIn == null || catalogIn.equals(table_cat)) &&
              (schemaIn == null || schemaIn.equals(table_schem)) &&
              (tableIn == null || tableIn.equals(table_name)))
          {
            nextRow[0] = table_cat;
            nextRow[1] = table_schem;
            nextRow[2] = table_name;
            nextRow[3] = column_name;
            nextRow[4] = key_seq;
            nextRow[5] = pk_name;
            return true;
          }
        }

        statement.close();
        statement = null;

        return false;
      }
    };
  }

  /**
   *
   * @param client
   * @param parentCatalog
   * @param parentSchema
   * @param parentTable
   * @param foreignCatalog
   * @param foreignSchema
   * @param foreignTable
   * @return
   */
  private ResultSet getForeignKeys(
      final String client, String parentCatalog,
      String parentSchema,
      final String parentTable, final String foreignCatalog,
      final String foreignSchema,
      final String foreignTable) throws SQLException
  {
    Statement statement = connection.createStatement();
    StringBuilder commandBuilder = new StringBuilder();

    if (client.equals("export") || client.equals("cross"))
    {
      commandBuilder.append("show /* JDBC:DatabaseMetaData.getForeignKeys() */ " +
          "exported keys in ");
    }
    else if (client.equals("import"))
    {
      commandBuilder.append("show /* JDBC:DatabaseMetaData.getForeignKeys() */ " +
          "imported keys in ");
    }

    if (parentCatalog == null)
    {
      commandBuilder.append("account");
    }
    else if (parentCatalog.isEmpty())
    {
      return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(GET_FOREIGN_KEYS, statement);
    }
    else
    {
      if (parentSchema == null)
      {
        commandBuilder.append("database \"" + parentCatalog + "\"");
      }
      else if (parentSchema.isEmpty())
      {
        return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(GET_FOREIGN_KEYS, statement);
      }
      else
      {
        if (parentTable == null)
        {
          commandBuilder.append("schema \"" + parentCatalog + "\".\"" +
                                    parentSchema + "\"");
        }
        else if (parentTable.isEmpty())
        {
          return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(GET_FOREIGN_KEYS, statement);
        }
        else
        {
          commandBuilder.append("table \"" + parentCatalog + "\".\"" +
                                 parentSchema + "\".\"" + parentTable + "\"");
        }
      }
    }

    final String finalParentCatalog = parentCatalog;
    final String finalParentSchema = parentSchema;

    String command = commandBuilder.toString();

    ResultSet resultSet = executeAndReturnEmptyResultIfNotFound(statement, command, GET_FOREIGN_KEYS);

    return new SnowflakeDatabaseMetaDataResultSet(GET_FOREIGN_KEYS, resultSet, statement)
    {
      @Override
      public boolean next() throws SQLException
      {
        logger.debug("public boolean next()");

        while (showObjectResultSet.next())
        {
          // Get the value for each field to display
          String pktable_cat = showObjectResultSet.getString(2);
          String pktable_schem = showObjectResultSet.getString(3);
          String pktable_name = showObjectResultSet.getString(4);
          String pkcolumn_name = showObjectResultSet.getString(5);
          String fktable_cat = showObjectResultSet.getString(6);
          String fktable_schem = showObjectResultSet.getString(7);
          String fktable_name = showObjectResultSet.getString(8);
          String fkcolumn_name = showObjectResultSet.getString(9);
          int key_seq = showObjectResultSet.getInt(10);
          short updateRule = getForeignKeyConstraintProperty(
              "update", showObjectResultSet.getString(11));
          short deleteRule = getForeignKeyConstraintProperty(
              "delete", showObjectResultSet.getString(12));
          String fk_name = showObjectResultSet.getString(13);
          String pk_name = showObjectResultSet.getString(14);
          short deferrability = getForeignKeyConstraintProperty(
              "deferrability", showObjectResultSet.getString(15));

          boolean passedFilter = false;

          // Post filter the results based on the clinet type
          if (client.equals("import"))
          {
            // For imported dkeys, filter on the foreign key table
            if ((finalParentCatalog == null || finalParentCatalog.equals(fktable_cat)) &&
                (finalParentSchema == null || finalParentSchema.equals(fktable_schem)) &&
                (parentTable == null || parentTable.equals(fktable_name)))
              passedFilter = true;
          }
          else if (client.equals("export"))
          {
            // For exported keys, filter on the primary key table
            if ((finalParentCatalog == null || finalParentCatalog.equals(pktable_cat)) &&
                (finalParentSchema == null || finalParentSchema.equals(pktable_schem)) &&
                (parentTable == null || parentTable.equals(pktable_name)))
              passedFilter = true;
          }
          else if (client.equals("cross"))
          {
            // For cross references, filter on both the primary key and foreign
            // key table
            if ((finalParentCatalog == null || finalParentCatalog.equals(pktable_cat)) &&
                (finalParentSchema == null || finalParentSchema.equals(pktable_schem)) &&
                (parentTable == null || parentTable.equals(pktable_name)) &&
                (foreignCatalog == null ||
                 foreignCatalog.equals(fktable_cat)) &&
                (foreignSchema == null ||
                 foreignSchema.equals(fktable_schem)) &&
                (foreignTable == null ||
                 foreignTable.equals(fktable_name)))
              passedFilter = true;
          }

          if (passedFilter)
          {
            nextRow[0] = pktable_cat;
            nextRow[1] = pktable_schem;
            nextRow[2] = pktable_name;
            nextRow[3] = pkcolumn_name;
            nextRow[4] = fktable_cat;
            nextRow[5] = fktable_schem;
            nextRow[6] = fktable_name;
            nextRow[7] = fkcolumn_name;
            nextRow[8] = key_seq;
            nextRow[9] = updateRule;
            nextRow[10] = deleteRule;
            nextRow[11] = fk_name;
            nextRow[12] = pk_name;
            nextRow[13] = deferrability;
            return true;
          }
        }

        statement.close();
        statement = null;

        return false;
      }
    };
  }

  /**
   * Returns the ODBC standard property string for the property string used
   * in our show constraint commands
   * @param property_name
   * @param property
   * @return
   */
  private short getForeignKeyConstraintProperty(
      String property_name, String property)
  {
    short result = 0;
    if (property_name.equals("update") || property_name.equals("delete"))
    {
      if (property.equals("NO ACTION"))
        result = importedKeyNoAction;
      if (property.equals("CASCADE"))
        result = importedKeyCascade;
      if (property.equals("SET NULL"))
        result = importedKeySetNull;
      if (property.equals("SET DEFAULT"))
        result = importedKeySetDefault;
      if (property.equals("RESTRICT"))
        result = importedKeyRestrict;
    }
    else // deferrability
    {
      if (property.equals("INITIALLY DEFERRED"))
        result = importedKeyInitiallyDeferred;
      else if (property.equals("INITIALLY IMMEDIATE"))
        result = importedKeyInitiallyImmediate;
      else if (property.equals("NOT DEFERRABLE"))
        result = importedKeyNotDeferrable;
    }

    return result;
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
          throws SQLException
  {
    logger.debug(
               "public ResultSet getImportedKeys(String catalog={}, "
               + "String schema={}, String table={})",
               new Object[]{catalog, schema, table});

    return getForeignKeys("import", catalog, schema, table, null, null, null);
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
          throws SQLException
  {
    logger.debug(
               "public ResultSet getExportedKeys(String catalog={}, "
               + "String schema={}, String table={})",
               new Object[]{catalog, schema, table});

    return getForeignKeys("export", catalog, schema, table, null, null, null);
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema,
                                     String parentTable, String foreignCatalog,
                                     String foreignSchema, String foreignTable)
          throws SQLException
  {
    logger.debug(
               "public ResultSet getCrossReference(String parentCatalog={}, "
               + "String parentSchema={}, String parentTable={}, "
               + "String foreignCatalog={}, String foreignSchema={}, "
               + "String foreignTable={})",
               new Object[]{parentCatalog, parentSchema, parentTable,
                            foreignCatalog, foreignSchema, foreignTable});

    return getForeignKeys("cross", parentCatalog, parentSchema, parentTable,
                          foreignCatalog, foreignSchema, foreignTable);
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException
  {
    logger.debug("public ResultSet getTypeInfo()");

    Statement statement = connection.createStatement();

    // Return empty result set since we don't have primary keys yet
    return new SnowflakeDatabaseMetaDataResultSet(
            Arrays.asList("TYPE_NAME", "DATA_TYPE", "PRECISION",
                          "LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS",
                          "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE",
                          "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE",
                          "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE",
                          "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
                          "NUM_PREC_RADIX"),
            Arrays.asList("TEXT", "INTEGER", "INTEGER", "TEXT", "TEXT",
                          "TEXT", "SHORT", "BOOLEAN", "SHORT", "BOOLEAN",
                          "BOOLEAN", "BOOLEAN", "TEXT", "SHORT", "SHORT",
                          "INTEGER", "INTEGER", "INTEGER"),
            Arrays.asList(Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                          Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                          Types.SMALLINT, Types.BOOLEAN, Types.SMALLINT,
                          Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN,
                          Types.VARCHAR, Types.SMALLINT, Types.SMALLINT,
                          Types.INTEGER, Types.INTEGER, Types.INTEGER),
            new Object[][]
            {
              {
                "NUMBER", Types.DECIMAL, 38, null, null, null, typeNullable,
                false, typeSearchable, false, true, true, null, 0, 37, -1,
                -1, -1
              },
              {
                "INTEGER", Types.INTEGER, 38, null, null, null, typeNullable,
                false, typeSearchable, false, true, true, null, 0, 0, -1,
                -1, -1
              },
              {
                "DOUBLE", Types.DOUBLE, 38, null, null, null, typeNullable,
                false, typeSearchable, false, true, true, null, 0, 37, -1,
                -1, -1
              },
              {
                "VARCHAR", Types.VARCHAR, -1, null, null, null, typeNullable,
                false, typeSearchable, false, true, true, null, -1, -1, -1,
                -1, -1
              },
              {
                "DATE", Types.DATE, -1, null, null, null, typeNullable,
                false, typeSearchable, false, true, true, null, -1, -1, -1,
                -1, -1
              },
              {
                "TIME", Types.TIME, -1, null, null, null, typeNullable,
                false, typeSearchable, false, true, true, null, -1, -1, -1,
                -1, -1
              },
              {
                "TIMESTAMP", Types.TIMESTAMP, -1, null, null, null, typeNullable,
                false, typeSearchable, false, true, true, null, -1, -1, -1,
                -1, -1
              },
              {
                "BOOLEAN", Types.BOOLEAN, -1, null, null, null, typeNullable,
                false, typeSearchable, false, true, true, null, -1, -1, -1,
                -1, -1
              }
            }, statement);
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table,
                                boolean unique, boolean approximate)
          throws SQLException
  {
    logger.debug(
               "public ResultSet getIndexInfo(String catalog, String schema, "
               + "String table,boolean unique, boolean approximate)");

    Statement statement = connection.createStatement();

    // Return empty result set since we don't have primary keys yet
    return new SnowflakeDatabaseMetaDataResultSet(
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                          "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME",
                          "TYPE", "ORDINAL_POSITION", "COLUMN_NAME",
                          "ASC_OR_DESC", "CARDINALITY", "PAGES",
                          "FILTER_CONDITION"),
            Arrays.asList("TEXT", "TEXT", "TEXT", "BOOLEAN", "TEXT", "TEXT",
                          "SHORT", "SHORT", "TEXT", "TEXT", "INTEGER",
                          "INTEGER", "TEXT"),
            Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                          Types.BOOLEAN, Types.VARCHAR, Types.VARCHAR,
                          Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                          Types.VARCHAR, Types.INTEGER, Types.INTEGER,
                          Types.VARCHAR),
            new Object[][]
            {
            }, statement);
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException
  {
    logger.debug(
               "public boolean supportsResultSetType(int type)");

    return (type == ResultSet.TYPE_FORWARD_ONLY);
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency)
          throws SQLException
  {
    logger.debug(
               "public boolean supportsResultSetConcurrency(int type, "
               + "int concurrency)");

    return (type == ResultSet.TYPE_FORWARD_ONLY
            && concurrency == ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException
  {
    logger.debug(
               "public boolean ownUpdatesAreVisible(int type)");

    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException
  {
    logger.debug(
               "public boolean ownDeletesAreVisible(int type)");

    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException
  {
    logger.debug(
               "public boolean ownInsertsAreVisible(int type)");

    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException
  {
    logger.debug(
               "public boolean othersUpdatesAreVisible(int type)");

    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException
  {
    logger.debug(
               "public boolean othersDeletesAreVisible(int type)");

    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException
  {
    logger.debug(
               "public boolean othersInsertsAreVisible(int type)");

    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException
  {
    logger.debug(
               "public boolean updatesAreDetected(int type)");

    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException
  {
    logger.debug(
               "public boolean deletesAreDetected(int type)");

    return false;
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException
  {
    logger.debug(
               "public boolean insertsAreDetected(int type)");

    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException
  {
    logger.debug("public boolean supportsBatchUpdates()");

    return true;
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern,
                           String typeNamePattern, int[] types)
          throws SQLException
  {
    logger.debug(
               "public ResultSet getUDTs(String catalog, "
               + "String schemaPattern,String typeNamePattern, int[] types)");

    // We don't user-defined types, so return an empty result set
    Statement statement = connection.createStatement();
    return new SnowflakeDatabaseMetaDataResultSet(
        Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME",
            "DATA_TYPE", "REMARKS", "BASE_TYPE"),
        Arrays.asList("TEXT", "TEXT", "TEXT", "TEXT", "INTEGER",
            "TEXT", "SHORT"),
        Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.SMALLINT),
        new Object[][]
            {
            }, statement);
  }

  @Override
  public Connection getConnection() throws SQLException
  {
    logger.debug("public Connection getConnection()");

    return connection;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException
  {
    logger.debug("public boolean supportsSavepoints()");

    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException
  {
    logger.debug(
               "public boolean supportsNamedParameters()");

    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException
  {
    logger.debug(
               "public boolean supportsMultipleOpenResults()");

    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException
  {
    logger.debug(
               "public boolean supportsGetGeneratedKeys()");

    return false;
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern,
                                 String typeNamePattern) throws SQLException
  {
    logger.debug(
               "public ResultSet getSuperTypes(String catalog, "
               + "String schemaPattern,String typeNamePattern)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern,
                                  String tableNamePattern) throws SQLException
  {
    logger.debug(
               "public ResultSet getSuperTables(String catalog, "
               + "String schemaPattern,String tableNamePattern)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern,
                                 String typeNamePattern,
                                 String attributeNamePattern)
          throws SQLException
  {
    logger.debug(
               "public ResultSet getAttributes(String catalog, String "
               + "schemaPattern,"
               + "String typeNamePattern,String attributeNamePattern)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability)
          throws SQLException
  {
    logger.debug(
               "public boolean supportsResultSetHoldability(int holdability)");

    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException
  {
    logger.debug("public int getResultSetHoldability()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException
  {
    logger.debug("public int getDatabaseMajorVersion()");

    return ((SnowflakeConnectionV1)connection).getDatabaseMajorVersion();
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException
  {
    logger.debug("public int getDatabaseMinorVersion()");

    return ((SnowflakeConnectionV1)connection).getDatabaseMinorVersion();
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException
  {
    logger.debug("public int getJDBCMajorVersion()");

    return Integer.parseInt(JDBCVersion.split("\\.", 2)[0]);
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException
  {
    logger.debug("public int getJDBCMinorVersion()");

    return Integer.parseInt(JDBCVersion.split("\\.", 2)[1]);
  }

  @Override
  public int getSQLStateType() throws SQLException
  {
    logger.debug("public int getSQLStateType()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException
  {
    logger.debug("public boolean locatorsUpdateCopy()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException
  {
    logger.debug(
               "public boolean supportsStatementPooling()");

    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException
  {
    logger.debug("public RowIdLifetime getRowIdLifetime()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern)
          throws SQLException
  {
    logger.debug(
               "public ResultSet getSchemas(String catalog={}, String "
               + "schemaPattern={})",
        new Object[]{catalog, schemaPattern});

    // try to determine if to use session database and schema
    if (catalog == null && schemaPattern == null && metadataRequestUseConnectionCtx)
    {
      catalog = session.getDatabase();
      schemaPattern = session.getSchema();
    }

    final Pattern compiledSchemaPattern = Wildcard.toRegexPattern(schemaPattern, true);

    String showSchemas = "show /* JDBC:DatabaseMetaData.getSchemas() */ schemas";

    Statement statement = connection.createStatement();
    // only add pattern if it is not empty and not matching all character.
    if (schemaPattern != null && !schemaPattern.isEmpty()
        && !schemaPattern.trim().equals("%")
        && !schemaPattern.trim().equals(".*"))
    {
      showSchemas += " like '" + schemaPattern + "'";
    }

    if (catalog == null)
    {
      showSchemas += " in account";
    }
    else if (catalog.isEmpty())
    {
      return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(GET_SCHEMAS, statement);
    }
    else
    {
      showSchemas += " in database \"" + catalog + "\"";
    }

    logger.debug("sql command to get schemas metadata: {}",
        showSchemas);

    ResultSet resultSet = executeAndReturnEmptyResultIfNotFound(statement, showSchemas, GET_SCHEMAS);
    return new SnowflakeDatabaseMetaDataResultSet(GET_SCHEMAS, resultSet, statement)
    {
      public boolean next() throws SQLException
      {
        logger.debug("public boolean next()");

        // iterate throw the show table result until we find an entry
        // that matches the table name
        while (showObjectResultSet.next())
        {
          String schemaName = showObjectResultSet.getString(2);
          String dbName = showObjectResultSet.getString(5);

          if (compiledSchemaPattern == null
              || compiledSchemaPattern.matcher(schemaName).matches())
          {
            nextRow[0] = schemaName;
            nextRow[1] = dbName;
            return true;
          }
        }

        statement.close();
        statement = null;

        return false;
      }
    };
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException
  {
    logger.debug(
               "public boolean supportsStoredFunctionsUsingCallSyntax()");

    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException
  {
    logger.debug(
               "public boolean autoCommitFailureClosesAllResultSets()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException
  {
    logger.debug(
               "public ResultSet getClientInfoProperties()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern,
                                String functionNamePattern) throws SQLException
  {
    Statement statement = connection.createStatement();
    logger.debug("public ResultSet getFunctions(String catalog={}, String schemaPattern={}, " +
            "String functionNamePattern={}",
        catalog, schemaPattern, functionNamePattern);

    if (catalog == null && schemaPattern == null && metadataRequestUseConnectionCtx)
    {
      catalog = session.getDatabase();
      schemaPattern = session.getSchema();
    }

    final Pattern compiledSchemaPattern = Wildcard.toRegexPattern(schemaPattern, true);
    final Pattern compiledFunctionPattern = Wildcard.toRegexPattern(functionNamePattern, true);

    String showFunctionCommand = "show /* JDBC:DatabaseMetaData.getFunctions() */ functions";

    if (functionNamePattern != null && !functionNamePattern.isEmpty() &&
        !functionNamePattern.trim().equals("%") &&
        !functionNamePattern.trim().equals(".*"))
    {
      showFunctionCommand += " like '" + functionNamePattern + "'";
    }

    if (catalog == null)
    {
      showFunctionCommand += " in account";
    }
    else if (catalog.isEmpty())
    {
      return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(GET_FUNCTIONS, statement);
    }
    else
    {
      if (schemaPattern == null || Wildcard.isWildcardPatternStr(schemaPattern))
      {
        showFunctionCommand += " in database \"" + catalog + "\"";
      }
      else if (schemaPattern.isEmpty())
      {
        return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(GET_FUNCTIONS, statement);
      }
      else
      {
        schemaPattern = schemaPattern.replace("\\", "");
        showFunctionCommand += " in schema \"" + catalog + "\".\"" +
            schemaPattern +  "\"";
      }
    }

    logger.debug("sql command to get column metadata: {}",
        showFunctionCommand);

    ResultSet resultSet = executeAndReturnEmptyResultIfNotFound(statement, showFunctionCommand, GET_FUNCTIONS);

    return new SnowflakeDatabaseMetaDataResultSet(GET_FUNCTIONS, resultSet, statement)
    {
      public boolean next() throws SQLException
      {
        logger.debug("public boolean next()");

        // iterate throw the show table result until we find an entry
        // that matches the table name
        while (showObjectResultSet.next())
        {
          String catalogName = showObjectResultSet.getString(11);
          String schemaName = showObjectResultSet.getString(3);
          String functionName = showObjectResultSet.getString(2);
          String remarks = showObjectResultSet.getString(10);
          int functionType = ("Y".equals(showObjectResultSet.getString(12)) ?
                                functionReturnsTable : functionNoTable);
          String specificName = functionName;
          if ((compiledFunctionPattern == null
              || compiledFunctionPattern.matcher(functionName).matches())
              && (compiledSchemaPattern == null
              || compiledSchemaPattern.matcher(schemaName).matches()))
          {
            logger.debug(
                "Found a matched function:" + schemaName
                    + "." + functionName);

            nextRow[0] = catalogName;
            nextRow[1] = schemaName;
            nextRow[2] = functionName;
            nextRow[3] = remarks;
            nextRow[4] = functionType;
            nextRow[5] = specificName;
            return true;
          }
        }
        statement.close();
        statement = null;
        return false;
      }
    };
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                      String functionNamePattern,
                                      String columnNamePattern)
          throws SQLException
  {
    logger.debug(
               "public ResultSet getFunctionColumns(String catalog, "
               + "String schemaPattern,String functionNamePattern,"
               + "String columnNamePattern)");

    return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(
        GET_FUNCTION_COLUMNS, connection.createStatement());
  }

  //@Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern,
                                    String tableNamePattern,
                                    String columnNamePattern)
          throws SQLException
  {
    logger.debug(
               "public ResultSet getPseudoColumns(String catalog, "
               + "String schemaPattern,String tableNamePattern,"
               + "String columnNamePattern)");

    throw new SQLFeatureNotSupportedException();
  }

  //@Override
  public boolean generatedKeyAlwaysReturned() throws SQLException
  {
    logger.debug(
               "public boolean generatedKeyAlwaysReturned()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(
          Class<T> iface) throws SQLException
  {
    logger.debug("public <T> T unwrap(Class<T> iface)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(
          Class<?> iface) throws SQLException
  {
    logger.debug(
               "public boolean isWrapperFor(Class<?> iface)");

    throw new SQLFeatureNotSupportedException();
  }

  /**
   * A small helper function to execute show command to get metadata,
   * And if object does not exist, return an empty result set instead of
   * throwing a SnowflakeSQLException
   */
  private ResultSet executeAndReturnEmptyResultIfNotFound(Statement statement, String sql,
                                                          DBMetadataResultSetMetadata metadataType)
      throws SQLException
  {
    ResultSet resultSet;
    try
    {
      resultSet = statement.executeQuery(sql);
    }
    catch(SnowflakeSQLException e)
    {
      if (e.getSQLState().equals(SqlState.NO_DATA))
      {
        return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(metadataType, statement);
      }
      else
      {
        throw e;
      }
    }
    return resultSet;
  }
}
