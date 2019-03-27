/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;
import net.snowflake.common.util.Wildcard;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static net.snowflake.client.jdbc.DBMetadataResultSetMetadata.GET_CATALOGS;
import static net.snowflake.client.jdbc.DBMetadataResultSetMetadata.GET_COLUMNS;
import static net.snowflake.client.jdbc.DBMetadataResultSetMetadata.GET_COLUMNS_EXTENDED_SET;
import static net.snowflake.client.jdbc.DBMetadataResultSetMetadata.GET_FOREIGN_KEYS;
import static net.snowflake.client.jdbc.DBMetadataResultSetMetadata.GET_FUNCTIONS;
import static net.snowflake.client.jdbc.DBMetadataResultSetMetadata.GET_FUNCTION_COLUMNS;
import static net.snowflake.client.jdbc.DBMetadataResultSetMetadata.GET_PRIMARY_KEYS;
import static net.snowflake.client.jdbc.DBMetadataResultSetMetadata.GET_SCHEMAS;
import static net.snowflake.client.jdbc.DBMetadataResultSetMetadata.GET_TABLES;

public class SnowflakeDatabaseMetaData implements DatabaseMetaData
{

  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeDatabaseMetaData.class);

  private static final ObjectMapper mapper =
      ObjectMapperFactory.getObjectMapper();

  static final private String DatabaseProductName = "Snowflake";

  static final private String DriverName = "Snowflake";

  static final private char SEARCH_STRING_ESCAPE = '\\';

  static final private String JDBCVersion = "1.0";
  // Open Group CLI Functions
  // LOG10 is not supported
  static final private String NumericFunctionsSupported =
      "ABS,ACOS,ASIN,"
      + "CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,MOD,PI,POWER,RADIANS,RAND,"
      + "ROUND,SIGN,SQRT,TAN,TRUNCATE";
  // DIFFERENCE and SOUNDEX are not supported
  static final private String StringFunctionsSupported =
      "ASCII,CHAR,"
      + "CONCAT,INSERT,LCASE,LEFT,LENGTH,LOCATE,LTRIM,REPEAT,REPLACE,"
      + "RIGHT,RTRIM,SPACE,SUBSTRING,UCASE";
  static final private String DateAndTimeFunctionsSupported =
      "CURDATE," +
      "CURTIME,DAYNAME,DAYOFMONTH,DAYOFWEEK,DAYOFYEAR,HOUR,MINUTE,MONTH," +
      "MONTHNAME,NOW,QUARTER,SECOND,TIMESTAMPADD,TIMESTAMPDIFF,WEEK,YEAR";
  static final private String SystemFunctionsSupported =
      "DATABASE,IFNULL,USER";

  // These are keywords not in SQL2003 standard
  static final private String notSQL2003Keywords =
      "ACCOUNT,DATABASE,SCHEMA,VIEW,ISSUE,DATE_PART,EXTRACT," +
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

  private final Connection connection;

  private final SFSession session;

  private final boolean metadataRequestUseConnectionCtx;

  SnowflakeDatabaseMetaData(Connection connection) throws SQLException
  {
    logger.debug("public SnowflakeDatabaseMetaData(SnowflakeConnection connection)");

    this.connection = connection;
    this.session = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
    this.metadataRequestUseConnectionCtx = session.getMetadataRequestUseConnectionCtx();
  }

  private void raiseSQLExceptionIfConnectionIsClosed() throws SQLException
  {
    if (connection.isClosed())
    {
      throw new SnowflakeSQLException(ErrorCode.CONNECTION_CLOSED);
    }
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException
  {
    logger.debug("public boolean allProceduresAreCallable()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException
  {
    logger.debug("public boolean allTablesAreSelectable()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public String getURL() throws SQLException
  {
    logger.debug("public String getURL()");
    raiseSQLExceptionIfConnectionIsClosed();
    String url = session.getUrl();
    return url.startsWith("http://") ? url.replace("http://", "jdbc:snowflake://")
                                     : url.replace("https://", "jdbc:snowflake://");
  }

  @Override
  public String getUserName() throws SQLException
  {
    logger.debug("public String getUserName()");
    raiseSQLExceptionIfConnectionIsClosed();
    return session.getUser();
  }

  @Override
  public boolean isReadOnly() throws SQLException
  {
    logger.debug("public boolean isReadOnly()");
    raiseSQLExceptionIfConnectionIsClosed();
    // no read only mode is supported.
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException
  {
    logger.debug("public boolean nullsAreSortedHigh()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException
  {
    logger.debug("public boolean nullsAreSortedLow()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException
  {
    logger.debug("public boolean nullsAreSortedAtStart()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException
  {
    logger.debug("public boolean nullsAreSortedAtEnd()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public String getDatabaseProductName() throws SQLException
  {
    logger.debug("public String getDatabaseProductName()");
    raiseSQLExceptionIfConnectionIsClosed();
    return DatabaseProductName;
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException
  {
    logger.debug("public String getDatabaseProductVersion()");
    raiseSQLExceptionIfConnectionIsClosed();
    return connection.unwrap(SnowflakeConnectionV1.class).getDatabaseVersion();
  }

  @Override
  public String getDriverName() throws SQLException
  {
    logger.debug("public String getDriverName()");
    raiseSQLExceptionIfConnectionIsClosed();
    return DriverName;
  }

  @Override
  public String getDriverVersion() throws SQLException
  {
    logger.debug("public String getDriverVersion()");
    raiseSQLExceptionIfConnectionIsClosed();
    return SnowflakeDriver.majorVersion + "." +
           SnowflakeDriver.minorVersion + "." +
           SnowflakeDriver.patchVersion;
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
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException
  {
    logger.debug("public boolean usesLocalFilePerTable()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException
  {
    logger.debug(
        "public boolean supportsMixedCaseIdentifiers()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException
  {
    logger.debug(
        "public boolean storesUpperCaseIdentifiers()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException
  {
    logger.debug(
        "public boolean storesLowerCaseIdentifiers()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException
  {
    logger.debug(
        "public boolean storesMixedCaseIdentifiers()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
  {
    logger.debug(
        "public boolean supportsMixedCaseQuotedIdentifiers()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
  {
    logger.debug(
        "public boolean storesUpperCaseQuotedIdentifiers()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
  {
    logger.debug(
        "public boolean storesLowerCaseQuotedIdentifiers()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
  {
    logger.debug(
        "public boolean storesMixedCaseQuotedIdentifiers()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException
  {
    logger.debug(
        "public String getIdentifierQuoteString()");
    raiseSQLExceptionIfConnectionIsClosed();
    return "\"";
  }

  @Override
  public String getSQLKeywords() throws SQLException
  {
    logger.debug("public String getSQLKeywords()");
    raiseSQLExceptionIfConnectionIsClosed();
    return notSQL2003Keywords;
  }

  @Override
  public String getNumericFunctions() throws SQLException
  {
    logger.debug("public String getNumericFunctions()");
    raiseSQLExceptionIfConnectionIsClosed();
    return NumericFunctionsSupported;
  }

  @Override
  public String getStringFunctions() throws SQLException
  {
    logger.debug("public String getStringFunctions()");
    raiseSQLExceptionIfConnectionIsClosed();
    return StringFunctionsSupported;
  }

  @Override
  public String getSystemFunctions() throws SQLException
  {
    logger.debug("public String getSystemFunctions()");
    raiseSQLExceptionIfConnectionIsClosed();
    return SystemFunctionsSupported;
  }

  @Override
  public String getTimeDateFunctions() throws SQLException
  {
    logger.debug("public String getTimeDateFunctions()");
    raiseSQLExceptionIfConnectionIsClosed();
    return DateAndTimeFunctionsSupported;
  }

  @Override
  public String getSearchStringEscape() throws SQLException
  {
    logger.debug("public String getSearchStringEscape()");
    raiseSQLExceptionIfConnectionIsClosed();
    return Character.toString(SEARCH_STRING_ESCAPE);
  }

  @Override
  public String getExtraNameCharacters() throws SQLException
  {
    logger.debug("public String getExtraNameCharacters()");
    raiseSQLExceptionIfConnectionIsClosed();
    return "$";
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException
  {
    logger.debug(
        "public boolean supportsAlterTableWithAddColumn()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException
  {
    logger.debug(
        "public boolean supportsAlterTableWithDropColumn()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException
  {
    logger.debug("public boolean supportsColumnAliasing()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException
  {
    logger.debug("public boolean nullPlusNonNullIsNull()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsConvert() throws SQLException
  {
    logger.debug("public boolean supportsConvert()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException
  {
    logger.debug(
        "public boolean supportsConvert(int fromType, int toType)");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException
  {
    logger.debug(
        "public boolean supportsTableCorrelationNames()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException
  {
    logger.debug(
        "public boolean supportsDifferentTableCorrelationNames()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException
  {
    logger.debug(
        "public boolean supportsExpressionsInOrderBy()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException
  {
    logger.debug(
        "public boolean supportsOrderByUnrelated()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException
  {
    logger.debug("public boolean supportsGroupBy()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException
  {
    logger.debug(
        "public boolean supportsGroupByUnrelated()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException
  {
    logger.debug(
        "public boolean supportsGroupByBeyondSelect()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException
  {
    logger.debug(
        "public boolean supportsLikeEscapeClause()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException
  {
    logger.debug(
        "public boolean supportsMultipleResultSets()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException
  {
    logger.debug(
        "public boolean supportsMultipleTransactions()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException
  {
    logger.debug(
        "public boolean supportsNonNullableColumns()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException
  {
    logger.debug(
        "public boolean supportsMinimumSQLGrammar()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException
  {
    logger.debug("public boolean supportsCoreSQLGrammar()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException
  {
    logger.debug(
        "public boolean supportsExtendedSQLGrammar()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException
  {
    logger.debug(
        "public boolean supportsANSI92EntryLevelSQL()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException
  {
    logger.debug(
        "public boolean supportsANSI92IntermediateSQL()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException
  {
    logger.debug("public boolean supportsANSI92FullSQL()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException
  {
    logger.debug(
        "public boolean supportsIntegrityEnhancementFacility()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException
  {
    logger.debug("public boolean supportsOuterJoins()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException
  {
    logger.debug("public boolean supportsFullOuterJoins()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException
  {
    logger.debug(
        "public boolean supportsLimitedOuterJoins()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public String getSchemaTerm() throws SQLException
  {
    logger.debug("public String getSchemaTerm()");
    raiseSQLExceptionIfConnectionIsClosed();
    return "schema";
  }

  @Override
  public String getProcedureTerm() throws SQLException
  {
    logger.debug("public String getProcedureTerm()");
    raiseSQLExceptionIfConnectionIsClosed();
    return "procedure";
  }

  @Override
  public String getCatalogTerm() throws SQLException
  {
    logger.debug("public String getCatalogTerm()");
    raiseSQLExceptionIfConnectionIsClosed();
    return "database";
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException
  {
    logger.debug("public boolean isCatalogAtStart()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public String getCatalogSeparator() throws SQLException
  {
    logger.debug("public String getCatalogSeparator()");
    raiseSQLExceptionIfConnectionIsClosed();
    return ".";
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException
  {
    logger.debug(
        "public boolean supportsSchemasInDataManipulation()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException
  {
    logger.debug(
        "public boolean supportsSchemasInProcedureCalls()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException
  {
    logger.debug(
        "public boolean supportsSchemasInTableDefinitions()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException
  {
    logger.debug(
        "public boolean supportsSchemasInIndexDefinitions()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
  {
    logger.debug(
        "public boolean supportsSchemasInPrivilegeDefinitions()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException
  {
    logger.debug(
        "public boolean supportsCatalogsInDataManipulation()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException
  {
    logger.debug(
        "public boolean supportsCatalogsInProcedureCalls()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException
  {
    logger.debug(
        "public boolean supportsCatalogsInTableDefinitions()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException
  {
    logger.debug(
        "public boolean supportsCatalogsInIndexDefinitions()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
  {
    logger.debug(
        "public boolean supportsCatalogsInPrivilegeDefinitions()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException
  {
    logger.debug(
        "public boolean supportsPositionedDelete()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException
  {
    logger.debug(
        "public boolean supportsPositionedUpdate()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException
  {
    logger.debug(
        "public boolean supportsSelectForUpdate()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException
  {
    logger.debug(
        "public boolean supportsStoredProcedures()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException
  {
    logger.debug(
        "public boolean supportsSubqueriesInComparisons()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException
  {
    logger.debug(
        "public boolean supportsSubqueriesInExists()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException
  {
    logger.debug(
        "public boolean supportsSubqueriesInIns()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException
  {
    logger.debug(
        "public boolean supportsSubqueriesInQuantifieds()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException
  {
    logger.debug(
        "public boolean supportsCorrelatedSubqueries()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsUnion() throws SQLException
  {
    logger.debug("public boolean supportsUnion()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException
  {
    logger.debug("public boolean supportsUnionAll()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException
  {
    logger.debug(
        "public boolean supportsOpenCursorsAcrossCommit()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException
  {
    logger.debug(
        "public boolean supportsOpenCursorsAcrossRollback()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException
  {
    logger.debug(
        "public boolean supportsOpenStatementsAcrossCommit()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException
  {
    logger.debug(
        "public boolean supportsOpenStatementsAcrossRollback()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException
  {
    logger.debug("public int getMaxBinaryLiteralLength()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 8388608;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException
  {
    logger.debug("public int getMaxCharLiteralLength()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 16777216;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException
  {
    logger.debug("public int getMaxColumnNameLength()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 255;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException
  {
    logger.debug("public int getMaxColumnsInGroupBy()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException
  {
    logger.debug("public int getMaxColumnsInIndex()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException
  {
    logger.debug("public int getMaxColumnsInOrderBy()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException
  {
    logger.debug("public int getMaxColumnsInSelect()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException
  {
    logger.debug("public int getMaxColumnsInTable()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException
  {
    logger.debug("public int getMaxConnections()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException
  {
    logger.debug("public int getMaxCursorNameLength()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException
  {
    logger.debug("public int getMaxIndexLength()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException
  {
    logger.debug("public int getMaxSchemaNameLength()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 255;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException
  {
    logger.debug("public int getMaxProcedureNameLength()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException
  {
    logger.debug("public int getMaxCatalogNameLength()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 255;
  }

  @Override
  public int getMaxRowSize() throws SQLException
  {
    logger.debug("public int getMaxRowSize()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
  {
    logger.debug(
        "public boolean doesMaxRowSizeIncludeBlobs()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public int getMaxStatementLength() throws SQLException
  {
    logger.debug("public int getMaxStatementLength()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException
  {
    logger.debug("public int getMaxStatements()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException
  {
    logger.debug("public int getMaxTableNameLength()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 255;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException
  {
    logger.debug("public int getMaxTablesInSelect()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException
  {
    logger.debug("public int getMaxUserNameLength()");
    raiseSQLExceptionIfConnectionIsClosed();
    return 255;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException
  {
    logger.debug(
        "public int getDefaultTransactionIsolation()");
    raiseSQLExceptionIfConnectionIsClosed();
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  @Override
  public boolean supportsTransactions() throws SQLException
  {
    logger.debug("public boolean supportsTransactions()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level)
  throws SQLException
  {
    logger.debug(
        "public boolean supportsTransactionIsolationLevel(int level)");
    raiseSQLExceptionIfConnectionIsClosed();
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
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException
  {
    logger.debug(
        "public boolean supportsDataManipulationTransactionsOnly()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException
  {
    logger.debug(
        "public boolean dataDefinitionCausesTransactionCommit()");
    raiseSQLExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException
  {
    logger.debug(
        "public boolean dataDefinitionIgnoredInTransactions()");
    raiseSQLExceptionIfConnectionIsClosed();
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
    raiseSQLExceptionIfConnectionIsClosed();
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
    raiseSQLExceptionIfConnectionIsClosed();
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
          catalog, schemaPattern, tableNamePattern,
          Arrays.toString(types));
    }
    raiseSQLExceptionIfConnectionIsClosed();

    Set<String> supportedTableTypes = new HashSet<>();
    ResultSet resultSet = getTableTypes();
    while (resultSet.next())
    {
      supportedTableTypes.add(resultSet.getString("TABLE_TYPE"));
    }
    resultSet.close();

    List<String> inputValidTableTypes = new ArrayList<>();
    // then filter on the input table types;
    if (types != null)
    {
      for (String t : types)
      {
        if (supportedTableTypes.contains(t))
        {
          inputValidTableTypes.add(t);
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

    // apply session context when catalog is unspecified
    if (catalog == null && metadataRequestUseConnectionCtx)
    {
      catalog = session.getDatabase();

      if (schemaPattern == null)
      {
        schemaPattern = session.getSchema();
      }
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

    return new SnowflakeDatabaseMetaDataQueryResultSet(GET_TABLES, resultSet, statement)
    {
      @Override
      public boolean next() throws SQLException
      {
        logger.debug("public boolean next()");
        increamentRow();

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

        close(); // close
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
    raiseSQLExceptionIfConnectionIsClosed();

    String showDB = "show /* JDBC:DatabaseMetaData.getCatalogs() */ databases in account";

    Statement statement = connection.createStatement();
    return new SnowflakeDatabaseMetaDataQueryResultSet(
        GET_CATALOGS,
        statement.executeQuery(showDB), statement)
    {
      @Override
      public boolean next() throws SQLException
      {
        logger.debug("public boolean next()");
        increamentRow();

        // iterate throw the show databases result
        if (showObjectResultSet.next())
        {
          String dbName = showObjectResultSet.getString(2);

          nextRow[0] = dbName;
          return true;
        }
        close();
        return false;
      }
    };
  }

  @Override
  public ResultSet getTableTypes() throws SQLException
  {
    logger.debug("public ResultSet getTableTypes()");
    raiseSQLExceptionIfConnectionIsClosed();

    Statement statement = connection.createStatement();

    // TODO: We should really get the list of table types from GS
    return new SnowflakeDatabaseMetaDataResultSet(
        Collections.singletonList("TABLE_TYPE"),
        Collections.singletonList("TEXT"),
        Collections.singletonList(Types.VARCHAR),
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
    logger.debug("public ResultSet getColumns(String catalog={}, String schemaPattern={}" +
                 "String tableNamePattern={}, String columnNamePattern={}, boolean extendedSet={}",
                 catalog, schemaPattern, tableNamePattern, columnNamePattern, extendedSet);
    raiseSQLExceptionIfConnectionIsClosed();
    Statement statement = connection.createStatement();

    // apply session context when catalog is unspecified
    if (catalog == null && metadataRequestUseConnectionCtx)
    {
      catalog = session.getDatabase();

      if (schemaPattern == null)
      {
        schemaPattern = session.getSchema();
      }
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

    return new SnowflakeDatabaseMetaDataQueryResultSet(
        extendedSet ? GET_COLUMNS_EXTENDED_SET : GET_COLUMNS,
        resultSet, statement)
    {
      int ordinalPosition = 0;

      String currentTableName = null;

      public boolean next() throws SQLException
      {
        logger.debug("public boolean next()");
        increamentRow();

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
            {
              externalColumnType = Types.TIMESTAMP;
            }

            nextRow[4] = externalColumnType;
            nextRow[5] = columnMetadata.getTypeName();

            int columnSize = 0;

            // The COLUMN_SIZE column specifies the column size for the given
            // column. For numeric data, this is the maximum precision. For
            // character data, this is the length in characters. For datetime
            // datatypes, this is the length in characters of the String
            // representation (assuming the maximum allowed precision of the
            // fractional seconds component). For binary data, this is the
            // length in bytes. For the ROWID datatype, this is the length in
            // bytes. Null is returned for data types where the column size
            // is not applicable.
            if (columnMetadata.getType() == Types.VARCHAR
                || columnMetadata.getType() == Types.CHAR
                || columnMetadata.getType() == Types.BINARY)
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

            nextRow[6] = columnSize;
            nextRow[7] = null;
            nextRow[8] = columnMetadata.getScale();
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
                          ? columnMetadata.getLength() : null;
            nextRow[16] = ordinalPosition;

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
        close();
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
    raiseSQLExceptionIfConnectionIsClosed();

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
    raiseSQLExceptionIfConnectionIsClosed();

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
    logger.debug(
        "public ResultSet getPrimaryKeys(String catalog={}, "
        + "String schema={}, String table={})", catalog, schema, table);
    raiseSQLExceptionIfConnectionIsClosed();
    Statement statement = connection.createStatement();
    String showPKCommand = "show /* JDBC:DatabaseMetaData.getPrimaryKeys() */ primary keys in ";

    // apply session context when catalog is unspecified
    if (catalog == null && metadataRequestUseConnectionCtx)
    {
      catalog = session.getDatabase();

      if (schema == null)
      {
        schema = session.getSchema();
      }
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
        {
          return SnowflakeDatabaseMetaDataResultSet.getEmptyResultSet(GET_PRIMARY_KEYS, statement);
        }
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
    return new SnowflakeDatabaseMetaDataQueryResultSet(GET_PRIMARY_KEYS, resultSet, statement)
    {
      @Override
      public boolean next() throws SQLException
      {
        logger.debug("public boolean next()");
        increamentRow();

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
        close();
        return false;
      }
    };
  }

  /**
   * Retrieves the foreign keys
   *
   * @param client         type of foreign key
   * @param parentCatalog  database name
   * @param parentSchema   schema name
   * @param parentTable    table name
   * @param foreignCatalog other database name
   * @param foreignSchema  other schema name
   * @param foreignTable   other table name
   * @return foreign key columns in result set
   */
  private ResultSet getForeignKeys(
      final String client, String parentCatalog,
      String parentSchema,
      final String parentTable, final String foreignCatalog,
      final String foreignSchema,
      final String foreignTable) throws SQLException
  {
    raiseSQLExceptionIfConnectionIsClosed();
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

    return new SnowflakeDatabaseMetaDataQueryResultSet(GET_FOREIGN_KEYS, resultSet, statement)
    {
      @Override
      public boolean next() throws SQLException
      {
        logger.debug("public boolean next()");
        increamentRow();

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
            {
              passedFilter = true;
            }
          }
          else if (client.equals("export"))
          {
            // For exported keys, filter on the primary key table
            if ((finalParentCatalog == null || finalParentCatalog.equals(pktable_cat)) &&
                (finalParentSchema == null || finalParentSchema.equals(pktable_schem)) &&
                (parentTable == null || parentTable.equals(pktable_name)))
            {
              passedFilter = true;
            }
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
            {
              passedFilter = true;
            }
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
        close();
        return false;
      }
    };
  }

  /**
   * Returns the JDBC standard property string for the property string used
   * in our show constraint commands
   *
   * @param property_name operation type
   * @param property      property value
   * @return metdata property value
   */
  private short getForeignKeyConstraintProperty(
      String property_name, String property)
  {
    short result = 0;
    switch (property_name)
    {
      case "update":
      case "delete":
        switch (property)
        {
          case "NO ACTION":
            result = importedKeyNoAction;
            break;
          case "CASCADE":
            result = importedKeyCascade;
            break;
          case "SET NULL":
            result = importedKeySetNull;
            break;
          case "SET DEFAULT":
            result = importedKeySetDefault;
            break;
          case "RESTRICT":
            result = importedKeyRestrict;
            break;
        }
      case "deferrability":
        switch (property)
        {
          case "INITIALLY DEFERRED":
            result = importedKeyInitiallyDeferred;
            break;
          case "INITIALLY IMMEDIATE":
            result = importedKeyInitiallyImmediate;
            break;
          case "NOT DEFERRABLE":
            result = importedKeyNotDeferrable;
            break;
        }
    }
    return result;
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
  throws SQLException
  {
    logger.debug(
        "public ResultSet getImportedKeys(String catalog={}, "
        + "String schema={}, String table={})", catalog, schema, table);

    return getForeignKeys("import", catalog, schema, table, null, null, null);
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
  throws SQLException
  {
    logger.debug(
        "public ResultSet getExportedKeys(String catalog={}, "
        + "String schema={}, String table={})", catalog, schema, table);

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
        parentCatalog, parentSchema, parentTable,
        foreignCatalog, foreignSchema, foreignTable);

    return getForeignKeys("cross", parentCatalog, parentSchema, parentTable,
                          foreignCatalog, foreignSchema, foreignTable);
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException
  {
    logger.debug("public ResultSet getTypeInfo()");
    raiseSQLExceptionIfConnectionIsClosed();

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
    raiseSQLExceptionIfConnectionIsClosed();

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
    raiseSQLExceptionIfConnectionIsClosed();
    return (type == ResultSet.TYPE_FORWARD_ONLY);
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency)
  throws SQLException
  {
    logger.debug(
        "public boolean supportsResultSetConcurrency(int type, "
        + "int concurrency)");
    raiseSQLExceptionIfConnectionIsClosed();
    return (type == ResultSet.TYPE_FORWARD_ONLY
            && concurrency == ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException
  {
    logger.debug(
        "public boolean ownUpdatesAreVisible(int type)");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException
  {
    logger.debug(
        "public boolean ownDeletesAreVisible(int type)");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException
  {
    logger.debug(
        "public boolean ownInsertsAreVisible(int type)");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException
  {
    logger.debug(
        "public boolean othersUpdatesAreVisible(int type)");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException
  {
    logger.debug(
        "public boolean othersDeletesAreVisible(int type)");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException
  {
    logger.debug(
        "public boolean othersInsertsAreVisible(int type)");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException
  {
    logger.debug(
        "public boolean updatesAreDetected(int type)");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException
  {
    logger.debug(
        "public boolean deletesAreDetected(int type)");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException
  {
    logger.debug(
        "public boolean insertsAreDetected(int type)");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException
  {
    logger.debug("public boolean supportsBatchUpdates()");
    raiseSQLExceptionIfConnectionIsClosed();
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
    raiseSQLExceptionIfConnectionIsClosed();
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
    raiseSQLExceptionIfConnectionIsClosed();
    return connection;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException
  {
    logger.debug("public boolean supportsSavepoints()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException
  {
    logger.debug(
        "public boolean supportsNamedParameters()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException
  {
    logger.debug(
        "public boolean supportsMultipleOpenResults()");
    raiseSQLExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException
  {
    logger.debug(
        "public boolean supportsGetGeneratedKeys()");
    raiseSQLExceptionIfConnectionIsClosed();
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
    raiseSQLExceptionIfConnectionIsClosed();
    return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getResultSetHoldability() throws SQLException
  {
    logger.debug("public int getResultSetHoldability()");
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException
  {
    logger.debug("public int getDatabaseMajorVersion()");
    raiseSQLExceptionIfConnectionIsClosed();
    return connection.unwrap(SnowflakeConnectionV1.class).getDatabaseMajorVersion();
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException
  {
    logger.debug("public int getDatabaseMinorVersion()");
    raiseSQLExceptionIfConnectionIsClosed();
    return connection.unwrap(SnowflakeConnectionV1.class).getDatabaseMinorVersion();
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException
  {
    logger.debug("public int getJDBCMajorVersion()");
    raiseSQLExceptionIfConnectionIsClosed();
    return Integer.parseInt(JDBCVersion.split("\\.", 2)[0]);
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException
  {
    logger.debug("public int getJDBCMinorVersion()");
    raiseSQLExceptionIfConnectionIsClosed();
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
    raiseSQLExceptionIfConnectionIsClosed();
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
        + "schemaPattern={})", catalog, schemaPattern);
    raiseSQLExceptionIfConnectionIsClosed();

    // apply session context when catalog is unspecified
    if (catalog == null && metadataRequestUseConnectionCtx)
    {
      catalog = session.getDatabase();

      if (schemaPattern == null)
      {
        schemaPattern = session.getSchema();
      }
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
    return new SnowflakeDatabaseMetaDataQueryResultSet(GET_SCHEMAS, resultSet, statement)
    {
      public boolean next() throws SQLException
      {
        logger.debug("public boolean next()");
        increamentRow();

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
        close();
        return false;
      }
    };
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException
  {
    logger.debug(
        "public boolean supportsStoredFunctionsUsingCallSyntax()");
    raiseSQLExceptionIfConnectionIsClosed();
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
    raiseSQLExceptionIfConnectionIsClosed();
    Statement statement = connection.createStatement();
    logger.debug("public ResultSet getFunctions(String catalog={}, String schemaPattern={}, " +
                 "String functionNamePattern={}",
                 catalog, schemaPattern, functionNamePattern);

    // apply session context when catalog is unspecified
    if (catalog == null && metadataRequestUseConnectionCtx)
    {
      catalog = session.getDatabase();

      if (schemaPattern == null)
      {
        schemaPattern = session.getSchema();
      }
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
                               schemaPattern + "\"";
      }
    }

    logger.debug("sql command to get column metadata: {}",
                 showFunctionCommand);

    ResultSet resultSet = executeAndReturnEmptyResultIfNotFound(statement, showFunctionCommand, GET_FUNCTIONS);

    return new SnowflakeDatabaseMetaDataQueryResultSet(GET_FUNCTIONS, resultSet, statement)
    {
      public boolean next() throws SQLException
      {
        logger.debug("public boolean next()");
        increamentRow();

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
        close();
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
    raiseSQLExceptionIfConnectionIsClosed();

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
    catch (SnowflakeSQLException e)
    {
      if (e.getSQLState().equals(SqlState.NO_DATA) ||
          e.getSQLState().equals(SqlState.BASE_TABLE_OR_VIEW_NOT_FOUND))
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
