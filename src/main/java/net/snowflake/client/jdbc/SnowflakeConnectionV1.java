/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.log.JDK14Logger;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.ClientAuthnDTO;
import net.snowflake.common.core.LoginInfoDTO;
import net.snowflake.common.core.ResourceBundleManager;
import net.snowflake.common.core.SqlState;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.security.PrivateKey;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Snowflake connection implementation
 *
 * @author jhuang
 */
public class SnowflakeConnectionV1 implements Connection
{

  static private final
  SFLogger logger = SFLoggerFactory.getLogger(SnowflakeConnectionV1.class);

  private static final String JDBC_PROTOCOL_PREFIX = "jdbc:snowflake";

  private static final String NATIVE_PROTOCOL = "http";

  private static final String SSL_NATIVE_PROTOCOL = "https";

  private boolean isClosed = true;

  private String userName;

  private String password;

  private String accountName;

  /**
   * this is just login database/schema/role specified in connection url/properties, i.e.
   * specified by the user, not necessarily actual db/schema/role
   * The actual session database/schema/role is stored in SFSession
   */
  private String loginDatabaseName;

  private String loginSchemaName;

  private String loginRole;

  private String warehouse;

  private Level tracingLevel = Level.INFO;

  private String serverUrl;

  private boolean sslOn = true;

  private boolean passcodeInPassword = false;

  private String passcode;

  private String authenticator;

  private SQLWarning sqlWarnings = null;

  private String newClientForUpdate;

  private Map<String, Object> sessionParameters = new HashMap<>();

  // Injected delay for the purpose of connection timeout testing
  // Any statement execution will sleep for the specified number of milliseconds
  private AtomicInteger _injectedDelay = new AtomicInteger(0);

  private String databaseVersion = null;
  private int databaseMajorVersion = 0;
  private int databaseMinorVersion = 0;

  /**
   * Amount of seconds a user is willing to tolerate for establishing the
   * connection with database. In our case, it means the first login
   * request to get authorization token.
   * <p>
   * A value of 0 means no timeout.
   * <p>
   * Default: to login timeout in driver manager if set or 60 seconds
   */
  private int loginTimeout = (DriverManager.getLoginTimeout() > 0) ?
      DriverManager.getLoginTimeout() : 60;

  /**
   * Amount of milliseconds a user is willing to tolerate for network related
   * issues (e.g. HTTP 503/504) or database transient issues (e.g. GS
   * not responding)
   * <p>
   * A value of 0 means no timeout
   * <p>
   * Default: 300 seconds
   */
  private int networkTimeoutInMilli = 0; // in milliseconds

  /**
   * Amount of seconds a user is willing to tolerate for an individual query.
   * Both network/GS issues and query processing itself can contribute
   * to the amount time spent for a query.
   * <p>
   * A value of 0 means no timeout
   * <p>
   * Default: 0
   */
  private int queryTimeout = 0; // in seconds

  private boolean useProxy = false;

  /**
   * Insecure mode will skip OCSP revocation check.
   *
   * NOTE true by default at the moment but will change to false later
   */
  private boolean insecureMode = true;

  private AtomicInteger sequenceId = new AtomicInteger(0);

  private Map sessionProperties = new HashMap<String, Object>(1);

  private static String IMPLEMENTATION_VERSION_TESTING =
      Integer.MAX_VALUE + ".0.0";

  static private final ResourceBundleManager errorResourceBundleManager =
      ResourceBundleManager.getSingleton(ErrorCode.errorMessageResource);

  boolean internalTesting = false;

  private Properties clientInfo = new Properties();

  // TODO this should be set to Connection.TRANSACTION_READ_COMMITTED
  // TODO There may not be many implications here since the call to
  // TODO setTransactionIsolation doesn't do anything.
  private int transactionIsolation = Connection.TRANSACTION_NONE;

  //--- Simulated failures for testing

  // whether we try to simulate a socket timeout (a default value of 0 means
  // no simulation). The value is in milliseconds
  private int injectSocketTimeout = 0;

  // simulate client pause after initial execute and before first get-result
  // call ( a default value of 0 means no pause). The value is in seconds
  private int injectClientPause = 0;

  //Generate exception while uploading file with a given name
  private String injectFileUploadFailure = null;

  private boolean retryQuery = false;

  private SFSession sfSession;

  private PrivateKey privateKey;

  /**
   * A connection will establish a session token from snowflake
   *
   * @param url  server url used to create snowflake connection
   * @param info properties about the snowflake connection
   * @throws java.sql.SQLException if failed to create a snowflake connection
   *                               i.e. username or password not specified
   */
  public SnowflakeConnectionV1(String url,
                               Properties info)
      throws SQLException
  {
    processParameters(url, info);

    // replace protocol name
    serverUrl = serverUrl.replace(JDBC_PROTOCOL_PREFIX,
        sslOn ? SSL_NATIVE_PROTOCOL : NATIVE_PROTOCOL);

    logger.debug("Connecting to: {} with userName={} " +
            "accountName={} " +
            "databaseName={} " +
            "schemaName={} " +
            "warehouse={} " +
            "ssl={}",
        serverUrl, userName, accountName, loginDatabaseName, loginSchemaName,
        warehouse, sslOn);

    // open connection to GS
    sfSession = new SFSession();

    try
    {
      // pass the parameters to sfSession
      initSessionProperties();

      sfSession.open();

      // SNOW-18963: return database version
      databaseVersion = sfSession.getDatabaseVersion();
      databaseMajorVersion = sfSession.getDatabaseMajorVersion();
      databaseMinorVersion = sfSession.getDatabaseMinorVersion();
      newClientForUpdate = sfSession.getNewClientForUpdate();
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(), ex.getSqlState(),
          ex.getVendorCode(), ex.getParams());
    }

    if (loginDatabaseName != null && !loginDatabaseName
        .equalsIgnoreCase(sfSession.getDatabase()))
    {
      SQLWarning w = new SnowflakeSQLWarning(
          ErrorCode.CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP.getSqlState(),
          ErrorCode.CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP.getMessageCode(),
          "Database", loginDatabaseName, sfSession.getDatabase());
      appendWarning(w);
    }

    if (loginSchemaName != null && !loginSchemaName
        .equalsIgnoreCase(sfSession.getSchema()))
    {
      SQLWarning w = new SnowflakeSQLWarning(
          ErrorCode.CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP.getSqlState(),
          ErrorCode.CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP.getMessageCode(),
          "Schema", loginSchemaName, sfSession.getSchema());
      appendWarning(w);
    }

    if (loginRole != null && !loginRole
        .equalsIgnoreCase(sfSession.getRole()))
    {
      SQLWarning w = new SnowflakeSQLWarning(
          ErrorCode.CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP.getSqlState(),
          ErrorCode.CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP.getMessageCode(),
          "Role", loginRole, sfSession.getRole());
      appendWarning(w);
    }

    isClosed = false;
  }

  private void initSessionProperties() throws SFException
  {
    sfSession.addProperty(
        SFSessionProperty.SERVER_URL.getPropertyKey(), serverUrl);
    sfSession.addProperty(SFSessionProperty.USER.getPropertyKey(), userName);
    sfSession.addProperty(SFSessionProperty.PASSWORD.getPropertyKey(), password);
    sfSession.addProperty(
        SFSessionProperty.ACCOUNT.getPropertyKey(), accountName);

    if (loginDatabaseName != null)
      sfSession.addProperty(
          SFSessionProperty.DATABASE.getPropertyKey(), loginDatabaseName);
    if (loginSchemaName != null)
      sfSession.addProperty(
          SFSessionProperty.SCHEMA.getPropertyKey(), loginSchemaName);
    if (warehouse != null)
      sfSession.addProperty(
          SFSessionProperty.WAREHOUSE.getPropertyKey(), warehouse);
    if (loginRole != null)
      sfSession.addProperty(SFSessionProperty.ROLE.getPropertyKey(), loginRole);
    if (authenticator != null)
      sfSession.addProperty(SFSessionProperty.AUTHENTICATOR.getPropertyKey(),
          authenticator);
    sfSession.addProperty(SFSessionProperty.APP_ID.getPropertyKey(),
        LoginInfoDTO.SF_JDBC_APP_ID);

    if (internalTesting)
    {
      sfSession.addProperty(SFSessionProperty.APP_VERSION.getPropertyKey(),
          SnowflakeConnectionV1.IMPLEMENTATION_VERSION_TESTING);
    }
    else
    {
      sfSession.addProperty(SFSessionProperty.APP_VERSION.getPropertyKey(),
          SnowflakeDriver.implementVersion);
    }

    sfSession.addProperty(SFSessionProperty.LOGIN_TIMEOUT.getPropertyKey(),
        loginTimeout);

    sfSession.addProperty(SFSessionProperty.NETWORK_TIMEOUT.getPropertyKey(),
        networkTimeoutInMilli);

    sfSession.addProperty(SFSessionProperty.USE_PROXY.getPropertyKey(),
        useProxy);

    sfSession.addProperty(
        SFSessionProperty.INJECT_SOCKET_TIMEOUT.getPropertyKey(),
        injectSocketTimeout);

    sfSession.addProperty(
        SFSessionProperty.INJECT_CLIENT_PAUSE.getPropertyKey(),
        injectClientPause);

    sfSession.addProperty(
        SFSessionProperty.PASSCODE.getPropertyKey(),
        passcode);

    sfSession.addProperty(
        SFSessionProperty.PASSCODE_IN_PASSWORD.getPropertyKey(),
        passcodeInPassword);

    sfSession.addProperty(SFSessionProperty.PRIVATE_KEY.getPropertyKey(),
        privateKey);

    sfSession.addProperty(SFSessionProperty.INSECURE_MODE.getPropertyKey(),
        insecureMode);
    // Now add the session parameters
    for (String param_name : sessionParameters.keySet())
    {
      Object param_value = sessionParameters.get(param_name);
      sfSession.addProperty(param_name, param_value);
    }
  }

  private boolean getBooleanTrueByDefault(Object value)
  {
    if (value instanceof String) {
      final String value0 = (String)value;
      return !("off".equalsIgnoreCase(value0) || "false".equalsIgnoreCase(value0));
    }
    else if (value instanceof Boolean)
    {
      return (boolean)value;
    }
    return true;
  }

  private boolean getBooleanFalseByDefault(Object value)
  {
    if (value instanceof String)
    {
      final String value0 = (String) value;
      return "on".equalsIgnoreCase(value0) || "true".equalsIgnoreCase(value0);
    }
    else if (value instanceof Boolean)
    {
      return (boolean)value;
    }
    return false;
  }

  /**
   * Processes parameters given in the connection string. This extracts
   * accountName, databaseName, schemaName from
   * the URL if it is specified there, where the URL is of the form:
   *
   * jdbc:snowflake://host:port/?user=v&password=v&account=v&
   * db=v&schema=v&ssl=v&[passcode=v|passcodeInPassword=on]
   * @param url
   * @param info
   */
  private void processParameters(String url, Properties info)
      throws SnowflakeSQLException
  {
    serverUrl = url;

    /*
     * Find the query parameter substring if it exists and extract properties
     * out of it
     */
    int queryParamsIndex = url.indexOf("?");

    if (queryParamsIndex > 0)
    {
      String queryParams = url.substring(queryParamsIndex + 1);

      String[] tokens = StringUtils.split(queryParams, "=&");

      // update the server url for REST call to GS
      serverUrl = serverUrl.substring(0, queryParamsIndex);

      logger.debug("server url: {}", serverUrl);

      // assert that tokens length is even so that there is a
      // value for each param
      if (tokens.length % 2 != 0)
      {
        throw new
            IllegalArgumentException("Missing value for some query param");
      }

      for (int paramIdx = 0; paramIdx < tokens.length; paramIdx = paramIdx + 2)
      {
        if ("user".equalsIgnoreCase(tokens[paramIdx]))
        {
          userName = tokens[paramIdx + 1];

          logger.debug("user name: {}", userName);

        }
        else if ("password".equalsIgnoreCase(tokens[paramIdx]))
        {
          password = tokens[paramIdx + 1];
        }
        else if ("account".equalsIgnoreCase(tokens[paramIdx]))
        {
          accountName = tokens[paramIdx + 1];

          logger.debug("account: {}", accountName);

        }
        else if ("db".equalsIgnoreCase(tokens[paramIdx]) ||
            "database".equalsIgnoreCase(tokens[paramIdx]))
        {
          loginDatabaseName = tokens[paramIdx + 1];

          logger.debug("db: {}", loginDatabaseName);
        }
        else if ("schema".equalsIgnoreCase(tokens[paramIdx]))
        {
          loginSchemaName = tokens[paramIdx + 1];
          logger.debug("schema: {}", loginSchemaName);
        }
        else if ("ssl".equalsIgnoreCase(tokens[paramIdx]))
        {
          sslOn = getBooleanTrueByDefault(tokens[paramIdx + 1]);
          logger.debug("ssl: {}", tokens[paramIdx + 1]);
        }
        else if ("passcodeInPassword".equalsIgnoreCase(tokens[paramIdx]))
        {
          passcodeInPassword = getBooleanFalseByDefault(tokens[paramIdx + 1]);

          logger.debug("passcodeInPassword: {}", tokens[paramIdx + 1]);
        }
        else if ("passcode".equalsIgnoreCase(tokens[paramIdx]))
        {
          passcode = tokens[paramIdx + 1];
        }
        else if ("role".equalsIgnoreCase(tokens[paramIdx]))
        {
          loginRole = tokens[paramIdx + 1];
          logger.debug("role: {}", loginRole);
        }
        else if ("authenticator".equalsIgnoreCase(tokens[paramIdx]))
        {
          authenticator = tokens[paramIdx + 1];
        }
        else if ("internal".equalsIgnoreCase(tokens[paramIdx]))
        {
          internalTesting = "true".equalsIgnoreCase(tokens[paramIdx + 1]);
        }
        else if ("warehouse".equalsIgnoreCase(tokens[paramIdx]))
        {
          warehouse = tokens[paramIdx + 1];

          logger.debug("warehouse: {}", warehouse);
        }
        else if ("loginTimeout".equalsIgnoreCase(tokens[paramIdx]))
        {
          loginTimeout = Integer.parseInt(tokens[paramIdx + 1]);

          logger.debug("login timeout: {}", loginTimeout);
        }
        else if ("networkTimeout".equalsIgnoreCase(tokens[paramIdx]))
        {
          networkTimeoutInMilli = Integer.parseInt(tokens[paramIdx + 1]) * 1000;

          logger.debug("network timeout in milli: {}",
              networkTimeoutInMilli);
        }
        else if ("queryTimeout".equalsIgnoreCase(tokens[paramIdx]))
        {
          queryTimeout = Integer.parseInt(tokens[paramIdx + 1]);

          logger.debug("queryTimeout: {}", queryTimeout);
        }
        else if ("useProxy".equalsIgnoreCase(tokens[paramIdx]))
        {
          useProxy = getBooleanFalseByDefault(tokens[paramIdx + 1]);

          logger.debug("useProxy: {}", tokens[paramIdx + 1]);
        }
        else if ("injectSocketTimeout".equalsIgnoreCase(tokens[paramIdx]))
        {
          injectSocketTimeout = Integer.parseInt(tokens[paramIdx + 1]);

          logger.debug("injectSocketTimeout: {}",
              injectSocketTimeout);
        }
        else if ("injectClientPause".equalsIgnoreCase(tokens[paramIdx]))
        {
          injectClientPause = Integer.parseInt(tokens[paramIdx + 1]);

          logger.debug("injectClientPause: {}", injectClientPause);
        }
        else if ("retryQuery".equalsIgnoreCase(tokens[paramIdx]))
        {
          retryQuery = getBooleanFalseByDefault(tokens[paramIdx + 1]);
          logger.debug("retryQuery: {}", tokens[paramIdx + 1]);
        }
        else if ("tracing".equalsIgnoreCase(tokens[paramIdx]))
        {
          String tracingLevelStr = tokens[paramIdx + 1].toUpperCase();

          logger.debug("tracing level specified in connection url: {}",
              tracingLevelStr);

          tracingLevel = Level.parse(tracingLevelStr);
          // tracingLevel is effective only if customer is using the new logging config/framework
          if (tracingLevel != null && System.getProperty("snowflake.jdbc.loggerImpl") == null
              && (logger instanceof JDK14Logger))
          {
            JDK14Logger.setLevel(tracingLevel);
          }
        }
        else if (SFSessionProperty.INSECURE_MODE.getPropertyKey().equalsIgnoreCase(tokens[paramIdx]))
        {
          // TODO: this should be flipped after OCSP check is included by default
          insecureMode = getBooleanTrueByDefault(tokens[paramIdx + 1]);
          logger.debug("{}: {}", SFSessionProperty.INSECURE_MODE.getPropertyKey(), tokens[paramIdx + 1]);
        }
        // If the name of the parameter does not match any of the built in
        // names, assume it is a session level parameter
        else
        {
          String param_name = tokens[paramIdx];
          String param_value = tokens[paramIdx + 1];

          logger.debug("parameter {} set to {}", param_name, param_value);
          sessionParameters.put(param_name, param_value);
        }
      }
    }

    // the properties can be overridden
    for (Object key : info.keySet())
    {
      if (key.equals("user"))
      {
        userName = info.getProperty("user");

        logger.debug("user name property: {}", userName);
      }
      else if (key.equals("password"))
      {
        password = info.getProperty("password");
      }
      else if (key.equals("account"))
      {
        accountName = info.getProperty("account");

        logger.debug("account name property: {}", accountName);
      }
      else if (key.equals("db"))
      {
        loginDatabaseName = info.getProperty("db");

        logger.debug("database name property: {}", loginDatabaseName);
      }
      else if (key.equals("database"))
      {
        loginDatabaseName = info.getProperty("database");

        logger.debug("database name property: {}", loginDatabaseName);
      }
      else if (key.equals("schema"))
      {
        loginSchemaName = info.getProperty("schema");

        logger.debug("schema name property: {}", loginSchemaName);
      }
      else if (key.equals("warehouse"))
      {
        warehouse = info.getProperty("warehouse");

        logger.debug("warehouse property: {}", warehouse);
      }
      else if (key.equals("role"))
      {
        loginRole = info.getProperty("role");

        logger.debug("role property: {}", loginRole);
      }
      else if (key.equals("authenticator"))
      {
        authenticator = info.getProperty("authenticator");

        logger.debug("authenticator property: {}", authenticator);
      }
      else if (key.equals("privateKey"))
      {
        Object val = info.get("privateKey");
        if (val instanceof PrivateKey)
        {
          privateKey = (PrivateKey) val;
        }
        else
        {
          throw new SnowflakeSQLException(ErrorCode.
              INVALID_OR_UNSUPPORTED_PRIVATE_KEY, "Please use java.security." +
              "PrivateKey.class");
        }
      }
      else if (key.equals("ssl"))
      {
        sslOn = getBooleanTrueByDefault(info.getProperty("ssl"));
        logger.debug("ssl property: {}", info.getProperty("ssl"));
      }
      else if (key.equals("passcodeInPassword"))
      {
        passcodeInPassword = getBooleanFalseByDefault(
            info.getProperty("passcodeInPassword"));
      }
      else if (key.equals("passcode"))
      {
        passcode = info.getProperty("passcode");
      }
      else if (key.equals("internal"))
      {
        internalTesting = getBooleanFalseByDefault(info.getProperty("internal"));
      }
      else if (key.equals("loginTimeout"))
      {
        loginTimeout = Integer.parseInt(info.getProperty("loginTimeout"));
      }
      else if (key.equals("netowrkTimeout"))
      {
        networkTimeoutInMilli =
            Integer.parseInt(info.getProperty("networkTimeout")) * 1000;
      }
      else if (key.equals("queryTimeout"))
      {
        queryTimeout = Integer.parseInt(info.getProperty("queryTimeout"));
      }
      else if (key.equals("injectSocketTimeout"))
      {
        injectSocketTimeout =
            Integer.parseInt(info.getProperty("injectSocketTimeout"));
      }
      else if (key.equals("injectClientPause"))
      {
        injectClientPause =
            Integer.parseInt(info.getProperty("injectClientPause"));
      }
      else if (key.equals("retryQuery"))
      {
        retryQuery = getBooleanFalseByDefault(info.getProperty("retryQuery"));
        logger.debug("retryQuery property: {}", retryQuery);
      }
      else if (key.equals("tracing"))
      {
        tracingLevel = Level.parse(info.getProperty("tracing").toUpperCase());
        // tracingLevel is effective only if customer is using the new logging config/framework
        if (tracingLevel != null && System.getProperty("snowflake.jdbc.loggerImpl") == null
            && logger instanceof JDK14Logger)
        {
          JDK14Logger.setLevel(tracingLevel);
        }

        logger.debug("tracingLevel property: {}",
            info.getProperty("tracing"));
      }
      else if (SFSessionProperty.INSECURE_MODE.getPropertyKey().equalsIgnoreCase((String)key))
      {
        insecureMode = getBooleanTrueByDefault(info.get(key));
        logger.debug("insecureMode property: {}", insecureMode);
      }
      // If the key does not match any of the built in values, assume it's a
      // session level parameter
      else
      {
        String param_name = key.toString();
        Object param_value = info.get(key);
        sessionParameters.put(param_name, param_value);
        logger.debug("parameter {} set to {}", param_name, param_value);
      }
    }

    // initialize account name from host if necessary
    if (accountName == null && serverUrl != null &&
        serverUrl.indexOf(".") > 0 &&
        serverUrl.indexOf("://") > 0)
    {
      accountName = serverUrl.substring(serverUrl.indexOf("://") + 3,
          serverUrl.indexOf("."));

      logger.debug("set account name to {}", accountName);
    }
  }

  /**
   * Execute a statement where the result isn't needed, and the statement is
   * closed before this method returns
   *
   * @param stmtText text of the statement
   * @throws java.sql.SQLException exception thrown it the statement fails to execute
   */
  private void executeImmediate(String stmtText) throws SQLException
  {
    // execute the statement and auto-close it as well
    try (final Statement statement = this.createStatement();)
    {
      statement.execute(stmtText);
    }
  }

  public String getNewClientForUpdate()
  {
    return newClientForUpdate;
  }

  public void setNewClientForUpdate(String newClientForUpdate)
  {
    this.newClientForUpdate = newClientForUpdate;
  }


  public int getAndIncrementSequenceId()
  {
    return sequenceId.getAndIncrement();
  }

  /**
   * get server url
   *
   * @return server url
   */
  protected String getServerUrl()
  {
    return serverUrl;
  }

  /**
   * Create a statement
   *
   * @return statement
   * @throws java.sql.SQLException if failed to create a snowflake statement
   */
  @Override
  public Statement createStatement() throws SQLException
  {
    Statement statement = new SnowflakeStatementV1(this);
    statement.setQueryTimeout(queryTimeout);
    return statement;
  }

  /**
   * Close the connection
   *
   * @throws java.sql.SQLException failed to close the connection
   */
  @Override
  public void close() throws SQLException
  {
    logger.debug(" public void close() throws SQLException");

    if (isClosed)
    {
      return;
    }

    try
    {
      if (sfSession != null)
      {
        sfSession.close();
        sfSession = null;
      }
      isClosed = true;
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public boolean isClosed() throws SQLException
  {
    logger.debug(" public boolean isClosed() throws SQLException");

    return isClosed;
  }

  /**
   * Return the database metadata
   *
   * @return Database metadata
   * @throws java.sql.SQLException f
   */
  @Override
  public DatabaseMetaData getMetaData() throws SQLException
  {
    logger.debug(
        " public DatabaseMetaData getMetaData() throws SQLException");

    return new SnowflakeDatabaseMetaData(this);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException
  {
    logger.debug(
        " public CallableStatement prepareCall(String sql) throws "
            + "SQLException");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
                                       int resultSetConcurrency)
      throws SQLException
  {
    logger.debug(
        " public CallableStatement prepareCall(String sql, int "
            + "resultSetType,");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
                                       int resultSetConcurrency,
                                       int resultSetHoldability)
      throws SQLException
  {
    logger.debug(
        " public CallableStatement prepareCall(String sql, int "
            + "resultSetType,");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException
  {
    logger.debug(
        " public String nativeSQL(String sql) throws SQLException");

    return sql;
  }

  @Override
  public void setAutoCommit(boolean isAutoCommit) throws SQLException
  {
    logger.debug(
        " public void setAutoCommit(boolean isAutoCommit) throws "
            + "SQLException");

    try
    {
      this.executeImmediate("alter session set autocommit=" +
          Boolean.toString(isAutoCommit));
    }
    catch (SnowflakeSQLException sse)
    {
      // check whether this is the autocommit api unsupported exception
      if (sse.getSQLState().equals(SqlState.FEATURE_NOT_SUPPORTED))
      {
        // autocommit api support has not yet been enabled in this session/account
        // do nothing for backward compatibility
        logger.debug("Autocommit API is not supported for this " +
            "connection.");
      }
      else
      {
        // this is a different exception, rethrow it
        throw sse;
      }
    }
  }

  /**
   * Look up the GS metadata using sql command,
   * provide a list of column names
   * and return these column values in the row one of result set
   *
   * @param querySQL,
   * @param columnNames
   * @return
   * @throws SQLException
   */
  private List<String> queryGSMetaData(String querySQL, List<String> columnNames) throws SQLException
  {
    // try with auto-closing statement resource
    try (Statement statement = this.createStatement())
    {
      statement.execute(querySQL);

      // handle the case where the result set is not what is expected
      // try with auto-closing resultset resource
      try (ResultSet rs = statement.getResultSet())
      {
        if (rs != null && rs.next())
        {
          List<String> columnValues = new ArrayList<>();
          for (String columnName : columnNames)
          {
            columnValues.add(rs.getString(columnName));
          }
          return columnValues;
        }
        else
        {
          // returned no results or an error
          throw new SQLException(
              errorResourceBundleManager.getLocalizedMessage(
                  ErrorCode.BAD_RESPONSE.getMessageCode().toString()));
        }
      }
    }
  }

  @Override
  public boolean getAutoCommit() throws SQLException
  {
    logger.debug(
        " public boolean getAutoCommit() throws SQLException");

    return sfSession.getAutoCommit();
  }

  @Override
  public void commit() throws SQLException
  {
    logger.debug(" public void commit() throws SQLException");

    // commit
    this.executeImmediate("commit");
  }

  @Override
  public void rollback() throws SQLException
  {
    logger.debug(" public void rollback() throws SQLException");

    // rollback
    this.executeImmediate("rollback");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException
  {
    logger.debug(
        " public void rollback(Savepoint savepoint) throws "
            + "SQLException");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException
  {
    logger.debug(
        " public void setReadOnly(boolean readOnly) throws "
            + "SQLException");

    if (readOnly)
    {
      throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public boolean isReadOnly() throws SQLException
  {
    logger.debug(" public boolean isReadOnly() throws SQLException");

    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException
  {
    logger.debug(
        " public void setCatalog(String catalog) throws SQLException");

    // switch db by running "use db"
    this.executeImmediate("use database \"" + catalog + "\"");
  }

  @Override
  public String getCatalog() throws SQLException
  {
    logger.debug(" public String getCatalog() throws SQLException");

    return sfSession.getDatabase();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException
  {
    logger.debug(
        " public void setTransactionIsolation(int level) "
            + "throws SQLException. level = {}", level);
    if (level == Connection.TRANSACTION_NONE
        || level == Connection.TRANSACTION_READ_COMMITTED)
    {
      this.transactionIsolation = level;
    }
    else
    {
      throw new SQLFeatureNotSupportedException(
          "Transaction Isolation " + Integer.toString(level) + " not supported.",
          ErrorCode.FEATURE_UNSUPPORTED.getSqlState(),
          ErrorCode.FEATURE_UNSUPPORTED.getMessageCode());
    }
  }

  @Override
  public int getTransactionIsolation() throws SQLException
  {
    logger.debug(
        " public int getTransactionIsolation() throws SQLException");

    return this.transactionIsolation;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    logger.debug(
        " public SQLWarning getWarnings() throws SQLException");

    return sqlWarnings;
  }

  @Override
  public void clearWarnings() throws SQLException
  {
    logger.debug(" public void clearWarnings() throws SQLException");

    sqlWarnings = null;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException
  {
    logger.debug(
        " public Statement createStatement(int resultSetType, "
            + "int resultSetConcurrency)");

    logger.debug("resultSetType=" + resultSetType +
        "; resultSetConcurrency=" + resultSetConcurrency);

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY
        || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY)
    {
      throw new SQLFeatureNotSupportedException();
    }
    return createStatement();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                   int resultSetHoldability)
      throws SQLException
  {
    logger.debug(
        " public Statement createStatement(int resultSetType, "
            + "int resultSetConcurrency,");

    if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
    {
      throw new SQLFeatureNotSupportedException();
    }
    return createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException
  {
    logger.debug(
        " public PreparedStatement prepareStatement(String sql) "
            + "throws SQLException");

    return new SnowflakePreparedStatementV1(this, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException
  {
    logger.debug(
        " public PreparedStatement prepareStatement(String sql, "
            + "int autoGeneratedKeys)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException
  {
    logger.debug(
        " public PreparedStatement prepareStatement(String sql, "
            + "int[] columnIndexes)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException
  {
    logger.debug(
        " public PreparedStatement prepareStatement(String sql, "
            + "String[] columnNames)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
                                            int resultSetConcurrency)
      throws SQLException
  {
    logger.debug(
        " public PreparedStatement prepareStatement(String sql, "
            + "int resultSetType,");

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY
        || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY)
    {
      logger.error(
          "result set type ({}) or result set concurrency ({}) "
              + "not supported",
          new Object[]{resultSetType, resultSetConcurrency});

      throw new SQLFeatureNotSupportedException();
    }
    return prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
                                            int resultSetConcurrency,
                                            int resultSetHoldability)
      throws SQLException
  {
    logger.debug(
        " public PreparedStatement prepareStatement(String sql, "
            + "int resultSetType,");

    if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
    {
      throw new SQLFeatureNotSupportedException();
    }
    return prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException
  {
    logger.debug(
        " public Map<String, Class<?>> getTypeMap() throws "
            + "SQLException");

    return Collections.emptyMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException
  {
    logger.debug(
        " public void setTypeMap(Map<String, Class<?>> map) "
            + "throws SQLException");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setHoldability(int holdability) throws SQLException
  {
    logger.debug(
        " public void setHoldability(int holdability) throws "
            + "SQLException");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability() throws SQLException
  {
    logger.debug(" public int getHoldability() throws SQLException");

    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException
  {
    logger.debug(
        " public Savepoint setSavepoint() throws SQLException");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException
  {
    logger.debug(
        " public Savepoint setSavepoint(String name) throws "
            + "SQLException");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException
  {
    logger.debug(
        " public void releaseSavepoint(Savepoint savepoint) throws "
            + "SQLException");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob createBlob() throws SQLException
  {
    logger.debug(" public Blob createBlob() throws SQLException");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob createClob() throws SQLException
  {
    logger.debug(" public Clob createClob() throws SQLException");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob createNClob() throws SQLException
  {
    logger.debug(" public NClob createNClob() throws SQLException");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException
  {
    logger.debug(
        " public SQLXML createSQLXML() throws SQLException");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException
  {
    logger.debug(
        " public boolean isValid(int timeout) throws SQLException");

    // TODO: run query here or ping
    return !isClosed;
  }

  @Override
  public void setClientInfo(Properties properties)
      throws SQLClientInfoException
  {
    logger.debug(
        " public void setClientInfo(Properties properties)");

    if (this.clientInfo == null)
      this.clientInfo = new Properties();

    // make a copy, don't point to the properties directly since we don't
    // own it.
    this.clientInfo.clear();
    this.clientInfo.putAll(properties);

    if (sfSession != null)
      sfSession.setClientInfo(properties);
  }

  @Override
  public void setClientInfo(String name, String value)
      throws SQLClientInfoException
  {
    logger.debug(" public void setClientInfo(String name, String value)");

    if (this.clientInfo == null)
      this.clientInfo = new Properties();

    this.clientInfo.setProperty(name, value);

    if (sfSession != null)
      sfSession.setClientInfo(name, value);
  }

  @Override
  public Properties getClientInfo() throws SQLException
  {
    logger.debug(
        " public Properties getClientInfo() throws SQLException");

    if (sfSession != null)
      return sfSession.getClientInfo();

    if (this.clientInfo != null)
    {
      // defensive copy to avoid client from changing the properties
      // directly w/o going through the API
      Properties copy = new Properties();
      copy.putAll(this.clientInfo);

      return copy;
    }
    else
      return null;
  }

  @Override
  public String getClientInfo(String name)
  {
    if (sfSession != null)
      return sfSession.getClientInfo(name);

    logger.debug(" public String getClientInfo(String name)");

    if (this.clientInfo != null)
      return this.clientInfo.getProperty(name);
    else
      return null;
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements)
      throws SQLException
  {
    logger.debug(
        " public Array createArrayOf(String typeName, Object[] "
            + "elements)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException
  {
    logger.debug(
        " public Struct createStruct(String typeName, Object[] "
            + "attributes)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setSchema(String schema) throws SQLException
  {
    logger.debug(
        " public void setSchema(String schema) throws SQLException");

    String databaseName = getCatalog();

    // switch schema by running "use db.schema"
    if (databaseName == null)
    {
      this.executeImmediate("use schema \"" + schema + "\"");
    }
    else
    {
      this.executeImmediate("use schema \"" + databaseName + "\".\"" + schema + "\"");
    }
  }

  @Override
  public String getSchema() throws SQLException
  {
    logger.debug(" public String getSchema() throws SQLException");

    return sfSession.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException
  {
    logger.debug(
        " public void abort(Executor executor) throws SQLException");

    close();
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds)
      throws SQLException
  {
    logger.debug(
        " public void setNetworkTimeout(Executor executor, int "
            + "milliseconds)");

    networkTimeoutInMilli = milliseconds;
  }

  @Override
  public int getNetworkTimeout() throws SQLException
  {
    logger.debug(
        " public int getNetworkTimeout() throws SQLException");

    return networkTimeoutInMilli;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    logger.debug(
        " public boolean isWrapperFor(Class<?> iface) throws "
            + "SQLException");

    return iface.isInstance(this);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    logger.debug(
        " public <T> T unwrap(Class<T> iface) throws SQLException");


    if (!iface.isInstance(this))
    {
      throw new RuntimeException(this.getClass().getName()
          + " not unwrappable from " + iface.getName());
    }
    return (T) this;
  }

  void setSFSessionProperty(String propertyName, boolean b)
  {
    this.sessionProperties.put(propertyName, b);
  }

  public Object getSFSessionProperty(String propertyName)
  {
    return this.sessionProperties.get(propertyName);
  }

  public static int getMajorVersion()
  {
    return SnowflakeDriver.majorVersion;
  }

  public static int getMinorVersion()
  {
    return SnowflakeDriver.minorVersion;
  }

  public int getDatabaseMajorVersion()
  {
    return databaseMajorVersion;
  }

  public int getDatabaseMinorVersion()
  {
    return databaseMinorVersion;
  }

  public String getDatabaseVersion()
  {
    return databaseVersion;
  }

  public String getUserName()
  {
    return userName;
  }

  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  public String getAccountName()
  {
    return accountName;
  }

  public void setAccountName(String accountName)
  {
    this.accountName = accountName;
  }

  public String getURL()
  {
    return serverUrl;
  }

  public int getQueryTimeout()
  {
    return queryTimeout;
  }

  /**
   * Method to put data from a stream at a stage location. The data will be
   * uploaded as one file. No splitting is done in this method.
   * <p>
   * Stream size must match the total size of data in the input stream unless
   * compressData parameter is set to true.
   * <p>
   * caller is responsible for passing the correct size for the data in the
   * stream and releasing the inputStream after the method is called.
   *
   * @param stageName    stage name: e.g. ~ or table name or stage name
   * @param destPrefix   path prefix under which the data should be uploaded on the stage
   * @param inputStream  input stream from which the data will be uploaded
   * @param destFileName destination file name to use
   * @param streamSize   data size in the stream
   * @throws java.sql.SQLException failed to put data from a stream at stage
   */
  public void uploadStream(String stageName,
                           String destPrefix,
                           InputStream inputStream,
                           String destFileName,
                           long streamSize)
      throws SQLException
  {
    uploadStreamInternal(stageName, destPrefix, inputStream,
        destFileName, streamSize, false);
  }

  /**
   * Method to compress data from a stream and upload it at a stage location.
   * The data will be uploaded as one file. No splitting is done in this method.
   * <p>
   * caller is responsible for releasing the inputStream after the method is
   * called.
   *
   * @param stageName    stage name: e.g. ~ or table name or stage name
   * @param destPrefix   path prefix under which the data should be uploaded on the stage
   * @param inputStream  input stream from which the data will be uploaded
   * @param destFileName destination file name to use
   * @throws java.sql.SQLException failed to compress and put data from a stream at stage
   */
  public void compressAndUploadStream(String stageName,
                                      String destPrefix,
                                      InputStream inputStream,
                                      String destFileName)
      throws SQLException
  {
    uploadStreamInternal(stageName, destPrefix, inputStream,
        destFileName, 0, true);
  }

  /**
   * Method to put data from a stream at a stage location. The data will be
   * uploaded as one file. No splitting is done in this method.
   * <p>
   * Stream size must match the total size of data in the input stream unless
   * compressData parameter is set to true.
   * <p>
   * caller is responsible for passing the correct size for the data in the
   * stream and releasing the inputStream after the method is called.
   *
   * @param stageName    stage name: e.g. ~ or table name or stage name
   * @param destPrefix   path prefix under which the data should be uploaded on the stage
   * @param inputStream  input stream from which the data will be uploaded
   * @param destFileName destination file name to use
   * @param streamSize   data size in the stream
   * @param compressData whether compression is requested fore uploading data
   * @throws java.sql.SQLException
   */
  private void uploadStreamInternal(String stageName,
                                    String destPrefix,
                                    InputStream inputStream,
                                    String destFileName,
                                    long streamSize,
                                    boolean compressData)
      throws SQLException
  {
    logger.debug("upload data from stream: stageName={}" +
            ", destPrefix={}, destFileName={}",
        new Object[]{stageName, destPrefix, destFileName});

    if (stageName == null)
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "stage name is null");

    if (destFileName == null)
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "stage name is null");

    SnowflakeStatementV1 stmt = new SnowflakeStatementV1(this);

    StringBuilder putCommand = new StringBuilder();

    // use a placeholder for source file
    putCommand.append("put file:///tmp/placeholder ");

    // add stage name
    if (!stageName.startsWith("@"))
      putCommand.append("@");
    putCommand.append(stageName);

    // add dest prefix
    if (destPrefix != null)
    {
      if (!destPrefix.startsWith("/"))
        putCommand.append("/");
      putCommand.append(destPrefix);
    }

    SnowflakeFileTransferAgent transferAgent = null;
    transferAgent = new SnowflakeFileTransferAgent(putCommand.toString(),
        sfSession, stmt.getSfStatement());

    transferAgent.setSourceStream(inputStream);
    transferAgent.setDestFileNameForStreamSource(destFileName);
    transferAgent.setSourceStreamSize(streamSize);
    transferAgent.setCompressSourceFromStream(compressData);
    transferAgent.setOverwrite(true);
    transferAgent.execute();

    stmt.close();
  }

  public void setInjectedDelay(int delay)
  {
    if (sfSession != null)
      sfSession.setInjectedDelay(delay);

    this._injectedDelay.set(delay);
  }

  void injectedDelay()
  {

    int d = _injectedDelay.get();

    if (d != 0)
    {
      _injectedDelay.set(0);
      try
      {
        logger.trace("delayed for {}", d);

        Thread.sleep(d);
      }
      catch (InterruptedException ex)
      {
      }
    }
  }

  public int getInjectSocketTimeout()
  {
    return injectSocketTimeout;
  }

  public void setInjectSocketTimeout(int injectSocketTimeout)
  {
    if (sfSession != null)
      sfSession.setInjectSocketTimeout(injectSocketTimeout);

    this.injectSocketTimeout = injectSocketTimeout;
  }

  public boolean isRetryQuery()
  {
    return retryQuery;
  }

  public void setRetryQuery(boolean retryQuery)
  {
    this.retryQuery = retryQuery;
  }

  public int getInjectClientPause()
  {
    return injectClientPause;
  }

  public void setInjectClientPause(int injectClientPause)
  {
    if (sfSession != null)
      sfSession.setInjectClientPause(injectClientPause);

    this.injectClientPause = injectClientPause;
  }

  public void setInjectFileUploadFailure(String fileToFail)
  {
    if (sfSession != null)
      sfSession.setInjectFileUploadFailure(fileToFail);

    this.injectFileUploadFailure = fileToFail;
  }


  public String getInjectFileUploadFailure()
  {
    return this.injectFileUploadFailure;
  }

  public SFSession getSfSession()
  {
    return sfSession;
  }

  private void appendWarning(SQLWarning w)
  {
    if (sqlWarnings == null)
    {
      sqlWarnings = w;
    }
    else
    {
      sqlWarnings.setNextWarning(w);
    }
  }
}
