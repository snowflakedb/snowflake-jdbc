/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.ClientAuthnDTO;
import net.snowflake.client.log.JDK14Logger;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.security.PrivateKey;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Snowflake session implementation
 *
 * @author jhuang
 */
public class SFSession
{

  static final SFLogger logger = SFLoggerFactory.getLogger(SFSession.class);

  private static final String SF_PATH_SESSION_HEARTBEAT = "/session/heartbeat";

  public static final String SF_QUERY_REQUEST_ID = "requestId";

  public static final String SF_HEADER_AUTHORIZATION = HttpHeaders.AUTHORIZATION;

  public static final String SF_HEADER_SNOWFLAKE_AUTHTYPE = "Snowflake";

  public static final String SF_HEADER_TOKEN_TAG = "Token";

  // increase heartbeat timeout from 60 sec to 300 sec
  // per https://support-snowflake.zendesk.com/agent/tickets/6629
  private static int SF_HEARTBEAT_TIMEOUT = 300;

  private boolean isClosed = true;

  private String sessionToken;
  private String masterToken;
  private long masterTokenValidityInSeconds;

  private String newClientForUpdate;

  // Injected delay for the purpose of connection timeout testing
  // Any statement execution will sleep for the specified number of milliseconds
  private AtomicInteger _injectedDelay = new AtomicInteger(0);

  private String databaseVersion = null;
  private int databaseMajorVersion = 0;
  private int databaseMinorVersion = 0;

  private AtomicInteger sequenceId = new AtomicInteger(0);

  /**
   * Amount of seconds a user is willing to tolerate for establishing the
   * connection with database. In our case, it means the first login
   * request to get authorization token.
   * <p>
   * Default:60 seconds
   */
  private int loginTimeout = 60;

  /**
   * Amount of milliseconds a user is willing to tolerate for network related
   * issues (e.g. HTTP 503/504) or database transient issues (e.g. GS
   * not responding)
   * <p>
   * A value of 0 means no timeout
   * <p>
   * Default: 0
   */
  private int networkTimeoutInMilli = 0; // in milliseconds

  private boolean enableCombineDescribe = false;


  private Map<String, Object> sessionProperties = new HashMap<>(1);

  private final static ObjectMapper mapper = new ObjectMapper();

  private Properties clientInfo = new Properties();

  // timeout setting for http client, they will be adjusted after first
  // login request to GS
  private int healthCheckInterval = 45; // seconds

  private int httpClientConnectionTimeout = 60000; // milliseconds

  private static int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300000; // millisec

  private int httpClientSocketTimeout =
      DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT; // milliseconds

  //--- Simulated failures for testing

  // whether we try to simulate a socket timeout (a default value of 0 means
  // no simulation). The value is in milliseconds
  private int injectSocketTimeout = 0;

  // simulate client pause after initial execute and before first get-result
  // call ( a default value of 0 means no pause). The value is in seconds
  private int injectClientPause = 0;

  //Generate exception while uploading file with a given name
  private String injectFileUploadFailure = null;

  private Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>();

  // session parameters
  private Map<String, Object> sessionParametersMap = new HashMap<String, Object>();

  final private static int MAX_SESSION_PARAMETERS = 1000;

  private boolean passcodeInPassword = false;

  private boolean executeReturnCountForDML = false;

  private boolean enableHeartbeat = false;

  private AtomicBoolean autoCommit = new AtomicBoolean(true);

  private boolean rsColumnCaseInsensitive = false;

  // database that current session is on
  private String database;

  // schema that current session is on
  private String schema;

  // role that current session is on
  private String role;

  // For Metadata request(i.e. DatabaseMetadata.getTables or
  // DatabaseMetadata.getSchemas,), whether to use connection ctx to
  // improve the request time
  private boolean metadataRequestUseConnectionCtx = false;

  private SnowflakeType timestampMappedType =
      SnowflakeType.TIMESTAMP_LTZ;

  private boolean jdbcTreatDecimalAsInt = true;

  // deprecated
  private Level tracingLevel = Level.INFO;

  private List<SFException> sqlWarnings = new ArrayList<>();

  // client to log session metrics to telemetry in GS
  private Telemetry telemetryClient;

  //default value is false will be updated when login
  private boolean clientTelemetryEnabled = false;

  // The server can read array binds from a stage instead of query payload.
  // When there as many bind values as this threshold, we should upload them to a stage.
  private int arrayBindStageThreshold = 0;

  // name of temporary stage to upload array binds to; null if none has been created yet
  private String arrayBindStage = null;

  public void addProperty(SFSessionProperty sfSessionProperty,
                          Object propertyValue)
      throws SFException
  {
    addProperty(sfSessionProperty.getPropertyKey(), propertyValue);
  }

  /**
   * Add a property
   * If a property is known for connection, add it to connection properties
   * If not, add it as a dynamic session parameters
   * <p>
   * Make sure a property is not added more than once and the number of
   * properties does not exceed limit.
   *
   * @param propertyName  property name
   * @param propertyValue property value
   * @throws SFException exception raised from Snowflake components
   */
  public void addProperty(String propertyName, Object propertyValue)
      throws SFException
  {
    SFSessionProperty connectionProperty =
        SFSessionProperty.lookupByKey(propertyName);

    if (connectionProperty != null)
    {
      // check if the value type is as expected
      propertyValue = SFSessionProperty
          .checkPropertyValue(connectionProperty, propertyValue);

      if (connectionPropertiesMap.containsKey(connectionProperty))
      {
        throw new SFException(ErrorCode.DUPLICATE_CONNECTION_PROPERTY_SPECIFIED,
            propertyName);
      }
      else
      {
        connectionPropertiesMap.put(connectionProperty, propertyValue);
      }

      switch (connectionProperty)
      {
        case LOGIN_TIMEOUT:
          if (propertyValue != null)
            loginTimeout = (Integer) propertyValue;
          break;

        case NETWORK_TIMEOUT:
          if (propertyValue != null)
            networkTimeoutInMilli = (Integer) propertyValue;
          break;

        case INJECT_CLIENT_PAUSE:
          if (propertyValue != null)
            injectClientPause = (Integer) propertyValue;
          break;

        case INJECT_SOCKET_TIMEOUT:
          if (propertyValue != null)
            injectSocketTimeout = (Integer) propertyValue;
          break;

        case PASSCODE_IN_PASSWORD:
          passcodeInPassword = (propertyValue != null && (Boolean) propertyValue);
          break;

        case TRACING:
          if (propertyValue != null)
          {
            tracingLevel = Level.parse(((String)propertyValue).toUpperCase());
            // tracingLevel is effective only if customer is using the new logging config/framework
            if (tracingLevel != null && System.getProperty("snowflake.jdbc.loggerImpl") == null
                && logger instanceof JDK14Logger)
            {
              JDK14Logger.setLevel(tracingLevel);
            }
          }
          break;

        default:
          break;
      }
    }
    else
    {
      // this property does not match any predefined property, treat it as
      // session parameter
      if (sessionParametersMap.containsKey(propertyName))
        throw new SFException(ErrorCode.DUPLICATE_CONNECTION_PROPERTY_SPECIFIED,
            propertyName);
      else
        sessionParametersMap.put(propertyName, propertyValue);

      // check if the number of session properties exceed limit
      if (sessionParametersMap.size() > MAX_SESSION_PARAMETERS)
        throw new SFException(ErrorCode.TOO_MANY_SESSION_PARAMETERS,
            MAX_SESSION_PARAMETERS);
    }
  }

  protected String getServerUrl()
  {
    if (connectionPropertiesMap.containsKey(SFSessionProperty.SERVER_URL))
    {
      return (String) connectionPropertiesMap.get(SFSessionProperty.SERVER_URL);
    }
    return null;
  }

  /**
   * If authenticator is null and private key is specified, jdbc will assume
   * key pair authentication
   *
   * @return true if authenticator type is SNOWFLAKE (meaning password)
   */
  private boolean isSnowflakeAuthenticator()
  {
    String authenticator = (String) connectionPropertiesMap.get(
        SFSessionProperty.AUTHENTICATOR);

    PrivateKey privateKey = (PrivateKey) connectionPropertiesMap.get(
        SFSessionProperty.PRIVATE_KEY);

    return (authenticator == null && privateKey == null) ||
        ClientAuthnDTO.AuthenticatorType.SNOWFLAKE.name()
            .equalsIgnoreCase(authenticator);
  }

  /**
   * Open a new database session
   *
   * @throws SFException           this is a runtime exception
   * @throws SnowflakeSQLException exception raised from Snowflake components
   */
  public synchronized void open() throws SFException, SnowflakeSQLException
  {
    performSanityCheckOnProperties();

    Boolean insecureMode = (Boolean)connectionPropertiesMap.get(
        SFSessionProperty.INSECURE_MODE);

    HttpUtil.initHttpClient(insecureMode != null ? insecureMode : false, null);

    SessionUtil.LoginInput loginInput = new SessionUtil.LoginInput();
    loginInput.setServerUrl(
        (String) connectionPropertiesMap.get(SFSessionProperty.SERVER_URL))
        .setDatabaseName(
            (String) connectionPropertiesMap.get(SFSessionProperty.DATABASE))
        .setSchemaName(
            (String) connectionPropertiesMap.get(SFSessionProperty.SCHEMA))
        .setWarehouse(
            (String) connectionPropertiesMap.get(SFSessionProperty.WAREHOUSE))
        .setRole((String) connectionPropertiesMap.get(SFSessionProperty.ROLE))
        .setAuthenticator(
            (String) connectionPropertiesMap.get(SFSessionProperty.AUTHENTICATOR))
        .setAccountName(
            (String) connectionPropertiesMap.get(SFSessionProperty.ACCOUNT))
        .setLoginTimeout(loginTimeout)
        .setUserName(
            (String) connectionPropertiesMap.get(SFSessionProperty.USER))
        .setPassword(
            (String) connectionPropertiesMap.get(SFSessionProperty.PASSWORD))
        .setToken(
            (String) connectionPropertiesMap.get(SFSessionProperty.TOKEN))
        .setClientInfo(this.getClientInfo())
        .setPasscodeInPassword(passcodeInPassword)
        .setPasscode(
            (String) connectionPropertiesMap.get(SFSessionProperty.PASSCODE))
        .setConnectionTimeout(httpClientConnectionTimeout)
        .setSocketTimeout(httpClientSocketTimeout)
        .setAppId((String) connectionPropertiesMap.get(SFSessionProperty.APP_ID))
        .setAppVersion(
            (String) connectionPropertiesMap.get(SFSessionProperty.APP_VERSION))
        .setSessionParameters(sessionParametersMap)
        .setPrivateKey((PrivateKey) connectionPropertiesMap.get(
            SFSessionProperty.PRIVATE_KEY))
        .setApplication((String) connectionPropertiesMap.get(
            SFSessionProperty.APPLICATION));

    SessionUtil.LoginOutput loginOutput = SessionUtil.openSession(loginInput);

    sessionToken = loginOutput.getSessionToken();
    masterToken = loginOutput.getMasterToken();
    databaseVersion = loginOutput.getDatabaseVersion();
    databaseMajorVersion = loginOutput.getDatabaseMajorVersion();
    databaseMinorVersion = loginOutput.getDatabaseMinorVersion();
    healthCheckInterval = loginOutput.getHealthCheckInterval();
    httpClientSocketTimeout = loginOutput.getHttpClientSocketTimeout();
    masterTokenValidityInSeconds = loginOutput.getMasterTokenValidityInSeconds();
    database = loginOutput.getSessionDatabase();
    schema = loginOutput.getSessionSchema();
    role = loginOutput.getSessionRole();

    // Update common parameter values for this session
    SessionUtil.updateSfDriverParamValues(loginOutput.getCommonParams(), this);

    String loginDatabaseName = (String)connectionPropertiesMap.get(
        SFSessionProperty.DATABASE);
    String loginSchemaName = (String)connectionPropertiesMap.get(
        SFSessionProperty.SCHEMA);
    String loginRole = (String)connectionPropertiesMap.get(
        SFSessionProperty.ROLE);

    if (loginDatabaseName != null && !loginDatabaseName
        .equalsIgnoreCase(database))
    {
      sqlWarnings.add(new SFException(ErrorCode
          .CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP,
          "Database", loginDatabaseName, database));
    }

    if (loginSchemaName != null && !loginSchemaName
        .equalsIgnoreCase(schema))
    {
      sqlWarnings.add(new SFException(ErrorCode.
          CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP,
          "Schema", loginSchemaName, schema));
    }

    if (loginRole != null && !loginRole
        .equalsIgnoreCase(role))
    {
      sqlWarnings.add(new SFException(ErrorCode.
          CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP,
          "Role", loginRole, role));
    }

    // start heartbeat for this session so that the master token will not expire
    startHeartbeatForThisSession();
    isClosed = false;
  }

  /**
   * Performs a sanity check on properties. Sanity checking includes:
   * - verifying that a server url is present
   * - verifying various combinations of properties given the authenticator
   * @throws SFException
   */
  private void performSanityCheckOnProperties() throws SFException
  {
    for (SFSessionProperty property : SFSessionProperty.values())
    {
      if (property.isRequired() &&
          !connectionPropertiesMap.containsKey(property))
      {
        switch (property)
        {
          case SERVER_URL:
            throw new SFException(ErrorCode.MISSING_SERVER_URL);

          default:
            throw new SFException(ErrorCode.MISSING_CONNECTION_PROPERTY,
                property.getPropertyKey());
        }
      }
    }

    String authenticator = (String) connectionPropertiesMap.get(
        SFSessionProperty.AUTHENTICATOR);
    if (isSnowflakeAuthenticator() ||
        ClientAuthnDTO.AuthenticatorType.OKTA.name().equalsIgnoreCase(
            authenticator))
    {
      // userName and password are expected for both Snowflake and Okta.
      String userName = (String) connectionPropertiesMap.get(
          SFSessionProperty.USER);
      if (userName == null || userName.isEmpty())
      {
        throw new SFException(ErrorCode.MISSING_USERNAME);
      }

      String password = (String) connectionPropertiesMap.get(
          SFSessionProperty.PASSWORD);
      if (password == null || password.isEmpty())

      {
        throw new SFException(ErrorCode.MISSING_PASSWORD);
      }
    }
  }

  public String getNewClientForUpdate()
  {
    return newClientForUpdate;
  }

  public String getDatabaseVersion()
  {
    return databaseVersion;
  }

  public int getDatabaseMajorVersion()
  {
    return databaseMajorVersion;
  }

  public int getDatabaseMinorVersion()
  {
    return databaseMinorVersion;
  }

  private void setNewClientForUpdate(String newClientForUpdate)
  {
    this.newClientForUpdate = newClientForUpdate;
  }

  /**
   * A helper function to call global service and renew session.
   *
   * @param prevSessionToken the session token that has expired
   * @throws java.sql.SQLException if failed to renew the session
   * @throws SFException           if failed to renew the session
   */
  synchronized void renewSession(String prevSessionToken)
      throws SFException, SnowflakeSQLException
  {
    // if session token has changed, don't renew again
    if (sessionToken != null &&
        !sessionToken.equals(prevSessionToken))
      return;

    SessionUtil.LoginInput loginInput = new SessionUtil.LoginInput();
    loginInput.setServerUrl(
        (String) connectionPropertiesMap.get(SFSessionProperty.SERVER_URL))
        .setSessionToken(sessionToken)
        .setMasterToken(masterToken)
        .setLoginTimeout(loginTimeout);

    SessionUtil.LoginOutput loginOutput = SessionUtil.renewSession(loginInput);

    sessionToken = loginOutput.getSessionToken();
    masterToken = loginOutput.getMasterToken();
  }

  /**
   * get session token
   *
   * @return session token
   */
  public String getSessionToken()
  {
    return sessionToken;
  }

  /**
   * Close the connection
   *
   * @throws SnowflakeSQLException if failed to close the connection
   * @throws SFException           if failed to close the connection
   */
  public void close() throws SFException, SnowflakeSQLException
  {
    logger.debug(" public void close() throws SFException");

    // stop heartbeat for this session
    stopHeartbeatForThisSession();

    if (isClosed)
    {
      return;
    }

    SessionUtil.LoginInput loginInput = new SessionUtil.LoginInput();
    loginInput.setServerUrl(
        (String) connectionPropertiesMap.get(SFSessionProperty.SERVER_URL))
        .setSessionToken(sessionToken)
        .setLoginTimeout(loginTimeout);

    SessionUtil.closeSession(loginInput);
    closeTelemetryClient();
    isClosed = true;
  }

  /**
   * Start heartbeat for this session
   */
  protected void startHeartbeatForThisSession()
  {
    if (enableHeartbeat)
    {
      logger.debug("start heartbeat, master token validity: " +
          masterTokenValidityInSeconds);

      HeartbeatBackground.getInstance().addSession(this,
          masterTokenValidityInSeconds);
    }
    else
    {
      logger.debug("heartbeat not enabled for the session");
    }
  }

  /**
   * Stop heartbeat for this session
   */
  protected void stopHeartbeatForThisSession()
  {
    if (enableHeartbeat)
    {
      logger.debug("stop heartbeat");

      HeartbeatBackground.getInstance().removeSession(this);
    }
    else
    {
      logger.debug("heartbeat not enabled for the session");
    }
  }

  /**
   * Send heartbeat for the session
   *
   * @throws SFException  exception raised from Snowflake
   * @throws SQLException exception raised from SQL generic layers
   */
  protected void heartbeat() throws SFException, SQLException
  {
    logger.debug(" public void heartbeat()");

    if (isClosed)
    {
      return;
    }

    HttpPost postRequest = null;

    String requestId = UUID.randomUUID().toString();

    boolean retry = false;

    // the loop for retrying if it runs into session expiration
    do
    {
      try
      {
        URIBuilder uriBuilder;

        uriBuilder = new URIBuilder(
            (String) connectionPropertiesMap.get(SFSessionProperty.SERVER_URL));

        uriBuilder.addParameter(SFSession.SF_QUERY_REQUEST_ID, requestId);

        uriBuilder.setPath(SF_PATH_SESSION_HEARTBEAT);

        postRequest = new HttpPost(uriBuilder.build());

        // remember the session token in case it expires we need to renew
        // the session only when no other thread has renewed it
        String prevSessionToken = sessionToken;

        postRequest.setHeader(SF_HEADER_AUTHORIZATION,
            SF_HEADER_SNOWFLAKE_AUTHTYPE + " "
                + SF_HEADER_TOKEN_TAG + "=\""
                + prevSessionToken + "\"");

        logger.debug("Executing heartbeat request: {}",
            postRequest.toString());

        // the following will retry transient network issues
        String theResponse = HttpUtil.executeRequest(postRequest,
            SF_HEARTBEAT_TIMEOUT,
            0,
            null);

        JsonNode rootNode;

        logger.debug("connection heartbeat response: {}", theResponse);

        rootNode = mapper.readTree(theResponse);

        // check the response to see if it is session expiration response
        if (rootNode != null &&
            (Constants.SESSION_EXPIRED_GS_CODE == rootNode.path("code").asInt()))
        {
          logger.debug("renew session and retry");
          this.renewSession(prevSessionToken);
          retry = true;
          continue;
        }

        SnowflakeUtil.checkErrorAndThrowException(rootNode);

        // success
        retry = false;
      }
      catch (Throwable ex)
      {
        // for snowflake exception, just rethrow it
        if (ex instanceof SnowflakeSQLException)
          throw (SnowflakeSQLException) ex;

        logger.error("unexpected exception", ex);

        SFException sfe =
            IncidentUtil.generateIncidentWithException(
                this,
                requestId,
                null,
                ex,
                ErrorCode.INTERNAL_ERROR,
                "unexpected exception: " + ex.getMessage());

        throw sfe;
      }
    }
    while (retry);
  }

  public void setClientInfo(Properties properties)
      throws SQLClientInfoException
  {
    logger.debug(" public void setClientInfo(Properties properties)");

    if (this.clientInfo == null)
      this.clientInfo = new Properties();

    // make a copy, don't point to the properties directly since we don't
    // own it.
    this.clientInfo.clear();
    this.clientInfo.putAll(properties);
  }

  public void setClientInfo(String name, String value)
      throws SQLClientInfoException
  {
    logger.debug(" public void setClientInfo(String name, String value)");

    if (this.clientInfo == null)
      this.clientInfo = new Properties();

    this.clientInfo.setProperty(name, value);
  }

  public Properties getClientInfo()
  {
    logger.debug(" public Properties getClientInfo()");

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

  public String getClientInfo(String name)
  {
    logger.debug(" public String getClientInfo(String name)");

    if (this.clientInfo != null)
      return this.clientInfo.getProperty(name);
    else
      return null;
  }

  void setSFSessionProperty(String propertyName, boolean propertyValue)
  {
    this.sessionProperties.put(propertyName, propertyValue);
  }

  public Object getSFSessionProperty(String propertyName)
  {
    return this.sessionProperties.get(propertyName);
  }

  public void setInjectedDelay(int delay)
  {
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
    this.injectSocketTimeout = injectSocketTimeout;
  }

  public void setInjectFileUploadFailure(String fileToFail)
  {
    this.injectFileUploadFailure = fileToFail;
  }

  public String getInjectFileUploadFailure()
  {
    return this.injectFileUploadFailure;
  }

  protected int getNetworkTimeoutInMilli()
  {
    return networkTimeoutInMilli;
  }

  public boolean isClosed()
  {
    return isClosed;
  }

  public int getInjectClientPause()
  {
    return injectClientPause;
  }

  public void setInjectClientPause(int injectClientPause)
  {
    this.injectClientPause = injectClientPause;
  }

  protected int getHttpClientConnectionTimeout()
  {
    return httpClientConnectionTimeout;
  }

  protected int getHttpClientSocketTimeout()
  {
    return httpClientSocketTimeout;
  }

  protected int getAndIncrementSequenceId()
  {
    return sequenceId.getAndIncrement();
  }

  public void setExecuteReturnCountForDML(boolean executeReturnCountForDML)
  {
    this.executeReturnCountForDML = executeReturnCountForDML;
  }

  public boolean isExecuteReturnCountForDML()
  {
    return this.executeReturnCountForDML;
  }

  public boolean isEnableHeartbeat()
  {
    return enableHeartbeat;
  }

  public void setEnableHeartbeat(boolean enableHeartbeat)
  {
    this.enableHeartbeat = enableHeartbeat;
  }

  public boolean getAutoCommit()
  {
    return autoCommit.get();
  }

  public void setAutoCommit(boolean autoCommit)
  {
    this.autoCommit.set(autoCommit);
  }

  public void setRsColumnCaseInsensitive(boolean rsColumnCaseInsensitive)
  {
    this.rsColumnCaseInsensitive = rsColumnCaseInsensitive;
  }

  public boolean getRsColumnCaseInsensitive()
  {
    return this.rsColumnCaseInsensitive;
  }

  public String getDatabase()
  {
    return this.database;
  }

  public void setDatabase(String database)
  {
    this.database = database;
  }

  public String getSchema()
  {
    return this.schema;
  }

  public void setSchema(String schema)
  {
    this.schema = schema;
  }

  public String getRole()
  {
    return role;
  }

  public void setRole(String role)
  {
    this.role = role;
  }

  public void setMetadataRequestUseConnectionCtx(boolean enabled)
  {
    this.metadataRequestUseConnectionCtx = enabled;
  }

  public boolean getMetadataRequestUseConnectionCtx()
  {
    return this.metadataRequestUseConnectionCtx;
  }

  public SnowflakeType getTimestampMappedType()
  {
    return timestampMappedType;
  }

  public void setTimestampMappedType(SnowflakeType timestampMappedType)
  {
    this.timestampMappedType = timestampMappedType;
  }

  public boolean isJdbcTreatDecimalAsInt()
  {
    return jdbcTreatDecimalAsInt;
  }

  public void setJdbcTreatDecimalAsInt(boolean jdbcTreatDecimalAsInt)
  {
    this.jdbcTreatDecimalAsInt = jdbcTreatDecimalAsInt;
  }

  public void setEnableCombineDescribe(boolean enable)
  {
    this.enableCombineDescribe = enable;
  }

  public boolean getEnableCombineDescribe()
  {
    return this.enableCombineDescribe;
  }

  public Integer getQueryTimeout()
  {
    return (Integer)this.connectionPropertiesMap.get(SFSessionProperty.QUERY_TIMEOUT);
  }

  public String getUser()
  {
    return (String)this.connectionPropertiesMap.get(SFSessionProperty.USER);
  }

  public String getUrl()
  {
    return (String)this.connectionPropertiesMap.get(SFSessionProperty.SERVER_URL);
  }

  public List<SFException> getSqlWarnings()
  {
    return sqlWarnings;
  }

  public void clearSqlWarnings()
  {
    sqlWarnings.clear();
  }

  public synchronized Telemetry getTelemetryClient()
  {
    // initialize for the first time. this should only be done after session
    // properties have been set, else the client won't properly resolve the URL.
    if (telemetryClient == null)
    {
      if (getUrl() == null)
      {
        logger.error("Telemetry client created before session properties set.");
        return null;
      }
      telemetryClient = Telemetry.createTelemetry(this);
    }
    return telemetryClient;
  }

  public void closeTelemetryClient()
  {
    if (telemetryClient != null)
    {
      try
      {
        telemetryClient.close();
      }
      catch (IOException ex)
      {
        logger.warn("Telemetry client failed to submit metrics on close.");
      }
    }
  }

  public boolean isClientTelemetryEnabled()
  {
    return this.clientTelemetryEnabled;
  }
  public void setClientTelemetryEnabled(boolean clientTelemetryEnabled)
  {
    this.clientTelemetryEnabled = clientTelemetryEnabled;
  }

  public int getArrayBindStageThreshold()
  {
    return arrayBindStageThreshold;
  }

  public void setArrayBindStageThreshold(int arrayBindStageThreshold)
  {
    this.arrayBindStageThreshold = arrayBindStageThreshold;
  }

  public String getArrayBindStage()
  {
    return arrayBindStage;
  }

  public void setArrayBindStage(String arrayBindStage)
  {
    this.arrayBindStage = String.format("%s.%s.%s",
        this.getDatabase(), this.getSchema(), arrayBindStage);
  }
}
