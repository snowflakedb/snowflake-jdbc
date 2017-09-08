/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.ClientAuthnDTO;
import net.snowflake.common.core.ResourceBundleManager;
import org.apache.http.HttpHeaders;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;

import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Snowflake session implementation
 *
 * @author jhuang
 */
public class SFSession
{

  static final
  SFLogger logger = SFLoggerFactory.getLogger(SFSession.class);

  private static final String SF_PATH_SESSION_HEARTBEAT = "/session/heartbeat";

  private static final String SF_PATH_AUTHENTICATOR_REQUEST
      = "/session/authenticator-request";

  public static final String SF_QUERY_REQUEST_ID = "requestId";

  public static final String SF_HEADER_AUTHORIZATION = HttpHeaders.AUTHORIZATION;

  public static final String SF_HEADER_BASIC_AUTHTYPE = "Basic";

  public static final String SF_HEADER_SNOWFLAKE_AUTHTYPE = "Snowflake";

  public static final String SF_HEADER_TOKEN_TAG = "Token";

  // heartbeat retry timeout in seconds

  // increase heartbeat timeout from 60 sec to 300 sec
  // per https://support-snowflake.zendesk.com/agent/tickets/6629
  private static int SF_HEARTBEAT_TIMEOUT = 300;

  private HttpClient httpClient;

  private boolean isClosed = true;

  private String sessionToken;
  private String masterToken;
  private long masterTokenValidityInSeconds;
  private String remMeToken;

  private String samlResponse;

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

  private boolean abortDetachedQuery = true;

  private Map<String, Object> sessionProperties = new HashMap<>(1);

  private final static ObjectMapper mapper = new ObjectMapper();

  static final ResourceBundleManager errorResourceBundleManager =
      ResourceBundleManager.getSingleton(ErrorCode.errorMessageResource);

  private Properties clientInfo = new Properties();

  // timeout setting for http client, they will be adjusted after first
  // login request to GS
  private int healthCheckInterval = 45; // seconds

  private int httpClientConnectionTimeout = 60000; // milliseconds

  private static int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300000; // millisec

  private int httpClientSocketTimeout =
      DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT; // milliseconds

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

  private HeartbeatBackground heartbeatBackground;

  Map<SFSessionProperty, Object> connectionPropertiesMap =
      new HashMap<SFSessionProperty, Object>();

  // session parameters
  Map<String, Object> sessionParametersMap = new HashMap<String, Object>();

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
      if (propertyValue != null &&
          !connectionProperty.getValueType().isAssignableFrom(
              propertyValue.getClass()))
      {
        throw new SFException(ErrorCode.INVALID_PARAMETER_TYPE,
            propertyValue.getClass().getName(),
            connectionProperty.getValueType().getName());
      }

      if (connectionPropertiesMap.containsKey(connectionProperty))
        throw new SFException(ErrorCode.DUPLICATE_CONNECTION_PROPERTY_SPECIFIED,
            propertyName);
      else
        connectionPropertiesMap.put(connectionProperty, propertyValue);

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

        case USE_PROXY:
          useProxy = (propertyValue != null && (Boolean) propertyValue);
          ;

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
    else
      return null;
  }

  /**
   * Is the authenticator Snowflake?
   *
   * @return true yes otherwise no
   */
  private boolean isSnowflakeAuthenticator()
  {
    String authenticator = (String) connectionPropertiesMap.get(
        SFSessionProperty.AUTHENTICATOR);
    return authenticator == null ||
        authenticator.equalsIgnoreCase(
            ClientAuthnDTO.AuthenticatorType.SNOWFLAKE.name());
  }

  /**
   * Open a new database session
   *
   * @throws SFException           this is a runtime exception
   * @throws SnowflakeSQLException exception raised from Snowfalke components
   */
  public synchronized void open() throws SFException, SnowflakeSQLException
  {
    performSanityCheckOnProperties();

    if (httpClient == null)
    {
      httpClient = HttpUtil.getHttpClient();
    }

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
        .setHttpClient(httpClient)
        .setAccountName(
            (String) connectionPropertiesMap.get(SFSessionProperty.ACCOUNT))
        .setLoginTimeout(loginTimeout)
        .setUserName(
            (String) connectionPropertiesMap.get(SFSessionProperty.USER))
        .setPassword(
            (String) connectionPropertiesMap.get(SFSessionProperty.PASSWORD))
        .setClientInfo(this.getClientInfo())
        .setPasscodeInPassword(passcodeInPassword)
        .setPasscode(
            (String) connectionPropertiesMap.get(SFSessionProperty.PASSCODE))
        .setConnectionTimeout(httpClientConnectionTimeout)
        .setSocketTimeout(httpClientSocketTimeout)
        .setAppId((String) connectionPropertiesMap.get(SFSessionProperty.APP_ID))
        .setAppVersion(
            (String) connectionPropertiesMap.get(SFSessionProperty.APP_VERSION))
        .setSessionParameters(sessionParametersMap);

    SessionUtil.LoginOutput loginOutput = SessionUtil.openSession(loginInput);

    sessionToken = loginOutput.getSessionToken();
    masterToken = loginOutput.getMasterToken();
    remMeToken = loginOutput.getRemMeToken();
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

    // start heartbeat for this session so that the master token will not expire
    startHeartbeatForThisSession();
    isClosed = false;
  }

  private void performSanityCheckOnProperties() throws SFException
  {
    // validate that serverURL, user and password are specified
    if (!connectionPropertiesMap.containsKey(SFSessionProperty.SERVER_URL))
      throw new SFException(ErrorCode.MISSING_SERVER_URL);

    if (!connectionPropertiesMap.containsKey(SFSessionProperty.USER))
      throw new SFException(ErrorCode.MISSING_USERNAME);

    if (isSnowflakeAuthenticator() &&
        !connectionPropertiesMap.containsKey(SFSessionProperty.PASSWORD))
      throw new SFException(ErrorCode.MISSING_PASSWORD);

    for (SFSessionProperty property : SFSessionProperty.values())
    {
      if (property.isRequired() &&
          !connectionPropertiesMap.containsKey(property))
      {
        throw new SFException(ErrorCode.MISSING_CONNECTION_PROPERTY,
            property.getPropertyKey());
      }
    }

    String userName = (String) connectionPropertiesMap.get(
        SFSessionProperty.USER);
    // userName and password are expected
    if (userName == null || userName.isEmpty())
    {
      throw new SFException(ErrorCode.MISSING_USERNAME);
    }

    String password = (String) connectionPropertiesMap.get(
        SFSessionProperty.PASSWORD);

    if (isSnowflakeAuthenticator() && (password == null || password.isEmpty()))
    {
      throw new SFException(ErrorCode.MISSING_PASSWORD);
    }
  }

  protected HttpClient getHttpClient()
  {
    return httpClient;
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
        .setHttpClient(httpClient)
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
  protected String getSessionToken()
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
        .setHttpClient(httpClient)
        .setLoginTimeout(loginTimeout);

    try
    {
      SessionUtil.closeSession(loginInput);
      isClosed = true;
    }
    finally
    {
      if (httpClient != null)
      {
        httpClient = null;
      }
    }
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
            httpClient,
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

  protected boolean isClosed()
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

  protected boolean isUseProxy()
  {
    return useProxy;
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


}
