/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeConnectString;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.log.JDK14Logger;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.ClientAuthnDTO;
import net.snowflake.common.core.SqlState;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;

import java.security.PrivateKey;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static net.snowflake.client.core.QueryStatus.getStatusFromString;
import static net.snowflake.client.core.QueryStatus.isAnError;

/**
 * Snowflake session implementation
 */
public class SFSession
{
  private static final ObjectMapper OBJECT_MAPPER =
      ObjectMapperFactory.getObjectMapper();

  static final SFLogger logger = SFLoggerFactory.getLogger(SFSession.class);

  private static final String SF_PATH_SESSION_HEARTBEAT = "/session/heartbeat";

  private static final String SF_PATH_QUERY_MONITOR = "/monitoring/queries/";

  public static final String SF_QUERY_REQUEST_ID = "requestId";

  public static final String SF_HEADER_AUTHORIZATION = HttpHeaders.AUTHORIZATION;

  public static final String SF_HEADER_SNOWFLAKE_AUTHTYPE = "Snowflake";

  public static final String SF_HEADER_TOKEN_TAG = "Token";

  // temporarily have this variable to avoid hardcode.
  // Need to be removed when a better way to organize session parameter is introduced.
  private static final String CLIENT_STORE_TEMPORARY_CREDENTIAL = "CLIENT_STORE_TEMPORARY_CREDENTIAL";

  // increase heartbeat timeout from 60 sec to 300 sec
  // per https://support-snowflake.zendesk.com/agent/tickets/6629
  private static int SF_HEARTBEAT_TIMEOUT = 300;

  private boolean isClosed = true;

  private String sessionToken;
  private String masterToken;
  private long masterTokenValidityInSeconds;
  private String sessionId;

  private String idToken;
  private String privateKeyFileLocation;
  private String privateKeyPassword;
  private PrivateKey privateKey;

  // Injected delay for the purpose of connection timeout testing
  // Any statement execution will sleep for the specified number of milliseconds
  private AtomicInteger _injectedDelay = new AtomicInteger(0);

  private String databaseVersion = null;
  private int databaseMajorVersion = 0;
  private int databaseMinorVersion = 0;

  private AtomicInteger sequenceId = new AtomicInteger(0);

  private List<DriverPropertyInfo> missingProperties = new ArrayList<>();

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

  private final static ObjectMapper mapper =
      ObjectMapperFactory.getObjectMapper();

  private final Properties clientInfo = new Properties();

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
  private Map<String, Object> sessionParametersMap = new HashMap<>();

  final private static int MAX_SESSION_PARAMETERS = 1000;

  private boolean passcodeInPassword = false;

  private boolean sfSQLMode = false;

  private boolean enableHeartbeat = false;

  private int heartbeatFrequency = 3600;

  private AtomicBoolean autoCommit = new AtomicBoolean(true);

  private boolean resultColumnCaseInsensitive = false;

  private boolean preparedStatementLogging = false;

  // database that current session is on
  private String database;

  // schema that current session is on
  private String schema;

  // role that current session is on
  private String role;

  // warehouse on the current session
  private String warehouse;

  // For Metadata request(i.e. DatabaseMetadata.getTables or
  // DatabaseMetadata.getSchemas,), whether to use connection ctx to
  // improve the request time
  private boolean metadataRequestUseConnectionCtx = false;

  // For Metadata request(i.e. DatabaseMetadata.getTables or
  // DatabaseMetadata.getSchemas), whether to search using multiple schemas with
  // session database
  private boolean metadataRequestUseSessionDatabase = false;

  // If customer wants Timestamp_NTZ values to be stored in UTC time
  // instead of a local/session timezone, set to true
  private boolean treatNTZAsUTC = false;

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

  // store the temporary credential
  private boolean storeTemporaryCredential = false;

  // service name for multi clustering support
  private String serviceName;

  // whether to enable conservative memory usage mode
  private boolean enableConservativeMemoryUsage;

  // the step in MB to adjust memory usage
  private int conservativeMemoryAdjustStep = 64;

  // parameters used for conservative memory usage
  private int clientMemoryLimit;
  private int clientResultChunkSize;
  private int clientPrefetchThreads;

  // validate the default parameters by GS?
  private boolean validateDefaultParameters;

  // list of active asynchronous queries. Used to see if session should be closed when connection closes
  protected Set<String> activeAsyncQueries = ConcurrentHashMap.newKeySet();

  /**
   * Function that checks if the active session can be closed when the
   * connection is closed. If there are active asynchronous queries running,
   * the session should stay open even if the connection closes so that the
   * queries can finish running.
   *
   * @return true if it is safe to close this session, false if not
   */
  public boolean isSafeToClose()
  {
    boolean canClose = true;
    // if the set of asynchronous queries is empty, return true
    if (this.activeAsyncQueries.isEmpty())
    {
      return canClose;
    }
    // if the set is not empty, iterate through each query and check its status
    for (String query : this.activeAsyncQueries)
    {
      try
      {
        QueryStatus qStatus = getQueryStatus(query);
        //  if any query is still running, it is not safe to close.
        if (QueryStatus.isStillRunning(qStatus))
        {
          canClose = false;
        }
      }
      catch (SQLException e)
      {
        logger.error(e.getMessage());
      }
    }
    return canClose;
  }

  /**
   * @param queryID query ID of the query whose status is being investigated
   * @return enum of type QueryStatus indicating the query's status
   * @throws SQLException
   */
  public QueryStatus getQueryStatus(String queryID)
  throws SQLException
  {
    // create the URL to check the query monitoring endpoint
    String statusUrl = "";
    String sessionUrl = getUrl();
    if (sessionUrl.endsWith("/"))
    {
      statusUrl =
          sessionUrl.substring(0, sessionUrl.length() - 1) + SF_PATH_QUERY_MONITOR + queryID;
    }
    else
    {
      statusUrl = sessionUrl + SF_PATH_QUERY_MONITOR + queryID;
    }
    // Create a new HTTP GET object and set appropriate headers
    HttpGet get = new HttpGet(statusUrl);
    get.setHeader("Content-type", "application/json");
    get.setHeader("Authorization", "Snowflake Token=\"" + this.sessionToken + "\"");
    String response = null;
    JsonNode jsonNode = null;
    try
    {
      response = HttpUtil.executeGeneralRequest(get, loginTimeout, getOCSPMode());
      jsonNode = OBJECT_MAPPER.readTree(response);
    }
    catch (Exception e)
    {
      throw new SQLException("No response or invalid response from GET request. Error: {}", e.getMessage());
    }
    // Get response as JSON and parse it to get the query status
    // check the success field first
    if (!jsonNode.path("success").asBoolean())
    {
      logger.debug("response = {}", response);

      int errorCode = jsonNode.path("code").asInt();
      throw new SnowflakeSQLException(
          queryID, jsonNode.path("message").asText(),
          SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, errorCode);
    }
    JsonNode queryNode = jsonNode.path("data").path("queries");
    String queryStatus = "";
    String errorMessage = "";
    int errorCode = 0;
    if (queryNode.size() > 0)
    {
      queryStatus = queryNode.get(0).path("status").asText();
      errorMessage = queryNode.get(0).path("errorMessage").asText();
      errorCode = queryNode.get(0).path("errorCode").asInt();
    }
    logger.debug("Query status: {}", queryNode.asText());
    // Turn string with query response into QueryStatus enum and return it
    QueryStatus result = getStatusFromString(queryStatus);
    // if an error code has been provided, set appropriate error code
    if (errorCode != 0)
    {
      result.setErrorCode(errorCode);
    }
    // if no code was provided but query status indicates an error, set
    // code to be an internal error and set error message
    else if (isAnError(result))
    {
      result.setErrorCode(ErrorCode.INTERNAL_ERROR.getMessageCode());
      result.setErrorMessage("no_error_code_from_server");
    }
    // if an error message has been provided, set appropriate error message.
    // This should override the default error message displayed when there is
    // an error with no code.
    if (!Strings.isNullOrEmpty(errorMessage) && !errorMessage.equalsIgnoreCase("null"))
    {
      result.setErrorMessage(errorMessage);
    }
    return result;
  }


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
      else if (propertyValue != null && connectionProperty == SFSessionProperty.AUTHENTICATOR)
      {
        String[] authenticatorWithParams = propertyValue.toString().split(";");
        if (authenticatorWithParams.length == 1)
        {
          connectionPropertiesMap.put(connectionProperty, propertyValue);
        }
        else
        {
          String[] oktaUserKeyPair = authenticatorWithParams[1].split("=");
          if (oktaUserKeyPair.length == 2)
          {
            connectionPropertiesMap.put(connectionProperty, authenticatorWithParams[0]);
            connectionPropertiesMap.put(SFSessionProperty.OKTA_USERNAME, oktaUserKeyPair[1]);
          }
          else
          {
            throw new SFException(ErrorCode.INVALID_OKTA_USERNAME, propertyName);
          }
        }
      }
      else
      {
        connectionPropertiesMap.put(connectionProperty, propertyValue);
      }

      switch (connectionProperty)
      {
        case LOGIN_TIMEOUT:
          if (propertyValue != null)
          {
            loginTimeout = (Integer) propertyValue;
          }
          break;

        case NETWORK_TIMEOUT:
          if (propertyValue != null)
          {
            networkTimeoutInMilli = (Integer) propertyValue;
          }
          break;

        case INJECT_CLIENT_PAUSE:
          if (propertyValue != null)
          {
            injectClientPause = (Integer) propertyValue;
          }
          break;

        case INJECT_SOCKET_TIMEOUT:
          if (propertyValue != null)
          {
            injectSocketTimeout = (Integer) propertyValue;
          }
          break;

        case PASSCODE_IN_PASSWORD:
          passcodeInPassword = (propertyValue != null && (Boolean) propertyValue);
          break;

        case TRACING:
          if (propertyValue != null)
          {
            tracingLevel = Level.parse(((String) propertyValue).toUpperCase());
            if (tracingLevel != null && logger instanceof JDK14Logger)
            {
              JDK14Logger.honorTracingParameter(tracingLevel);
            }
          }
          break;

        case DISABLE_SOCKS_PROXY:
          // note: if any session has this parameter, it will be used for all
          // sessions on the current JVM.
          if (propertyValue != null)
          {
            HttpUtil.setSocksProxyDisabled((Boolean) propertyValue);
          }
          break;

        case VALIDATE_DEFAULT_PARAMETERS:
          if (propertyValue != null)
          {
            validateDefaultParameters = SFLoginInput.getBooleanValue(propertyValue);
          }
          break;

        case PRIVATE_KEY_FILE:
          if (propertyValue != null)
          {
            privateKeyFileLocation = (String) propertyValue;
          }
          break;

        case PRIVATE_KEY_FILE_PWD:
          if (propertyValue != null)
          {
            privateKeyPassword = (String) propertyValue;
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
      {
        throw new SFException(ErrorCode.DUPLICATE_CONNECTION_PROPERTY_SPECIFIED,
                              propertyName);
      }
      else
      {
        sessionParametersMap.put(propertyName, propertyValue);
      }

      // check if the number of session properties exceed limit
      if (sessionParametersMap.size() > MAX_SESSION_PARAMETERS)
      {
        throw new SFException(ErrorCode.TOO_MANY_SESSION_PARAMETERS,
                              MAX_SESSION_PARAMETERS);
      }
    }
  }

  public boolean containProperty(String key)
  {
    return sessionParametersMap.containsKey(key);
  }

  protected String getServerUrl()
  {
    if (connectionPropertiesMap.containsKey(SFSessionProperty.SERVER_URL))
    {
      return (String) connectionPropertiesMap.get(SFSessionProperty.SERVER_URL);
    }
    return null;
  }

  public boolean isStringQuoted()
  {
    if (connectionPropertiesMap.containsKey(SFSessionProperty.STRINGS_QUOTED))
    {
      return (Boolean) connectionPropertiesMap.get(SFSessionProperty.STRINGS_QUOTED);
    }
    return false;
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

    PrivateKey privateKey = (PrivateKey) connectionPropertiesMap.get(SFSessionProperty.PRIVATE_KEY);
    return (authenticator == null && privateKey == null && privateKeyFileLocation == null) ||
           ClientAuthnDTO.AuthenticatorType.SNOWFLAKE.name()
               .equalsIgnoreCase(authenticator);
  }

  /**
   * Returns true If authenticator is EXTERNALBROWSER.
   *
   * @return true if authenticator type is EXTERNALBROWSER
   */
  boolean isExternalbrowserAuthenticator()
  {
    String authenticator = (String) connectionPropertiesMap.get(
        SFSessionProperty.AUTHENTICATOR);
    return ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER.name()
        .equalsIgnoreCase(authenticator);
  }

  /**
   * Returns true if authenticator is OKTA native
   *
   * @return true or false
   */
  boolean isOKTAAuthenticator()
  {
    String authenticator = (String) connectionPropertiesMap.get(SFSessionProperty.AUTHENTICATOR);
    return !Strings.isNullOrEmpty(authenticator) && authenticator.startsWith("https://");
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

    HttpUtil.configureCustomProxyProperties(connectionPropertiesMap);

    logger.debug(
        "input: server={}, account={}, user={}, password={}, role={}, " +
        "database={}, schema={}, warehouse={}, validate_default_parameters={}, authenticator={}, ocsp_mode={}, " +
        "passcode_in_password={}, passcode={}, private_key={}, " +
        "use_proxy={}, proxy_host={}, proxy_port={}, proxy_user={}, proxy_password={}, disable_socks_proxy={}, " +
        "application={}, app_id={}, app_version={}, " +
        "login_timeout={}, network_timeout={}, query_timeout={}, tracing={}, private_key_file={}, private_key_file_pwd={}. " +
        "session_parameters: client_store_temporary_credential={}",
        connectionPropertiesMap.get(SFSessionProperty.SERVER_URL),
        connectionPropertiesMap.get(SFSessionProperty.ACCOUNT),
        connectionPropertiesMap.get(SFSessionProperty.USER),
        !Strings.isNullOrEmpty((String) connectionPropertiesMap.get(SFSessionProperty.PASSWORD)) ? "***" : "(empty)",
        connectionPropertiesMap.get(SFSessionProperty.ROLE),
        connectionPropertiesMap.get(SFSessionProperty.DATABASE),
        connectionPropertiesMap.get(SFSessionProperty.SCHEMA),
        connectionPropertiesMap.get(SFSessionProperty.WAREHOUSE),
        connectionPropertiesMap.get(SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS),
        connectionPropertiesMap.get(SFSessionProperty.AUTHENTICATOR),
        getOCSPMode().name(),
        connectionPropertiesMap.get(SFSessionProperty.PASSCODE_IN_PASSWORD),
        !Strings.isNullOrEmpty((String) connectionPropertiesMap.get(SFSessionProperty.PASSCODE)) ? "***" : "(empty)",

        connectionPropertiesMap.get(SFSessionProperty.PRIVATE_KEY) != null ? "(not null)" : "(null)",

        connectionPropertiesMap.get(SFSessionProperty.USE_PROXY),
        connectionPropertiesMap.get(SFSessionProperty.PROXY_HOST),
        connectionPropertiesMap.get(SFSessionProperty.PROXY_PORT),
        connectionPropertiesMap.get(SFSessionProperty.PROXY_USER),
        !Strings.isNullOrEmpty((String) connectionPropertiesMap.get(SFSessionProperty.PROXY_PASSWORD)) ? "***" :
        "(empty)",
        connectionPropertiesMap.get(SFSessionProperty.DISABLE_SOCKS_PROXY),

        connectionPropertiesMap.get(SFSessionProperty.APPLICATION),
        connectionPropertiesMap.get(SFSessionProperty.APP_ID),
        connectionPropertiesMap.get(SFSessionProperty.APP_VERSION),

        connectionPropertiesMap.get(SFSessionProperty.LOGIN_TIMEOUT),
        connectionPropertiesMap.get(SFSessionProperty.NETWORK_TIMEOUT),
        connectionPropertiesMap.get(SFSessionProperty.QUERY_TIMEOUT),
        connectionPropertiesMap.get(SFSessionProperty.TRACING),
        connectionPropertiesMap.get(SFSessionProperty.PRIVATE_KEY_FILE),
        !Strings.isNullOrEmpty((String) connectionPropertiesMap.get(SFSessionProperty.PRIVATE_KEY_FILE_PWD)) ? "***" :
        "(empty)",
        sessionParametersMap.get(CLIENT_STORE_TEMPORARY_CREDENTIAL)
    );
    // TODO: temporarily hardcode sessionParameter debug info. will be changed in the future
    SFLoginInput loginInput = new SFLoginInput();

    loginInput.setServerUrl(
        (String) connectionPropertiesMap.get(SFSessionProperty.SERVER_URL))
        .setDatabaseName(
            (String) connectionPropertiesMap.get(SFSessionProperty.DATABASE))
        .setSchemaName(
            (String) connectionPropertiesMap.get(SFSessionProperty.SCHEMA))
        .setWarehouse(
            (String) connectionPropertiesMap.get(SFSessionProperty.WAREHOUSE))
        .setRole((String) connectionPropertiesMap.get(SFSessionProperty.ROLE))
        .setValidateDefaultParameters(
            connectionPropertiesMap.get(SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS))
        .setAuthenticator(
            (String) connectionPropertiesMap.get(SFSessionProperty.AUTHENTICATOR))
        .setOKTAUserName(
            (String) connectionPropertiesMap.get(SFSessionProperty.OKTA_USERNAME))
        .setAccountName(
            (String) connectionPropertiesMap.get(SFSessionProperty.ACCOUNT))
        .setLoginTimeout(loginTimeout)
        .setUserName(
            (String) connectionPropertiesMap.get(SFSessionProperty.USER))
        .setPassword(
            (String) connectionPropertiesMap.get(SFSessionProperty.PASSWORD))
        .setToken(
            (String) connectionPropertiesMap.get(SFSessionProperty.TOKEN))
        .setIdToken(
            (String) connectionPropertiesMap.get(SFSessionProperty.ID_TOKEN))
        .setPasscodeInPassword(passcodeInPassword)
        .setPasscode(
            (String) connectionPropertiesMap.get(SFSessionProperty.PASSCODE))
        .setConnectionTimeout(httpClientConnectionTimeout)
        .setSocketTimeout(httpClientSocketTimeout)
        .setAppId((String) connectionPropertiesMap.get(SFSessionProperty.APP_ID))
        .setAppVersion(
            (String) connectionPropertiesMap.get(SFSessionProperty.APP_VERSION))
        .setSessionParameters(sessionParametersMap)
        .setPrivateKey((PrivateKey) connectionPropertiesMap.get(SFSessionProperty.PRIVATE_KEY))
        .setPrivateKeyFile((String) connectionPropertiesMap.get(SFSessionProperty.PRIVATE_KEY_FILE))
        .setPrivateKeyFilePwd((String) connectionPropertiesMap.get(SFSessionProperty.PRIVATE_KEY_FILE_PWD))
        .setApplication((String) connectionPropertiesMap.get(
            SFSessionProperty.APPLICATION))
        .setServiceName(this.getServiceName())
        .setOCSPMode(getOCSPMode());


    // propagate OCSP mode to SFTrustManager. Note OCSP setting is global on JVM.
    HttpUtil.initHttpClient(loginInput.getOCSPMode(), null);
    SFLoginOutput loginOutput = SessionUtil.openSession(loginInput, connectionPropertiesMap, tracingLevel.toString());
    isClosed = false;

    sessionToken = loginOutput.getSessionToken();
    masterToken = loginOutput.getMasterToken();
    idToken = loginOutput.getIdToken();
    databaseVersion = loginOutput.getDatabaseVersion();
    databaseMajorVersion = loginOutput.getDatabaseMajorVersion();
    databaseMinorVersion = loginOutput.getDatabaseMinorVersion();
    httpClientSocketTimeout = loginOutput.getHttpClientSocketTimeout();
    masterTokenValidityInSeconds = loginOutput.getMasterTokenValidityInSeconds();
    database = loginOutput.getSessionDatabase();
    schema = loginOutput.getSessionSchema();
    role = loginOutput.getSessionRole();
    warehouse = loginOutput.getSessionWarehouse();
    sessionId = loginOutput.getSessionId();
    autoCommit.set(loginOutput.getAutoCommit());

    // Update common parameter values for this session
    SessionUtil.updateSfDriverParamValues(loginOutput.getCommonParams(), this);

    String loginDatabaseName = (String) connectionPropertiesMap.get(
        SFSessionProperty.DATABASE);
    String loginSchemaName = (String) connectionPropertiesMap.get(
        SFSessionProperty.SCHEMA);
    String loginRole = (String) connectionPropertiesMap.get(
        SFSessionProperty.ROLE);
    String loginWarehouse = (String) connectionPropertiesMap.get(
        SFSessionProperty.WAREHOUSE);

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

    if (loginWarehouse != null && !loginWarehouse
        .equalsIgnoreCase(warehouse))
    {
      sqlWarnings.add(new SFException(ErrorCode.
                                          CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP,
                                      "Warehouse", loginWarehouse, warehouse));
    }

    // start heartbeat for this session so that the master token will not expire
    startHeartbeatForThisSession();
  }

  public OCSPMode getOCSPMode()
  {
    OCSPMode ret;

    Boolean insecureMode = (Boolean) connectionPropertiesMap.get(
        SFSessionProperty.INSECURE_MODE);
    if (insecureMode != null && insecureMode)
    {
      // skip OCSP checks
      ret = OCSPMode.INSECURE;
    }
    else if (!connectionPropertiesMap.containsKey(SFSessionProperty.OCSP_FAIL_OPEN) ||
             (boolean) connectionPropertiesMap.get(SFSessionProperty.OCSP_FAIL_OPEN))
    {
      // fail open (by default, not set)
      ret = OCSPMode.FAIL_OPEN;
    }
    else
    {
      // explicitly set ocspFailOpen=false
      ret = OCSPMode.FAIL_CLOSED;
    }
    return ret;
  }

  /**
   * Performs a sanity check on properties. Sanity checking includes:
   * - verifying that a server url is present
   * - verifying various combinations of properties given the authenticator
   *
   * @throws SFException Will be thrown if any of the necessary properties
   *                     are missing
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

    if (isSnowflakeAuthenticator() || isOKTAAuthenticator())
    {
      // userName and password are expected for both Snowflake and Okta.
      String userName = (String) connectionPropertiesMap.get(SFSessionProperty.USER);
      if (Strings.isNullOrEmpty(userName))
      {
        throw new SFException(ErrorCode.MISSING_USERNAME);
      }

      String password = (String) connectionPropertiesMap.get(SFSessionProperty.PASSWORD);
      if (Strings.isNullOrEmpty(password))

      {
        throw new SFException(ErrorCode.MISSING_PASSWORD);
      }
    }

    // perform sanity check on proxy settings
    boolean useProxy = (boolean) connectionPropertiesMap.getOrDefault(
        SFSessionProperty.USE_PROXY, false);
    if (useProxy)
    {
      if (!connectionPropertiesMap.containsKey(SFSessionProperty.PROXY_HOST) ||
          connectionPropertiesMap.get(SFSessionProperty.PROXY_HOST) == null ||
          ((String) connectionPropertiesMap.get(SFSessionProperty.PROXY_HOST)).isEmpty() ||
          !connectionPropertiesMap.containsKey(SFSessionProperty.PROXY_PORT) ||
          connectionPropertiesMap.get(SFSessionProperty.PROXY_HOST) == null)
      {
        throw new SFException(ErrorCode.INVALID_PROXY_PROPERTIES, "Both proxy host and port values are needed.");
      }
    }
  }

  private DriverPropertyInfo addNewDriverProperty(String name, String description)
  {
    DriverPropertyInfo info = new DriverPropertyInfo(name, null);
    info.description = description;
    return info;
  }

  public List<DriverPropertyInfo> checkProperties()
  {
    for (SFSessionProperty property : SFSessionProperty.values())
    {
      if (property.isRequired() &&
          !connectionPropertiesMap.containsKey(property))
      {
        missingProperties.add(addNewDriverProperty(property.getPropertyKey(), null));
      }
    }
    if (isSnowflakeAuthenticator() || isOKTAAuthenticator())
    {
      // userName and password are expected for both Snowflake and Okta.
      String userName = (String) connectionPropertiesMap.get(SFSessionProperty.USER);
      if (Strings.isNullOrEmpty(userName))
      {
        missingProperties.add(addNewDriverProperty(SFSessionProperty.USER.getPropertyKey(), "username for account"));
      }

      String password = (String) connectionPropertiesMap.get(SFSessionProperty.PASSWORD);
      if (Strings.isNullOrEmpty(password))
      {
        missingProperties.add(addNewDriverProperty(SFSessionProperty.PASSWORD.getPropertyKey(), "password for " +
                                                                                                "account"));
      }
    }

    boolean useProxy = (boolean) connectionPropertiesMap.getOrDefault(
        SFSessionProperty.USE_PROXY, false);
    if (useProxy)
    {
      if (!connectionPropertiesMap.containsKey(SFSessionProperty.PROXY_HOST))
      {
        missingProperties.add(addNewDriverProperty(SFSessionProperty.PROXY_HOST.getPropertyKey(), "proxy host name"));
      }
      if (!connectionPropertiesMap.containsKey(SFSessionProperty.PROXY_PORT))
      {
        missingProperties.add(addNewDriverProperty(SFSessionProperty.PROXY_PORT.getPropertyKey(), "proxy port; " +
                                                                                                  "should be an integer"));
      }
    }
    return missingProperties;
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

  public String getSessionId()
  {
    return sessionId;
  }

  /**
   * A helper function to call global service and renew session.
   *
   * @param prevSessionToken the session token that has expired
   * @throws SnowflakeSQLException if failed to renew the session
   * @throws SFException           if failed to renew the session
   */
  synchronized void renewSession(String prevSessionToken)
  throws SFException, SnowflakeSQLException
  {
    if (sessionToken != null &&
        !sessionToken.equals(prevSessionToken))
    {
      logger.debug("not renew session because session token has not been updated.");
      return;
    }

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setServerUrl(
        (String) connectionPropertiesMap.get(SFSessionProperty.SERVER_URL))
        .setSessionToken(sessionToken)
        .setMasterToken(masterToken)
        .setIdToken(idToken)
        .setLoginTimeout(loginTimeout)
        .setDatabaseName(this.getDatabase())
        .setSchemaName(this.getSchema())
        .setRole(this.getRole())
        .setWarehouse(this.getWarehouse())
        .setOCSPMode(getOCSPMode());

    SFLoginOutput loginOutput = SessionUtil.renewSession(loginInput);

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
    logger.debug(" public void close()");

    // stop heartbeat for this session
    stopHeartbeatForThisSession();

    if (isClosed)
    {
      return;
    }

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setServerUrl(
        (String) connectionPropertiesMap.get(SFSessionProperty.SERVER_URL))
        .setSessionToken(sessionToken)
        .setLoginTimeout(loginTimeout)
        .setOCSPMode(getOCSPMode());

    SessionUtil.closeSession(loginInput);
    closeTelemetryClient();
    clientInfo.clear();
    isClosed = true;
  }

  /**
   * Start heartbeat for this session
   */
  protected void startHeartbeatForThisSession()
  {
    if (enableHeartbeat && !Strings.isNullOrEmpty(masterToken))
    {
      logger.debug("start heartbeat, master token validity: " +
                   masterTokenValidityInSeconds);

      HeartbeatBackground.getInstance().addSession(this,
                                                   masterTokenValidityInSeconds, this.heartbeatFrequency);
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
    if (enableHeartbeat && !Strings.isNullOrEmpty(masterToken))
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
        String theResponse = HttpUtil.executeGeneralRequest(
            postRequest,
            SF_HEARTBEAT_TIMEOUT,
            getOCSPMode());

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
        {
          throw (SnowflakeSQLException) ex;
        }

        logger.error("unexpected exception", ex);

        throw (SFException) IncidentUtil.generateIncidentV2WithException(
            this,
            new SFException(ErrorCode.INTERNAL_ERROR,
                            IncidentUtil.oneLiner("unexpected exception", ex)),
            null,
            requestId);
      }
    }
    while (retry);
  }

  public Properties getClientInfo()
  {
    logger.debug(" public Properties getClientInfo()");

    // defensive copy to avoid client from changing the properties
    // directly w/o going through the API
    Properties copy = new Properties();
    copy.putAll(this.clientInfo);
    return copy;
  }

  public String getClientInfo(String name)
  {
    logger.debug(" public String getClientInfo(String name)");
    return this.clientInfo.getProperty(name);
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

  public int getNetworkTimeoutInMilli()
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

  public void setSfSQLMode(boolean sfSQLMode)
  {
    this.sfSQLMode = sfSQLMode;
  }

  public boolean isSfSQLMode()
  {
    return this.sfSQLMode;
  }

  public boolean isEnableHeartbeat()
  {
    return enableHeartbeat;
  }

  public void setEnableHeartbeat(boolean enableHeartbeat)
  {
    this.enableHeartbeat = enableHeartbeat;
  }

  public void setHeartbeatFrequency(int frequency)
  {
    this.heartbeatFrequency = frequency;
  }

  public long getHeartbeatFrequency()
  {
    return this.heartbeatFrequency;
  }

  public boolean getAutoCommit()
  {
    return autoCommit.get();
  }

  public void setAutoCommit(boolean autoCommit)
  {
    this.autoCommit.set(autoCommit);
  }

  public boolean getPreparedStatementLogging()
  {
    return this.preparedStatementLogging;
  }

  public void setPreparedStatementLogging(boolean value)
  {
    this.preparedStatementLogging = value;
  }

  public void setResultColumnCaseInsensitive(boolean resultColumnCaseInsensitive)
  {
    this.resultColumnCaseInsensitive = resultColumnCaseInsensitive;
  }

  public boolean isResultColumnCaseInsensitive()
  {
    return this.resultColumnCaseInsensitive;
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

  public String getWarehouse()
  {
    return warehouse;
  }

  public void setWarehouse(String warehouse)
  {
    this.warehouse = warehouse;
  }

  public void setMetadataRequestUseConnectionCtx(boolean enabled)
  {
    this.metadataRequestUseConnectionCtx = enabled;
  }

  public void setMetadataRequestUseSessionDatabase(boolean enabled)
  {
    this.metadataRequestUseSessionDatabase = enabled;
  }

  public boolean getMetadataRequestUseConnectionCtx()
  {
    return this.metadataRequestUseConnectionCtx;
  }

  public void setTreatNTZAsUTC(boolean enabled)
  {
    this.treatNTZAsUTC = enabled;
  }

  public boolean getTreatNTZAsUTC()
  {
    return this.treatNTZAsUTC;
  }

  public boolean getMetadataRequestUseSessionDatabase()
  {
    return this.metadataRequestUseSessionDatabase;
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
    return (Integer) this.connectionPropertiesMap.get(SFSessionProperty.QUERY_TIMEOUT);
  }

  public String getUser()
  {
    return (String) this.connectionPropertiesMap.get(SFSessionProperty.USER);
  }

  public String getUrl()
  {
    return (String) this.connectionPropertiesMap.get(SFSessionProperty.SERVER_URL);
  }

  public int getInjectWaitInPut()
  {
    Object retVal = this.connectionPropertiesMap.get(SFSessionProperty.INJECT_WAIT_IN_PUT);
    if (retVal != null)
    {
      try
      {
        return (int) retVal;
      }
      catch (Exception e)
      {
        return 0;
      }
    }
    return 0;
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
      telemetryClient = TelemetryClient.createTelemetry(this);
    }
    return telemetryClient;
  }

  public void closeTelemetryClient()
  {
    if (telemetryClient != null)
    {
      telemetryClient.close();
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

  public String getIdToken()
  {
    return idToken;
  }

  public boolean isStoreTemporaryCredential()
  {
    return this.storeTemporaryCredential;
  }

  public void setStoreTemporaryCredential(boolean storeTemporaryCredential)
  {
    this.storeTemporaryCredential = storeTemporaryCredential;
  }

  /**
   * Sets the service name provided from GS.
   *
   * @param serviceName service name
   */
  public void setServiceName(String serviceName)
  {
    this.serviceName = serviceName;
  }

  /**
   * Gets the service name provided from GS.
   *
   * @return the service name
   */
  public String getServiceName()
  {
    return serviceName;
  }

  /**
   * Sets the current objects if the session is not up to date. It can happen
   * if the session is created by the id token, which doesn't carry the current
   * objects.
   *
   * @param loginInput  The login input to use for this session
   * @param loginOutput The login output to ose for this session
   */
  void setCurrentObjects(
      SFLoginInput loginInput, SFLoginOutput loginOutput)
  {
    this.sessionToken = loginOutput.getSessionToken(); // used to run the commands.
    runInternalCommand(
        "USE ROLE IDENTIFIER(?)", loginInput.getRole());
    runInternalCommand(
        "USE WAREHOUSE IDENTIFIER(?)", loginInput.getWarehouse());
    runInternalCommand(
        "USE DATABASE IDENTIFIER(?)", loginInput.getDatabaseName());
    runInternalCommand(
        "USE SCHEMA IDENTIFIER(?)", loginInput.getSchemaName());
    // This ensures the session returns the current objects and refresh
    // the local cache.
    SFBaseResultSet result = runInternalCommand("SELECT ?", "1");

    // refresh the current objects
    loginOutput.setSessionDatabase(this.database);
    loginOutput.setSessionSchema(this.schema);
    loginOutput.setSessionWarehouse(this.warehouse);
    loginOutput.setSessionRole(this.role);
    loginOutput.setIdToken(loginInput.getIdToken());

    // no common parameter is updated.
    if (result != null)
    {
      loginOutput.setCommonParams(result.parameters);
    }
  }

  private SFBaseResultSet runInternalCommand(String sql, String value)
  {
    if (value == null)
    {
      return null;
    }

    try
    {
      Map<String, ParameterBindingDTO> bindValues = new HashMap<>();
      bindValues.put("1", new ParameterBindingDTO("TEXT", value));
      SFStatement statement = new SFStatement(this);
      return statement.executeQueryInternal(
          sql,
          bindValues,
          false, // not describe only
          true, // internal
          false, // asyncExec
          null // caller isn't a JDBC interface method
      );
    }
    catch (SFException | SQLException ex)
    {
      logger.debug("Failed to run a command: {}, err={}", sql, ex);
    }
    return null;
  }


  public boolean isConservativeMemoryUsageEnabled()
  {
    return enableConservativeMemoryUsage;
  }

  public void setEnableConservativeMemoryUsage(boolean value)
  {
    enableConservativeMemoryUsage = value;
  }

  public void setConservativeMemoryAdjustStep(int step)
  {
    conservativeMemoryAdjustStep = step;
  }

  public int getConservativeMemoryAdjustStep()
  {
    return conservativeMemoryAdjustStep;
  }

  public void setClientMemoryLimit(int clientMemoryLimit)
  {
    this.clientMemoryLimit = clientMemoryLimit;
  }

  public int getClientMemoryLimit()
  {
    return clientMemoryLimit;
  }

  public void setClientResultChunkSize(int clientResultChunkSize)
  {
    this.clientResultChunkSize = clientResultChunkSize;
  }

  public int getClientResultChunkSize()
  {
    return clientResultChunkSize;
  }

  public void setClientPrefetchThreads(int clientPrefetchThreads)
  {
    this.clientPrefetchThreads = clientPrefetchThreads;
  }

  public int getClientPrefetchThreads()
  {
    return clientPrefetchThreads;
  }

  public boolean isValidateDefaultParameters()
  {
    return validateDefaultParameters;
  }

  public void setValidateDefaultParameters(boolean v)
  {
    validateDefaultParameters = v;
  }

  private SnowflakeConnectString sfConnStr;

  public void setSnowflakeConnectionString(SnowflakeConnectString connStr)
  {
    sfConnStr = connStr;
  }

  public SnowflakeConnectString getSnowflakeConnectionString()
  {
    return sfConnStr;
  }
}
