/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import net.snowflake.client.jdbc.*;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static net.snowflake.client.core.QueryStatus.getStatusFromString;
import static net.snowflake.client.core.QueryStatus.isAnError;

/** Snowflake session implementation */
public class SFSession implements SessionHandler {
  public static final String SF_QUERY_REQUEST_ID = "requestId";
  public static final String SF_HEADER_AUTHORIZATION = HttpHeaders.AUTHORIZATION;
  public static final String SF_HEADER_SNOWFLAKE_AUTHTYPE = "Snowflake";
  public static final String SF_HEADER_TOKEN_TAG = "Token";
  static final SFLogger logger = SFLoggerFactory.getLogger(SFSession.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();
  private static final String SF_PATH_SESSION_HEARTBEAT = "/session/heartbeat";
  private static final String SF_PATH_QUERY_MONITOR = "/monitoring/queries/";
  // temporarily have this variable to avoid hardcode.
  // Need to be removed when a better way to organize session parameter is introduced.
  private static final String CLIENT_STORE_TEMPORARY_CREDENTIAL =
      "CLIENT_STORE_TEMPORARY_CREDENTIAL";
  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
  private static final int MAX_SESSION_PARAMETERS = 1000;
  // increase heartbeat timeout from 60 sec to 300 sec
  // per https://support-snowflake.zendesk.com/agent/tickets/6629
  private static int SF_HEARTBEAT_TIMEOUT = 300;
  private static int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300000; // millisec
  private final Properties clientInfo = new Properties();
  // list of active asynchronous queries. Used to see if session should be closed when connection
  // closes
  protected Set<String> activeAsyncQueries = ConcurrentHashMap.newKeySet();
  private boolean isClosed = true;
  private String sessionToken;
  private String masterToken;
  private long masterTokenValidityInSeconds;
  private String sessionId;
  private String idToken;
  private String mfaToken;
  private String privateKeyFileLocation;
  private String privateKeyPassword;
  private PrivateKey privateKey;
  private String databaseVersion = null;
  private int databaseMajorVersion = 0;
  private int databaseMinorVersion = 0;
  private AtomicInteger sequenceId = new AtomicInteger(0);
  private List<DriverPropertyInfo> missingProperties = new ArrayList<>();
  /**
   * Amount of seconds a user is willing to tolerate for establishing the connection with database.
   * In our case, it means the first login request to get authorization token.
   *
   * <p>Default:60 seconds
   */
  private int loginTimeout = 60;
  /**
   * Amount of milliseconds a user is willing to tolerate for network related issues (e.g. HTTP
   * 503/504) or database transient issues (e.g. GS not responding)
   *
   * <p>A value of 0 means no timeout
   *
   * <p>Default: 0
   */
  private int networkTimeoutInMilli = 0; // in milliseconds
  private boolean enableCombineDescribe = false;
  private Map<String, Object> sfSessionProperties = new HashMap<>(1);

  // --- Simulated failures for testing
  private int httpClientConnectionTimeout = 60000; // milliseconds
  private int httpClientSocketTimeout = DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT; // milliseconds
  // whether we try to simulate a socket timeout (a default value of 0 means
  // no simulation). The value is in milliseconds
  private int injectSocketTimeout = 0;
  // simulate client pause after initial execute and before first get-result
  // call ( a default value of 0 means no pause). The value is in seconds
  private int injectClientPause = 0;
  private SessionProperties sessionProperties = new SessionProperties();
  // session parameters
  private Map<String, Object> sessionParametersMap = new HashMap<>();
  private boolean passcodeInPassword = false;
  private boolean sfSQLMode = false;
  private boolean enableHeartbeat = false;
  private int heartbeatFrequency = 3600;
  private AtomicBoolean autoCommit = new AtomicBoolean(true);
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
  // parameter to guard against behavior change to getDate() with Calendar timezone
  private boolean formatDateWithTimezone = false;
  // parameter to guard against behavior change to getTime()
  private boolean useSessionTimezone = false;
  private SnowflakeType timestampMappedType = SnowflakeType.TIMESTAMP_LTZ;
  private boolean jdbcTreatDecimalAsInt = true;
  // deprecated
  private Level tracingLevel = Level.INFO;
  private List<SFException> sqlWarnings = new ArrayList<>();
  // client to log session metrics to telemetry in GS
  private Telemetry telemetryClient;
  // default value is false will be updated when login
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
  private SnowflakeConnectString sfConnStr;

  /**
   * Function that checks if the active session can be closed when the connection is closed. If
   * there are active asynchronous queries running, the session should stay open even if the
   * connection closes so that the queries can finish running.
   *
   * @return true if it is safe to close this session, false if not
   */
  public boolean isSafeToClose() {
    boolean canClose = true;
    // if the set of asynchronous queries is empty, return true
    if (this.activeAsyncQueries.isEmpty()) {
      return canClose;
    }
    // if the set is not empty, iterate through each query and check its status
    for (String query : this.activeAsyncQueries) {
      try {
        QueryStatus qStatus = getQueryStatus(query);
        //  if any query is still running, it is not safe to close.
        if (QueryStatus.isStillRunning(qStatus)) {
          canClose = false;
        }
      } catch (SQLException e) {
        logger.error(e.getMessage());
      }
    }
    return canClose;
  }

  @Override
  public SessionProperties sessionProperties() {
    return this.sessionProperties;
  }

  /**
   * @param queryID query ID of the query whose status is being investigated
   * @return enum of type QueryStatus indicating the query's status
   * @throws SQLException
   */
  public QueryStatus getQueryStatus(String queryID) throws SQLException {
    // create the URL to check the query monitoring endpoint
    String statusUrl = "";
    String sessionUrl = getUrl();
    if (sessionUrl.endsWith("/")) {
      statusUrl =
          sessionUrl.substring(0, sessionUrl.length() - 1) + SF_PATH_QUERY_MONITOR + queryID;
    } else {
      statusUrl = sessionUrl + SF_PATH_QUERY_MONITOR + queryID;
    }
    // Create a new HTTP GET object and set appropriate headers
    HttpGet get = new HttpGet(statusUrl);
    get.setHeader("Content-type", "application/json");
    get.setHeader("Authorization", "Snowflake Token=\"" + this.sessionToken + "\"");
    String response = null;
    JsonNode jsonNode = null;
    try {
      response = HttpUtil.executeGeneralRequest(get, loginTimeout, sessionProperties.getOCSPMode());
      jsonNode = OBJECT_MAPPER.readTree(response);
    } catch (Exception e) {
      throw new SnowflakeSQLLoggedException(
          this, e.getMessage(), "No response or invalid response from GET request. Error: {}");
    }
    // Get response as JSON and parse it to get the query status
    // check the success field first
    if (!jsonNode.path("success").asBoolean()) {
      logger.debug("response = {}", response);

      int errorCode = jsonNode.path("code").asInt();
      throw new SnowflakeSQLException(
          queryID,
          jsonNode.path("message").asText(),
          SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
          errorCode);
    }
    JsonNode queryNode = jsonNode.path("data").path("queries");
    String queryStatus = "";
    String errorMessage = "";
    int errorCode = 0;
    if (queryNode.size() > 0) {
      queryStatus = queryNode.get(0).path("status").asText();
      errorMessage = queryNode.get(0).path("errorMessage").asText();
      errorCode = queryNode.get(0).path("errorCode").asInt();
    }
    logger.debug("Query status: {}", queryNode.asText());
    // Turn string with query response into QueryStatus enum and return it
    QueryStatus result = getStatusFromString(queryStatus);
    // if an error code has been provided, set appropriate error code
    if (errorCode != 0) {
      result.setErrorCode(errorCode);
    }
    // if no code was provided but query status indicates an error, set
    // code to be an internal error and set error message
    else if (isAnError(result)) {
      result.setErrorCode(ErrorCode.INTERNAL_ERROR.getMessageCode());
      result.setErrorMessage("no_error_code_from_server");
    }
    // if an error message has been provided, set appropriate error message.
    // This should override the default error message displayed when there is
    // an error with no code.
    if (!Strings.isNullOrEmpty(errorMessage) && !errorMessage.equalsIgnoreCase("null")) {
      result.setErrorMessage(errorMessage);
    }
    return result;
  }

  /**
   * Add a property If a property is known for connection, add it to connection properties If not,
   * add it as a dynamic session parameters
   *
   * <p>Make sure a property is not added more than once and the number of properties does not
   * exceed limit.
   *
   * @param propertyName property name
   * @param propertyValue property value
   * @throws SFException exception raised from Snowflake components
   */
  public void addProperty(String propertyName, Object propertyValue) throws SFException {
    SFConnectionProperty connectionProperty = SFConnectionProperty.lookupByKey(propertyName);

    if (connectionProperty != null) {
      sessionProperties.addProperty(propertyName, propertyValue);
      // check if the value type is as expected
      propertyValue = SFConnectionProperty.checkPropertyValue(connectionProperty, propertyValue);

      switch (connectionProperty) {
        case LOGIN_TIMEOUT:
          if (propertyValue != null) {
            loginTimeout = (Integer) propertyValue;
          }
          break;

        case NETWORK_TIMEOUT:
          if (propertyValue != null) {
            networkTimeoutInMilli = (Integer) propertyValue;
          }
          break;

        case INJECT_CLIENT_PAUSE:
          if (propertyValue != null) {
            injectClientPause = (Integer) propertyValue;
          }
          break;

        case INJECT_SOCKET_TIMEOUT:
          if (propertyValue != null) {
            injectSocketTimeout = (Integer) propertyValue;
          }
          break;

        case PASSCODE_IN_PASSWORD:
          passcodeInPassword = (propertyValue != null && (Boolean) propertyValue);
          break;

        case TRACING:
          if (propertyValue != null) {
            tracingLevel = Level.parse(((String) propertyValue).toUpperCase());
            if (tracingLevel != null && logger instanceof JDK14Logger) {
              JDK14Logger.honorTracingParameter(tracingLevel);
            }
          }
          break;

        case DISABLE_SOCKS_PROXY:
          // note: if any session has this parameter, it will be used for all
          // sessions on the current JVM.
          if (propertyValue != null) {
            HttpUtil.setSocksProxyDisabled((Boolean) propertyValue);
          }
          break;

        case VALIDATE_DEFAULT_PARAMETERS:
          if (propertyValue != null) {
            validateDefaultParameters = SFLoginInput.getBooleanValue(propertyValue);
          }
          break;

        case PRIVATE_KEY_FILE:
          if (propertyValue != null) {
            privateKeyFileLocation = (String) propertyValue;
          }
          break;

        case PRIVATE_KEY_FILE_PWD:
          if (propertyValue != null) {
            privateKeyPassword = (String) propertyValue;
          }
          break;

        default:
          break;
      }
    } else {
      // this property does not match any predefined property, treat it as
      // session parameter
      if (sessionParametersMap.containsKey(propertyName)) {
        throw new SFException(ErrorCode.DUPLICATE_CONNECTION_PROPERTY_SPECIFIED, propertyName);
      } else {
        sessionParametersMap.put(propertyName, propertyValue);
      }

      // check if the number of session properties exceed limit
      if (sessionParametersMap.size() > MAX_SESSION_PARAMETERS) {
        throw new SFException(ErrorCode.TOO_MANY_SESSION_PARAMETERS, MAX_SESSION_PARAMETERS);
      }
    }
  }

  public boolean containProperty(String key) {
    return sessionParametersMap.containsKey(key);
  }

  /**
   * Open a new database session
   *
   * @throws SFException this is a runtime exception
   * @throws SnowflakeSQLException exception raised from Snowflake components
   */
  public synchronized void open() throws SFException, SnowflakeSQLException {
    performSanityCheckOnProperties();
    Map<SFConnectionProperty, Object> connectionPropertiesMap =
        sessionProperties.getConnectionPropertiesMap();

    HttpUtil.configureCustomProxyProperties(connectionPropertiesMap);

    logger.debug(
        "input: server={}, account={}, user={}, password={}, role={}, "
            + "database={}, schema={}, warehouse={}, validate_default_parameters={}, authenticator={}, ocsp_mode={}, "
            + "passcode_in_password={}, passcode={}, private_key={}, "
            + "use_proxy={}, proxy_host={}, proxy_port={}, proxy_user={}, proxy_password={}, disable_socks_proxy={}, "
            + "application={}, app_id={}, app_version={}, "
            + "login_timeout={}, network_timeout={}, query_timeout={}, tracing={}, private_key_file={}, private_key_file_pwd={}. "
            + "session_parameters: client_store_temporary_credential={}",
        connectionPropertiesMap.get(SFConnectionProperty.SERVER_URL),
        connectionPropertiesMap.get(SFConnectionProperty.ACCOUNT),
        connectionPropertiesMap.get(SFConnectionProperty.USER),
        !Strings.isNullOrEmpty((String) connectionPropertiesMap.get(SFConnectionProperty.PASSWORD))
            ? "***"
            : "(empty)",
        connectionPropertiesMap.get(SFConnectionProperty.ROLE),
        connectionPropertiesMap.get(SFConnectionProperty.DATABASE),
        connectionPropertiesMap.get(SFConnectionProperty.SCHEMA),
        connectionPropertiesMap.get(SFConnectionProperty.WAREHOUSE),
        connectionPropertiesMap.get(SFConnectionProperty.VALIDATE_DEFAULT_PARAMETERS),
        connectionPropertiesMap.get(SFConnectionProperty.AUTHENTICATOR),
        sessionProperties.getOCSPMode().name(),
        connectionPropertiesMap.get(SFConnectionProperty.PASSCODE_IN_PASSWORD),
        !Strings.isNullOrEmpty((String) connectionPropertiesMap.get(SFConnectionProperty.PASSCODE))
            ? "***"
            : "(empty)",
        connectionPropertiesMap.get(SFConnectionProperty.PRIVATE_KEY) != null
            ? "(not null)"
            : "(null)",
        connectionPropertiesMap.get(SFConnectionProperty.USE_PROXY),
        connectionPropertiesMap.get(SFConnectionProperty.PROXY_HOST),
        connectionPropertiesMap.get(SFConnectionProperty.PROXY_PORT),
        connectionPropertiesMap.get(SFConnectionProperty.PROXY_USER),
        !Strings.isNullOrEmpty(
                (String) connectionPropertiesMap.get(SFConnectionProperty.PROXY_PASSWORD))
            ? "***"
            : "(empty)",
        connectionPropertiesMap.get(SFConnectionProperty.DISABLE_SOCKS_PROXY),
        connectionPropertiesMap.get(SFConnectionProperty.APPLICATION),
        connectionPropertiesMap.get(SFConnectionProperty.APP_ID),
        connectionPropertiesMap.get(SFConnectionProperty.APP_VERSION),
        connectionPropertiesMap.get(SFConnectionProperty.LOGIN_TIMEOUT),
        connectionPropertiesMap.get(SFConnectionProperty.NETWORK_TIMEOUT),
        connectionPropertiesMap.get(SFConnectionProperty.QUERY_TIMEOUT),
        connectionPropertiesMap.get(SFConnectionProperty.TRACING),
        connectionPropertiesMap.get(SFConnectionProperty.PRIVATE_KEY_FILE),
        !Strings.isNullOrEmpty(
                (String) connectionPropertiesMap.get(SFConnectionProperty.PRIVATE_KEY_FILE_PWD))
            ? "***"
            : "(empty)",
        sessionParametersMap.get(CLIENT_STORE_TEMPORARY_CREDENTIAL));
    // TODO: temporarily hardcode sessionParameter debug info. will be changed in the future
    SFLoginInput loginInput = new SFLoginInput();

    loginInput
        .setServerUrl((String) connectionPropertiesMap.get(SFConnectionProperty.SERVER_URL))
        .setDatabaseName((String) connectionPropertiesMap.get(SFConnectionProperty.DATABASE))
        .setSchemaName((String) connectionPropertiesMap.get(SFConnectionProperty.SCHEMA))
        .setWarehouse((String) connectionPropertiesMap.get(SFConnectionProperty.WAREHOUSE))
        .setRole((String) connectionPropertiesMap.get(SFConnectionProperty.ROLE))
        .setValidateDefaultParameters(
            connectionPropertiesMap.get(SFConnectionProperty.VALIDATE_DEFAULT_PARAMETERS))
        .setAuthenticator((String) connectionPropertiesMap.get(SFConnectionProperty.AUTHENTICATOR))
        .setOKTAUserName((String) connectionPropertiesMap.get(SFConnectionProperty.OKTA_USERNAME))
        .setAccountName((String) connectionPropertiesMap.get(SFConnectionProperty.ACCOUNT))
        .setLoginTimeout(loginTimeout)
        .setUserName((String) connectionPropertiesMap.get(SFConnectionProperty.USER))
        .setPassword((String) connectionPropertiesMap.get(SFConnectionProperty.PASSWORD))
        .setToken((String) connectionPropertiesMap.get(SFConnectionProperty.TOKEN))
        .setPasscodeInPassword(passcodeInPassword)
        .setPasscode((String) connectionPropertiesMap.get(SFConnectionProperty.PASSCODE))
        .setConnectionTimeout(httpClientConnectionTimeout)
        .setSocketTimeout(httpClientSocketTimeout)
        .setAppId((String) connectionPropertiesMap.get(SFConnectionProperty.APP_ID))
        .setAppVersion((String) connectionPropertiesMap.get(SFConnectionProperty.APP_VERSION))
        .setSessionParameters(sessionParametersMap)
        .setPrivateKey((PrivateKey) connectionPropertiesMap.get(SFConnectionProperty.PRIVATE_KEY))
        .setPrivateKeyFile(
            (String) connectionPropertiesMap.get(SFConnectionProperty.PRIVATE_KEY_FILE))
        .setPrivateKeyFilePwd(
            (String) connectionPropertiesMap.get(SFConnectionProperty.PRIVATE_KEY_FILE_PWD))
        .setApplication((String) connectionPropertiesMap.get(SFConnectionProperty.APPLICATION))
        .setServiceName(this.getServiceName())
        .setOCSPMode(sessionProperties.getOCSPMode());

    // propagate OCSP mode to SFTrustManager. Note OCSP setting is global on JVM.
    HttpUtil.initHttpClient(loginInput.getOCSPMode(), null);
    SFLoginOutput loginOutput =
        SessionUtil.openSession(loginInput, connectionPropertiesMap, tracingLevel.toString());
    isClosed = false;

    sessionToken = loginOutput.getSessionToken();
    masterToken = loginOutput.getMasterToken();
    idToken = loginOutput.getIdToken();
    mfaToken = loginOutput.getMfaToken();
    sessionProperties.setDatabaseVersion(loginOutput.getDatabaseVersion());
    sessionProperties.setDatabaseMajorVersion(loginOutput.getDatabaseMajorVersion());
    sessionProperties.setDatabaseMinorVersion(loginOutput.getDatabaseMinorVersion());
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

    String loginDatabaseName = (String) connectionPropertiesMap.get(SFConnectionProperty.DATABASE);
    String loginSchemaName = (String) connectionPropertiesMap.get(SFConnectionProperty.SCHEMA);
    String loginRole = (String) connectionPropertiesMap.get(SFConnectionProperty.ROLE);
    String loginWarehouse = (String) connectionPropertiesMap.get(SFConnectionProperty.WAREHOUSE);

    if (loginDatabaseName != null && !loginDatabaseName.equalsIgnoreCase(database)) {
      sqlWarnings.add(
          new SFException(
              ErrorCode.CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP,
              "Database",
              loginDatabaseName,
              database));
    }

    if (loginSchemaName != null && !loginSchemaName.equalsIgnoreCase(schema)) {
      sqlWarnings.add(
          new SFException(
              ErrorCode.CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP,
              "Schema",
              loginSchemaName,
              schema));
    }

    if (loginRole != null && !loginRole.equalsIgnoreCase(role)) {
      sqlWarnings.add(
          new SFException(
              ErrorCode.CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP, "Role", loginRole, role));
    }

    if (loginWarehouse != null && !loginWarehouse.equalsIgnoreCase(warehouse)) {
      sqlWarnings.add(
          new SFException(
              ErrorCode.CONNECTION_ESTABLISHED_WITH_DIFFERENT_PROP,
              "Warehouse",
              loginWarehouse,
              warehouse));
    }

    // start heartbeat for this session so that the master token will not expire
    startHeartbeatForThisSession();
  }

  /**
   * If authenticator is null and private key is specified, jdbc will assume key pair authentication
   *
   * @return true if authenticator type is SNOWFLAKE (meaning password)
   */
  private boolean isSnowflakeAuthenticator() {
    Map<SFConnectionProperty, Object> connectionPropertiesMap =
        sessionProperties.getConnectionPropertiesMap();
    String authenticator = (String) connectionPropertiesMap.get(SFConnectionProperty.AUTHENTICATOR);
    PrivateKey privateKey =
        (PrivateKey) connectionPropertiesMap.get(SFConnectionProperty.PRIVATE_KEY);
    return (authenticator == null && privateKey == null && privateKeyFileLocation == null)
        || ClientAuthnDTO.AuthenticatorType.SNOWFLAKE.name().equalsIgnoreCase(authenticator);
  }

  /**
   * Returns true If authenticator is EXTERNALBROWSER.
   *
   * @return true if authenticator type is EXTERNALBROWSER
   */
  boolean isExternalbrowserAuthenticator() {
    Map<SFConnectionProperty, Object> connectionPropertiesMap =
        sessionProperties.getConnectionPropertiesMap();
    String authenticator = (String) connectionPropertiesMap.get(SFConnectionProperty.AUTHENTICATOR);
    return ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER.name().equalsIgnoreCase(authenticator);
  }

  /**
   * Returns true if authenticator is OKTA native
   *
   * @return true or false
   */
  boolean isOKTAAuthenticator() {
    Map<SFConnectionProperty, Object> connectionPropertiesMap =
        sessionProperties.getConnectionPropertiesMap();
    String authenticator = (String) connectionPropertiesMap.get(SFConnectionProperty.AUTHENTICATOR);
    return !Strings.isNullOrEmpty(authenticator) && authenticator.startsWith("https://");
  }

  /**
   * Returns true if authenticator is UsernamePasswordMFA native
   *
   * @return true or false
   */
  boolean isUsernamePasswordMFAAuthenticator() {
    Map<SFConnectionProperty, Object> connectionPropertiesMap =
        sessionProperties.getConnectionPropertiesMap();
    String authenticator = (String) connectionPropertiesMap.get(SFConnectionProperty.AUTHENTICATOR);
    return ClientAuthnDTO.AuthenticatorType.USERNAME_PASSWORD_MFA
        .name()
        .equalsIgnoreCase(authenticator);
  }

  public String getDatabaseVersion() {
    return sessionProperties.getDatabaseVersion();
  }

  public int getDatabaseMajorVersion() {
    return databaseMajorVersion;
  }

  public int getDatabaseMinorVersion() {
    return databaseMinorVersion;
  }

  public String getSessionId() {
    return sessionId;
  }

  /**
   * A helper function to call global service and renew session.
   *
   * @param prevSessionToken the session token that has expired
   * @throws SnowflakeSQLException if failed to renew the session
   * @throws SFException if failed to renew the session
   */
  synchronized void renewSession(String prevSessionToken)
      throws SFException, SnowflakeSQLException {
    if (sessionToken != null && !sessionToken.equals(prevSessionToken)) {
      logger.debug("not renew session because session token has not been updated.");
      return;
    }

    SFLoginInput loginInput = new SFLoginInput();
    loginInput
        .setServerUrl((String) sessionProperties.getServerUrl())
        .setSessionToken(sessionToken)
        .setMasterToken(masterToken)
        .setIdToken(idToken)
        .setMfaToken(mfaToken)
        .setLoginTimeout(loginTimeout)
        .setDatabaseName(this.getDatabase())
        .setSchemaName(this.getSchema())
        .setRole(this.getRole())
        .setWarehouse(this.getWarehouse())
        .setOCSPMode(sessionProperties.getOCSPMode());

    SFLoginOutput loginOutput = SessionUtil.renewSession(loginInput);

    sessionToken = loginOutput.getSessionToken();
    masterToken = loginOutput.getMasterToken();
  }

  /**
   * get session token
   *
   * @return session token
   */
  public String getSessionToken() {
    return sessionToken;
  }

  /**
   * Close the connection
   *
   * @throws SnowflakeSQLException if failed to close the connection
   * @throws SFException if failed to close the connection
   */
  public void close() throws SFException, SnowflakeSQLException {
    logger.debug(" public void close()");

    // stop heartbeat for this session
    stopHeartbeatForThisSession();

    if (isClosed) {
      return;
    }

    SFLoginInput loginInput = new SFLoginInput();
    loginInput
        .setServerUrl((String) sessionProperties.getServerUrl())
        .setSessionToken(sessionToken)
        .setLoginTimeout(loginTimeout)
        .setOCSPMode(sessionProperties.getOCSPMode());

    SessionUtil.closeSession(loginInput);
    closeTelemetryClient();
    clientInfo.clear();
    isClosed = true;
  }

  /** Start heartbeat for this session */
  protected void startHeartbeatForThisSession() {
    if (enableHeartbeat && !Strings.isNullOrEmpty(masterToken)) {
      logger.debug("start heartbeat, master token validity: " + masterTokenValidityInSeconds);

      HeartbeatBackground.getInstance()
          .addSession(this, masterTokenValidityInSeconds, this.heartbeatFrequency);
    } else {
      logger.debug("heartbeat not enabled for the session");
    }
  }

  /** Stop heartbeat for this session */
  protected void stopHeartbeatForThisSession() {
    if (enableHeartbeat && !Strings.isNullOrEmpty(masterToken)) {
      logger.debug("stop heartbeat");

      HeartbeatBackground.getInstance().removeSession(this);
    } else {
      logger.debug("heartbeat not enabled for the session");
    }
  }

  /**
   * Send heartbeat for the session
   *
   * @throws SFException exception raised from Snowflake
   * @throws SQLException exception raised from SQL generic layers
   */
  protected void heartbeat() throws SFException, SQLException {
    logger.debug(" public void heartbeat()");

    if (isClosed) {
      return;
    }

    HttpPost postRequest = null;

    String requestId = UUID.randomUUID().toString();

    boolean retry = false;

    // the loop for retrying if it runs into session expiration
    do {
      try {
        URIBuilder uriBuilder;

        uriBuilder = new URIBuilder((String) sessionProperties.getServerUrl());

        uriBuilder.addParameter(SFSession.SF_QUERY_REQUEST_ID, requestId);

        uriBuilder.setPath(SF_PATH_SESSION_HEARTBEAT);

        postRequest = new HttpPost(uriBuilder.build());

        // remember the session token in case it expires we need to renew
        // the session only when no other thread has renewed it
        String prevSessionToken = sessionToken;

        postRequest.setHeader(
            SF_HEADER_AUTHORIZATION,
            SF_HEADER_SNOWFLAKE_AUTHTYPE
                + " "
                + SF_HEADER_TOKEN_TAG
                + "=\""
                + prevSessionToken
                + "\"");

        logger.debug("Executing heartbeat request: {}", postRequest.toString());

        // the following will retry transient network issues
        String theResponse =
            HttpUtil.executeGeneralRequest(
                postRequest, SF_HEARTBEAT_TIMEOUT, sessionProperties.getOCSPMode());

        JsonNode rootNode;

        logger.debug("connection heartbeat response: {}", theResponse);

        rootNode = mapper.readTree(theResponse);

        // check the response to see if it is session expiration response
        if (rootNode != null
            && (Constants.SESSION_EXPIRED_GS_CODE == rootNode.path("code").asInt())) {
          logger.debug("renew session and retry");
          this.renewSession(prevSessionToken);
          retry = true;
          continue;
        }

        SnowflakeUtil.checkErrorAndThrowException(rootNode);

        // success
        retry = false;
      } catch (Throwable ex) {
        // for snowflake exception, just rethrow it
        if (ex instanceof SnowflakeSQLException) {
          throw (SnowflakeSQLException) ex;
        }

        logger.error("unexpected exception", ex);

        throw (SFException)
            IncidentUtil.generateIncidentV2WithException(
                this,
                new SFException(
                    ErrorCode.INTERNAL_ERROR, IncidentUtil.oneLiner("unexpected exception", ex)),
                null,
                requestId);
      }
    } while (retry);
  }

  public Properties getClientInfo() {
    logger.debug(" public Properties getClientInfo()");

    // defensive copy to avoid client from changing the properties
    // directly w/o going through the API
    Properties copy = new Properties();
    copy.putAll(this.clientInfo);
    return copy;
  }

  public String getClientInfo(String name) {
    logger.debug(" public String getClientInfo(String name)");
    return this.clientInfo.getProperty(name);
  }

  @Override
  public void raiseErrorInSession() {}

  public void setSFSessionProperty(String propertyName, boolean propertyValue) {
    this.sfSessionProperties.put(propertyName, propertyValue);
  }

  public Object getSFSessionProperty(String propertyName) {
    return this.sfSessionProperties.get(propertyName);
  }

  void injectedDelay() {

    AtomicInteger injectedDelay = this.sessionProperties.injectedDelay();
    int d = injectedDelay.get();

    if (d != 0) {
      injectedDelay.set(0);
      try {
        logger.trace("delayed for {}", d);

        Thread.sleep(d);
      } catch (InterruptedException ex) {
      }
    }
  }

  public int getInjectSocketTimeout() {
    return injectSocketTimeout;
  }

  public void setInjectSocketTimeout(int injectSocketTimeout) {
    this.injectSocketTimeout = injectSocketTimeout;
  }

  public String getInjectFileUploadFailure() {
    return this.sessionProperties.getInjectFileUploadFailure();
  }

  public void setInjectFileUploadFailure(String fileToFail) {
    this.sessionProperties.setInjectFileUploadFailure(fileToFail);
  }

  public int getNetworkTimeoutInMilli() {
    return networkTimeoutInMilli;
  }

  public boolean isClosed() {
    return isClosed;
  }

  public int getInjectClientPause() {
    return injectClientPause;
  }

  public void setInjectClientPause(int injectClientPause) {
    this.injectClientPause = injectClientPause;
  }

  protected int getHttpClientConnectionTimeout() {
    return httpClientConnectionTimeout;
  }

  protected int getHttpClientSocketTimeout() {
    return httpClientSocketTimeout;
  }

  protected int getAndIncrementSequenceId() {
    return sequenceId.getAndIncrement();
  }

  public boolean isSfSQLMode() {
    return this.sfSQLMode;
  }

  public void setSfSQLMode(boolean sfSQLMode) {
    this.sessionProperties.setSfSQLMode(sfSQLMode);
  }

  public boolean isEnableHeartbeat() {
    return enableHeartbeat;
  }

  public void setEnableHeartbeat(boolean enableHeartbeat) {
    this.enableHeartbeat = enableHeartbeat;
  }

  public long getHeartbeatFrequency() {
    return this.heartbeatFrequency;
  }

  public void setHeartbeatFrequency(int frequency) {
    this.heartbeatFrequency = frequency;
  }

  public boolean getAutoCommit() {
    return autoCommit.get();
  }

  public void setAutoCommit(boolean autoCommit) {
    this.autoCommit.set(autoCommit);
  }

  public boolean getPreparedStatementLogging() {
    return this.sessionProperties.getPreparedStatementLogging();
  }

  public void setPreparedStatementLogging(boolean value) {
    this.sessionProperties.setPreparedStatementLogging(value);
  }

  public boolean isResultColumnCaseInsensitive() {
    return this.sessionProperties.isResultColumnCaseInsensitive();
  }

  public void setResultColumnCaseInsensitive(boolean resultColumnCaseInsensitive) {
    this.sessionProperties.setResultColumnCaseInsensitive(resultColumnCaseInsensitive);
  }

  public String getDatabase() {
    return this.database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getSchema() {
    return this.schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getRole() {
    return role;
  }

  public void setRole(String role) {
    this.role = role;
  }

  public String getWarehouse() {
    return warehouse;
  }

  public void setWarehouse(String warehouse) {
    this.warehouse = warehouse;
  }

  public boolean getMetadataRequestUseConnectionCtx() {
    return this.sessionProperties.getMetadataRequestUseConnectionCtx();
  }

  public void setMetadataRequestUseConnectionCtx(boolean enabled) {
    this.sessionProperties.setMetadataRequestUseConnectionCtx(enabled);
  }

  public boolean getFormatDateWithTimezone() {
    return this.formatDateWithTimezone;
  }

  public void setFormatDateWithTimezone(boolean useTimezone) {
    this.formatDateWithTimezone = useTimezone;
  }

  public boolean getUseSessionTimezone() {
    return this.useSessionTimezone;
  }

  public void setUseSessionTimezone(boolean useSessionTimezone) {
    this.useSessionTimezone = useSessionTimezone;
  }

  public boolean getTreatNTZAsUTC() {
    return this.treatNTZAsUTC;
  }

  public void setTreatNTZAsUTC(boolean enabled) {
    this.treatNTZAsUTC = enabled;
  }

  public boolean getMetadataRequestUseSessionDatabase() {
    return this.metadataRequestUseSessionDatabase;
  }

  public void setMetadataRequestUseSessionDatabase(boolean enabled) {
    this.sessionProperties.setMetadataRequestUseSessionDatabase(enabled);
  }

  public SnowflakeType getTimestampMappedType() {
    return this.sessionProperties.getTimestampMappedType();
  }

  public void setTimestampMappedType(SnowflakeType timestampMappedType) {
    this.sessionProperties.setTimestampMappedType(timestampMappedType);
  }

  public boolean isJdbcTreatDecimalAsInt() {
    return jdbcTreatDecimalAsInt;
  }

  public void setJdbcTreatDecimalAsInt(boolean jdbcTreatDecimalAsInt) {
    this.jdbcTreatDecimalAsInt = jdbcTreatDecimalAsInt;
  }

  public boolean getEnableCombineDescribe() {
    return this.enableCombineDescribe;
  }

  public void setEnableCombineDescribe(boolean enable) {
    this.enableCombineDescribe = enable;
  }

  public Integer getQueryTimeout() {
    return this.sessionProperties.getQueryTimeout();
  }

  public String getUser() {
    return sessionProperties.getUser();
  }

  public String getUrl() {
    return sessionProperties.getUrl();
  }

  public int getInjectWaitInPut() {
    return sessionProperties.getInjectWaitInPut();
  }

  public List<SFException> getSqlWarnings() {
    return sqlWarnings;
  }

  public void clearSqlWarnings() {
    sqlWarnings.clear();
  }

  public synchronized Telemetry getTelemetryClient() {
    // initialize for the first time. this should only be done after session
    // properties have been set, else the client won't properly resolve the URL.
    if (telemetryClient == null) {
      if (getUrl() == null) {
        logger.error("Telemetry client created before session properties set.");
        return null;
      }
      telemetryClient = TelemetryClient.createTelemetry(this);
    }
    return telemetryClient;
  }

  public void closeTelemetryClient() {
    if (telemetryClient != null) {
      telemetryClient.close();
    }
  }

  public boolean isClientTelemetryEnabled() {
    return this.clientTelemetryEnabled;
  }

  public void setClientTelemetryEnabled(boolean clientTelemetryEnabled) {
    this.clientTelemetryEnabled = clientTelemetryEnabled;
  }

  public int getArrayBindStageThreshold() {
    return arrayBindStageThreshold;
  }

  public void setArrayBindStageThreshold(int arrayBindStageThreshold) {
    this.arrayBindStageThreshold = arrayBindStageThreshold;
  }

  public String getArrayBindStage() {
    return arrayBindStage;
  }

  public void setArrayBindStage(String arrayBindStage) {
    this.arrayBindStage =
        String.format("%s.%s.%s", this.getDatabase(), this.getSchema(), arrayBindStage);
  }

  public String getIdToken() {
    return idToken;
  }

  public String getMfaToken() {
    return mfaToken;
  }

  public boolean isStoreTemporaryCredential() {
    return this.storeTemporaryCredential;
  }

  public void setStoreTemporaryCredential(boolean storeTemporaryCredential) {
    this.storeTemporaryCredential = storeTemporaryCredential;
  }

  /**
   * Gets the service name provided from GS.
   *
   * @return the service name
   */
  public String getServiceName() {
    return serviceName;
  }

  /**
   * Sets the service name provided from GS.
   *
   * @param serviceName service name
   */
  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  /**
   * Sets the current objects if the session is not up to date. It can happen if the session is
   * created by the id token, which doesn't carry the current objects.
   *
   * @param loginInput The login input to use for this session
   * @param loginOutput The login output to ose for this session
   */
  void setCurrentObjects(SFLoginInput loginInput, SFLoginOutput loginOutput) {
    this.sessionToken = loginOutput.getSessionToken(); // used to run the commands.
    runInternalCommand("USE ROLE IDENTIFIER(?)", loginInput.getRole());
    runInternalCommand("USE WAREHOUSE IDENTIFIER(?)", loginInput.getWarehouse());
    runInternalCommand("USE DATABASE IDENTIFIER(?)", loginInput.getDatabaseName());
    runInternalCommand("USE SCHEMA IDENTIFIER(?)", loginInput.getSchemaName());
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
    if (result != null) {
      loginOutput.setCommonParams(result.parameters);
    }
  }

  private SFBaseResultSet runInternalCommand(String sql, String value) {
    if (value == null) {
      return null;
    }

    try {
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
    } catch (SFException | SQLException ex) {
      logger.debug("Failed to run a command: {}, err={}", sql, ex);
    }
    return null;
  }

  public boolean isConservativeMemoryUsageEnabled() {
    return enableConservativeMemoryUsage;
  }

  public void setEnableConservativeMemoryUsage(boolean value) {
    enableConservativeMemoryUsage = value;
  }

  public int getConservativeMemoryAdjustStep() {
    return conservativeMemoryAdjustStep;
  }

  public void setConservativeMemoryAdjustStep(int step) {
    conservativeMemoryAdjustStep = step;
  }

  public int getClientMemoryLimit() {
    return clientMemoryLimit;
  }

  public void setClientMemoryLimit(int clientMemoryLimit) {
    this.clientMemoryLimit = clientMemoryLimit;
  }

  public int getClientResultChunkSize() {
    return clientResultChunkSize;
  }

  public void setClientResultChunkSize(int clientResultChunkSize) {
    this.clientResultChunkSize = clientResultChunkSize;
  }

  public int getClientPrefetchThreads() {
    return clientPrefetchThreads;
  }

  public void setClientPrefetchThreads(int clientPrefetchThreads) {
    this.clientPrefetchThreads = clientPrefetchThreads;
  }

  public boolean isValidateDefaultParameters() {
    return validateDefaultParameters;
  }

  public void setValidateDefaultParameters(boolean v) {
    validateDefaultParameters = v;
  }

  public SnowflakeConnectString getSnowflakeConnectionString() {
    return sfConnStr;
  }

  public void setSnowflakeConnectionString(SnowflakeConnectString connStr) {
    sfConnStr = connStr;
  }

  /**
   * Performs a sanity check on properties. Sanity checking includes: - verifying that a server url
   * is present - verifying various combinations of properties given the authenticator
   *
   * @throws SFException Will be thrown if any of the necessary properties are missing
   */
  private void performSanityCheckOnProperties() throws SFException {
    Map<SFConnectionProperty, Object> connectionPropertiesMap =
        sessionProperties.getConnectionPropertiesMap();

    for (SFConnectionProperty property : SFConnectionProperty.values()) {
      if (property.isRequired() && !connectionPropertiesMap.containsKey(property)) {
        switch (property) {
          case SERVER_URL:
            throw new SFException(ErrorCode.MISSING_SERVER_URL);

          default:
            throw new SFException(ErrorCode.MISSING_CONNECTION_PROPERTY, property.getPropertyKey());
        }
      }
    }

    if (isSnowflakeAuthenticator()
        || isOKTAAuthenticator()
        || isUsernamePasswordMFAAuthenticator()) {
      // userName and password are expected for both Snowflake and Okta.
      String userName = (String) connectionPropertiesMap.get(SFConnectionProperty.USER);
      if (Strings.isNullOrEmpty(userName)) {
        throw new SFException(ErrorCode.MISSING_USERNAME);
      }

      String password = (String) connectionPropertiesMap.get(SFConnectionProperty.PASSWORD);
      if (Strings.isNullOrEmpty(password)) {

        throw new SFException(ErrorCode.MISSING_PASSWORD);
      }
    }

    // perform sanity check on proxy settings
    boolean useProxy =
        (boolean) connectionPropertiesMap.getOrDefault(SFConnectionProperty.USE_PROXY, false);
    if (useProxy) {
      if (!connectionPropertiesMap.containsKey(SFConnectionProperty.PROXY_HOST)
          || connectionPropertiesMap.get(SFConnectionProperty.PROXY_HOST) == null
          || ((String) connectionPropertiesMap.get(SFConnectionProperty.PROXY_HOST)).isEmpty()
          || !connectionPropertiesMap.containsKey(SFConnectionProperty.PROXY_PORT)
          || connectionPropertiesMap.get(SFConnectionProperty.PROXY_HOST) == null) {
        throw new SFException(
            ErrorCode.INVALID_PROXY_PROPERTIES, "Both proxy host and port values are needed.");
      }
    }
  }

  public List<DriverPropertyInfo> checkProperties() {
    Map<SFConnectionProperty, Object> connectionPropertiesMap =
        sessionProperties.getConnectionPropertiesMap();
    for (SFConnectionProperty property : SFConnectionProperty.values()) {
      if (property.isRequired() && !connectionPropertiesMap.containsKey(property)) {
        missingProperties.add(addNewDriverProperty(property.getPropertyKey(), null));
      }
    }
    if (isSnowflakeAuthenticator() || isOKTAAuthenticator()) {
      // userName and password are expected for both Snowflake and Okta.
      String userName = (String) connectionPropertiesMap.get(SFConnectionProperty.USER);
      if (Strings.isNullOrEmpty(userName)) {
        missingProperties.add(
            addNewDriverProperty(
                SFConnectionProperty.USER.getPropertyKey(), "username for account"));
      }

      String password = (String) connectionPropertiesMap.get(SFConnectionProperty.PASSWORD);
      if (Strings.isNullOrEmpty(password)) {
        missingProperties.add(
            addNewDriverProperty(
                SFConnectionProperty.PASSWORD.getPropertyKey(), "password for " + "account"));
      }
    }

    boolean useProxy =
        (boolean) connectionPropertiesMap.getOrDefault(SFConnectionProperty.USE_PROXY, false);
    if (useProxy) {
      if (!connectionPropertiesMap.containsKey(SFConnectionProperty.PROXY_HOST)) {
        missingProperties.add(
            addNewDriverProperty(
                SFConnectionProperty.PROXY_HOST.getPropertyKey(), "proxy host name"));
      }
      if (!connectionPropertiesMap.containsKey(SFConnectionProperty.PROXY_PORT)) {
        missingProperties.add(
            addNewDriverProperty(
                SFConnectionProperty.PROXY_PORT.getPropertyKey(),
                "proxy port; " + "should be an integer"));
      }
    }
    return missingProperties;
  }

  private DriverPropertyInfo addNewDriverProperty(String name, String description) {
    DriverPropertyInfo info = new DriverPropertyInfo(name, null);
    info.description = description;
    return info;
  }
}
