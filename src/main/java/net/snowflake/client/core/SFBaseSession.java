package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.QueryStatusV2;
import net.snowflake.client.jdbc.SFConnectionHandler;
import net.snowflake.client.jdbc.SnowflakeConnectString;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Snowflake session implementation base. The methods and fields contained within this class are
 * setters and getters for shared session properties, i.e., those that may be set via connection
 * time, in properties, as well as those that may be parsed from response headers from Snowflake
 * (i.e., session parameters).
 *
 * <p>New connection properties and session parameters can be added here, as SFBaseSession contains
 * the logic for storing, setting, and getting these session properties.
 *
 * <p>For logic that is specific to a particular type of Session, four methods need to be
 * implemented:
 *
 * <p>open(), for establishing a connection. close(), for closing a connection. isSafeToClose(), for
 * checking whether the connection can be closed. checkProperties(), invoked at connection time to
 * verify if the requisite properties are set; and if not, returns a list of missing properties
 * raiseError(), which handles exceptions that may be raised in the session isTelemetryEnabled(),
 * which signals whether to enable client telemetry
 */
public abstract class SFBaseSession {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFBaseSession.class);
  private final Properties clientInfo = new Properties();
  private final AtomicBoolean autoCommit = new AtomicBoolean(true);
  // Injected delay for the purpose of connection timeout testing
  // Any statement execution will sleep for the specified number of milliseconds
  private final AtomicInteger _injectedDelay = new AtomicInteger(0);
  // Connection properties map
  private final Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>();
  // Custom key-value map for other options values *not* defined in SFSessionProperties,
  // i.e., session-implementation specific
  private final Map<String, Object> customSessionProperties = new HashMap<>(1);
  private SFConnectionHandler sfConnectionHandler;
  protected List<SFException> sqlWarnings = new ArrayList<>();
  // Unique Session ID
  private String sessionId;
  // Database versions
  private String databaseVersion = null;
  private int databaseMajorVersion = 0;
  private int databaseMinorVersion = 0;
  // Used for parsing results
  private SnowflakeType timestampMappedType = SnowflakeType.TIMESTAMP_LTZ;
  private boolean isResultColumnCaseInsensitive;
  private boolean isJdbcTreatDecimalAsInt = true;
  private boolean treatNTZAsUTC;
  private boolean preparedStatementLogging = false;
  // Inject failure for testing
  private String injectFileUploadFailure;
  private boolean enableHeartbeat;
  protected int heartbeatFrequency = 3600;
  private boolean formatDateWithTimezone;
  private boolean enableCombineDescribe;
  private boolean clientTelemetryEnabled = false;
  private boolean useSessionTimezone;
  private boolean defaultFormatDateWithTimezone = true;
  private boolean getDateUseNullTimezone = true;
  // The server can read array binds from a stage instead of query payload.
  // When there as many bind values as this threshold, we should upload them to a stage.
  private int arrayBindStageThreshold = 0;
  private boolean storeTemporaryCredential;
  private String serviceName;
  private boolean sfSQLMode;
  // whether to enable conservative memory usage mode
  private boolean enableConservativeMemoryUsage;
  // the step in MB to adjust memory usage
  private int conservativeMemoryAdjustStep = 64;
  private int clientMemoryLimit;
  private int clientResultChunkSize;
  private int clientPrefetchThreads;
  // validate the default parameters by GS?
  private boolean validateDefaultParameters;
  // Current session context w/ re to database, schema, role, warehouse
  private String database;
  private String schema;
  private String role;
  private String warehouse;
  // For Metadata request(i.e. DatabaseMetadata.getTables or
  // DatabaseMetadata.getSchemas,), whether to use connection ctx to
  // improve the request time
  private boolean metadataRequestUseConnectionCtx = false;
  // For Metadata request(i.e. DatabaseMetadata.getTables or
  // DatabaseMetadata.getSchemas), whether to search using multiple schemas with
  // session database
  private boolean metadataRequestUseSessionDatabase = false;
  // Forces to use regional s3 end point API to generate regional url for aws endpoints
  private boolean useRegionalS3EndpointsForPresignedURL = false;
  // Stores other parameters sent by server
  private final Map<String, Object> otherParameters = new HashMap<>();
  private HttpClientSettingsKey ocspAndProxyAndGzipKey = null;
  // Default value for memory limit in SFBaseSession
  public static long MEMORY_LIMIT_UNSET = -1;
  // Memory limit for SnowflakeChunkDownloader. This gets set from SFBaseSession for testing
  // purposes only.
  private long memoryLimitForTesting = MEMORY_LIMIT_UNSET;
  // name of temporary stage to upload array binds to; null if none has been created yet
  private String arrayBindStage = null;

  // Maximum size of the query context cache for current session
  private int queryContextCacheSize = 5;

  // Whether enable returning timestamp with timezone as data type
  private boolean enableReturnTimestampWithTimeZone = true;

  // Server side value
  private boolean jdbcEnablePutGet = true;

  // Connection string setting
  private boolean enablePutGet = true;

  // Enables the use of pattern searches for certain DatabaseMetaData methods
  // which do not by definition allow the use of patterns, but
  // we need to allow for it to maintain backwards compatibility.
  private boolean enablePatternSearch = true;

  // Enables the use of exact schema searches for certain DatabaseMetaData methods
  // that should use schema from context (CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true)
  // value is false for backwards compatibility.
  private boolean enableExactSchemaSearch = false;

  /** Disable lookup for default credentials by GCS library */
  private boolean disableGcsDefaultCredentials = true;

  private Map<String, Object> commonParameters;

  private boolean isJdbcArrowTreatDecimalAsInt = true;

  private boolean implicitServerSideQueryTimeout = false;

  private boolean clearBatchOnlyAfterSuccessfulExecution = false;

  protected SFBaseSession(SFConnectionHandler sfConnectionHandler) {
    this.sfConnectionHandler = sfConnectionHandler;
  }

  public void setMemoryLimitForTesting(long memLimit) {
    this.memoryLimitForTesting = memLimit;
  }

  public long getMemoryLimitForTesting() {
    return this.memoryLimitForTesting;
  }

  /**
   * Part of the JDBC API, where client applications may fetch a Map of Properties to set various
   * attributes. This is not used internally by any driver component, but should be maintained by
   * the Session object.
   *
   * @return client info as Properties
   */
  public Properties getClientInfo() {
    // defensive copy to avoid client from changing the properties
    // directly w/o going through the API
    Properties copy = new Properties();
    copy.putAll(this.clientInfo);
    return copy;
  }

  /**
   * Set common parameters
   *
   * @param parameters the parameters to set
   */
  public void setCommonParameters(Map<String, Object> parameters) {
    this.commonParameters = parameters;
  }

  /**
   * Get common parameters
   *
   * @return Map of common parameters
   */
  public Map<String, Object> getCommonParameters() {
    return this.commonParameters;
  }

  /**
   * Gets the Property associated with the key 'name' in the ClientInfo map.
   *
   * @param name The key from which to fetch the Property.
   * @return The ClientInfo entry property.
   */
  public String getClientInfo(String name) {
    return this.clientInfo.getProperty(name);
  }

  /**
   * Returns a unique id for this session.
   *
   * @return unique id for session
   */
  public String getSessionId() {
    return sessionId;
  }

  /**
   * Sets the session-id attribute in this session.
   *
   * @param sessionId The session id as a string.
   */
  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  /**
   * @return true if session is in SQLMode
   */
  public boolean isSfSQLMode() {
    return sfSQLMode;
  }

  /**
   * Set sfSQLMode
   *
   * @param sfSQLMode boolean
   */
  public void setSfSQLMode(boolean sfSQLMode) {
    this.sfSQLMode = sfSQLMode;
  }

  /**
   * Get the database version
   *
   * @return database version
   */
  public String getDatabaseVersion() {
    return databaseVersion;
  }

  /**
   * Set database version
   *
   * @param databaseVersion the version to set
   */
  public void setDatabaseVersion(String databaseVersion) {
    this.databaseVersion = databaseVersion;
  }

  /**
   * Get databse major version
   *
   * @return the database major version
   */
  public int getDatabaseMajorVersion() {
    return databaseMajorVersion;
  }

  /**
   * Set database major version
   *
   * @param databaseMajorVersion the database major version
   */
  public void setDatabaseMajorVersion(int databaseMajorVersion) {
    this.databaseMajorVersion = databaseMajorVersion;
  }

  /**
   * Get the database minor version
   *
   * @return database minor version
   */
  public int getDatabaseMinorVersion() {
    return databaseMinorVersion;
  }

  /**
   * Set the database minor version
   *
   * @param databaseMinorVersion the minor version
   */
  public void setDatabaseMinorVersion(int databaseMinorVersion) {
    this.databaseMinorVersion = databaseMinorVersion;
  }

  /**
   * Gets the value of CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS if one has been set. False by
   * default.
   *
   * @see <a
   *     href="https://docs.snowflake.com/en/sql-reference/parameters#client-enable-log-info-statement-parameters">CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS</a>
   * @return true if enabled
   */
  public boolean getPreparedStatementLogging() {
    return this.preparedStatementLogging;
  }

  /**
   * Set prepared statement logging
   *
   * @see SFBaseSession#getPreparedStatementLogging()
   * @param value boolean
   */
  public void setPreparedStatementLogging(boolean value) {
    this.preparedStatementLogging = value;
  }

  /**
   * Get inject file upload failure. Note: Should only be used in internal tests!
   *
   * @return file to fail
   */
  public String getInjectFileUploadFailure() {
    return this.injectFileUploadFailure;
  }

  /**
   * Set inject file upload failure Note: Should only be used in internal tests!
   *
   * @param fileToFail the file to fail
   */
  public void setInjectFileUploadFailure(String fileToFail) {
    this.injectFileUploadFailure = fileToFail;
  }

  /**
   * Get timestamp mapped type
   *
   * @see <a
   *     href="https://docs.snowflake.com/en/sql-reference/parameters#client-timestamp-type-mapping">CLIENT_TIMESTAMP_TYPE_MAPPING</a>
   * @return {@link SnowflakeType}
   */
  public SnowflakeType getTimestampMappedType() {
    return timestampMappedType;
  }

  /**
   * Set the timestamp mapped type
   *
   * @see SFBaseSession#getTimestampMappedType()
   * @param timestampMappedType SnowflakeType
   */
  public void setTimestampMappedType(SnowflakeType timestampMappedType) {
    this.timestampMappedType = timestampMappedType;
  }

  /**
   * Get if result column is case-insensitive
   *
   * @see SFBaseSession#setResultColumnCaseInsensitive(boolean)
   * @return true if result column is case-insensitive
   */
  public boolean isResultColumnCaseInsensitive() {
    return isResultColumnCaseInsensitive;
  }

  /**
   * Set if result column is case-insensitive
   *
   * @see <a
   *     href="https://docs.snowflake.com/en/sql-reference/parameters#client-result-column-case-insensitive">CLIENT_RESULT_COLUMN_CASE_INSENSITIVE</a>
   * @param resultColumnCaseInsensitive boolean
   */
  public void setResultColumnCaseInsensitive(boolean resultColumnCaseInsensitive) {
    isResultColumnCaseInsensitive = resultColumnCaseInsensitive;
  }

  /**
   * Check if we want to treat decimal as int JDBC types
   *
   * @see <a
   *     href="https://docs.snowflake.com/en/sql-reference/parameters#jdbc-treat-decimal-as-int">JDBC_TREAT_DECIMAL_AS_INT</a>
   * @return true if decimal is treated as int
   */
  public boolean isJdbcTreatDecimalAsInt() {
    return isJdbcTreatDecimalAsInt;
  }

  /**
   * Set if decimal should be treated as int type
   *
   * @see SFBaseSession#isJdbcTreatDecimalAsInt()
   * @param jdbcTreatDecimalAsInt boolean
   */
  public void setJdbcTreatDecimalAsInt(boolean jdbcTreatDecimalAsInt) {
    isJdbcTreatDecimalAsInt = jdbcTreatDecimalAsInt;
  }

  /**
   * @return true if decimal should be treated as int for arrow types
   */
  public boolean isJdbcArrowTreatDecimalAsInt() {
    return isJdbcArrowTreatDecimalAsInt;
  }

  /**
   * Set if decimal should be treated as int for arrow types
   *
   * @param jdbcArrowTreatDecimalAsInt boolean
   */
  public void setJdbcArrowTreatDecimalAsInt(boolean jdbcArrowTreatDecimalAsInt) {
    isJdbcArrowTreatDecimalAsInt = jdbcArrowTreatDecimalAsInt;
  }

  /**
   * Get the server url
   *
   * @return the server url or null if it is not set
   */
  public String getServerUrl() {
    if (connectionPropertiesMap.containsKey(SFSessionProperty.SERVER_URL)) {
      return (String) connectionPropertiesMap.get(SFSessionProperty.SERVER_URL);
    }
    return null;
  }

  /**
   * Get whether columns strings are quoted.
   *
   * @return value of 'stringsQuotedForColumnDef' connection property or false if not set.
   */
  public boolean isStringQuoted() {
    if (connectionPropertiesMap.containsKey(SFSessionProperty.STRINGS_QUOTED)) {
      return (Boolean) connectionPropertiesMap.get(SFSessionProperty.STRINGS_QUOTED);
    }
    return false;
  }

  /**
   * Wrapper function for the other addProperty(String, Object) method that takes an
   * SFSessionProperty instead of a String key.
   *
   * @param sfSessionProperty The property for which to set the value.
   * @param propertyValue The value to set for the property.
   * @throws SFException If the value already exists for the given key (should only be set once), or
   *     if the value is invalid.
   */
  public void addProperty(SFSessionProperty sfSessionProperty, Object propertyValue)
      throws SFException {
    addProperty(sfSessionProperty.getPropertyKey(), propertyValue);
  }

  /**
   * Adds a connection property to the connection-properties map. Connection properties are those
   * that are defined in SFSessionProperty. They are set typically through instantiation of the
   * Session.
   *
   * @param propertyName The name of the property, as a string. Recognized ones are defined in the
   *     SFSessionProperty enum.
   * @param propertyValue The value to set for this key.
   * @throws SFException If the value already exists for the given key (should only be set once), or
   *     if the value is invalid.
   */
  public void addProperty(String propertyName, Object propertyValue) throws SFException {
    SFSessionProperty connectionProperty = SFSessionProperty.lookupByKey(propertyName);
    // check if the value type is as expected
    propertyValue = SFSessionProperty.checkPropertyValue(connectionProperty, propertyValue);

    if (connectionPropertiesMap.containsKey(connectionProperty)) {
      throw new SFException(ErrorCode.DUPLICATE_CONNECTION_PROPERTY_SPECIFIED, propertyName);
    } else if (propertyValue != null && connectionProperty == SFSessionProperty.AUTHENTICATOR) {
      String[] authenticatorWithParams = propertyValue.toString().split(";");
      if (authenticatorWithParams.length == 1) {
        connectionPropertiesMap.put(connectionProperty, propertyValue);
      } else {
        String[] oktaUserKeyPair = authenticatorWithParams[1].split("=");
        if (oktaUserKeyPair.length == 2) {
          connectionPropertiesMap.put(connectionProperty, authenticatorWithParams[0]);
          connectionPropertiesMap.put(SFSessionProperty.OKTA_USERNAME, oktaUserKeyPair[1]);
        } else {
          throw new SFException(ErrorCode.INVALID_OKTA_USERNAME, propertyName);
        }
      }
    } else {
      connectionPropertiesMap.put(connectionProperty, propertyValue);
    }
  }

  /**
   * Get the connection properties map
   *
   * @return the connection properties map
   */
  public Map<SFSessionProperty, Object> getConnectionPropertiesMap() {
    return connectionPropertiesMap;
  }

  /**
   * Get the http client key
   *
   * @return HttpClientSettingsKey
   * @throws SnowflakeSQLException if exception encountered
   */
  public HttpClientSettingsKey getHttpClientKey() throws SnowflakeSQLException {
    // if key is already created, return it without making a new one
    if (ocspAndProxyAndGzipKey != null) {
      return ocspAndProxyAndGzipKey;
    }

    OCSPMode ocspMode = getOCSPMode();

    Boolean gzipDisabled = false;
    if (connectionPropertiesMap.containsKey(SFSessionProperty.GZIP_DISABLED)) {
      gzipDisabled = (Boolean) connectionPropertiesMap.get(SFSessionProperty.GZIP_DISABLED);
    }

    // if not, create a new key
    boolean useProxy = false;
    if (connectionPropertiesMap.containsKey(SFSessionProperty.USE_PROXY)) {
      useProxy = (boolean) connectionPropertiesMap.get(SFSessionProperty.USE_PROXY);
    }
    // Check for any user agent suffix
    String userAgentSuffix = "";
    if (connectionPropertiesMap.containsKey(SFSessionProperty.USER_AGENT_SUFFIX)) {
      userAgentSuffix = (String) connectionPropertiesMap.get(SFSessionProperty.USER_AGENT_SUFFIX);
    }

    if (useProxy) {
      int proxyPort;
      try {
        proxyPort =
            Integer.parseInt(connectionPropertiesMap.get(SFSessionProperty.PROXY_PORT).toString());
      } catch (NumberFormatException | NullPointerException e) {
        throw new SnowflakeSQLException(
            ErrorCode.INVALID_PROXY_PROPERTIES, "Could not parse port number");
      }
      String proxyHost = (String) connectionPropertiesMap.get(SFSessionProperty.PROXY_HOST);
      String proxyUser = (String) connectionPropertiesMap.get(SFSessionProperty.PROXY_USER);
      String proxyPassword = (String) connectionPropertiesMap.get(SFSessionProperty.PROXY_PASSWORD);
      String nonProxyHosts =
          (String) connectionPropertiesMap.get(SFSessionProperty.NON_PROXY_HOSTS);
      String proxyProtocol = (String) connectionPropertiesMap.get(SFSessionProperty.PROXY_PROTOCOL);
      ocspAndProxyAndGzipKey =
          new HttpClientSettingsKey(
              ocspMode,
              proxyHost,
              proxyPort,
              nonProxyHosts,
              proxyUser,
              proxyPassword,
              proxyProtocol,
              userAgentSuffix,
              gzipDisabled);

      logHttpClientInitInfo(ocspAndProxyAndGzipKey);

      return ocspAndProxyAndGzipKey;
    }
    // If JVM proxy parameters are specified, proxies need to go through the JDBC driver's
    // HttpClientSettingsKey logic in order to work properly.
    else {
      boolean httpUseProxy = Boolean.parseBoolean(systemGetProperty("http.useProxy"));
      String httpProxyHost = systemGetProperty("http.proxyHost");
      String httpProxyPort = systemGetProperty("http.proxyPort");
      String httpProxyUser = systemGetProperty("http.proxyUser");
      String httpProxyPassword = systemGetProperty("http.proxyPassword");
      String httpsProxyHost = systemGetProperty("https.proxyHost");
      String httpsProxyPort = systemGetProperty("https.proxyPort");
      String httpsProxyUser = systemGetProperty("https.proxyUser");
      String httpsProxyPassword = systemGetProperty("https.proxyPassword");
      String httpProxyProtocol = systemGetProperty("http.proxyProtocol");
      String noProxy = systemGetEnv("NO_PROXY");
      String nonProxyHosts = systemGetProperty("http.nonProxyHosts");
      // log the JVM parameters that are being used
      if (httpUseProxy) {
        logger.debug(
            "Using JVM parameters for proxy setup: http.useProxy={}, http.proxyHost={}, http.proxyPort={}, http.proxyUser={}, "
                + "http.proxyPassword is {}, https.proxyHost={}, https.proxyPort={}, https.proxyUser={}, "
                + "https.proxyPassword is {}, http.nonProxyHosts={}, NO_PROXY={}, http.proxyProtocol={}",
            httpUseProxy,
            httpProxyHost,
            httpProxyPort,
            httpProxyUser,
            httpProxyPassword == null || httpProxyPassword.isEmpty() ? "not set" : "set",
            httpsProxyHost,
            httpsProxyPort,
            httpsProxyUser,
            httpsProxyPassword == null || httpsProxyPassword.isEmpty() ? "not set" : "set",
            nonProxyHosts,
            noProxy,
            httpProxyProtocol,
            userAgentSuffix,
            gzipDisabled);
        // There are 2 possible parameters for non proxy hosts that can be combined into 1
        String combinedNonProxyHosts = isNullOrEmpty(nonProxyHosts) ? "" : nonProxyHosts;
        if (!isNullOrEmpty(noProxy)) {
          combinedNonProxyHosts += combinedNonProxyHosts.length() == 0 ? "" : "|";
          combinedNonProxyHosts += noProxy;
        }

        // It is possible that a user can have both http and https proxies specified in the JVM
        // parameters. The default protocol is http.
        String proxyProtocol = "http";
        if (!isNullOrEmpty(httpProxyProtocol)) {
          proxyProtocol = httpProxyProtocol;
        } else if (!isNullOrEmpty(httpsProxyHost)
            && !isNullOrEmpty(httpsProxyPort)
            && isNullOrEmpty(httpProxyHost)
            && isNullOrEmpty(httpProxyPort)) {
          proxyProtocol = "https";
        }

        if (proxyProtocol.equals("https")
            && !isNullOrEmpty(httpsProxyHost)
            && !isNullOrEmpty(httpsProxyPort)) {
          logger.debug("Using https proxy configuration from JVM parameters");
          int proxyPort;
          try {
            proxyPort = Integer.parseInt(httpsProxyPort);
          } catch (NumberFormatException | NullPointerException e) {
            throw new SnowflakeSQLException(
                ErrorCode.INVALID_PROXY_PROPERTIES, "Could not parse port number");
          }
          ocspAndProxyAndGzipKey =
              new HttpClientSettingsKey(
                  ocspMode,
                  httpsProxyHost,
                  proxyPort,
                  combinedNonProxyHosts,
                  httpsProxyUser,
                  httpsProxyPassword,
                  "https",
                  userAgentSuffix,
                  gzipDisabled);
          logHttpClientInitInfo(ocspAndProxyAndGzipKey);
        } else if (proxyProtocol.equals("http")
            && !isNullOrEmpty(httpProxyHost)
            && !isNullOrEmpty(httpProxyPort)) {
          logger.debug("Using http proxy configuration from JVM parameters");
          int proxyPort;
          try {
            proxyPort = Integer.parseInt(httpProxyPort);
          } catch (NumberFormatException | NullPointerException e) {
            throw new SnowflakeSQLException(
                ErrorCode.INVALID_PROXY_PROPERTIES, "Could not parse port number");
          }
          ocspAndProxyAndGzipKey =
              new HttpClientSettingsKey(
                  ocspMode,
                  httpProxyHost,
                  proxyPort,
                  combinedNonProxyHosts,
                  httpProxyUser,
                  httpProxyPassword,
                  "http",
                  userAgentSuffix,
                  gzipDisabled);
          logHttpClientInitInfo(ocspAndProxyAndGzipKey);
        } else {
          // Not enough parameters set to use the proxy.
          logger.warn(
              "Failed parsing the proxy settings from JVM parameters as http.useProxy={},"
                  + " but valid host and port were not provided.",
              httpUseProxy);
          ocspAndProxyAndGzipKey =
              new HttpClientSettingsKey(ocspMode, userAgentSuffix, gzipDisabled);
          logHttpClientInitInfo(ocspAndProxyAndGzipKey);
        }
      } else {
        // If no proxy is used or JVM http proxy is used, no need for setting parameters
        logger.debug("http.useProxy={}. JVM proxy not used.", httpUseProxy);
        unsetInvalidProxyHostAndPort();
        ocspAndProxyAndGzipKey = new HttpClientSettingsKey(ocspMode, userAgentSuffix, gzipDisabled);
        logHttpClientInitInfo(ocspAndProxyAndGzipKey);
      }
    }
    return ocspAndProxyAndGzipKey;
  }

  private void logHttpClientInitInfo(HttpClientSettingsKey key) {
    if (key.usesProxy()) {
      logger.info(
          "Driver OCSP mode: {}, gzip disabled: {}, proxy protocol: {},"
              + " proxy host: {}, proxy port: {}, non proxy hosts: {}, proxy user: {}, proxy password is {}",
          key.getOcspMode(),
          key.getGzipDisabled(),
          key.getProxyHttpProtocol(),
          key.getProxyHost(),
          key.getProxyPort(),
          key.getNonProxyHosts(),
          key.getProxyUser(),
          key.getProxyPassword().isEmpty() ? "not set" : "set");
    } else {
      logger.debug(
          "Driver OCSP mode: {}, gzip disabled: {} and no proxy",
          key.getOcspMode(),
          key.getGzipDisabled());
    }
  }

  /** Unset invalid proxy host and port values. */
  public void unsetInvalidProxyHostAndPort() {
    // If proxyHost and proxyPort are used without http or https unset them, so they are not used
    // later by the ProxySelector.
    if (!isNullOrEmpty(systemGetProperty("proxyHost"))) {
      System.clearProperty("proxyHost");
    }
    if (!isNullOrEmpty(systemGetProperty("proxyPort"))) {
      System.clearProperty("proxyPort");
    }
  }

  /**
   * Get OCSP mode
   *
   * @return {@link OCSPMode}
   * @throws SnowflakeSQLException
   */
  public OCSPMode getOCSPMode() throws SnowflakeSQLException {
    OCSPMode ret;

    Boolean disableOCSPChecks =
        (Boolean) connectionPropertiesMap.get(SFSessionProperty.DISABLE_OCSP_CHECKS);
    Boolean insecureMode = (Boolean) connectionPropertiesMap.get(SFSessionProperty.INSECURE_MODE);
    if (insecureMode != null && insecureMode) {
      logger.warn(
          "The 'insecureMode' connection property is deprecated. Please use 'disableOCSPChecks' instead.");
    }

    if ((disableOCSPChecks != null && insecureMode != null)
        && (disableOCSPChecks != insecureMode)) {
      logger.error(
          "The values for 'disableOCSPChecks' and 'insecureMode' must be identical. "
              + "Please unset insecureMode.");
      throw new SnowflakeSQLException(
          ErrorCode.DISABLEOCSP_INSECUREMODE_VALUE_MISMATCH,
          "The values for 'disableOCSPChecks' and 'insecureMode' " + "must be identical.");
    }
    if ((disableOCSPChecks != null && disableOCSPChecks)
        || (insecureMode != null && insecureMode)) {
      // skip OCSP checks
      ret = OCSPMode.DISABLE_OCSP_CHECKS;
    } else if (!connectionPropertiesMap.containsKey(SFSessionProperty.OCSP_FAIL_OPEN)
        || (boolean) connectionPropertiesMap.get(SFSessionProperty.OCSP_FAIL_OPEN)) {
      // fail open (by default, not set)
      ret = OCSPMode.FAIL_OPEN;
    } else {
      // explicitly set ocspFailOpen=false
      ret = OCSPMode.FAIL_CLOSED;
    }
    return ret;
  }

  /**
   * Get the query timeout
   *
   * @return the query timeout value
   */
  public Integer getQueryTimeout() {
    return (Integer) this.connectionPropertiesMap.get(SFSessionProperty.QUERY_TIMEOUT);
  }

  /**
   * Get the user name
   *
   * @return user name
   */
  public String getUser() {
    return (String) this.connectionPropertiesMap.get(SFSessionProperty.USER);
  }

  /**
   * Get the server URL
   *
   * @return the server URL
   */
  public String getUrl() {
    return (String) this.connectionPropertiesMap.get(SFSessionProperty.SERVER_URL);
  }

  /**
   * Get inject wait input
   *
   * @return the value of 'inject_wait_in_put' or 0 if not set
   */
  public int getInjectWaitInPut() {
    Object retVal = this.connectionPropertiesMap.get(SFSessionProperty.INJECT_WAIT_IN_PUT);
    if (retVal != null) {
      try {
        return (int) retVal;
      } catch (Exception e) {
        return 0;
      }
    }
    return 0;
  }

  /**
   * Get whether the metadata request should use the session database.
   *
   * @return true if it should use the session database
   */
  public boolean getMetadataRequestUseSessionDatabase() {
    return metadataRequestUseSessionDatabase;
  }

  /**
   * Set to true if the metadata request should use the session database.
   *
   * @param enabled boolean
   */
  public void setMetadataRequestUseSessionDatabase(boolean enabled) {
    this.metadataRequestUseSessionDatabase = enabled;
  }

  /**
   * Get if metadata request should use the connection ctx
   *
   * @return true if it should use the connection ctx
   */
  public boolean getMetadataRequestUseConnectionCtx() {
    return this.metadataRequestUseConnectionCtx;
  }

  /**
   * Set to true if metadata request should use connection ctx
   *
   * @param enabled boolean
   */
  public void setMetadataRequestUseConnectionCtx(boolean enabled) {
    this.metadataRequestUseConnectionCtx = enabled;
  }

  /**
   * Get injected delay
   *
   * @return {@link AtomicInteger}
   */
  AtomicInteger getInjectedDelay() {
    return _injectedDelay;
  }

  /**
   * Set the injected delay
   *
   * @param injectedDelay injectedDelay value
   */
  public void setInjectedDelay(int injectedDelay) {
    this._injectedDelay.set(injectedDelay);
  }

  /**
   * Get if NTZ should be treated as UTC
   *
   * @return true if NTZ should be treated as UTC
   */
  public boolean getTreatNTZAsUTC() {
    return treatNTZAsUTC;
  }

  /**
   * Set whether NTZ should be treated as UTC
   *
   * @param treatNTZAsUTC boolean
   */
  public void setTreatNTZAsUTC(boolean treatNTZAsUTC) {
    this.treatNTZAsUTC = treatNTZAsUTC;
  }

  /**
   * Get if heartbeat is enabled
   *
   * @return true if enabled
   */
  public boolean getEnableHeartbeat() {
    return enableHeartbeat;
  }

  /**
   * Set if heartbeat is enabled
   *
   * @param enableHeartbeat boolean
   */
  public void setEnableHeartbeat(boolean enableHeartbeat) {
    this.enableHeartbeat = enableHeartbeat;
  }

  /**
   * Set the heartbeat frequency in seconds. This is the frequency with which the session token is
   * refreshed.
   *
   * @param frequency heartbeat frequency in seconds
   */
  public void setHeartbeatFrequency(int frequency) {
    if (frequency < 900) {
      this.heartbeatFrequency = 900;
    } else if (frequency > 3600) {
      this.heartbeatFrequency = 3600;
    } else {
      this.heartbeatFrequency = frequency;
    }
  }

  /**
   * Retrieve session heartbeat frequency in seconds
   *
   * @return the heartbeat frequency in seconds
   */
  public int getHeartbeatFrequency() {
    return this.heartbeatFrequency;
  }

  /**
   * autoCommit field specifies whether autocommit is enabled for the session. Autocommit determines
   * whether a DML statement, when executed without an active transaction, is automatically
   * committed after the statement successfully completes. default: true
   *
   * @see <a
   *     href="https://docs.snowflake.com/en/sql-reference/transactions#label-txn-autocommit">Transactions/Autocommit</a>
   * @return a boolean value of autocommit field
   */
  public boolean getAutoCommit() {
    return autoCommit.get();
  }

  /**
   * Sets value of autoCommit field
   *
   * @see SFBaseSession#getAutoCommit()
   * @param autoCommit boolean
   */
  public void setAutoCommit(boolean autoCommit) {
    this.autoCommit.set(autoCommit);
  }

  /**
   * Get if date should be formatted with timezone
   *
   * @return true if date should be formatted with timezone
   */
  public boolean getFormatDateWithTimezone() {
    return formatDateWithTimezone;
  }

  /**
   * Set if date should be formatted with timezone
   *
   * @param formatDateWithTimezone boolean
   */
  public void setFormatDateWithTimezone(boolean formatDateWithTimezone) {
    this.formatDateWithTimezone = formatDateWithTimezone;
  }

  /**
   * Get if session timezone should be used.
   *
   * @return true if using session timezone
   */
  public boolean getUseSessionTimezone() {
    return useSessionTimezone;
  }

  /**
   * Get if using default date format with timezone.
   *
   * @return true if using default date format with timezone.
   */
  public boolean getDefaultFormatDateWithTimezone() {
    return defaultFormatDateWithTimezone;
  }

  /**
   * Set if session timezone should be used.
   *
   * @param useSessionTimezone boolean
   */
  public void setUseSessionTimezone(boolean useSessionTimezone) {
    this.useSessionTimezone = useSessionTimezone;
  }

  /**
   * Set if default date format with timezone should be used
   *
   * @param defaultFormatDateWithTimezone boolean
   */
  public void setDefaultFormatDateWithTimezone(boolean defaultFormatDateWithTimezone) {
    this.defaultFormatDateWithTimezone = defaultFormatDateWithTimezone;
  }

  public boolean getGetDateUseNullTimezone() {
    return getDateUseNullTimezone;
  }

  public void setGetDateUseNullTimezone(boolean getDateUseNullTimezone) {
    this.getDateUseNullTimezone = getDateUseNullTimezone;
  }

  public boolean getEnableCombineDescribe() {
    return enableCombineDescribe;
  }

  public void setEnableCombineDescribe(boolean enableCombineDescribe) {
    this.enableCombineDescribe = enableCombineDescribe;
  }

  public boolean isClientTelemetryEnabled() {
    return clientTelemetryEnabled;
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

  public boolean getStoreTemporaryCredential() {
    return storeTemporaryCredential;
  }

  public void setStoreTemporaryCredential(boolean storeTemporaryCredential) {
    this.storeTemporaryCredential = storeTemporaryCredential;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public void setEnableConservativeMemoryUsage(boolean enableConservativeMemoryUsage) {
    this.enableConservativeMemoryUsage = enableConservativeMemoryUsage;
  }

  public boolean isConservativeMemoryUsageEnabled() {
    return enableConservativeMemoryUsage;
  }

  public int getConservativeMemoryAdjustStep() {
    return conservativeMemoryAdjustStep;
  }

  public void setConservativeMemoryAdjustStep(int conservativeMemoryAdjustStep) {
    this.conservativeMemoryAdjustStep = conservativeMemoryAdjustStep;
  }

  public int getClientMemoryLimit() {
    return clientMemoryLimit;
  }

  public void setClientMemoryLimit(int clientMemoryLimit) {
    this.clientMemoryLimit = clientMemoryLimit;
  }

  public int getQueryContextCacheSize() {
    return queryContextCacheSize;
  }

  public void setQueryContextCacheSize(int queryContextCacheSize) {
    this.queryContextCacheSize = queryContextCacheSize;
  }

  public boolean getJdbcEnablePutGet() {
    return jdbcEnablePutGet;
  }

  public void setJdbcEnablePutGet(boolean jdbcEnablePutGet) {
    this.jdbcEnablePutGet = jdbcEnablePutGet;
  }

  public boolean getEnablePutGet() {
    return enablePutGet;
  }

  public boolean setEnablePutGet(boolean enablePutGet) {
    return this.enablePutGet = enablePutGet;
  }

  public boolean getEnablePatternSearch() {
    return enablePatternSearch;
  }

  public void setEnablePatternSearch(boolean enablePatternSearch) {
    this.enablePatternSearch = enablePatternSearch;
  }

  public boolean getEnableExactSchemaSearch() {
    return enableExactSchemaSearch;
  }

  void setEnableExactSchemaSearch(boolean enableExactSchemaSearch) {
    this.enableExactSchemaSearch = enableExactSchemaSearch;
  }

  public boolean getDisableGcsDefaultCredentials() {
    return disableGcsDefaultCredentials;
  }

  public void setDisableGcsDefaultCredentials(boolean disableGcsDefaultCredentials) {
    this.disableGcsDefaultCredentials = disableGcsDefaultCredentials;
  }

  public int getClientResultChunkSize() {
    return clientResultChunkSize;
  }

  public void setClientResultChunkSize(int clientResultChunkSize) {
    this.clientResultChunkSize = clientResultChunkSize;
  }

  public Object getOtherParameter(String key) {
    return this.otherParameters.get(key);
  }

  public void setOtherParameter(String key, Object value) {
    this.otherParameters.put(key, value);
  }

  public int getClientPrefetchThreads() {
    return clientPrefetchThreads;
  }

  public void setClientPrefetchThreads(int clientPrefetchThreads) {
    this.clientPrefetchThreads = clientPrefetchThreads;
  }

  public boolean getValidateDefaultParameters() {
    return validateDefaultParameters;
  }

  public void setValidateDefaultParameters(boolean validateDefaultParameters) {
    this.validateDefaultParameters = validateDefaultParameters;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    if (!isNullOrEmpty(database)) {
      this.database = database;
    }
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    if (!isNullOrEmpty(schema)) {
      this.schema = schema;
    }
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
    if (!isNullOrEmpty(warehouse)) {
      this.warehouse = warehouse;
    }
  }

  public void setUseRegionalS3EndpointsForPresignedURL(boolean regionalS3Endpoint) {
    this.useRegionalS3EndpointsForPresignedURL = regionalS3Endpoint;
  }

  public boolean getUseRegionalS3EndpointsForPresignedURL() {
    return useRegionalS3EndpointsForPresignedURL;
  }

  public String getArrayBindStage() {
    return arrayBindStage;
  }

  public void setArrayBindStage(String arrayBindStage) {
    this.arrayBindStage = String.format("%s.%s.%s", getDatabase(), getSchema(), arrayBindStage);
  }

  /**
   * Enables setting a value in the custom-properties map. This is used for properties that are
   * implementation specific to the session, and not shared by the different implementations.
   *
   * @param propertyName A string key for the property to set.
   * @param propertyValue The property value.
   */
  public void setSessionPropertyByKey(String propertyName, Object propertyValue) {
    this.customSessionProperties.put(propertyName, propertyValue);
  }

  /**
   * Fetch the value for a custom session property.
   *
   * @param propertyName The key of the session property to fetch.
   * @return session property value
   */
  public Object getSessionPropertyByKey(String propertyName) {
    return this.customSessionProperties.get(propertyName);
  }

  /**
   * Function that checks if the active session can be closed when the connection is closed. Called
   * by SnowflakeConnectionV1.
   *
   * @return true if the active session is safe to close.
   */
  public abstract boolean isSafeToClose();

  /**
   * @param queryID query ID of the query whose status is being investigated
   * @return enum of type QueryStatus indicating the query's status
   * @deprecated Use {@link #getQueryStatusV2(String)}
   * @throws SQLException if error encountered
   */
  @Deprecated
  public abstract QueryStatus getQueryStatus(String queryID) throws SQLException;

  /**
   * @param queryID query ID of the query whose status is being investigated
   * @return QueryStatusV2 indicating the query's status
   * @throws SQLException if error encountered
   */
  public abstract QueryStatusV2 getQueryStatusV2(String queryID) throws SQLException;

  /**
   * Validates the connection properties used by this session, and returns a list of missing
   * properties.
   *
   * @return List of DriverPropertyInfo
   */
  public abstract List<DriverPropertyInfo> checkProperties();

  /**
   * Close the connection
   *
   * @throws SnowflakeSQLException if failed to close the connection
   * @throws SFException if failed to close the connection
   */
  public abstract void close() throws SFException, SnowflakeSQLException;

  /**
   * @return Returns the telemetry client, if supported, by this session. If not, should return a
   *     NoOpTelemetryClient.
   */
  public abstract Telemetry getTelemetryClient();

  /**
   * Makes a heartbeat call to check for session validity.
   *
   * @param timeout timeout value
   * @throws Exception if exception occurs
   * @throws SFException if exception occurs
   */
  public abstract void callHeartBeat(int timeout) throws Exception, SFException;

  /**
   * JDBC API. Returns a list of warnings generated since starting this session, or the last time it
   * was cleared.
   *
   * @return List of SFException's
   */
  public List<SFException> getSqlWarnings() {
    return sqlWarnings;
  }

  /**
   * JDBC API. Clears the list of warnings generated since the start of the session, or the last
   * time it was cleared.
   */
  public void clearSqlWarnings() {
    sqlWarnings.clear();
  }

  /**
   * Get the SFConnectionHandler
   *
   * @return {@link SFConnectionHandler}
   */
  public SFConnectionHandler getSfConnectionHandler() {
    return sfConnectionHandler;
  }

  /**
   * Get network timeout in milliseconds
   *
   * @return network timeout in milliseconds
   */
  public abstract int getNetworkTimeoutInMilli();

  /**
   * @return auth timeout in seconds
   */
  public abstract int getAuthTimeout();

  /**
   * @return max http retries
   */
  public abstract int getMaxHttpRetries();

  /**
   * @return {@link SnowflakeConnectString}
   */
  public abstract SnowflakeConnectString getSnowflakeConnectionString();

  /**
   * @return true if this is an async session
   */
  public abstract boolean isAsyncSession();

  /**
   * @return QueryContextDTO containing opaque information shared with the cloud service.
   */
  public abstract QueryContextDTO getQueryContextDTO();

  /**
   * Set query context
   *
   * @param queryContext the query context string
   */
  public abstract void setQueryContext(String queryContext);

  /**
   * @return If true, JDBC will enable returning TIMESTAMP_WITH_TIMEZONE as column type, otherwise
   *     it will not. This function will always return true for JDBC client, so that the client JDBC
   *     will not have any behavior change. Stored proc JDBC will override this function to return
   *     the value of SP_JDBC_ENABLE_TIMESTAMP_WITH_TIMEZONE from server for backward compatibility.
   */
  public boolean getEnableReturnTimestampWithTimeZone() {
    return enableReturnTimestampWithTimeZone;
  }

  boolean getImplicitServerSideQueryTimeout() {
    return implicitServerSideQueryTimeout;
  }

  void setImplicitServerSideQueryTimeout(boolean value) {
    this.implicitServerSideQueryTimeout = value;
  }

  void setClearBatchOnlyAfterSuccessfulExecution(boolean value) {
    this.clearBatchOnlyAfterSuccessfulExecution = value;
  }

  @SnowflakeJdbcInternalApi
  public boolean getClearBatchOnlyAfterSuccessfulExecution() {
    return this.clearBatchOnlyAfterSuccessfulExecution;
  }
}
