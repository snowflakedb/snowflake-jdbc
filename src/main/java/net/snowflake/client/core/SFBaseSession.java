/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import com.google.common.base.Strings;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import net.snowflake.client.jdbc.*;
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
  static final SFLogger logger = SFLoggerFactory.getLogger(SFBaseSession.class);
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

  private Map<String, Object> commonParameters;

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
   */
  public Properties getClientInfo() {
    // defensive copy to avoid client from changing the properties
    // directly w/o going through the API
    Properties copy = new Properties();
    copy.putAll(this.clientInfo);
    return copy;
  }

  public void setCommonParameters(Map<String, Object> parameters) {
    this.commonParameters = parameters;
  }

  public Map<String, Object> getCommonParameters() {
    return this.commonParameters;
  }

  /**
   * Gets the Property associated with the key 'name' in the ClientInfo map.
   *
   * @param name The key from which to fetch the Property.
   */
  public String getClientInfo(String name) {
    return this.clientInfo.getProperty(name);
  }

  /** Returns a unique id for this session. */
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

  public boolean isSfSQLMode() {
    return sfSQLMode;
  }

  public void setSfSQLMode(boolean sfSQLMode) {
    this.sfSQLMode = sfSQLMode;
  }

  public String getDatabaseVersion() {
    return databaseVersion;
  }

  public void setDatabaseVersion(String databaseVersion) {
    this.databaseVersion = databaseVersion;
  }

  public int getDatabaseMajorVersion() {
    return databaseMajorVersion;
  }

  public void setDatabaseMajorVersion(int databaseMajorVersion) {
    this.databaseMajorVersion = databaseMajorVersion;
  }

  public int getDatabaseMinorVersion() {
    return databaseMinorVersion;
  }

  public void setDatabaseMinorVersion(int databaseMinorVersion) {
    this.databaseMinorVersion = databaseMinorVersion;
  }

  public boolean getPreparedStatementLogging() {
    return this.preparedStatementLogging;
  }

  public void setPreparedStatementLogging(boolean value) {
    this.preparedStatementLogging = value;
  }

  public String getInjectFileUploadFailure() {
    return this.injectFileUploadFailure;
  }

  public void setInjectFileUploadFailure(String fileToFail) {
    this.injectFileUploadFailure = fileToFail;
  }

  public SnowflakeType getTimestampMappedType() {
    return timestampMappedType;
  }

  public void setTimestampMappedType(SnowflakeType timestampMappedType) {
    this.timestampMappedType = timestampMappedType;
  }

  public boolean isResultColumnCaseInsensitive() {
    return isResultColumnCaseInsensitive;
  }

  public void setResultColumnCaseInsensitive(boolean resultColumnCaseInsensitive) {
    isResultColumnCaseInsensitive = resultColumnCaseInsensitive;
  }

  public boolean isJdbcTreatDecimalAsInt() {
    return isJdbcTreatDecimalAsInt;
  }

  public void setJdbcTreatDecimalAsInt(boolean jdbcTreatDecimalAsInt) {
    isJdbcTreatDecimalAsInt = jdbcTreatDecimalAsInt;
  }

  public String getServerUrl() {
    if (connectionPropertiesMap.containsKey(SFSessionProperty.SERVER_URL)) {
      return (String) connectionPropertiesMap.get(SFSessionProperty.SERVER_URL);
    }
    return null;
  }

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

  public Map<SFSessionProperty, Object> getConnectionPropertiesMap() {
    return connectionPropertiesMap;
  }

  public HttpClientSettingsKey getHttpClientKey() throws SnowflakeSQLException {
    // if key is already created, return it without making a new one
    if (ocspAndProxyAndGzipKey != null) {
      return ocspAndProxyAndGzipKey;
    }

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
              getOCSPMode(),
              proxyHost,
              proxyPort,
              nonProxyHosts,
              proxyUser,
              proxyPassword,
              proxyProtocol,
              userAgentSuffix,
              gzipDisabled);

      return ocspAndProxyAndGzipKey;
    }
    // If JVM proxy parameters are specified, proxies need to go through the JDBC driver's
    // HttpClientSettingsKey logic in order to work properly.
    else {
      boolean httpUseProxy = Boolean.parseBoolean(systemGetProperty("http.useProxy"));
      String httpProxyHost = systemGetProperty("http.proxyHost");
      String httpProxyPort = systemGetProperty("http.proxyPort");
      String httpsProxyHost = systemGetProperty("https.proxyHost");
      String httpsProxyPort = systemGetProperty("https.proxyPort");
      String httpProxyProtocol = systemGetProperty("http.proxyProtocol");
      String noProxy = systemGetEnv("NO_PROXY");
      String nonProxyHosts = systemGetProperty("http.nonProxyHosts");
      // log the JVM parameters that are being used
      if (httpUseProxy) {
        logger.debug(
            "http.useProxy={}, http.proxyHost={}, http.proxyPort={}, https.proxyHost={},"
                + " https.proxyPort={}, http.nonProxyHosts={}, NO_PROXY={}, http.proxyProtocol={}",
            httpUseProxy,
            httpProxyHost,
            httpProxyPort,
            httpsProxyHost,
            httpsProxyPort,
            nonProxyHosts,
            noProxy,
            httpProxyProtocol,
            userAgentSuffix,
            gzipDisabled);
        // There are 2 possible parameters for non proxy hosts that can be combined into 1
        String combinedNonProxyHosts = Strings.isNullOrEmpty(nonProxyHosts) ? "" : nonProxyHosts;
        if (!Strings.isNullOrEmpty(noProxy)) {
          combinedNonProxyHosts += combinedNonProxyHosts.length() == 0 ? "" : "|";
          combinedNonProxyHosts += noProxy;
        }

        // It is possible that a user can have both http and https proxies specified in the JVM
        // parameters. The default protocol is http.
        String proxyProtocol = "http";
        if (!Strings.isNullOrEmpty(httpProxyProtocol)) {
          proxyProtocol = httpProxyProtocol;
        } else if (!Strings.isNullOrEmpty(httpsProxyHost)
            && !Strings.isNullOrEmpty(httpsProxyPort)
            && Strings.isNullOrEmpty(httpProxyHost)
            && Strings.isNullOrEmpty(httpProxyPort)) {
          proxyProtocol = "https";
        }

        if (proxyProtocol.equals("https")
            && !Strings.isNullOrEmpty(httpsProxyHost)
            && !Strings.isNullOrEmpty(httpsProxyPort)) {
          int proxyPort;
          try {
            proxyPort = Integer.parseInt(httpsProxyPort);
          } catch (NumberFormatException | NullPointerException e) {
            throw new SnowflakeSQLException(
                ErrorCode.INVALID_PROXY_PROPERTIES, "Could not parse port number");
          }
          ocspAndProxyAndGzipKey =
              new HttpClientSettingsKey(
                  getOCSPMode(),
                  httpsProxyHost,
                  proxyPort,
                  combinedNonProxyHosts,
                  "", /* user = empty */
                  "", /* password = empty */
                  "https",
                  userAgentSuffix,
                  gzipDisabled);
        } else if (proxyProtocol.equals("http")
            && !Strings.isNullOrEmpty(httpProxyHost)
            && !Strings.isNullOrEmpty(httpProxyPort)) {
          int proxyPort;
          try {
            proxyPort = Integer.parseInt(httpProxyPort);
          } catch (NumberFormatException | NullPointerException e) {
            throw new SnowflakeSQLException(
                ErrorCode.INVALID_PROXY_PROPERTIES, "Could not parse port number");
          }
          ocspAndProxyAndGzipKey =
              new HttpClientSettingsKey(
                  getOCSPMode(),
                  httpProxyHost,
                  proxyPort,
                  combinedNonProxyHosts,
                  "", /* user = empty */
                  "", /* password = empty */
                  "http",
                  userAgentSuffix,
                  gzipDisabled);
        } else {
          // Not enough parameters set to use the proxy.
          logger.debug(
              "http.useProxy={} but valid host and port were not provided. No proxy in use.",
              httpUseProxy);
          ocspAndProxyAndGzipKey =
              new HttpClientSettingsKey(getOCSPMode(), userAgentSuffix, gzipDisabled);
        }
      } else {
        // If no proxy is used or JVM http proxy is used, no need for setting parameters
        logger.debug("http.useProxy={}. JVM proxy not used.", httpUseProxy);
        unsetInvalidProxyHostAndPort();
        ocspAndProxyAndGzipKey =
            new HttpClientSettingsKey(getOCSPMode(), userAgentSuffix, gzipDisabled);
      }
    }
    return ocspAndProxyAndGzipKey;
  }

  public void unsetInvalidProxyHostAndPort() {
    // If proxyHost and proxyPort are used without http or https unset them, so they are not used
    // later by the ProxySelector.
    if (!Strings.isNullOrEmpty(systemGetProperty("proxyHost"))) {
      System.clearProperty("proxyHost");
    }
    if (!Strings.isNullOrEmpty(systemGetProperty("proxyPort"))) {
      System.clearProperty("proxyPort");
    }
  }

  public OCSPMode getOCSPMode() {
    OCSPMode ret;

    Boolean insecureMode = (Boolean) connectionPropertiesMap.get(SFSessionProperty.INSECURE_MODE);
    if (insecureMode != null && insecureMode) {
      // skip OCSP checks
      ret = OCSPMode.INSECURE;
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

  public Integer getQueryTimeout() {
    return (Integer) this.connectionPropertiesMap.get(SFSessionProperty.QUERY_TIMEOUT);
  }

  public String getUser() {
    return (String) this.connectionPropertiesMap.get(SFSessionProperty.USER);
  }

  public String getUrl() {
    return (String) this.connectionPropertiesMap.get(SFSessionProperty.SERVER_URL);
  }

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

  public boolean getMetadataRequestUseSessionDatabase() {
    return metadataRequestUseSessionDatabase;
  }

  public void setMetadataRequestUseSessionDatabase(boolean enabled) {
    this.metadataRequestUseSessionDatabase = enabled;
  }

  public boolean getMetadataRequestUseConnectionCtx() {
    return this.metadataRequestUseConnectionCtx;
  }

  public void setMetadataRequestUseConnectionCtx(boolean enabled) {
    this.metadataRequestUseConnectionCtx = enabled;
  }

  AtomicInteger getInjectedDelay() {
    return _injectedDelay;
  }

  public void setInjectedDelay(int injectedDelay) {
    this._injectedDelay.set(injectedDelay);
  }

  public boolean getTreatNTZAsUTC() {
    return treatNTZAsUTC;
  }

  public void setTreatNTZAsUTC(boolean treatNTZAsUTC) {
    this.treatNTZAsUTC = treatNTZAsUTC;
  }

  public boolean getEnableHeartbeat() {
    return enableHeartbeat;
  }

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

  /** Retrieve session heartbeat frequency in seconds */
  public int getHeartbeatFrequency() {
    return this.heartbeatFrequency;
  }

  public boolean getAutoCommit() {
    return autoCommit.get();
  }

  public void setAutoCommit(boolean autoCommit) {
    this.autoCommit.set(autoCommit);
  }

  public boolean getFormatDateWithTimezone() {
    return formatDateWithTimezone;
  }

  public void setFormatDateWithTimezone(boolean formatDateWithTimezone) {
    this.formatDateWithTimezone = formatDateWithTimezone;
  }

  public boolean getUseSessionTimezone() {
    return useSessionTimezone;
  }

  public void setUseSessionTimezone(boolean useSessionTimezone) {
    this.useSessionTimezone = useSessionTimezone;
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
    if (!Strings.isNullOrEmpty(database)) {
      this.database = database;
    }
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    if (!Strings.isNullOrEmpty(schema)) {
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
    if (!Strings.isNullOrEmpty(warehouse)) {
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
   */
  public Object getSessionPropertyByKey(String propertyName) {
    return this.customSessionProperties.get(propertyName);
  }

  /**
   * Function that checks if the active session can be closed when the connection is closed. Called
   * by SnowflakeConnectionV1.
   */
  public abstract boolean isSafeToClose();

  /**
   * @param queryID query ID of the query whose status is being investigated
   * @return enum of type QueryStatus indicating the query's status
   * @throws SQLException
   */
  public abstract QueryStatus getQueryStatus(String queryID) throws SQLException;

  /**
   * Validates the connection properties used by this session, and returns a list of missing
   * properties.
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
   * Returns the telemetry client, if supported, by this session. If not, should return a
   * NoOpTelemetryClient.
   */
  public abstract Telemetry getTelemetryClient();

  /** Makes a heartbeat call to check for session validity. */
  public abstract void callHeartBeat(int timeout) throws Exception, SFException;

  /**
   * JDBC API. Returns a list of warnings generated since starting this session, or the last time it
   * was cleared.
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

  public SFConnectionHandler getSfConnectionHandler() {
    return sfConnectionHandler;
  }

  public abstract int getNetworkTimeoutInMilli();

  public abstract int getAuthTimeout();

  public abstract int getMaxHttpRetries();

  public abstract SnowflakeConnectString getSnowflakeConnectionString();

  public abstract boolean isAsyncSession();

  public abstract QueryContextDTO getQueryContextDTO();

  public abstract void setQueryContext(String queryContext);

  /**
   * If true, JDBC will enable returning TIMESTAMP_WITH_TIMEZONE as column type, otherwise it will
   * not. This function will always return true for JDBC client, so that the client JDBC will not
   * have any behavior change. Stored proc JDBC will override this function to return the value of
   * SP_JDBC_ENABLE_TIMESTAMP_WITH_TIMEZONE from server for backward compatibility.
   */
  public boolean getEnableReturnTimestampWithTimeZone() {
    return enableReturnTimestampWithTimeZone;
  }
}
