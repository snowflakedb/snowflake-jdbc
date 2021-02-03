/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.sql.DriverPropertyInfo;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.telemetry.Telemetry;

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
    this.database = database;
  }

  public String getSchema() {
    return schema;
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
   * Raise an error within the current session. By default, this may log an incident with Snowflake.
   *
   * @param exc The throwable exception
   * @param jobId jobId that failed
   * @param requestId requestId that failed
   */
  public abstract void raiseError(Throwable exc, String jobId, String requestId);

  /**
   * Returns the telemetry client, if supported, by this session. If not, should return a
   * NoOpTelemetryClient.
   */
  public abstract Telemetry getTelemetryClient();

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
}
