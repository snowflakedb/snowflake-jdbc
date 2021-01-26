package net.snowflake.client.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;

public class SessionProperties {
  private final Properties clientInfo = new Properties();
  private String databaseVersion = null;
  private int databaseMajorVersion = 0;
  private int databaseMinorVersion = 0;
  private boolean showStatementParameters;
  private SnowflakeType timestampMappedType;
  private boolean isResultColumnCaseInsensitive;
  private boolean isJdbcTreatDecimalAsInt;
  private String injectFileUploadFailure;
  private boolean preparedStatementLogging = false;
  private boolean treatNTZAsUTC;
  private boolean enableHeartbeat;
  private final AtomicBoolean autoCommit = new AtomicBoolean(true);
  private boolean formatDateWithTimezone;
  private boolean enableCombineDescribe;
  private boolean clientTelemetryEnabled;
  private boolean useSessionTimezone;
  // The server can read array binds from a stage instead of query payload.
  // When there as many bind values as this threshold, we should upload them to a stage.
  private int arrayBindStageThreshold;
  private boolean storeTemporaryCredential;
  private String serviceName;
  // whether to enable conservative memory usage mode
  private boolean enableConservativeMemoryUsage;
  // the step in MB to adjust memory usage
  private int conservativeMemoryAdjustStep = 64;
  private int clientMemoryLimit;
  private int clientResultChunkSize;
  private int clientPrefetchThreads;
  // validate the default parameters by GS?
  private boolean validateDefaultParameters;
  private String database;
  private String schema;
  private String role;
  private String warehouse;
  private String sessionId;
  private boolean sfSQLMode;
  // Injected delay for the purpose of connection timeout testing
  // Any statement execution will sleep for the specified number of milliseconds
  private final AtomicInteger _injectedDelay = new AtomicInteger(0);
  // For Metadata request(i.e. DatabaseMetadata.getTables or
  // DatabaseMetadata.getSchemas,), whether to use connection ctx to
  // improve the request time
  private boolean metadataRequestUseConnectionCtx = false;
  // For Metadata request(i.e. DatabaseMetadata.getTables or
  // DatabaseMetadata.getSchemas), whether to search using multiple schemas with
  // session database
  private boolean metadataRequestUseSessionDatabase = false;
  private final Map<SFConnectionProperty, Object> connectionPropertiesMap = new HashMap<>();

  public Properties getClientInfo() {
    // defensive copy to avoid client from changing the properties
    // directly w/o going through the API
    Properties copy = new Properties();
    copy.putAll(this.clientInfo);
    return copy;
  }

  public String getClientInfo(String name) {
    return this.clientInfo.getProperty(name);
  }

  public String getSessionId() {
    return sessionId;
  }

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

  public boolean isShowStatementParameters() {
    return showStatementParameters;
  }

  public void setShowStatementParameters(boolean showStatementParameters) {
    this.showStatementParameters = showStatementParameters;
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

  public void setInjectedDelay(int injectedDelay) {
    this._injectedDelay.set(injectedDelay);
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
    if (connectionPropertiesMap.containsKey(SFConnectionProperty.SERVER_URL)) {
      return (String) connectionPropertiesMap.get(SFConnectionProperty.SERVER_URL);
    }
    return null;
  }

  public boolean isStringQuoted() {
    if (connectionPropertiesMap.containsKey(SFConnectionProperty.STRINGS_QUOTED)) {
      return (Boolean) connectionPropertiesMap.get(SFConnectionProperty.STRINGS_QUOTED);
    }
    return false;
  }

  public void addProperty(SFConnectionProperty sfConnectionProperty, Object propertyValue)
      throws SFException {
    addProperty(sfConnectionProperty.getPropertyKey(), propertyValue);
  }

  public void addProperty(String propertyName, Object propertyValue) throws SFException {
    SFConnectionProperty connectionProperty = SFConnectionProperty.lookupByKey(propertyName);
    // check if the value type is as expected
    propertyValue = SFConnectionProperty.checkPropertyValue(connectionProperty, propertyValue);

    if (connectionPropertiesMap.containsKey(connectionProperty)) {
      throw new SFException(ErrorCode.DUPLICATE_CONNECTION_PROPERTY_SPECIFIED, propertyName);
    } else if (propertyValue != null && connectionProperty == SFConnectionProperty.AUTHENTICATOR) {
      String[] authenticatorWithParams = propertyValue.toString().split(";");
      if (authenticatorWithParams.length == 1) {
        connectionPropertiesMap.put(connectionProperty, propertyValue);
      } else {
        String[] oktaUserKeyPair = authenticatorWithParams[1].split("=");
        if (oktaUserKeyPair.length == 2) {
          connectionPropertiesMap.put(connectionProperty, authenticatorWithParams[0]);
          connectionPropertiesMap.put(SFConnectionProperty.OKTA_USERNAME, oktaUserKeyPair[1]);
        } else {
          throw new SFException(ErrorCode.INVALID_OKTA_USERNAME, propertyName);
        }
      }
    } else {
      connectionPropertiesMap.put(connectionProperty, propertyValue);
    }
  }

  public Map<SFConnectionProperty, Object> getConnectionPropertiesMap() {
    return connectionPropertiesMap;
  }

  public OCSPMode getOCSPMode() {
    OCSPMode ret;

    Boolean insecureMode =
        (Boolean) connectionPropertiesMap.get(SFConnectionProperty.INSECURE_MODE);
    if (insecureMode != null && insecureMode) {
      // skip OCSP checks
      ret = OCSPMode.INSECURE;
    } else if (!connectionPropertiesMap.containsKey(SFConnectionProperty.OCSP_FAIL_OPEN)
        || (boolean) connectionPropertiesMap.get(SFConnectionProperty.OCSP_FAIL_OPEN)) {
      // fail open (by default, not set)
      ret = OCSPMode.FAIL_OPEN;
    } else {
      // explicitly set ocspFailOpen=false
      ret = OCSPMode.FAIL_CLOSED;
    }
    return ret;
  }

  public Integer getQueryTimeout() {
    return (Integer) this.connectionPropertiesMap.get(SFConnectionProperty.QUERY_TIMEOUT);
  }

  public String getUser() {
    return (String) this.connectionPropertiesMap.get(SFConnectionProperty.USER);
  }

  public String getUrl() {
    return (String) this.connectionPropertiesMap.get(SFConnectionProperty.SERVER_URL);
  }

  public int getInjectWaitInPut() {
    Object retVal = this.connectionPropertiesMap.get(SFConnectionProperty.INJECT_WAIT_IN_PUT);
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

  AtomicInteger injectedDelay() {
    return _injectedDelay;
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
}
