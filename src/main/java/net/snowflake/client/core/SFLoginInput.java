/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.PrivateKey;
import java.util.Map;
import net.snowflake.client.jdbc.ErrorCode;

/** A class for holding all information required for login */
public class SFLoginInput {
  private static int DEFAULT_HTTP_CLIENT_CONNECTION_TIMEOUT = 60000; // millisec
  private static int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300000; // millisec

  private String serverUrl;
  private String databaseName;
  private String schemaName;
  private String warehouse;
  private String role;
  private boolean validateDefaultParameters;
  private String authenticator;
  private String oktaUserName;
  private String accountName;
  private int loginTimeout = -1; // default is invalid
  private int authTimeout = 0;
  private String userName;
  private String password;
  private boolean passcodeInPassword;
  private String passcode;
  private String token;
  private int connectionTimeout = DEFAULT_HTTP_CLIENT_CONNECTION_TIMEOUT;
  private int socketTimeout = DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT;
  private String appId;
  private String appVersion;
  private String sessionToken;
  private String masterToken;
  private Map<String, Object> sessionParameters;
  private PrivateKey privateKey;
  private String application;
  private String idToken;
  private String mfaToken;
  private String serviceName;
  private OCSPMode ocspMode;
  private HttpClientSettingsKey httpClientKey;
  private String privateKeyFile;
  private String privateKeyFilePwd;

  SFLoginInput() {}

  public String getServerUrl() {
    return serverUrl;
  }

  SFLoginInput setServerUrl(String serverUrl) {
    this.serverUrl = serverUrl;
    return this;
  }

  String getDatabaseName() {
    return databaseName;
  }

  SFLoginInput setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
    return this;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public SFLoginInput setSchemaName(String schemaName) {
    this.schemaName = schemaName;
    return this;
  }

  public String getWarehouse() {
    return warehouse;
  }

  public SFLoginInput setWarehouse(String warehouse) {
    this.warehouse = warehouse;
    return this;
  }

  public String getRole() {
    return role;
  }

  public SFLoginInput setRole(String role) {
    this.role = role;
    return this;
  }

  public boolean isValidateDefaultParameters() {
    return validateDefaultParameters;
  }

  public SFLoginInput setValidateDefaultParameters(Object v) {
    validateDefaultParameters = getBooleanValue(v);
    return this;
  }

  public String getAuthenticator() {
    return authenticator;
  }

  public SFLoginInput setAuthenticator(String authenticator) {
    this.authenticator = authenticator;
    return this;
  }

  public String getOKTAUserName() {
    return oktaUserName;
  }

  public SFLoginInput setOKTAUserName(String oktaUserName) {
    this.oktaUserName = oktaUserName;
    return this;
  }

  public String getAccountName() {
    return accountName;
  }

  public SFLoginInput setAccountName(String accountName) {
    this.accountName = accountName;
    return this;
  }

  int getLoginTimeout() {
    return loginTimeout;
  }

  SFLoginInput setLoginTimeout(int loginTimeout) {
    this.loginTimeout = loginTimeout;
    return this;
  }

  int getAuthTimeout() {
    return authTimeout;
  }

  SFLoginInput setAuthTimeout(int authTimeout) {
    this.authTimeout = authTimeout;
    return this;
  }

  public String getUserName() {
    return userName;
  }

  SFLoginInput setUserName(String userName) {
    this.userName = userName;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public SFLoginInput setPassword(String password) {
    this.password = password;
    return this;
  }

  String getPasscode() {
    return passcode;
  }

  SFLoginInput setPasscode(String passcode) {
    this.passcode = passcode;
    return this;
  }

  public String getToken() {
    return token;
  }

  public SFLoginInput setToken(String token) {
    this.token = token;
    return this;
  }

  int getConnectionTimeout() {
    return connectionTimeout;
  }

  SFLoginInput setConnectionTimeout(int connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
    return this;
  }

  int getSocketTimeout() {
    return socketTimeout;
  }

  SFLoginInput setSocketTimeout(int socketTimeout) {
    this.socketTimeout = socketTimeout;
    return this;
  }

  boolean isPasscodeInPassword() {
    return passcodeInPassword;
  }

  SFLoginInput setPasscodeInPassword(boolean passcodeInPassword) {
    this.passcodeInPassword = passcodeInPassword;
    return this;
  }

  String getAppId() {
    return appId;
  }

  SFLoginInput setAppId(String appId) {
    this.appId = appId;
    return this;
  }

  String getAppVersion() {
    return appVersion;
  }

  SFLoginInput setAppVersion(String appVersion) {
    this.appVersion = appVersion;
    return this;
  }

  public String getSessionToken() {
    return sessionToken;
  }

  public SFLoginInput setSessionToken(String sessionToken) {
    this.sessionToken = sessionToken;
    return this;
  }

  String getMasterToken() {
    return masterToken;
  }

  SFLoginInput setMasterToken(String masterToken) {
    this.masterToken = masterToken;
    return this;
  }

  String getIdToken() {
    return idToken;
  }

  SFLoginInput setIdToken(String idToken) {
    this.idToken = idToken;
    return this;
  }

  String getMfaToken() {
    return mfaToken;
  }

  SFLoginInput setMfaToken(String mfaToken) {
    this.mfaToken = mfaToken;
    return this;
  }

  Map<String, Object> getSessionParameters() {
    return sessionParameters;
  }

  SFLoginInput setSessionParameters(Map<String, Object> sessionParameters) {
    this.sessionParameters = sessionParameters;
    return this;
  }

  PrivateKey getPrivateKey() {
    return privateKey;
  }

  SFLoginInput setPrivateKey(PrivateKey privateKey) {
    this.privateKey = privateKey;
    return this;
  }

  SFLoginInput setPrivateKeyFile(String privateKeyFile) {
    this.privateKeyFile = privateKeyFile;
    return this;
  }

  SFLoginInput setPrivateKeyFilePwd(String privateKeyFilePwd) {
    this.privateKeyFilePwd = privateKeyFilePwd;
    return this;
  }

  String getPrivateKeyFile() {
    return privateKeyFile;
  }

  String getPrivateKeyFilePwd() {
    return privateKeyFilePwd;
  }

  public String getApplication() {
    return application;
  }

  public SFLoginInput setApplication(String application) {
    this.application = application;
    return this;
  }

  String getServiceName() {
    return serviceName;
  }

  SFLoginInput setServiceName(String serviceName) {
    this.serviceName = serviceName;
    return this;
  }

  OCSPMode getOCSPMode() {
    return ocspMode;
  }

  SFLoginInput setOCSPMode(OCSPMode ocspMode) {
    this.ocspMode = ocspMode;
    return this;
  }

  HttpClientSettingsKey getHttpClientSettingsKey() {
    return httpClientKey;
  }

  SFLoginInput setHttpClientSettingsKey(HttpClientSettingsKey key) {
    this.httpClientKey = key;
    return this;
  }

  static boolean getBooleanValue(Object v) {
    if (v instanceof Boolean) {
      return (Boolean) v;
    } else if (v instanceof String) {
      return !Boolean.FALSE.toString().equalsIgnoreCase((String) v)
          && !"off".equalsIgnoreCase((String) v)
          && (Boolean.TRUE.toString().equalsIgnoreCase((String) v)
              || "on".equalsIgnoreCase((String) v));
    }
    return false;
  }

  String getHostFromServerUrl() throws SFException {
    URL url;
    try {
      url = new URL(serverUrl);
    } catch (MalformedURLException e) {
      throw new SFException(
          e, ErrorCode.INTERNAL_ERROR, "Invalid serverUrl for retrieving host name");
    }
    return url.getHost();
  }
}
