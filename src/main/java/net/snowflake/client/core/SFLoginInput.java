package net.snowflake.client.core;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.PrivateKey;
import java.time.Duration;
import java.util.Map;
import net.snowflake.client.core.auth.wif.WorkloadIdentityAttestation;
import net.snowflake.client.jdbc.ErrorCode;
import org.apache.http.client.methods.HttpRequestBase;

/** A class for holding all information required for login */
public class SFLoginInput {
  private String serverUrl;
  private String databaseName;
  private String schemaName;
  private String warehouse;
  private String role;
  private boolean validateDefaultParameters;
  private String originalAuthenticator;
  private String authenticator;
  private String oktaUserName;
  private String accountName;
  private int loginTimeout = -1; // default is invalid
  private int retryTimeout = 300;
  private int authTimeout = 0;
  private String userName;
  private String password;
  private boolean passcodeInPassword;
  private String passcode;
  private String token;
  private Duration connectionTimeout = HttpUtil.getConnectionTimeout();
  private Duration socketTimeout = HttpUtil.getSocketTimeout();
  private String appId;
  private String appVersion;
  private String sessionToken;
  private String masterToken;
  private Map<String, Object> sessionParameters;
  private PrivateKey privateKey;
  private String application;
  private String idToken;
  private String mfaToken;
  private String oauthAccessToken;
  private String oauthRefreshToken;
  private String dpopPublicKey;
  private boolean dpopEnabled = false;
  private String serviceName;
  private OCSPMode ocspMode;
  private HttpClientSettingsKey httpClientKey;
  private String privateKeyFile;
  private String privateKeyBase64;
  private String privateKeyPwd;
  private String inFlightCtx; // Opaque string sent for Snowsight account activation

  private SFOauthLoginInput oauthLoginInput;

  private boolean disableConsoleLogin = true;
  private boolean disableSamlURLCheck = false;
  private boolean enableClientStoreTemporaryCredential;
  private boolean enableClientRequestMfaToken;

  // Workload Identity Federation
  private String workloadIdentityProvider;
  private WorkloadIdentityAttestation workloadIdentityAttestation;
  private String workloadIdentityEntraResource;

  // OAuth
  private int redirectUriPort = -1;
  private String clientId;
  private String clientSecret;

  private Duration browserResponseTimeout;

  // Additional headers to add for Snowsight.
  Map<String, String> additionalHttpHeadersForSnowsight;

  @SnowflakeJdbcInternalApi
  public SFLoginInput() {}

  Duration getBrowserResponseTimeout() {
    return browserResponseTimeout;
  }

  SFLoginInput setBrowserResponseTimeout(Duration browserResponseTimeout) {
    this.browserResponseTimeout = browserResponseTimeout;
    return this;
  }

  public String getServerUrl() {
    return serverUrl;
  }

  @SnowflakeJdbcInternalApi
  public SFLoginInput setServerUrl(String serverUrl) {
    this.serverUrl = serverUrl;
    return this;
  }

  public boolean getDisableConsoleLogin() {
    return disableConsoleLogin;
  }

  SFLoginInput setDisableConsoleLogin(boolean disableConsoleLogin) {
    this.disableConsoleLogin = disableConsoleLogin;
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

  @SnowflakeJdbcInternalApi
  public int getLoginTimeout() {
    return loginTimeout;
  }

  // We want to choose the smaller of the two values between retryTimeout and loginTimeout for the
  // new retry strategy.
  SFLoginInput setLoginTimeout(int loginTimeout) {
    if (loginTimeout > retryTimeout && retryTimeout != 0) {
      this.loginTimeout = retryTimeout;
    } else {
      this.loginTimeout = loginTimeout;
    }
    return this;
  }

  int getRetryTimeout() {
    return retryTimeout;
  }

  SFLoginInput setRetryTimeout(int retryTimeout) {
    this.retryTimeout = retryTimeout;
    return this;
  }

  @SnowflakeJdbcInternalApi
  public int getAuthTimeout() {
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

  int getConnectionTimeoutInMillis() {
    return (int) connectionTimeout.toMillis();
  }

  SFLoginInput setConnectionTimeout(Duration connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
    return this;
  }

  @SnowflakeJdbcInternalApi
  public int getSocketTimeoutInMillis() {
    return (int) socketTimeout.toMillis();
  }

  @SnowflakeJdbcInternalApi
  public SFLoginInput setSocketTimeout(Duration socketTimeout) {
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

  String getOauthAccessToken() {
    return oauthAccessToken;
  }

  SFLoginInput setOauthAccessToken(String oauthAccessToken) {
    this.oauthAccessToken = oauthAccessToken;
    return this;
  }

  @SnowflakeJdbcInternalApi
  public String getOauthRefreshToken() {
    return oauthRefreshToken;
  }

  SFLoginInput setOauthRefreshToken(String oauthRefreshToken) {
    this.oauthRefreshToken = oauthRefreshToken;
    return this;
  }

  String getWorkloadIdentityProvider() {
    return workloadIdentityProvider;
  }

  SFLoginInput setWorkloadIdentityProvider(String workloadIdentityProvider) {
    this.workloadIdentityProvider = workloadIdentityProvider;
    return this;
  }

  @SnowflakeJdbcInternalApi
  public String getDPoPPublicKey() {
    return dpopPublicKey;
  }

  SFLoginInput setDPoPPublicKey(String dpopPublicKey) {
    this.dpopPublicKey = dpopPublicKey;
    return this;
  }

  @SnowflakeJdbcInternalApi
  public boolean isDPoPEnabled() {
    return dpopEnabled;
  }

  // Currently only used for testing purpose
  @SnowflakeJdbcInternalApi
  public void setDPoPEnabled(boolean dpopEnabled) {
    this.dpopEnabled = dpopEnabled;
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

  String getPrivateKeyBase64() {
    return privateKeyBase64;
  }

  SFLoginInput setPrivateKeyBase64(String privateKeyBase64) {
    this.privateKeyBase64 = privateKeyBase64;
    return this;
  }

  SFLoginInput setPrivateKeyFile(String privateKeyFile) {
    this.privateKeyFile = privateKeyFile;
    return this;
  }

  SFLoginInput setPrivateKeyPwd(String privateKeyPwd) {
    this.privateKeyPwd = privateKeyPwd;
    return this;
  }

  String getPrivateKeyFile() {
    return privateKeyFile;
  }

  String getPrivateKeyPwd() {
    return privateKeyPwd;
  }

  boolean isPrivateKeyProvided() {
    return (getPrivateKey() != null
        || getPrivateKeyFile() != null
        || getPrivateKeyBase64() != null);
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

  @SnowflakeJdbcInternalApi
  public HttpClientSettingsKey getHttpClientSettingsKey() {
    return httpClientKey;
  }

  @SnowflakeJdbcInternalApi
  public SFLoginInput setHttpClientSettingsKey(HttpClientSettingsKey key) {
    this.httpClientKey = key;
    return this;
  }

  // Opaque string sent for Snowsight account activation
  String getInFlightCtx() {
    return inFlightCtx;
  }

  // Opaque string sent for Snowsight account activation
  SFLoginInput setInFlightCtx(String inFlightCtx) {
    this.inFlightCtx = inFlightCtx;
    return this;
  }

  boolean getDisableSamlURLCheck() {
    return disableSamlURLCheck;
  }

  SFLoginInput setDisableSamlURLCheck(boolean disableSamlURLCheck) {
    this.disableSamlURLCheck = disableSamlURLCheck;
    return this;
  }

  public int getRedirectUriPort() {
    return redirectUriPort;
  }

  public SFLoginInput setRedirectUriPort(int redirectUriPort) {
    this.redirectUriPort = redirectUriPort;
    return this;
  }

  public String getClientId() {
    return clientId;
  }

  public SFLoginInput setClientId(String clientId) {
    this.clientId = clientId;
    return this;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public SFLoginInput setClientSecret(String clientSecret) {
    this.clientSecret = clientSecret;
    return this;
  }

  Map<String, String> getAdditionalHttpHeadersForSnowsight() {
    return additionalHttpHeadersForSnowsight;
  }
  /**
   * Set additional http headers to apply to the outgoing request. The additional headers cannot be
   * used to replace or overwrite a header in use by the driver. These will be applied to the
   * outgoing request. Primarily used by Snowsight, as described in {@link
   * HttpUtil#applyAdditionalHeadersForSnowsight(HttpRequestBase, Map)}
   *
   * @param additionalHttpHeaders The new headers to add
   * @return The input object, for chaining
   * @see HttpUtil#applyAdditionalHeadersForSnowsight(HttpRequestBase, Map)
   */
  public SFLoginInput setAdditionalHttpHeadersForSnowsight(
      Map<String, String> additionalHttpHeaders) {
    this.additionalHttpHeadersForSnowsight = additionalHttpHeaders;
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
      if (!serverUrl.startsWith("http")) {
        url = new URL("https://" + serverUrl);
      } else {
        url = new URL(serverUrl);
      }
    } catch (MalformedURLException e) {
      throw new SFException(
          e, ErrorCode.INTERNAL_ERROR, "Invalid serverUrl for retrieving host name");
    }
    return url.getHost();
  }

  boolean isEnableClientStoreTemporaryCredential() {
    return enableClientStoreTemporaryCredential;
  }

  SFLoginInput setEnableClientStoreTemporaryCredential(
      boolean enableClientStoreTemporaryCredential) {
    this.enableClientStoreTemporaryCredential = enableClientStoreTemporaryCredential;
    return this;
  }

  boolean isEnableClientRequestMfaToken() {
    return enableClientRequestMfaToken;
  }

  SFLoginInput setEnableClientRequestMfaToken(boolean enableClientRequestMfaToken) {
    this.enableClientRequestMfaToken = enableClientRequestMfaToken;
    return this;
  }

  @SnowflakeJdbcInternalApi
  public SFOauthLoginInput getOauthLoginInput() {
    return oauthLoginInput;
  }

  @SnowflakeJdbcInternalApi
  public SFLoginInput setOauthLoginInput(SFOauthLoginInput oauthLoginInput) {
    this.oauthLoginInput = oauthLoginInput;
    return this;
  }

  void restoreOriginalAuthenticator() {
    this.authenticator = this.originalAuthenticator;
  }

  String getOriginalAuthenticator() {
    return this.originalAuthenticator;
  }

  SFLoginInput setOriginalAuthenticator(String originalAuthenticator) {
    this.originalAuthenticator = originalAuthenticator;
    return this;
  }

  public void setWorkloadIdentityAttestation(
      WorkloadIdentityAttestation workloadIdentityAttestation) {
    this.workloadIdentityAttestation = workloadIdentityAttestation;
  }

  public WorkloadIdentityAttestation getWorkloadIdentityAttestation() {
    return workloadIdentityAttestation;
  }

  public String getWorkloadIdentityEntraResource() {
    return this.workloadIdentityEntraResource;
  }

  public SFLoginInput setWorkloadIdentityEntraResource(String workloadIdentityEntraResource) {
    this.workloadIdentityEntraResource = workloadIdentityEntraResource;
    return this;
  }
}
