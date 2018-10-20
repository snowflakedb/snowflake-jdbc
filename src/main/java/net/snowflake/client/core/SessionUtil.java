/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeDriver;
import net.snowflake.client.jdbc.SnowflakeReauthenticationRequest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.ClientAuthnDTO;
import net.snowflake.common.core.ClientAuthnParameter;
import net.snowflake.common.core.SqlState;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static net.snowflake.client.core.SFTrustManager.resetOCSPResponseCacherServerURL;

/**
 * Created by jhuang on 1/23/16.
 */
public class SessionUtil
{
  public static final String SF_QUERY_DATABASE = "databaseName";

  public static final String SF_QUERY_SCHEMA = "schemaName";

  public static final String SF_QUERY_WAREHOUSE = "warehouse";

  public static final String SF_QUERY_ROLE = "roleName";

  public static final String SF_QUERY_REQUEST_ID = "requestId";

  public static final String SF_PATH_AUTHENTICATOR_REQUEST
      = "/session/authenticator-request";

  private static final String SF_PATH_LOGIN_REQUEST =
      "/session/v1/login-request";

  private static final String SF_PATH_TOKEN_REQUEST = "/session/token-request";

  public static final String SF_QUERY_SESSION_DELETE = "delete";

  private static final String SF_PATH_SESSION = "/session";

  private static ObjectMapper mapper = new ObjectMapper();

  public static final String SF_HEADER_AUTHORIZATION = HttpHeaders.AUTHORIZATION;

  public static final String SF_HEADER_BASIC_AUTHTYPE = "Basic";

  public static final String SF_HEADER_SNOWFLAKE_AUTHTYPE = "Snowflake";

  public static final String SF_HEADER_TOKEN_TAG = "Token";

  static final String SF_HEADER_SERVICE_NAME = "X-Snowflake-Service";

  private static int DEFAULT_HTTP_CLIENT_CONNECTION_TIMEOUT = 60000; // millisec

  private static int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300000; // millisec

  private static int DEFAULT_HEALTH_CHECK_INTERVAL = 45; // sec

  public static final String CLIENT_STORE_TEMPORARY_CREDENTIAL = "CLIENT_STORE_TEMPORARY_CREDENTIAL";
  public static final String SERVICE_NAME = "SERVICE_NAME";
  public static final String CLIENT_RESULT_COLUMN_CASE_INSENSITIVE = "CLIENT_RESULT_COLUMN_CASE_INSENSITIVE";
  public static final String JDBC_RS_COLUMN_CASE_INSENSITIVE = "JDBC_RS_COLUMN_CASE_INSENSITIVE";

  static final
  SFLogger logger = SFLoggerFactory.getLogger(SessionUtil.class);

  private static Set<String> STRING_PARAMS = new HashSet<>(Arrays.asList(
      "TIMEZONE",
      "TIMESTAMP_OUTPUT_FORMAT",
      "TIMESTAMP_NTZ_OUTPUT_FORMAT",
      "TIMESTAMP_LTZ_OUTPUT_FORMAT",
      "TIMESTAMP_TZ_OUTPUT_FORMAT",
      "DATE_OUTPUT_FORMAT",
      "TIME_OUTPUT_FORMAT",
      "BINARY_OUTPUT_FORMAT",
      "CLIENT_TIMESTAMP_TYPE_MAPPING",
      SERVICE_NAME));

  private static Set<String> INT_PARAMS = new HashSet<>(Arrays.asList(
      "CLIENT_RESULT_PREFETCH_SLOTS",
      "CLIENT_RESULT_PREFETCH_THREADS",
      "CLIENT_PREFETCH_THREADS",
      "CLIENT_MEMORY_LIMIT",
      "CLIENT_STAGE_ARRAY_BINDING_THRESHOLD"));

  private static Set<String> BOOLEAN_PARAMS = new HashSet<>(Arrays.asList(
      "CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ",
      "JDBC_EXECUTE_RETURN_COUNT_FOR_DML",
      "CLIENT_DISABLE_INCIDENTS",
      "CLIENT_SESSION_KEEP_ALIVE",
      "CLIENT_TELEMETRY_ENABLED",
      CLIENT_STORE_TEMPORARY_CREDENTIAL,
      "JDBC_USE_JSON_PARSER",
      "AUTOCOMMIT",
      "JDBC_EFFICIENT_CHUNK_STORAGE",
      JDBC_RS_COLUMN_CASE_INSENSITIVE,
      CLIENT_RESULT_COLUMN_CASE_INSENSITIVE,
      "CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX",
      "JDBC_TREAT_DECIMAL_AS_INT",
      "JDBC_ENABLE_COMBINED_DESCRIBE"));

  enum TokenRequestType
  {
    RENEW("RENEW"),
    CLONE("CLONE"),
    ISSUE("ISSUE");

    private String value;

    TokenRequestType(String value)
    {
      this.value = value;
    }
  }

  private static final String CACHE_DIR_PROP = "net.snowflake.jdbc.temporaryCredentialCacheDir";
  private static final String CACHE_DIR_ENV = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR";
  public static final String CACHE_FILE_NAME = "temporary_credential.json";
  private static final long CACHE_EXPIRATION_IN_SECONDS = 86400L;
  private static final long CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS = 60L;

  protected static final FileCacheManager fileCacheManager;

  static
  {
    fileCacheManager = FileCacheManager
        .builder()
        .setCacheDirectorySystemProperty(CACHE_DIR_PROP)
        .setCacheDirectoryEnvironmentVariable(CACHE_DIR_ENV)
        .setBaseCacheFileName(CACHE_FILE_NAME)
        .setCacheExpirationInSeconds(CACHE_EXPIRATION_IN_SECONDS)
        .setCacheFileLockExpirationInSeconds(CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS).build();
  }

  private final static Map<String, Map<String, String>> ID_TOKEN_CACHE = new HashMap<>();
  private final static Object ID_TOKEN_CACHE_LOCK = new Object();

  /**
   * A class for holding all information required for login
   */
  public static class LoginInput
  {
    private String serverUrl;
    private String databaseName;
    private String schemaName;
    private String warehouse;
    private String role;
    private String authenticator;
    private String accountName;
    private int loginTimeout = -1; // default is invalid
    private String userName;
    private String password;
    private Properties clientInfo;
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
    private String idTokenPassword;
    private String serviceName;

    public LoginInput()
    {
    }

    public LoginInput setServerUrl(String serverUrl)
    {
      this.serverUrl = serverUrl;
      return this;
    }

    public LoginInput setDatabaseName(String databaseName)
    {
      this.databaseName = databaseName;
      return this;
    }

    public LoginInput setSchemaName(String schemaName)
    {
      this.schemaName = schemaName;
      return this;
    }

    public LoginInput setWarehouse(String warehouse)
    {
      this.warehouse = warehouse;
      return this;
    }

    public LoginInput setRole(String role)
    {
      this.role = role;
      return this;
    }

    public LoginInput setAuthenticator(String authenticator)
    {
      this.authenticator = authenticator;
      return this;
    }

    public LoginInput setAccountName(String accountName)
    {
      this.accountName = accountName;
      return this;
    }

    public LoginInput setLoginTimeout(int loginTimeout)
    {
      this.loginTimeout = loginTimeout;
      return this;
    }

    public LoginInput setUserName(String userName)
    {
      this.userName = userName;
      return this;
    }

    public LoginInput setPassword(String password)
    {
      this.password = password;
      return this;
    }

    public LoginInput setToken(String token)
    {
      this.token = token;
      return this;
    }

    public LoginInput setClientInfo(Properties clientInfo)
    {
      this.clientInfo = clientInfo;
      return this;
    }

    public LoginInput setPasscodeInPassword(boolean passcodeInPassword)
    {
      this.passcodeInPassword = passcodeInPassword;
      return this;
    }

    public LoginInput setPasscode(String passcode)
    {
      this.passcode = passcode;
      return this;
    }

    public LoginInput setConnectionTimeout(int connectionTimeout)
    {
      this.connectionTimeout = connectionTimeout;
      return this;
    }

    public LoginInput setSocketTimeout(int socketTimeout)
    {
      this.socketTimeout = socketTimeout;
      return this;
    }

    public LoginInput setAppId(String appId)
    {
      this.appId = appId;
      return this;
    }

    public LoginInput setAppVersion(String appVersion)
    {
      this.appVersion = appVersion;
      return this;
    }

    public LoginInput setSessionToken(String sessionToken)
    {
      this.sessionToken = sessionToken;
      return this;
    }

    public LoginInput setMasterToken(String masterToken)
    {
      this.masterToken = masterToken;
      return this;
    }

    public LoginInput setIdToken(String idToken)
    {
      this.idToken = idToken;
      return this;
    }

    public LoginInput setSessionParameters(Map<String, Object> sessionParameters)
    {
      this.sessionParameters = sessionParameters;
      return this;
    }

    public LoginInput setPrivateKey(PrivateKey privateKey)
    {
      this.privateKey = privateKey;
      return this;
    }

    public LoginInput setApplication(String application)
    {
      this.application = application;
      return this;
    }

    public LoginInput setServiceName(String serviceName)
    {
      this.serviceName = serviceName;
      return this;
    }

    public String getServerUrl()
    {
      return serverUrl;
    }

    public String getDatabaseName()
    {
      return databaseName;
    }

    public String getSchemaName()
    {
      return schemaName;
    }

    public String getWarehouse()
    {
      return warehouse;
    }

    public String getRole()
    {
      return role;
    }

    public String getAuthenticator()
    {
      return authenticator;
    }

    public String getAccountName()
    {
      return accountName;
    }

    public int getLoginTimeout()
    {
      return loginTimeout;
    }

    public String getUserName()
    {
      return userName;
    }

    public String getPassword()
    {
      return password;
    }

    public Properties getClientInfo()
    {
      return clientInfo;
    }

    public String getPasscode()
    {
      return passcode;
    }

    public String getToken()
    {
      return token;
    }

    public int getConnectionTimeout()
    {
      return connectionTimeout;
    }

    public int getSocketTimeout()
    {
      return socketTimeout;
    }

    public boolean isPasscodeInPassword()
    {
      return passcodeInPassword;
    }

    public String getAppId()
    {
      return appId;
    }

    public String getAppVersion()
    {
      return appVersion;
    }

    public String getSessionToken()
    {
      return sessionToken;
    }

    public String getMasterToken()
    {
      return masterToken;
    }

    public String getIdToken()
    {
      return idToken;
    }

    public String getIdTokenPassword()
    {
      return idTokenPassword;
    }

    public Map<String, Object> getSessionParameters()
    {
      return sessionParameters;
    }

    public PrivateKey getPrivateKey()
    {
      return privateKey;
    }

    public String getApplication()
    {
      return application;
    }

    public String getServiceName()
    {
      return serviceName;
    }

  }

  /**
   * Login output information including session tokens, database versions
   */
  public static class LoginOutput
  {
    String sessionToken;
    String masterToken;
    long masterTokenValidityInSeconds;
    String remMeToken;
    String idToken;
    String idTokenPassword;
    String databaseVersion;
    int databaseMajorVersion;
    int databaseMinorVersion;
    String newClientForUpgrade;
    int healthCheckInterval;
    int httpClientSocketTimeout;
    String sessionDatabase;
    String sessionSchema;
    String sessionRole;
    String sessionWarehouse;
    Map<String, Object> commonParams;
    boolean updatedByTokenRequest;
    boolean updatedByTokenRequestIssue;

    public LoginOutput()
    {
    }

    public LoginOutput(String sessionToken, String masterToken,
                       long masterTokenValidityInSeconds,
                       String remMeToken,
                       String idToken,
                       String idTokenPassword,
                       String databaseVersion,
                       int databaseMajorVersion, int databaseMinorVersion,
                       String newClientForUpgrade, int healthCheckInterval,
                       int httpClientSocketTimeout,
                       String sessionDatabase,
                       String sessionSchema,
                       String sessionRole,
                       String sessionWarehouse,
                       Map<String, Object> commonParams)
    {
      this.sessionToken = sessionToken;
      this.masterToken = masterToken;
      this.remMeToken = remMeToken;
      this.idToken = idToken;
      this.idTokenPassword = idTokenPassword;
      this.databaseVersion = databaseVersion;
      this.databaseMajorVersion = databaseMajorVersion;
      this.databaseMinorVersion = databaseMinorVersion;
      this.newClientForUpgrade = newClientForUpgrade;
      this.healthCheckInterval = healthCheckInterval;
      this.httpClientSocketTimeout = httpClientSocketTimeout;
      this.sessionDatabase = sessionDatabase;
      this.sessionSchema = sessionSchema;
      this.sessionRole = sessionRole;
      this.sessionWarehouse = sessionWarehouse;
      this.commonParams = commonParams;
      this.masterTokenValidityInSeconds = masterTokenValidityInSeconds;
    }

    public LoginOutput setSessionToken(String sessionToken)
    {
      this.sessionToken = sessionToken;
      return this;
    }

    public LoginOutput setMasterToken(String masterToken)
    {
      this.masterToken = masterToken;
      return this;
    }

    public LoginOutput setIdToken(String idToken)
    {
      this.idToken = idToken;
      return this;
    }

    public LoginOutput setRemMeToken(String remMeToken)
    {
      this.remMeToken = remMeToken;
      return this;
    }

    public LoginOutput setDatabaseVersion(String databaseVersion)
    {
      this.databaseVersion = databaseVersion;
      return this;
    }

    public LoginOutput setDatabaseMajorVersion(int databaseMajorVersion)
    {
      this.databaseMajorVersion = databaseMajorVersion;
      return this;
    }

    public LoginOutput setDatabaseMinorVersion(int databaseMinorVersion)
    {
      this.databaseMinorVersion = databaseMinorVersion;
      return this;
    }

    public LoginOutput setNewClientForUpgrade(String newClientForUpgrade)
    {
      this.newClientForUpgrade = newClientForUpgrade;
      return this;
    }

    public LoginOutput setHealthCheckInterval(int healthCheckInterval)
    {
      this.healthCheckInterval = healthCheckInterval;
      return this;
    }

    public LoginOutput setHttpClientSocketTimeout(int httpClientSocketTimeout)
    {
      this.httpClientSocketTimeout = httpClientSocketTimeout;
      return this;
    }

    public LoginOutput setCommonParams(Map<String, Object> commonParams)
    {
      this.commonParams = commonParams;
      return this;
    }

    public String getSessionToken()
    {
      return sessionToken;
    }

    public String getMasterToken()
    {
      return masterToken;
    }

    public String getRemMeToken()
    {
      return remMeToken;
    }

    public String getIdToken()
    {
      return idToken;
    }

    public String getIdTokenPassword()
    {
      return idTokenPassword;
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

    public String getNewClientForUpgrade()
    {
      return newClientForUpgrade;
    }

    public int getHealthCheckInterval()
    {
      return healthCheckInterval;
    }

    public int getHttpClientSocketTimeout()
    {
      return httpClientSocketTimeout;
    }

    public Map<String, Object> getCommonParams()
    {
      return commonParams;
    }

    public String getSessionDatabase()
    {
      return sessionDatabase;
    }

    public void setSessionDatabase(String sessionDatabase)
    {
      this.sessionDatabase = sessionDatabase;
    }

    public String getSessionSchema()
    {
      return sessionSchema;
    }

    public void setSessionSchema(String sessionSchema)
    {
      this.sessionSchema = sessionSchema;
    }

    void setSessionRole(String sessionRole)
    {
      this.sessionRole = sessionRole;
    }

    public String getSessionRole()
    {
      return sessionRole;
    }

    void setSessionWarehouse(String sessionWarehouse)
    {
      this.sessionWarehouse = sessionWarehouse;
    }

    String getSessionWarehouse()
    {
      return sessionWarehouse;
    }

    public long getMasterTokenValidityInSeconds()
    {
      return masterTokenValidityInSeconds;
    }

    boolean isUpdatedByTokenRequest()
    {
      return updatedByTokenRequest;
    }

    LoginOutput setUpdatedByTokenRequest(boolean updatedByTokenRequest)
    {
      this.updatedByTokenRequest = updatedByTokenRequest;
      return this;
    }

    boolean isUpdatedByTokenRequestIssue()
    {
      return updatedByTokenRequestIssue;
    }

    LoginOutput setUpdatedByTokenRequestIssue(boolean updatedByTokenRequestIssue)
    {
      this.updatedByTokenRequestIssue = updatedByTokenRequestIssue;
      return this;
    }
  }

  /**
   * Returns Authenticator type
   *
   * @param loginInput login information
   * @return Authenticator type
   */
  static private ClientAuthnDTO.AuthenticatorType getAuthenticator(
      LoginInput loginInput)
  {
    if (loginInput.getAuthenticator() != null)
    {
      if (loginInput.getAuthenticator().equalsIgnoreCase(
          ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER.name()))
      {
        // SAML 2.0 compliant service/application
        return ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER;
      }
      else if (loginInput.getAuthenticator().equalsIgnoreCase(
          ClientAuthnDTO.AuthenticatorType.OAUTH.name()))
      {
        // OAuth Authentication
        return ClientAuthnDTO.AuthenticatorType.OAUTH;
      }
      else if (loginInput.getAuthenticator().equalsIgnoreCase(
          ClientAuthnDTO.AuthenticatorType.SNOWFLAKE_JWT.name()))
      {
        return ClientAuthnDTO.AuthenticatorType.SNOWFLAKE_JWT;
      }
      else if (!loginInput.getAuthenticator().equalsIgnoreCase(
          ClientAuthnDTO.AuthenticatorType.SNOWFLAKE.name()))
      {
        // OKTA authenticator v1. This will be deprecated once externalbrowser
        // is in production.
        return ClientAuthnDTO.AuthenticatorType.OKTA;
      }
    }

    // authenticator is null, then jdbc will decide authenticator depends on
    // if privateKey is specified or not. If yes, authenticator type will be
    // SNOWFLAKE_JWT, otherwise it will use SNOWFLAKE.
    return loginInput.getPrivateKey() != null ?
        ClientAuthnDTO.AuthenticatorType.SNOWFLAKE_JWT :
        ClientAuthnDTO.AuthenticatorType.SNOWFLAKE;
  }

  /**
   * Open a new session
   *
   * @param loginInput login information
   * @return information get after login such as token information
   * @throws SFException           if unexpected uri syntax
   * @throws SnowflakeSQLException if failed to establish connection with snowflake
   */
  static public LoginOutput openSession(LoginInput loginInput)
      throws SFException, SnowflakeSQLException
  {
    AssertUtil.assertTrue(loginInput.getServerUrl() != null,
        "missing server URL for opening session");

    AssertUtil.assertTrue(loginInput.getUserName() != null,
        "missing user name for opening session");

    AssertUtil.assertTrue(loginInput.getAppId() != null,
        "missing app id for opening session");

    AssertUtil.assertTrue(loginInput.getLoginTimeout() >= 0,
        "negative login timeout for opening session");

    final ClientAuthnDTO.AuthenticatorType authenticator = getAuthenticator(
        loginInput);
    if (!authenticator.equals(ClientAuthnDTO.AuthenticatorType.OAUTH))
    {
      // OAuth does not require a username
      AssertUtil.assertTrue(loginInput.getUserName() != null,
          "missing user name for opening session");
    }
    if (authenticator.equals(ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER))
    {
      // force to set the flag.
      loginInput.sessionParameters.put(CLIENT_STORE_TEMPORARY_CREDENTIAL, true);
    }
    else
    {
      // TODO: patch for now. We should update mergeProperteis
      // to normalize all parameters using STRING_PARAMS, INT_PARAMS and
      // BOOLEAN_PARAMS.
      Object value = loginInput.sessionParameters.get(
          CLIENT_STORE_TEMPORARY_CREDENTIAL);
      if (value != null)
      {
        loginInput.sessionParameters.put(
            CLIENT_STORE_TEMPORARY_CREDENTIAL, asBoolean(value));
      }
    }

    boolean isClientStoreTemporaryCredential = asBoolean(
        loginInput.sessionParameters.get(CLIENT_STORE_TEMPORARY_CREDENTIAL));
    LoginOutput loginOutput;
    if (isClientStoreTemporaryCredential &&
        (loginOutput = readTemporaryCredential(loginInput)) != null)
    {
      return loginOutput;
    }
    return newSession(loginInput);
  }

  static private LoginOutput readTemporaryCredential(LoginInput loginInput)
      throws SFException, SnowflakeSQLException
  {
    String idToken;
    synchronized (ID_TOKEN_CACHE_LOCK)
    {
      JsonNode res = fileCacheManager.readCacheFile();
      readJsonStoreCache(res);

      Map<String, String> userMap = ID_TOKEN_CACHE.get(
          loginInput.getAccountName().toUpperCase());
      if (userMap == null)
      {
        return null;
      }
      idToken = userMap.get(loginInput.getUserName().toUpperCase());
    }
    if (idToken == null)
    {
      return null;
    }
    loginInput.setIdToken(idToken);
    try
    {
      return issueSession(loginInput);
    }
    catch (SnowflakeReauthenticationRequest ex)
    {
      logger.debug("The token expired. errorCode. Reauthenticating...");
    }
    return null;
  }

  static private void readJsonStoreCache(JsonNode m)
  {
    if (m == null || !m.getNodeType().equals(JsonNodeType.OBJECT))
    {
      logger.debug("Invalid cache file format.");
      return;
    }
    for (Iterator<Map.Entry<String, JsonNode>> itr = m.fields(); itr.hasNext(); )
    {
      Map.Entry<String, JsonNode> accountMap = itr.next();
      String account = accountMap.getKey();
      if (!ID_TOKEN_CACHE.containsKey(account))
      {
        ID_TOKEN_CACHE.put(account, new HashMap<String, String>());
      }
      JsonNode userJsonNode = accountMap.getValue();
      for (Iterator<Map.Entry<String, JsonNode>> itr0 = userJsonNode.fields(); itr0.hasNext(); )
      {
        Map.Entry<String, JsonNode> userMap = itr0.next();
        ID_TOKEN_CACHE.get(account).put(
            userMap.getKey(), userMap.getValue().asText());
      }
    }
  }

  static private void writeTemporaryCredential(
      LoginInput loginInput, LoginOutput loginOutput)
  {
    if (Strings.isNullOrEmpty(loginOutput.getIdToken()))
    {
      return; // no idToken
    }
    synchronized (ID_TOKEN_CACHE_LOCK)
    {
      String currentAccount = loginInput.getAccountName().toUpperCase();
      Map<String, String> currentUserMap = ID_TOKEN_CACHE.get(currentAccount);
      if (currentUserMap == null)
      {
        currentUserMap = new HashMap<>();
        ID_TOKEN_CACHE.put(currentAccount, currentUserMap);
      }
      currentUserMap.put(loginInput.getUserName().toUpperCase(), loginOutput.getIdToken());

      ObjectNode out = mapper.createObjectNode();
      for (Map.Entry<String, Map<String, String>> elem : ID_TOKEN_CACHE.entrySet())
      {
        String account = elem.getKey();
        Map<String, String> userMap = elem.getValue();
        ObjectNode userNode = mapper.createObjectNode();
        for (Map.Entry<String, String> elem0 : userMap.entrySet())
        {
          userNode.put(elem0.getKey(), elem0.getValue());
        }
        out.set(account, userNode);
      }
      fileCacheManager.writeCacheFile(out);
    }
  }

  static private boolean asBoolean(Object value)
  {
    if (value == null)
    {
      return false;
    }
    switch (value.getClass().getName())
    {
      case "java.lang.Boolean":
        return (Boolean) value;
      case "java.lang.String":
        return Boolean.valueOf((String) value);
    }
    return false;
  }

  static private LoginOutput newSession(LoginInput loginInput)
      throws SFException, SnowflakeSQLException
  {
    // build URL for login request
    URIBuilder uriBuilder;
    URI loginURI;
    String tokenOrSamlResponse = null;
    String samlProofKey = null;
    boolean consentCacheIdToken = true;

    String sessionToken;
    String masterToken;
    String sessionDatabase;
    String sessionSchema;
    String sessionRole;
    String sessionWarehouse;
    long masterTokenValidityInSeconds;
    String remMeToken;
    String idToken;
    String idTokenPassword;
    String databaseVersion = null;
    int databaseMajorVersion = 0;
    int databaseMinorVersion = 0;
    String newClientForUpgrade = null;
    int healthCheckInterval = DEFAULT_HEALTH_CHECK_INTERVAL;
    int httpClientSocketTimeout = loginInput.getSocketTimeout();
    final ClientAuthnDTO.AuthenticatorType authenticator = getAuthenticator(
        loginInput);
    Map<String, Object> commonParams;

    try
    {
      uriBuilder = new URIBuilder(loginInput.getServerUrl());

      // add database name and schema name as query parameters
      if (loginInput.getDatabaseName() != null)
      {
        uriBuilder.addParameter(SF_QUERY_DATABASE, loginInput.getDatabaseName());
      }

      if (loginInput.getSchemaName() != null)
      {
        uriBuilder.addParameter(SF_QUERY_SCHEMA, loginInput.getSchemaName());
      }

      if (loginInput.getWarehouse() != null)
      {
        uriBuilder.addParameter(SF_QUERY_WAREHOUSE, loginInput.getWarehouse());
      }

      if (loginInput.getRole() != null)
      {
        uriBuilder.addParameter(SF_QUERY_ROLE, loginInput.getRole());
      }

      if (authenticator == ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER)
      {
        // SAML 2.0 compliant service/application
        SessionUtilExternalBrowser s =
            SessionUtilExternalBrowser.createInstance(loginInput);
        s.authenticate();
        tokenOrSamlResponse = s.getToken();
        samlProofKey = s.getProofKey();
        consentCacheIdToken = s.isConsentCacheIdToken();
      }
      else if (authenticator == ClientAuthnDTO.AuthenticatorType.OKTA)
      {
        // okta authenticator v1
        tokenOrSamlResponse = getSamlResponseUsingOkta(loginInput);
      }
      else if (authenticator == ClientAuthnDTO.AuthenticatorType.SNOWFLAKE_JWT)
      {
        SessionUtilKeyPair s = new SessionUtilKeyPair(loginInput.getPrivateKey(),
            loginInput.getAccountName(),
            loginInput.getUserName());

        loginInput.setToken(s.issueJwtToken());
      }

      uriBuilder.addParameter(SF_QUERY_REQUEST_ID, UUID.randomUUID().toString());


      uriBuilder.setPath(SF_PATH_LOGIN_REQUEST);
      loginURI = uriBuilder.build();
    }
    catch (URISyntaxException ex)
    {
      logger.error("Exception when building URL", ex);

      throw new SFException(ex, ErrorCode.INTERNAL_ERROR,
          "unexpected URI syntax exception:1");
    }

    if (loginInput.getServerUrl().indexOf(".privatelink.snowflakecomputing.com") > 0)
    {
      // Privatelink uses special OCSP Cache server
      try
      {
        URL url = new URL(loginInput.getServerUrl());
        String host = url.getHost();
        logger.debug("HOST: {}", host);
        String ocspCacheServerUrl = String.format(
            "http://ocsp%s/%s",
            host.substring(host.indexOf('.')),
            SFTrustManager.CACHE_FILE_NAME);
        logger.debug("OCSP Cache Server for Privatelink: {}",
            ocspCacheServerUrl);
        resetOCSPResponseCacherServerURL(ocspCacheServerUrl);
      }
      catch (IOException ex)
      {
        throw new SFException(ex, ErrorCode.INTERNAL_ERROR,
            "unexpected URL syntax exception");
      }
    }

    HttpPost postRequest = null;

    try
    {
      ClientAuthnDTO authnData = new ClientAuthnDTO();
      Map<String, Object> data = new HashMap<>();
      data.put(ClientAuthnParameter.CLIENT_APP_ID.name(), loginInput.getAppId());

      /*
       * username is always included regardless of authenticator to identify
       * the user.
       */
      data.put(ClientAuthnParameter.LOGIN_NAME.name(),
          loginInput.getUserName());

      /*
       * only include password information in the request to GS if federated
       * authentication method is not specified.
       * When specified, this password information is really to be used to
       * authenticate with the IDP provider only, and GS should not have any
       * trace for this information.
       */
      if (authenticator == ClientAuthnDTO.AuthenticatorType.SNOWFLAKE)
      {
        data.put(ClientAuthnParameter.PASSWORD.name(), loginInput.getPassword());
      }
      else if (authenticator == ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER)
      {
        data.put(ClientAuthnParameter.AUTHENTICATOR.name(),
            ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER.name());
        data.put(ClientAuthnParameter.PROOF_KEY.name(), samlProofKey);
        data.put(ClientAuthnParameter.TOKEN.name(), tokenOrSamlResponse);
      }
      else if (authenticator == ClientAuthnDTO.AuthenticatorType.OKTA)
      {
        data.put(ClientAuthnParameter.RAW_SAML_RESPONSE.name(), tokenOrSamlResponse);
      }
      else if (authenticator == ClientAuthnDTO.AuthenticatorType.OAUTH ||
          authenticator == ClientAuthnDTO.AuthenticatorType.SNOWFLAKE_JWT)
      {
        data.put(ClientAuthnParameter.AUTHENTICATOR.name(), authenticator.name());
        data.put(ClientAuthnParameter.TOKEN.name(), loginInput.getToken());
      }

      Map<String, Object> clientEnv = new HashMap<String, Object>();
      clientEnv.put("OS", System.getProperty("os.name"));
      clientEnv.put("OS_VERSION", System.getProperty("os.version"));
      clientEnv.put("JAVA_VERSION", System.getProperty("java.version"));
      clientEnv.put("JAVA_RUNTIME", System.getProperty("java.runtime.name"));
      clientEnv.put("JAVA_VM", System.getProperty("java.vm.name"));

      // SNOW-15780: find out if application has set
      // -Dcom.sun.security.enableCRLDP=true and
      // -Dcom.sun.net.ssl.checkRevocation=true
      boolean CRLEnabled = SessionUtil.checkCRLSystemProperty();
      clientEnv.put("CRL_ENABLED", CRLEnabled);

      if (loginInput.getApplication() != null)
      {
        clientEnv.put("APPLICATION", loginInput.getApplication());
      }
      else
      {
        // When you add new client environment info, please add new keys to
        // messages_en_US.src.json so that they can be displayed properly in UI
        // detect app name
        String appName = System.getProperty("sun.java.command");
        // remove the arguments
        if (appName != null)
        {
          if (appName.indexOf(" ") > 0)
            appName = appName.substring(0, appName.indexOf(" "));

          clientEnv.put("APPLICATION", appName);
        }
      }

      // add properties from client info
      Properties clientInfo = loginInput.getClientInfo();
      if (clientInfo != null)
        for (Map.Entry property : clientInfo.entrySet())
        {
          if (property != null && property.getKey() != null &&
              property.getValue() != null)
            clientEnv.put(property.getKey().toString(),
                property.getValue().toString());
        }

      // SNOW-20103: track additional client info in session
      String clientInfoJSONStr = System.getProperty("snowflake.client.info");
      if (clientInfoJSONStr != null)
      {
        JsonNode clientInfoJSON = null;

        try
        {
          clientInfoJSON = mapper.readTree(clientInfoJSONStr);
        }
        catch (Throwable ex)
        {
          logger.warn(
              "failed to process snowflake.client.info property as JSON: "
                  + clientInfoJSONStr, ex);
        }

        if (clientInfoJSON != null)
        {
          Iterator<Map.Entry<String, JsonNode>> fields = clientInfoJSON.fields();
          while (fields.hasNext())
          {
            Map.Entry<String, JsonNode> field = fields.next();
            clientEnv.put(field.getKey(), field.getValue().asText());
          }
        }
      }

      data.put(ClientAuthnParameter.CLIENT_ENVIRONMENT.name(), clientEnv);

      // Initialize the session parameters
      Map<String, Object> sessionParameter = loginInput.getSessionParameters();

      if (sessionParameter != null)
      {
        data.put(ClientAuthnParameter.SESSION_PARAMETERS.name(), loginInput
            .getSessionParameters());
      }

      if (loginInput.getAccountName() != null)
      {
        data.put(ClientAuthnParameter.ACCOUNT_NAME.name(),
            loginInput.getAccountName());
      }

      // Second Factor Authentication
      if (loginInput.isPasscodeInPassword())
      {
        data.put(ClientAuthnParameter.EXT_AUTHN_DUO_METHOD.name(), "passcode");
      }
      else if (loginInput.getPasscode() != null)
      {
        data.put(ClientAuthnParameter.EXT_AUTHN_DUO_METHOD.name(), "passcode");
        data.put(ClientAuthnParameter.PASSCODE.name(),
            loginInput.getPasscode());
      }
      else
      {
        data.put(ClientAuthnParameter.EXT_AUTHN_DUO_METHOD.name(), "push");
      }

      logger.debug(
          "implementation version = {}",
          SnowflakeDriver.implementVersion);

      data.put(ClientAuthnParameter.CLIENT_APP_VERSION.name(),
          loginInput.getAppVersion());

      authnData.setData(data);
      String json = mapper.writeValueAsString(authnData);

      postRequest = new HttpPost(loginURI);

      // attach the login info json body to the post request
      StringEntity input = new StringEntity(json, Charset.forName("UTF-8"));
      input.setContentType("application/json");
      postRequest.setEntity(input);

      postRequest.addHeader("accept", "application/json");

      /*
       * HttpClient should take authorization header from char[] instead of
       * String.
       */
      postRequest.setHeader(SF_HEADER_AUTHORIZATION,
          SF_HEADER_BASIC_AUTHTYPE);

      setServiceNameHeader(loginInput, postRequest);

      String theString = HttpUtil.executeRequest(postRequest,
          loginInput.getLoginTimeout(),
          0, null);

      logger.debug("login response: {}", theString);

      // general method, same as with data binding
      JsonNode jsonNode = mapper.readTree(theString);

      // check the success field first
      if (!jsonNode.path("success").asBoolean())
      {
        logger.debug("response = {}", theString);

        String errorCode = jsonNode.path("code").asText();

        throw new SnowflakeSQLException(
            SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
            ErrorCode.CONNECTION_ERROR.getMessageCode(),
            errorCode, jsonNode.path("message").asText());
      }

      // session token is in the data field of the returned json response
      sessionToken = jsonNode.path("data").path("token").asText();
      masterToken = jsonNode.path("data").path("masterToken").asText();
      remMeToken = jsonNode.path("data").path("remMeToken").asText();
      idToken = nullStringAsEmptyString(
          jsonNode.path("data").path("idToken").asText());
      idTokenPassword = nullStringAsEmptyString(
          jsonNode.path("data").path("idTokenPassword").asText());
      masterTokenValidityInSeconds = jsonNode.path("data").
          path("masterValidityInSeconds").asLong();
      String serverVersion =
          jsonNode.path("data").path("serverVersion").asText();

      JsonNode dbNode = jsonNode.path("data").path("sessionInfo").path("databaseName");
      sessionDatabase = dbNode.isNull() ? null : dbNode.asText();
      JsonNode schemaNode = jsonNode.path("data").path("sessionInfo").path("schemaName");
      sessionSchema = schemaNode.isNull() ? null : schemaNode.asText();
      JsonNode roleNode = jsonNode.path("data").path("sessionInfo").path("roleName");
      sessionRole = roleNode.isNull() ? null : roleNode.asText();
      JsonNode warehouseNode = jsonNode.path("data").path("sessionInfo").path("warehouseName");
      sessionWarehouse = warehouseNode.isNull() ? null : warehouseNode.asText();

      commonParams =
          SessionUtil.getCommonParams(jsonNode.path("data").path("parameters"));

      if (serverVersion != null)
      {
        logger.debug("server version = {}", serverVersion);

        if (serverVersion.indexOf(" ") > 0)
          databaseVersion = serverVersion.substring(0,
              serverVersion.indexOf(" "));
        else
          databaseVersion = serverVersion;
      }
      else
      {
        logger.warn("server version is null");
      }

      if (databaseVersion != null)
      {
        String[] components = databaseVersion.split("\\.");
        if (components != null && components.length >= 2)
        {
          try
          {
            databaseMajorVersion = Integer.parseInt(components[0]);
            databaseMinorVersion = Integer.parseInt(components[1]);
          }
          catch (Exception ex)
          {
            logger.error("Exception encountered when parsing server "
                    + "version: {} Exception: {}",
                databaseVersion, ex.getMessage());
          }
        }
      }
      else
        logger.warn("database version is null");


      if (!jsonNode.path("data").path("newClientForUpgrade").isNull())
      {
        newClientForUpgrade =
            jsonNode.path("data").path("newClientForUpgrade").asText();

        logger.debug("new client: {}", newClientForUpgrade);
      }

      // get health check interval and adjust network timeouts if different
      int healthCheckIntervalFromGS =
          jsonNode.path("data").path("healthCheckInterval").asInt();

      logger.debug(
          "health check interval = {}", healthCheckIntervalFromGS);

      if (healthCheckIntervalFromGS > 0 &&
          healthCheckIntervalFromGS != healthCheckInterval)
      {
        healthCheckInterval = healthCheckIntervalFromGS;

        // add health check interval to socket timeout
        httpClientSocketTimeout = loginInput.getSocketTimeout() +
            (healthCheckIntervalFromGS * 1000);

        final HttpParams httpParams = new BasicHttpParams();

        // set timeout so that we don't wait forever
        HttpConnectionParams.setConnectionTimeout(httpParams,
            loginInput.getConnectionTimeout());

        HttpConnectionParams.setSoTimeout(httpParams,
            httpClientSocketTimeout);

        ((SystemDefaultHttpClient) HttpUtil.getHttpClient()).setParams(httpParams);

        logger.debug(
            "adjusted connection timeout to = {}",
            loginInput.getConnectionTimeout());

        logger.debug(
            "adjusted socket timeout to = {}", httpClientSocketTimeout);
      }
    }
    catch (SnowflakeSQLException ex)
    {
      throw ex; // must catch here to avoid Throwable to get the exception
    }
    catch (IOException ex)
    {
      logger.error("IOException when creating session: " +
          postRequest, ex);

      throw new SnowflakeSQLException(ex, SqlState.IO_ERROR,
          ErrorCode.NETWORK_ERROR.getMessageCode(),
          "Exception encountered when opening connection: " +
              ex.getMessage());
    }
    catch (Throwable ex)
    {
      logger.error("Exception when creating session: " +
          postRequest, ex);

      throw new SnowflakeSQLException(ex,
          SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
          ErrorCode.CONNECTION_ERROR.getMessageCode(),
          ErrorCode.CONNECTION_ERROR.getMessageCode(), ex.getMessage());
    }

    LoginOutput ret = new LoginOutput(sessionToken,
        masterToken,
        masterTokenValidityInSeconds,
        remMeToken,
        idToken,
        idTokenPassword,
        databaseVersion,
        databaseMajorVersion,
        databaseMinorVersion,
        newClientForUpgrade,
        healthCheckInterval,
        httpClientSocketTimeout,
        sessionDatabase,
        sessionSchema,
        sessionRole,
        sessionWarehouse,
        commonParams);
    ret.setUpdatedByTokenRequest(false);

    if (consentCacheIdToken)
    {
      writeTemporaryCredential(loginInput, ret);
    }
    return ret;
  }

  private static void setServiceNameHeader(LoginInput loginInput, HttpPost postRequest)
  {
    if (!Strings.isNullOrEmpty(loginInput.getServiceName()))
    {
      // service name is used to route a request to appropriate cluster.
      postRequest.setHeader(SF_HEADER_SERVICE_NAME,
          loginInput.getServiceName());
    }
  }

  static private String nullStringAsEmptyString(String value)
  {
    if (Strings.isNullOrEmpty(value) || "null".equals(value))
    {
      return "";
    }
    return value;
  }


  /**
   * Delete the id token cache
   */
  static public void deleteIdTokenCache()
  {
    fileCacheManager.deleteCacheFile();
  }

  /**
   * Renew a session.
   * <p>
   * Use cases:
   * - Session and Master tokens are provided. No Id token:
   * - succeed in getting a new Session token.
   * - fail and raise SnowflakeReauthenticationRequest because Master
   * token expires. Since no id token exists, the exception is thrown
   * to the upstream.
   * - Session and Id tokens are provided. No Master token:
   * - fail and raise SnowflakeReauthenticationRequest and
   * issue a new Session token
   * - fail and raise SnowflakeReauthenticationRequest and fail
   * to issue a new Session token as the
   *
   * @param loginInput login information
   * @return login output
   * @throws SFException           if unexpected uri information
   * @throws SnowflakeSQLException if failed to renew the session
   */
  static public LoginOutput renewSession(LoginInput loginInput)
      throws SFException, SnowflakeSQLException
  {
    try
    {
      return tokenRequest(loginInput, TokenRequestType.RENEW);
    }
    catch (SnowflakeReauthenticationRequest ex)
    {
      if (Strings.isNullOrEmpty(loginInput.getIdToken()))
      {
        throw ex;
      }
      return tokenRequest(loginInput, TokenRequestType.ISSUE);
    }
  }

  /**
   * Issue a session
   *
   * @param loginInput login information
   * @return login output
   * @throws SFException           if unexpected uri information
   * @throws SnowflakeSQLException if failed to renew the session
   */
  static public LoginOutput issueSession(LoginInput loginInput)
      throws SFException, SnowflakeSQLException
  {
    return tokenRequest(loginInput, TokenRequestType.ISSUE);
  }

  static private LoginOutput tokenRequest(
      LoginInput loginInput, TokenRequestType requestType)
      throws SFException, SnowflakeSQLException
  {
    AssertUtil.assertTrue(loginInput.getServerUrl() != null,
        "missing server URL for tokenRequest");

    if (requestType == TokenRequestType.RENEW)
    {
      AssertUtil.assertTrue(loginInput.getMasterToken() != null,
          "missing master token for tokenRequest");
      AssertUtil.assertTrue(loginInput.getSessionToken() != null,
          "missing session token for tokenRequest");
    }
    else if (requestType == TokenRequestType.ISSUE)
    {
      AssertUtil.assertTrue(loginInput.getIdToken() != null,
          "missing id token for tokenRequest");
    }

    AssertUtil.assertTrue(loginInput.getLoginTimeout() >= 0,
        "negative login timeout for tokenRequest");

    // build URL for login request
    URIBuilder uriBuilder;
    HttpPost postRequest;
    String sessionToken;
    String masterToken;

    try
    {
      uriBuilder = new URIBuilder(loginInput.getServerUrl());
      uriBuilder.setPath(SF_PATH_TOKEN_REQUEST);

      uriBuilder.addParameter(SFSession.SF_QUERY_REQUEST_ID,
          UUID.randomUUID().toString());

      postRequest = new HttpPost(uriBuilder.build());
    }
    catch (URISyntaxException ex)
    {
      logger.error("Exception when creating http request", ex);

      throw new SFException(ex, ErrorCode.INTERNAL_ERROR,
          "unexpected URI syntax exception:3");
    }

    try
    {
      // input json with old session token and request type, notice the
      // session token needs to be quoted.
      Map<String, String> payload = new HashMap<>();
      String headerToken;
      if (requestType == TokenRequestType.RENEW)
      {
        headerToken = loginInput.getMasterToken();
        payload.put("oldSessionToken", loginInput.getSessionToken());
      }
      else
      {
        headerToken = loginInput.getIdToken();
        payload.put("idToken", loginInput.getIdToken());
      }
      payload.put("requestType", requestType.value);
      String json = mapper.writeValueAsString(payload);

      // attach the login info json body to the post request
      StringEntity input = new StringEntity(json, Charset.forName("UTF-8"));
      input.setContentType("application/json");
      postRequest.setEntity(input);

      postRequest.addHeader("accept", "application/json");

      postRequest.setHeader(
          SF_HEADER_AUTHORIZATION,
          SF_HEADER_SNOWFLAKE_AUTHTYPE + " " +
              SF_HEADER_TOKEN_TAG + "=\"" + headerToken + "\"");

      setServiceNameHeader(loginInput, postRequest);

      logger.debug(
          "request type: {}, old session token: {}, " +
              "master token: {}, id token: {}",
          requestType.value,
          loginInput.getSessionToken() != null ? "******" : null,
          loginInput.getMasterToken() != null ? "******" : null,
          loginInput.getIdToken() != null ? "******" : null);

      String theString = HttpUtil.executeRequest(postRequest,
          loginInput.getLoginTimeout(), 0, null);

      // general method, same as with data binding
      JsonNode jsonNode = mapper.readTree(theString);

      // check the success field first
      if (!jsonNode.path("success").asBoolean())
      {
        logger.debug("response = {}", theString);

        String errorCode = jsonNode.path("code").asText();
        String message = jsonNode.path("message").asText();

        EventUtil.triggerBasicEvent(
            Event.EventType.NETWORK_ERROR,
            "SessionUtil:renewSession failure, error code="
                + errorCode + ", message=" + message,
            true);

        SnowflakeUtil.checkErrorAndThrowExceptionIncludingReauth(jsonNode);
      }

      // session token is in the data field of the returned json response
      sessionToken = jsonNode.path("data").path("sessionToken").asText();
      masterToken = jsonNode.path("data").path("masterToken").asText();
    }
    catch (IOException ex)
    {
      logger.error("IOException when renewing session: " +
          postRequest, ex);

      // Any EventType.NETWORK_ERRORs should have been triggered before
      // exception was thrown.
      throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
    }

    LoginOutput loginOutput = new LoginOutput();
    loginOutput
        .setSessionToken(sessionToken)
        .setMasterToken(masterToken)
        .setUpdatedByTokenRequest(true)
        .setUpdatedByTokenRequestIssue(requestType == TokenRequestType.ISSUE);

    return loginOutput;
  }

  /**
   * Close a session
   *
   * @param loginInput login information
   * @throws SnowflakeSQLException if failed to close session
   * @throws SFException           if failed to close session
   */
  static public void closeSession(LoginInput loginInput)
      throws SFException, SnowflakeSQLException
  {
    logger.debug(" public void close() throws SFException");

    // assert the following inputs are valid
    AssertUtil.assertTrue(loginInput.getServerUrl() != null,
        "missing server URL for closing session");

    AssertUtil.assertTrue(loginInput.getSessionToken() != null,
        "missing session token for closing session");

    AssertUtil.assertTrue(loginInput.getLoginTimeout() >= 0,
        "missing login timeout for closing session");

    HttpPost postRequest = null;

    try
    {
      URIBuilder uriBuilder;

      uriBuilder = new URIBuilder(loginInput.getServerUrl());

      uriBuilder.addParameter(SF_QUERY_SESSION_DELETE, "true");
      uriBuilder.addParameter(SFSession.SF_QUERY_REQUEST_ID,
          UUID.randomUUID().toString());

      uriBuilder.setPath(SF_PATH_SESSION);

      postRequest = new HttpPost(uriBuilder.build());

      postRequest.setHeader(SF_HEADER_AUTHORIZATION,
          SF_HEADER_SNOWFLAKE_AUTHTYPE + " "
              + SF_HEADER_TOKEN_TAG + "=\""
              + loginInput.getSessionToken() + "\"");

      setServiceNameHeader(loginInput, postRequest);

      String theString = HttpUtil.executeRequest(postRequest,
          loginInput.getLoginTimeout(),
          0, null);

      JsonNode rootNode;

      logger.debug(
          "connection close response: {}",
          theString);

      rootNode = mapper.readTree(theString);

      SnowflakeUtil.checkErrorAndThrowException(rootNode);
    }
    catch (URISyntaxException ex)
    {
      throw new RuntimeException("unexpected URI syntax exception", ex);
    }
    catch (IOException ex)
    {
      logger.error("unexpected IO exception for: " + postRequest,
          ex);
    }
    catch (SnowflakeSQLException ex)
    {
      // ignore session expiration exception
      if (ex.getErrorCode() != Constants.SESSION_EXPIRED_GS_CODE)
        throw ex;
    }
  }

  /**
   * Given access token, query IDP URL snowflake app to get SAML response
   * We also need to perform important client side validation:
   * validate the post back url come back with the SAML response
   * contains the same prefix as the Snowflake's server url, which is the
   * intended destination url to Snowflake.
   * Explanation:
   * This emulates the behavior of IDP initiated login flow in the user
   * browser where the IDP instructs the browser to POST the SAML
   * assertion to the specific SP endpoint.  This is critical in
   * preventing a SAML assertion issued to one SP from being sent to
   * another SP.
   *
   * @param loginInput
   * @param ssoUrl
   * @param oneTimeToken
   * @return
   * @throws SnowflakeSQLException
   */
  private static String federatedFlowStep4(
      LoginInput loginInput,
      String ssoUrl,
      String oneTimeToken) throws SnowflakeSQLException
  {
    String responseHtml = "";
    try
    {

      final URL url = new URL(ssoUrl);
      URI oktaGetUri = new URIBuilder()
          .setScheme(url.getProtocol())
          .setHost(url.getHost())
          .setPath(url.getPath())
          .setParameter("RelayState", "%2Fsome%2Fdeep%2Flink")
          .setParameter("onetimetoken", oneTimeToken).build();
      HttpGet httpGet = new HttpGet(oktaGetUri);

      HeaderGroup headers = new HeaderGroup();
      headers.addHeader(new BasicHeader(HttpHeaders.ACCEPT, "*/*"));
      httpGet.setHeaders(headers.getAllHeaders());

      responseHtml = HttpUtil.executeRequest(httpGet,
          loginInput.getLoginTimeout(), 0, null);

      // step 5
      String postBackUrl = getPostBackUrlFromHTML(responseHtml);
      if (!isPrefixEqual(postBackUrl, loginInput.getServerUrl()))
      {
        logger.debug("The specified authenticator {} and the destination URL " +
                "in the SAML assertion {} do not match.",
            loginInput.getAuthenticator(), postBackUrl);
        throw new SnowflakeSQLException(
            SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
            ErrorCode.IDP_INCORRECT_DESTINATION.getMessageCode());
      }
    }
    catch (IOException | URISyntaxException ex)
    {
      handleFederatedFlowError(loginInput, ex);
    }
    return responseHtml;
  }

  /**
   * Query IDP token url to authenticate and retrieve access token
   *
   * @param loginInput
   * @param tokenUrl
   * @return
   * @throws SnowflakeSQLException
   */
  private static String federatedFlowStep3(LoginInput loginInput, String tokenUrl)
      throws SnowflakeSQLException

  {
    String oneTimeToken = "";
    try
    {
      URL url = new URL(tokenUrl);
      URI tokenUri = url.toURI();
      final HttpPost postRequest = new HttpPost(tokenUri);

      StringEntity params = new StringEntity("{\"username\":\"" +
          loginInput.getUserName() + "\",\"password\":\"" +
          loginInput.getPassword() + "\"}");
      postRequest.setEntity(params);

      HeaderGroup headers = new HeaderGroup();
      headers.addHeader(new BasicHeader(HttpHeaders.ACCEPT, "application/json"));
      headers.addHeader(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"));
      postRequest.setHeaders(headers.getAllHeaders());

      final String idpResponse = HttpUtil.executeRequestWithoutCookies(postRequest,
          loginInput.getLoginTimeout(), 0, null);

      logger.debug("user is authenticated against {}.",
          loginInput.getAuthenticator());

      // session token is in the data field of the returned json response
      final JsonNode jsonNode = mapper.readTree(idpResponse);
      oneTimeToken = jsonNode.get("cookieToken").asText();
    }
    catch (IOException | URISyntaxException ex)
    {
      handleFederatedFlowError(loginInput, ex);
    }
    return oneTimeToken;
  }

  /**
   * Perform important client side validation:
   * validate both token url and sso url contains same prefix
   * (protocol + host + port) as the given authenticator url.
   * Explanation:
   * This provides a way for the user to 'authenticate' the IDP it is
   * sending his/her credentials to.  Without such a check, the user could
   * be coerced to provide credentials to an IDP impersonator.
   *
   * @param loginInput
   * @param tokenUrl
   * @param ssoUrl
   * @throws SnowflakeSQLException
   */
  private static void federatedFlowStep2(
      LoginInput loginInput,
      String tokenUrl,
      String ssoUrl) throws SnowflakeSQLException
  {
    try
    {
      if (!isPrefixEqual(loginInput.getAuthenticator(), tokenUrl) ||
          !isPrefixEqual(loginInput.getAuthenticator(), ssoUrl))
      {
        logger.debug("The specified authenticator {} is not supported.",
            loginInput.getAuthenticator());
        throw new SnowflakeSQLException(
            SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
            ErrorCode.IDP_CONNECTION_ERROR.getMessageCode());
      }
    }
    catch (MalformedURLException ex)
    {
      handleFederatedFlowError(loginInput, ex);
    }
  }

  /**
   * Query Snowflake to obtain IDP token url and IDP SSO url
   *
   * @param loginInput
   * @throws SnowflakeSQLException
   */
  private static JsonNode federatedFlowStep1(LoginInput loginInput)
      throws SnowflakeSQLException
  {
    JsonNode dataNode = null;
    try
    {
      URIBuilder fedUriBuilder = new URIBuilder(loginInput.getServerUrl());
      fedUriBuilder.setPath(SF_PATH_AUTHENTICATOR_REQUEST);
      URI fedUrlUri = fedUriBuilder.build();

      Map<String, Object> data = new HashMap<>();
      data.put(ClientAuthnParameter.ACCOUNT_NAME.name(),
          loginInput.getAccountName());
      data.put(ClientAuthnParameter.AUTHENTICATOR.name(),
          loginInput.getAuthenticator());
      data.put(ClientAuthnParameter.CLIENT_APP_ID.name(), loginInput.getAppId());
      data.put(ClientAuthnParameter.CLIENT_APP_VERSION.name(),
          loginInput.getAppVersion());

      ClientAuthnDTO authnData = new ClientAuthnDTO();
      authnData.setData(data);
      String json = mapper.writeValueAsString(authnData);

      // attach the login info json body to the post request
      StringEntity input = new StringEntity(json, Charset.forName("UTF-8"));
      input.setContentType("application/json");
      HttpPost postRequest = new HttpPost(fedUrlUri);
      postRequest.setEntity(input);
      postRequest.addHeader("accept", "application/json");

      final String gsResponse = HttpUtil.executeRequest(postRequest,
          loginInput.getLoginTimeout(), 0, null);
      logger.debug("authenticator-request response: {}", gsResponse);
      JsonNode jsonNode = mapper.readTree(gsResponse);

      // check the success field first
      if (!jsonNode.path("success").asBoolean())
      {
        logger.debug("response = {}", gsResponse);
        String errorCode = jsonNode.path("code").asText();
        throw new SnowflakeSQLException(
            SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
            ErrorCode.CONNECTION_ERROR.getMessageCode(),
            errorCode, jsonNode.path("message").asText());
      }

      // session token is in the data field of the returned json response
      dataNode = jsonNode.path("data");
    }
    catch (IOException | URISyntaxException ex)
    {
      handleFederatedFlowError(loginInput, ex);
    }
    return dataNode;
  }

  /**
   * Logs an error generated during the federated authentication flow and
   * re-throws it as a SnowflakeSQLException.
   * Note that we seperate IOExceptions since those tend to be network related.
   *
   * @param loginInput
   * @param ex
   * @throws SnowflakeSQLException
   */
  private static void handleFederatedFlowError(LoginInput loginInput, Exception ex)
      throws SnowflakeSQLException

  {
    if (ex instanceof IOException)
    {
      logger.error("IOException when authenticating with " +
          loginInput.getAuthenticator(), ex);
      throw new SnowflakeSQLException(ex, SqlState.IO_ERROR,
          ErrorCode.NETWORK_ERROR.getMessageCode(),
          "Exception encountered when opening connection: " +
              ex.getMessage());
    }
    logger.error("Exception when authenticating with " +
        loginInput.getAuthenticator(), ex);
    throw new SnowflakeSQLException(ex,
        SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
        ErrorCode.CONNECTION_ERROR.getMessageCode(),
        ErrorCode.CONNECTION_ERROR.getMessageCode(), ex.getMessage());
  }

  /**
   * FEDERATED FLOW
   * See SNOW-27798 for additional details.
   *
   * @return saml response
   * @throws SnowflakeSQLException
   */
  static private String getSamlResponseUsingOkta(LoginInput loginInput)
      throws SnowflakeSQLException
  {
    JsonNode dataNode = federatedFlowStep1(loginInput);
    String tokenUrl = dataNode.path("tokenUrl").asText();
    String ssoUrl = dataNode.path("ssoUrl").asText();
    federatedFlowStep2(loginInput, tokenUrl, ssoUrl);
    final String oneTimeToken = federatedFlowStep3(loginInput, tokenUrl);
    final String responseHtml = federatedFlowStep4(
        loginInput, ssoUrl, oneTimeToken);
    return responseHtml;
  }

  /**
   * Verify if two input urls have the same protocol, host, and port.
   *
   * @param aUrlStr a source URL string
   * @param bUrlStr a target URL string
   * @return true if matched otherwise false
   * @throws MalformedURLException raises if a URL string is not valid.
   */
  static boolean isPrefixEqual(String aUrlStr, String bUrlStr)
      throws MalformedURLException
  {
    URL aUrl = new URL(aUrlStr);
    URL bUrl = new URL(bUrlStr);
    int aPort = aUrl.getPort();
    int bPort = bUrl.getPort();
    if (aPort == -1 && "https".equals(aUrl.getProtocol()))
    {
      // default port number for HTTPS
      aPort = 443;
    }
    if (bPort == -1 && "https".equals(bUrl.getProtocol()))
    {
      // default port number for HTTPS
      bPort = 443;
    }
    // no default port number for HTTP is supported.
    return aUrl.getHost().equalsIgnoreCase(bUrl.getHost()) &&
        aUrl.getProtocol().equalsIgnoreCase(bUrl.getProtocol()) &&
        aPort == bPort;
  }

  /**
   * Extracts post back url from the HTML returned by the IDP
   *
   * @param html
   * @return
   */
  static private String getPostBackUrlFromHTML(String html)
  {
    Document doc = Jsoup.parse(html);
    Elements e1 = doc.getElementsByTag("body");
    Elements e2 = e1.get(0).getElementsByTag("form");
    String postBackUrl = e2.first().attr("action");
    return postBackUrl;
  }

  /**
   * Check if com.sun.security.enableCRLDP and com.sun.net.ssl.checkRevocation
   * are set to true
   *
   * @return true if both system properties set to true, false otherwise.
   */
  static public boolean checkCRLSystemProperty()
  {
    String enableCRLDP = System.getProperty("com.sun.security.enableCRLDP");
    String checkRevocation = System.getProperty("com.sun.net.ssl.checkRevocation");
    boolean CRLEnabled = false;

    if ((enableCRLDP != null && "true".equalsIgnoreCase(enableCRLDP)) &&
        (checkRevocation != null && "true".equalsIgnoreCase(checkRevocation)))
      CRLEnabled = true;
    return CRLEnabled;
  }

  /**
   * Helper function to parse a JsonNode from a GS response
   * containing CommonParameters, emitting an EnumMap of parameters
   *
   * @param paramsNode parameters in JSON form
   * @return map object including key and value pairs
   */
  public static Map<String, Object> getCommonParams(JsonNode paramsNode)
  {
    Map<String, Object> parameters = new HashMap<>();

    for (JsonNode child : paramsNode)
    {
      // If there isn't a name then the response from GS must be erroneous.
      if (!child.hasNonNull("name"))
      {
        logger.error("Common Parameter JsonNode encountered with "
            + "no parameter name!");
        continue;
      }

      // Look up the parameter based on the "name" attribute of the node.
      String paramName = child.path("name").asText();

      // What type of value is it and what's the value?
      if (!child.hasNonNull("value"))
      {
        logger.debug("No value found for Common Parameter {}",
            child.path("name").asText());
        continue;
      }

      if (STRING_PARAMS.contains(paramName.toUpperCase()))
      {
        parameters.put(paramName, child.path("value").asText());
      }
      else if (INT_PARAMS.contains(paramName.toUpperCase()))
      {
        parameters.put(paramName, child.path("value").asInt());
      }
      else if (BOOLEAN_PARAMS.contains(paramName.toUpperCase()))
      {
        parameters.put(paramName, child.path("value").asBoolean());
      }
      else
      {
        logger.debug("Unknown Common Parameter: {}", paramName);
      }

      logger.debug("Parameter {}: {}",
          paramName, child.path("value").asText());
    }

    return parameters;
  }

  public static void updateSfDriverParamValues(
      Map<String, Object> parameters,
      SFSession session)
  {
    for (Map.Entry<String, Object> entry : parameters.entrySet())
    {
      logger.debug("processing parameter {}", entry.getKey());

      if ("CLIENT_DISABLE_INCIDENTS".equalsIgnoreCase(entry.getKey()))
      {
        SnowflakeDriver.setDisableIncidents((Boolean) entry.getValue());
      }
      else if (
          "JDBC_EXECUTE_RETURN_COUNT_FOR_DML".equalsIgnoreCase(entry.getKey()))
      {
        if (session != null)
        {
          session.setExecuteReturnCountForDML((Boolean) entry.getValue());
        }
      }
      else if (
          "CLIENT_SESSION_KEEP_ALIVE".equalsIgnoreCase(entry.getKey()))
      {
        if (session != null)
        {
          session.setEnableHeartbeat((Boolean) entry.getValue());
        }
      }
      else if (
          "AUTOCOMMIT".equalsIgnoreCase(entry.getKey()))
      {
        boolean autoCommit = (Boolean) entry.getValue();
        if (session != null && session.getAutoCommit() != autoCommit)
        {
          session.setAutoCommit(autoCommit);
        }
      }
      else if (
          JDBC_RS_COLUMN_CASE_INSENSITIVE.equalsIgnoreCase(entry.getKey()) ||
              CLIENT_RESULT_COLUMN_CASE_INSENSITIVE.equalsIgnoreCase(entry.getKey()))
      {
        if (session != null && !session.isResultColumnCaseInsensitive())
        {
          session.setResultColumnCaseInsensitive((boolean) entry.getValue());
        }
      }
      else if ("CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX".equalsIgnoreCase(entry.getKey()))
      {
        if (session != null)
        {
          session.setMetadataRequestUseConnectionCtx((boolean) entry.getValue());
        }
      }
      else if ("CLIENT_TIMESTAMP_TYPE_MAPPING".equalsIgnoreCase(entry.getKey()))
      {
        if (session != null)
        {
          session.setTimestampMappedType(SnowflakeType.valueOf(
              ((String) entry.getValue()).toUpperCase()));
        }
      }
      else if ("JDBC_TREAT_DECIMAL_AS_INT".equalsIgnoreCase(entry.getKey()))
      {
        if (session != null)
        {
          session.setJdbcTreatDecimalAsInt((boolean) entry.getValue());
        }
      }
      else if ("JDBC_ENABLE_COMBINED_DESCRIBE".equalsIgnoreCase(entry.getKey()))
      {
        if (session != null)
        {
          session.setEnableCombineDescribe((boolean) entry.getValue());
        }
      }
      else if ("CLIENT_TELEMETRY_ENABLED".equalsIgnoreCase(entry.getKey()))
      {
        if (session != null)
        {
          session.setClientTelemetryEnabled((boolean) entry.getValue());
        }
      }
      else if ("CLIENT_STAGE_ARRAY_BINDING_THRESHOLD".equalsIgnoreCase(entry.getKey()))
      {
        if (session != null)
        {
          session.setArrayBindStageThreshold((int) entry.getValue());
        }
      }
      else if (CLIENT_STORE_TEMPORARY_CREDENTIAL.equalsIgnoreCase(entry.getKey()))
      {
        if (session != null)
        {
          session.setStoreTemporaryCredential((boolean) entry.getValue());
        }
      }
      else if (SERVICE_NAME.equalsIgnoreCase(entry.getKey()))
      {
        if (session != null)
        {
          session.setServiceName((String) entry.getValue());
        }
      }
    }
  }
}
