package net.snowflake.client.core;

import static net.snowflake.client.core.SFTrustManager.resetOCSPResponseCacherServerURL;
import static net.snowflake.client.core.SFTrustManager.setOCSPResponseCacheServerURL;
import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.client.core.auth.AuthenticatorType;
import net.snowflake.client.core.auth.ClientAuthnDTO;
import net.snowflake.client.core.auth.ClientAuthnParameter;
import net.snowflake.client.core.auth.oauth.AccessTokenProvider;
import net.snowflake.client.core.auth.oauth.DPoPUtil;
import net.snowflake.client.core.auth.oauth.OAuthAccessTokenForRefreshTokenProvider;
import net.snowflake.client.core.auth.oauth.OAuthAccessTokenProviderFactory;
import net.snowflake.client.core.auth.oauth.TokenResponseDTO;
import net.snowflake.client.core.auth.wif.AwsAttestationService;
import net.snowflake.client.core.auth.wif.AwsIdentityAttestationCreator;
import net.snowflake.client.core.auth.wif.AzureAttestationService;
import net.snowflake.client.core.auth.wif.AzureIdentityAttestationCreator;
import net.snowflake.client.core.auth.wif.GcpIdentityAttestationCreator;
import net.snowflake.client.core.auth.wif.OidcIdentityAttestationCreator;
import net.snowflake.client.core.auth.wif.WorkloadIdentityAttestation;
import net.snowflake.client.core.auth.wif.WorkloadIdentityAttestationProvider;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.RetryContext;
import net.snowflake.client.jdbc.RetryContextManager;
import net.snowflake.client.jdbc.SnowflakeDriver;
import net.snowflake.client.jdbc.SnowflakeReauthenticationRequest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.client.util.Stopwatch;
import net.snowflake.client.util.ThrowingFunction;
import net.snowflake.common.core.SqlState;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

/** Low level session util */
public class SessionUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SessionUtil.class);
  // Response Field Name
  private static final String SF_QUERY_DATABASE = "databaseName";
  private static final String SF_QUERY_SCHEMA = "schemaName";
  private static final String SF_QUERY_WAREHOUSE = "warehouse";
  private static final String SF_QUERY_ROLE = "roleName";

  // Request path
  private static final String SF_PATH_LOGIN_REQUEST = "/session/v1/login-request";
  private static final String SF_PATH_TOKEN_REQUEST = "/session/token-request";
  private static final String SF_PATH_OKTA_TOKEN_REQUEST_SUFFIX = "/api/v1/authn";
  private static final String SF_PATH_OKTA_SSO_REQUEST_SUFFIX = "/sso/saml";
  public static final String SF_PATH_AUTHENTICATOR_REQUEST = "/session/authenticator-request";
  public static final String SF_PATH_CONSOLE_LOGIN_REQUEST = "/console/login";

  public static final String SF_QUERY_SESSION_DELETE = "delete";

  // Headers
  @Deprecated
  public static final String SF_HEADER_AUTHORIZATION = SFSession.SF_HEADER_AUTHORIZATION;

  // Authentication type
  private static final String SF_HEADER_BASIC_AUTHTYPE = "Basic";
  private static final String CLIENT_STORE_TEMPORARY_CREDENTIAL =
      "CLIENT_STORE_TEMPORARY_CREDENTIAL";
  private static final String CLIENT_REQUEST_MFA_TOKEN = "CLIENT_REQUEST_MFA_TOKEN";
  private static final String SERVICE_NAME = "SERVICE_NAME";
  private static final String CLIENT_IN_BAND_TELEMETRY_ENABLED = "CLIENT_TELEMETRY_ENABLED";
  private static final String CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED =
      "CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED";
  private static final String CLIENT_RESULT_COLUMN_CASE_INSENSITIVE =
      "CLIENT_RESULT_COLUMN_CASE_INSENSITIVE";
  private static final String JDBC_RS_COLUMN_CASE_INSENSITIVE = "JDBC_RS_COLUMN_CASE_INSENSITIVE";
  private static final String JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC = "JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC";
  private static final String JDBC_FORMAT_DATE_WITH_TIMEZONE = "JDBC_FORMAT_DATE_WITH_TIMEZONE";
  private static final String JDBC_USE_SESSION_TIMEZONE = "JDBC_USE_SESSION_TIMEZONE";
  public static final String JDBC_CHUNK_DOWNLOADER_MAX_RETRY = "JDBC_CHUNK_DOWNLOADER_MAX_RETRY";
  private static final String CLIENT_RESULT_CHUNK_SIZE_JVM =
      "net.snowflake.jdbc.clientResultChunkSize";
  public static final String CLIENT_RESULT_CHUNK_SIZE = "CLIENT_RESULT_CHUNK_SIZE";
  public static final String CLIENT_MEMORY_LIMIT_JVM = "net.snowflake.jdbc.clientMemoryLimit";
  public static final String CLIENT_MEMORY_LIMIT = "CLIENT_MEMORY_LIMIT";
  public static final String QUERY_CONTEXT_CACHE_SIZE = "QUERY_CONTEXT_CACHE_SIZE";
  public static final String JDBC_ENABLE_PUT_GET = "JDBC_ENABLE_PUT_GET";
  public static final String CLIENT_PREFETCH_THREADS_JVM =
      "net.snowflake.jdbc.clientPrefetchThreads";
  public static final String CLIENT_PREFETCH_THREADS = "CLIENT_PREFETCH_THREADS";
  public static final String CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE_JVM =
      "net.snowflake.jdbc.clientEnableConservativeMemoryUsage";
  public static final String CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE =
      "CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE";
  public static final String CLIENT_CONSERVATIVE_MEMORY_ADJUST_STEP =
      "CLIENT_CONSERVATIVE_MEMORY_ADJUST_STEP";
  public static final String OCSP_FAIL_OPEN_JVM = "net.snowflake.jdbc.ocspFailOpen";
  private static final String OCSP_FAIL_OPEN = "ocspFailOpen";
  public static final String CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY =
      "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY";
  public static final String CLIENT_SFSQL = "CLIENT_SFSQL";
  public static final String CLIENT_VALIDATE_DEFAULT_PARAMETERS =
      "CLIENT_VALIDATE_DEFAULT_PARAMETERS";
  public static final String CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS =
      "CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS";
  public static final String CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX =
      "CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX";
  public static final String CLIENT_METADATA_USE_SESSION_DATABASE =
      "CLIENT_METADATA_USE_SESSION_DATABASE";
  public static final String ENABLE_STAGE_S3_PRIVATELINK_FOR_US_EAST_1 =
      "ENABLE_STAGE_S3_PRIVATELINK_FOR_US_EAST_1";

  static final String SF_HEADER_SERVICE_NAME = "X-Snowflake-Service";

  public static final String SF_HEADER_CLIENT_APP_ID = "CLIENT_APP_ID";

  public static final String SF_HEADER_CLIENT_APP_VERSION = "CLIENT_APP_VERSION";

  private static final String ID_TOKEN_AUTHENTICATOR = "ID_TOKEN";

  private static final String NO_QUERY_ID = "";
  private static final String SF_PATH_SESSION = "/session";
  public static long DEFAULT_CLIENT_MEMORY_LIMIT = 1536; // MB
  public static int DEFAULT_CLIENT_PREFETCH_THREADS = 4;
  public static int MIN_CLIENT_CHUNK_SIZE = 48;
  public static int MAX_CLIENT_CHUNK_SIZE = 160;

  public static Map<String, String> JVM_PARAMS_TO_PARAMS =
      Stream.of(
              new String[][] {
                {CLIENT_RESULT_CHUNK_SIZE_JVM, CLIENT_RESULT_CHUNK_SIZE},
                {CLIENT_MEMORY_LIMIT_JVM, CLIENT_MEMORY_LIMIT},
                {CLIENT_PREFETCH_THREADS_JVM, CLIENT_PREFETCH_THREADS},
                {OCSP_FAIL_OPEN_JVM, OCSP_FAIL_OPEN},
                {
                  CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE_JVM,
                  CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE
                }
              })
          .collect(Collectors.toMap(data -> data[0], data -> data[1]));
  private static ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
  private static int DEFAULT_HEALTH_CHECK_INTERVAL = 45; // sec
  private static Set<String> STRING_PARAMS =
      new HashSet<>(
          Arrays.asList(
              "TIMEZONE",
              "TIMESTAMP_OUTPUT_FORMAT",
              "TIMESTAMP_NTZ_OUTPUT_FORMAT",
              "TIMESTAMP_LTZ_OUTPUT_FORMAT",
              "TIMESTAMP_TZ_OUTPUT_FORMAT",
              "DATE_OUTPUT_FORMAT",
              "TIME_OUTPUT_FORMAT",
              "BINARY_OUTPUT_FORMAT",
              "CLIENT_TIMESTAMP_TYPE_MAPPING",
              SERVICE_NAME,
              "GEOGRAPHY_OUTPUT_FORMAT"));
  private static final Set<String> INT_PARAMS =
      new HashSet<>(
          Arrays.asList(
              CLIENT_PREFETCH_THREADS,
              CLIENT_MEMORY_LIMIT,
              CLIENT_RESULT_CHUNK_SIZE,
              "CLIENT_STAGE_ARRAY_BINDING_THRESHOLD",
              "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY"));
  private static final Set<String> BOOLEAN_PARAMS =
      new HashSet<>(
          Arrays.asList(
              CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY,
              "CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ",
              "CLIENT_DISABLE_INCIDENTS",
              "CLIENT_SESSION_KEEP_ALIVE",
              CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS,
              CLIENT_IN_BAND_TELEMETRY_ENABLED,
              CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED,
              CLIENT_STORE_TEMPORARY_CREDENTIAL,
              CLIENT_REQUEST_MFA_TOKEN,
              "JDBC_USE_JSON_PARSER",
              "AUTOCOMMIT",
              "JDBC_EFFICIENT_CHUNK_STORAGE",
              JDBC_RS_COLUMN_CASE_INSENSITIVE,
              JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC,
              JDBC_FORMAT_DATE_WITH_TIMEZONE,
              JDBC_USE_SESSION_TIMEZONE,
              CLIENT_RESULT_COLUMN_CASE_INSENSITIVE,
              CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX,
              CLIENT_METADATA_USE_SESSION_DATABASE,
              "JDBC_TREAT_DECIMAL_AS_INT",
              "JDBC_ENABLE_COMBINED_DESCRIBE",
              CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE,
              CLIENT_VALIDATE_DEFAULT_PARAMETERS,
              ENABLE_STAGE_S3_PRIVATELINK_FOR_US_EAST_1,
              "SNOWPARK_LAZY_ANALYSIS"));

  /**
   * Returns Authenticator type
   *
   * @param loginInput login information
   * @return Authenticator type
   */
  private static AuthenticatorType getAuthenticator(SFLoginInput loginInput) {
    if (loginInput.getAuthenticator() != null) {
      if (loginInput
          .getAuthenticator()
          .equalsIgnoreCase(AuthenticatorType.EXTERNALBROWSER.name())) {
        // SAML 2.0 compliant service/application
        return AuthenticatorType.EXTERNALBROWSER;
      } else if (loginInput
          .getAuthenticator()
          .equalsIgnoreCase(AuthenticatorType.OAUTH_AUTHORIZATION_CODE.name())) {
        return AuthenticatorType.OAUTH_AUTHORIZATION_CODE;
      } else if (loginInput
          .getAuthenticator()
          .equalsIgnoreCase(AuthenticatorType.OAUTH_CLIENT_CREDENTIALS.name())) {
        return AuthenticatorType.OAUTH_CLIENT_CREDENTIALS;
      } else if (loginInput.getAuthenticator().equalsIgnoreCase(AuthenticatorType.OAUTH.name())) {
        // OAuth access code Authentication
        return AuthenticatorType.OAUTH;
      } else if (loginInput
          .getAuthenticator()
          .equalsIgnoreCase(AuthenticatorType.PROGRAMMATIC_ACCESS_TOKEN.name())) {
        return AuthenticatorType.PROGRAMMATIC_ACCESS_TOKEN;
      } else if (loginInput
          .getAuthenticator()
          .equalsIgnoreCase(AuthenticatorType.WORKLOAD_IDENTITY.name())) {
        return AuthenticatorType.WORKLOAD_IDENTITY;
      } else if (loginInput
          .getAuthenticator()
          .equalsIgnoreCase(AuthenticatorType.SNOWFLAKE_JWT.name())) {
        return AuthenticatorType.SNOWFLAKE_JWT;
      } else if (loginInput
          .getAuthenticator()
          .equalsIgnoreCase(AuthenticatorType.USERNAME_PASSWORD_MFA.name())) {
        return AuthenticatorType.USERNAME_PASSWORD_MFA;
      } else if (!loginInput
          .getAuthenticator()
          .equalsIgnoreCase(AuthenticatorType.SNOWFLAKE.name())) {
        // OKTA authenticator v1.
        return AuthenticatorType.OKTA;
      }
    }

    // authenticator is null, then jdbc will decide authenticator depends on
    // if privateKey is specified or not. If yes, authenticator type will be
    // SNOWFLAKE_JWT, otherwise it will use SNOWFLAKE.
    return loginInput.isPrivateKeyProvided()
        ? AuthenticatorType.SNOWFLAKE_JWT
        : AuthenticatorType.SNOWFLAKE;
  }

  /**
   * Open a new session
   *
   * @param loginInput login information
   * @return information get after login such as token information
   * @throws SFException if unexpected uri syntax
   * @throws SnowflakeSQLException if failed to establish connection with snowflake
   */
  static SFLoginOutput openSession(
      SFLoginInput loginInput,
      Map<SFSessionProperty, Object> connectionPropertiesMap,
      String tracingLevel)
      throws SFException, SnowflakeSQLException {
    AssertUtil.assertTrue(
        loginInput.getServerUrl() != null, "missing server URL for opening session");

    AssertUtil.assertTrue(loginInput.getAppId() != null, "missing app id for opening session");

    AssertUtil.assertTrue(
        loginInput.getLoginTimeout() >= 0, "negative login timeout for opening session");

    final AuthenticatorType authenticator = getAuthenticator(loginInput);
    checkIfExperimentalAuthnEnabled(authenticator);

    if (isTokenOrPasswordRequired(authenticator)) {
      AssertUtil.assertTrue(
          loginInput.getToken() != null || loginInput.getPassword() != null,
          "missing token or password for opening session");
    }
    if (isUsernameRequired(authenticator)) {
      AssertUtil.assertTrue(
          loginInput.getUserName() != null, "missing user name for opening session");
    }
    if (isEligibleForTokenCaching(authenticator)) {
      if ((Constants.getOS() == Constants.OS.MAC || Constants.getOS() == Constants.OS.WINDOWS)
          && loginInput.isEnableClientStoreTemporaryCredential()) {
        // force to set the flag for Mac/Windows users
        loginInput.getSessionParameters().put(CLIENT_STORE_TEMPORARY_CREDENTIAL, true);
      } else {
        // Linux should read from JDBC configuration. For other unsupported OS, we set it to false
        // as default value
        if (!loginInput.getSessionParameters().containsKey(CLIENT_STORE_TEMPORARY_CREDENTIAL)) {
          loginInput.getSessionParameters().put(CLIENT_STORE_TEMPORARY_CREDENTIAL, false);
        }
      }
    } else {
      // TODO: patch for now. We should update mergeProperties
      // to normalize all parameters using STRING_PARAMS, INT_PARAMS and
      // BOOLEAN_PARAMS.
      Object value = loginInput.getSessionParameters().get(CLIENT_STORE_TEMPORARY_CREDENTIAL);
      if (value != null) {
        loginInput.getSessionParameters().put(CLIENT_STORE_TEMPORARY_CREDENTIAL, asBoolean(value));
      }
    }

    if (authenticator.equals(AuthenticatorType.USERNAME_PASSWORD_MFA)) {
      if ((Constants.getOS() == Constants.OS.MAC || Constants.getOS() == Constants.OS.WINDOWS)
          && loginInput.isEnableClientRequestMfaToken()) {
        loginInput.getSessionParameters().put(CLIENT_REQUEST_MFA_TOKEN, true);
      }
    }

    if (authenticator.equals(AuthenticatorType.WORKLOAD_IDENTITY)) {
      WorkloadIdentityAttestation attestation = getWorkloadIdentityAttestation(loginInput);
      if (attestation != null) {
        loginInput.setWorkloadIdentityAttestation(attestation);
      } else {
        throw new SFException(
            ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
            "Unable to obtain workload identity attestation. Make sure that correct workload identity provider has been set and that Snowflake-JDBC driver runs on supported environment.");
      }
    }

    convertSessionParameterStringValueToBooleanIfGiven(loginInput, CLIENT_REQUEST_MFA_TOKEN);

    readCachedCredentialsIfPossible(loginInput);
    if (OAuthAccessTokenProviderFactory.isEligible(getAuthenticator(loginInput))) {
      obtainAuthAccessTokenAndUpdateInput(loginInput);
    }

    try {
      return newSession(loginInput, connectionPropertiesMap, tracingLevel);
    } catch (SnowflakeReauthenticationRequest ex) {
      if (ex.getErrorCode() == Constants.OAUTH_ACCESS_TOKEN_EXPIRED_GS_CODE) {
        if (loginInput.getOauthRefreshToken() != null) {
          refreshOAuthAccessTokenAndUpdateInput(loginInput);
        } else {
          loginInput.restoreOriginalAuthenticator();
          fetchOAuthAccessTokenAndUpdateInput(loginInput);
        }
      }
      return newSession(loginInput, connectionPropertiesMap, tracingLevel);
    }
  }

  private static WorkloadIdentityAttestation getWorkloadIdentityAttestation(SFLoginInput loginInput)
      throws SFException {
    WorkloadIdentityAttestationProvider attestationProvider =
        new WorkloadIdentityAttestationProvider(
            new AwsIdentityAttestationCreator(new AwsAttestationService()),
            new GcpIdentityAttestationCreator(loginInput),
            new AzureIdentityAttestationCreator(new AzureAttestationService(), loginInput),
            new OidcIdentityAttestationCreator(loginInput.getToken()));
    return attestationProvider.getAttestation(loginInput.getWorkloadIdentityProvider());
  }

  static void checkIfExperimentalAuthnEnabled(AuthenticatorType authenticator) throws SFException {
    if (authenticator.equals(AuthenticatorType.PROGRAMMATIC_ACCESS_TOKEN)
        || authenticator.equals(AuthenticatorType.WORKLOAD_IDENTITY)) {
      boolean experimentalAuthenticationMethodsEnabled =
          Boolean.parseBoolean(systemGetEnv("SF_ENABLE_EXPERIMENTAL_AUTHENTICATION"));
      AssertUtil.assertTrue(
          experimentalAuthenticationMethodsEnabled,
          "Following authentication method not yet supported: " + authenticator);
    }
  }

  private static boolean isEligibleForTokenCaching(AuthenticatorType authenticator) {
    return authenticator.equals(AuthenticatorType.EXTERNALBROWSER)
        || authenticator.equals(AuthenticatorType.OAUTH_AUTHORIZATION_CODE)
        || authenticator.equals(AuthenticatorType.OAUTH_CLIENT_CREDENTIALS);
  }

  private static boolean isTokenOrPasswordRequired(AuthenticatorType authenticator) {
    return authenticator.equals(AuthenticatorType.OAUTH)
        || authenticator.equals(AuthenticatorType.PROGRAMMATIC_ACCESS_TOKEN);
  }

  private static boolean isUsernameRequired(AuthenticatorType authenticator) {
    return !authenticator.equals(AuthenticatorType.OAUTH)
        && !authenticator.equals(AuthenticatorType.PROGRAMMATIC_ACCESS_TOKEN)
        && !authenticator.equals(AuthenticatorType.OAUTH_AUTHORIZATION_CODE)
        && !authenticator.equals(AuthenticatorType.OAUTH_CLIENT_CREDENTIALS);
  }

  private static void obtainAuthAccessTokenAndUpdateInput(SFLoginInput loginInput)
      throws SFException {
    if (loginInput.getOauthAccessToken() != null) { // Access Token was cached
      loginInput.setAuthenticator(AuthenticatorType.OAUTH.name());
      loginInput.setToken(loginInput.getOauthAccessToken());
    } else { // Access Token not cached
      fetchOAuthAccessTokenAndUpdateInput(loginInput);
    }
  }

  private static void fetchOAuthAccessTokenAndUpdateInput(SFLoginInput loginInput)
      throws SFException {
    OAuthAccessTokenProviderFactory accessTokenProviderFactory =
        new OAuthAccessTokenProviderFactory(
            new SessionUtilExternalBrowser.DefaultAuthExternalBrowserHandlers(),
            loginInput.getBrowserResponseTimeout().getSeconds());
    AccessTokenProvider accessTokenProvider =
        accessTokenProviderFactory.createAccessTokenProvider(
            getAuthenticator(loginInput), loginInput);
    TokenResponseDTO tokenResponse = accessTokenProvider.getAccessToken(loginInput);
    loginInput.setAuthenticator(AuthenticatorType.OAUTH.name());
    loginInput.setToken(tokenResponse.getAccessToken());
    loginInput.setOauthAccessToken(tokenResponse.getAccessToken());
    loginInput.setOauthRefreshToken(tokenResponse.getRefreshToken());
    if (loginInput.isDPoPEnabled() && accessTokenProvider.getDPoPPublicKey() != null) {
      loginInput.setDPoPPublicKey(accessTokenProvider.getDPoPPublicKey());
    }
  }

  private static void refreshOAuthAccessTokenAndUpdateInput(SFLoginInput loginInput)
      throws SFException {
    try {
      OAuthAccessTokenForRefreshTokenProvider tokenRefresher =
          new OAuthAccessTokenForRefreshTokenProvider();
      TokenResponseDTO tokenResponse = tokenRefresher.getAccessToken(loginInput);
      loginInput.setToken(tokenResponse.getAccessToken());
      loginInput.setOauthAccessToken(tokenResponse.getAccessToken());
      loginInput.setAuthenticator(AuthenticatorType.OAUTH.name());
      if (loginInput.isDPoPEnabled() && tokenRefresher.getDPoPPublicKey() != null) {
        loginInput.setDPoPPublicKey(tokenRefresher.getDPoPPublicKey());
      }
      if (tokenResponse.getRefreshToken() != null) {
        loginInput.setOauthRefreshToken(tokenResponse.getRefreshToken());
      }
    } catch (SFException | Exception e) {
      logger.debug(
          "Refreshing OAuth access token failed. Removing OAuth refresh token from cache and restarting OAuth flow...",
          e);
      CredentialManager.deleteOAuthRefreshTokenCacheEntry(loginInput);
      loginInput.restoreOriginalAuthenticator();
      fetchOAuthAccessTokenAndUpdateInput(loginInput);
    }
  }

  private static void convertSessionParameterStringValueToBooleanIfGiven(
      SFLoginInput loginInput, String parameterName) {
    Object currentClientRequestMfaToken = loginInput.getSessionParameters().get(parameterName);
    if (currentClientRequestMfaToken instanceof String) {
      loginInput
          .getSessionParameters()
          .put(parameterName, Boolean.parseBoolean((String) currentClientRequestMfaToken));
    }
  }

  private static void readCachedCredentialsIfPossible(SFLoginInput loginInput) throws SFException {
    if (!isNullOrEmpty(loginInput.getUserName())) {
      if (asBoolean(loginInput.getSessionParameters().get(CLIENT_STORE_TEMPORARY_CREDENTIAL))) {
        CredentialManager.fillCachedIdToken(loginInput);
        CredentialManager.fillCachedOAuthRefreshToken(loginInput);
        if (loginInput.isDPoPEnabled()) {
          CredentialManager.fillCachedDPoPBundledAccessToken(loginInput);
        }
        if (loginInput.getOauthAccessToken() == null && loginInput.getDPoPPublicKey() == null) {
          CredentialManager.fillCachedOAuthAccessToken(loginInput);
        }
      }

      if (asBoolean(loginInput.getSessionParameters().get(CLIENT_REQUEST_MFA_TOKEN))) {
        CredentialManager.fillCachedMfaToken(loginInput);
      }
    }
  }

  private static boolean asBoolean(Object value) {
    if (value == null) {
      return false;
    }
    switch (value.getClass().getName()) {
      case "java.lang.Boolean":
        return (Boolean) value;
      case "java.lang.String":
        return Boolean.valueOf((String) value);
    }
    return false;
  }

  static SFLoginOutput newSession(
      SFLoginInput loginInput,
      Map<SFSessionProperty, Object> connectionPropertiesMap,
      String tracingLevel)
      throws SFException, SnowflakeSQLException {
    try {
      // Adjust OCSP cache server if it is private link
      resetOCSPUrlIfNecessary(loginInput.getServerUrl());
    } catch (IOException ex) {
      throw new SFException(ex, ErrorCode.IO_ERROR, "unexpected URL syntax exception");
    }

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
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
    String sessionId;
    long masterTokenValidityInSeconds;
    String idToken;
    String mfaToken;
    String databaseVersion = null;
    int databaseMajorVersion = 0;
    int databaseMinorVersion = 0;
    String newClientForUpgrade;
    int healthCheckInterval = DEFAULT_HEALTH_CHECK_INTERVAL;
    int httpClientSocketTimeout = loginInput.getSocketTimeoutInMillis();
    int httpClientConnectionTimeout = loginInput.getConnectionTimeoutInMillis();
    final AuthenticatorType authenticatorType = getAuthenticator(loginInput);
    Map<String, Object> commonParams;

    String oktaUsername = loginInput.getOKTAUserName();
    logger.debug(
        "Authenticating user: {}, host: {} with authentication method: {}."
            + " Login timeout: {} s, auth timeout: {} s, OCSP mode: {}{}",
        loginInput.getUserName(),
        loginInput.getHostFromServerUrl(),
        authenticatorType,
        loginInput.getLoginTimeout(),
        loginInput.getAuthTimeout(),
        loginInput.getOCSPMode(),
        isNullOrEmpty(oktaUsername) ? "" : ", okta username: " + oktaUsername);

    try {

      uriBuilder = new URIBuilder(loginInput.getServerUrl());
      // add database name and schema name as query parameters
      if (loginInput.getDatabaseName() != null) {
        uriBuilder.addParameter(SF_QUERY_DATABASE, loginInput.getDatabaseName());
      }

      if (loginInput.getSchemaName() != null) {
        uriBuilder.addParameter(SF_QUERY_SCHEMA, loginInput.getSchemaName());
      }

      if (loginInput.getWarehouse() != null) {
        uriBuilder.addParameter(SF_QUERY_WAREHOUSE, loginInput.getWarehouse());
      }

      if (loginInput.getRole() != null) {
        uriBuilder.addParameter(SF_QUERY_ROLE, loginInput.getRole());
      }

      if (authenticatorType == AuthenticatorType.EXTERNALBROWSER) {
        // try to reuse id_token if exists
        if (loginInput.getIdToken() == null) {
          // SAML 2.0 compliant service/application
          SessionUtilExternalBrowser s = SessionUtilExternalBrowser.createInstance(loginInput);
          s.authenticate();
          tokenOrSamlResponse = s.getToken();
          samlProofKey = s.getProofKey();
          consentCacheIdToken = s.isConsentCacheIdToken();
        }
      } else if (authenticatorType == AuthenticatorType.OKTA) {
        // okta authenticator v1
        tokenOrSamlResponse = getSamlResponseUsingOkta(loginInput);
      } else if (authenticatorType == AuthenticatorType.SNOWFLAKE_JWT) {
        SessionUtilKeyPair s =
            new SessionUtilKeyPair(
                loginInput.getPrivateKey(),
                loginInput.getPrivateKeyFile(),
                loginInput.getPrivateKeyBase64(),
                loginInput.getPrivateKeyPwd(),
                loginInput.getAccountName(),
                loginInput.getUserName());

        loginInput.setToken(s.issueJwtToken());
        loginInput.setAuthTimeout(SessionUtilKeyPair.getTimeout());
      }

      uriBuilder.addParameter(SFSession.SF_QUERY_REQUEST_ID, UUIDUtils.getUUID().toString());

      uriBuilder.setPath(SF_PATH_LOGIN_REQUEST);
      loginURI = uriBuilder.build();
    } catch (URISyntaxException ex) {
      logger.error("Exception when building URL", ex);

      throw new SFException(ex, ErrorCode.INTERNAL_ERROR, "unexpected URI syntax exception:1");
    }

    HttpPost postRequest = null;

    try {
      Map<String, Object> data = new HashMap<>();
      data.put(ClientAuthnParameter.CLIENT_APP_ID.name(), loginInput.getAppId());

      /*
       * username is always included regardless of authenticator to identify
       * the user.
       */
      data.put(ClientAuthnParameter.LOGIN_NAME.name(), loginInput.getUserName());

      /*
       * only include password information in the request to GS if federated
       * authentication method is not specified.
       * When specified, this password information is really to be used to
       * authenticate with the IDP provider only, and GS should not have any
       * trace for this information.
       */
      if (authenticatorType == AuthenticatorType.SNOWFLAKE) {
        data.put(ClientAuthnParameter.PASSWORD.name(), loginInput.getPassword());
      } else if (authenticatorType == AuthenticatorType.EXTERNALBROWSER) {
        if (loginInput.getIdToken() != null) {
          data.put(ClientAuthnParameter.AUTHENTICATOR.name(), ID_TOKEN_AUTHENTICATOR);
          data.put(ClientAuthnParameter.TOKEN.name(), loginInput.getIdToken());
        } else {
          data.put(
              ClientAuthnParameter.AUTHENTICATOR.name(), AuthenticatorType.EXTERNALBROWSER.name());
          data.put(ClientAuthnParameter.PROOF_KEY.name(), samlProofKey);
          data.put(ClientAuthnParameter.TOKEN.name(), tokenOrSamlResponse);
        }
      } else if (authenticatorType == AuthenticatorType.OKTA) {
        data.put(ClientAuthnParameter.RAW_SAML_RESPONSE.name(), tokenOrSamlResponse);
      } else if (authenticatorType == AuthenticatorType.OAUTH) {
        data.put(ClientAuthnParameter.AUTHENTICATOR.name(), authenticatorType.name());

        // Fix for HikariCP refresh token issue:SNOW-533673.
        // If token value is not set but password field is set then
        // the driver treats password as token.
        if (loginInput.getToken() != null) {
          data.put(ClientAuthnParameter.TOKEN.name(), loginInput.getToken());
        } else {
          data.put(ClientAuthnParameter.TOKEN.name(), loginInput.getPassword());
        }

      } else if (authenticatorType == AuthenticatorType.PROGRAMMATIC_ACCESS_TOKEN) {
        data.put(ClientAuthnParameter.AUTHENTICATOR.name(), authenticatorType.name());
        data.put(ClientAuthnParameter.TOKEN.name(), loginInput.getToken());
      } else if (authenticatorType == AuthenticatorType.SNOWFLAKE_JWT) {
        data.put(ClientAuthnParameter.AUTHENTICATOR.name(), authenticatorType.name());
        data.put(ClientAuthnParameter.TOKEN.name(), loginInput.getToken());
      } else if (authenticatorType == AuthenticatorType.USERNAME_PASSWORD_MFA) {
        // No authenticator name should be added here, since this will be treated as snowflake
        // default authenticator by backend
        data.put(ClientAuthnParameter.PASSWORD.name(), loginInput.getPassword());
        if (loginInput.getMfaToken() != null) {
          data.put(ClientAuthnParameter.TOKEN.name(), loginInput.getMfaToken());
        }
      }

      // OAuth metrics data
      if (authenticatorType == AuthenticatorType.OAUTH
          && loginInput.getOriginalAuthenticator() != null) {
        data.put(ClientAuthnParameter.OAUTH_TYPE.name(), loginInput.getOriginalAuthenticator());
      }

      if (authenticatorType == AuthenticatorType.WORKLOAD_IDENTITY) {
        data.put(
            ClientAuthnParameter.TOKEN.name(),
            loginInput.getWorkloadIdentityAttestation().getCredential());
        data.put(
            ClientAuthnParameter.PROVIDER.name(),
            loginInput.getWorkloadIdentityAttestation().getProvider());
      }

      // map of client environment parameters, including connection parameters
      // and environment properties like OS version, etc.
      Map<String, Object> clientEnv = new HashMap<>();

      clientEnv.put("OS", systemGetProperty("os.name"));
      clientEnv.put("OS_VERSION", systemGetProperty("os.version"));
      clientEnv.put("JAVA_VERSION", systemGetProperty("java.version"));
      clientEnv.put("JAVA_RUNTIME", systemGetProperty("java.runtime.name"));
      clientEnv.put("JAVA_VM", systemGetProperty("java.vm.name"));
      clientEnv.put("OCSP_MODE", loginInput.getOCSPMode().name());

      if (loginInput.getApplication() != null) {
        clientEnv.put("APPLICATION", loginInput.getApplication());
      } else {
        // When you add new client environment info, please add new keys to
        // messages_en_US.src.json so that they can be displayed properly in UI
        // detect app name
        String appName = systemGetProperty("sun.java.command");
        // remove the arguments
        if (appName != null) {
          if (appName.indexOf(" ") > 0) {
            appName = appName.substring(0, appName.indexOf(" "));
          }

          clientEnv.put("APPLICATION", appName);
        }
      }

      // SNOW-20103: track additional client info in session
      String clientInfoJSONStr;
      if (connectionPropertiesMap.containsKey(SFSessionProperty.CLIENT_INFO)) {
        clientInfoJSONStr = (String) connectionPropertiesMap.get(SFSessionProperty.CLIENT_INFO);
      }
      // if connection property is not set, check session property
      else {
        clientInfoJSONStr = systemGetProperty("snowflake.client.info");
      }
      if (clientInfoJSONStr != null) {
        JsonNode clientInfoJSON = null;

        try {
          clientInfoJSON = mapper.readTree(clientInfoJSONStr);
        } catch (Throwable ex) {
          logger.debug(
              "failed to process snowflake.client.info property as JSON: {}",
              clientInfoJSONStr,
              ex);
        }

        if (clientInfoJSON != null) {
          Iterator<Map.Entry<String, JsonNode>> fields = clientInfoJSON.fields();
          while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            clientEnv.put(field.getKey(), field.getValue().asText());
          }
        }
      }
      /*
       Add all connection parameters and their values that have been set for this
       * current session into clientEnv. These are the params set via the Properties map or in the
       * connection string. Includes username, password, serverUrl, timeout values, etc
      */

      for (Map.Entry<SFSessionProperty, Object> entry : connectionPropertiesMap.entrySet()) {
        // exclude client parameters already covered by other runtime parameters that have been
        // added to clientEnv
        if (entry.getKey().equals(SFSessionProperty.APP_ID)
            || entry.getKey().equals(SFSessionProperty.APP_VERSION)) {
          continue;
        }
        String propKey = entry.getKey().getPropertyKey();
        // mask sensitive values like passwords, tokens, etc
        String propVal = SecretDetector.maskParameterValue(propKey, entry.getValue().toString());
        clientEnv.put(propKey, propVal);
      }
      // if map does not contain the tracing property, the default is set. Add
      // this default value to the map.
      if (!connectionPropertiesMap.containsKey(SFSessionProperty.TRACING)) {
        clientEnv.put(SFSessionProperty.TRACING.getPropertyKey(), tracingLevel);
      }

      clientEnv.put("JDBC_JAR_NAME", SnowflakeDriver.getJdbcJarname());

      data.put(ClientAuthnParameter.CLIENT_ENVIRONMENT.name(), clientEnv);

      // Initialize the session parameters
      Map<String, Object> sessionParameter = loginInput.getSessionParameters();
      if (loginInput.isValidateDefaultParameters()) {
        sessionParameter.put(CLIENT_VALIDATE_DEFAULT_PARAMETERS, true);
      }

      if (sessionParameter != null) {
        data.put(ClientAuthnParameter.SESSION_PARAMETERS.name(), loginInput.getSessionParameters());
      }

      if (loginInput.getAccountName() != null) {
        data.put(ClientAuthnParameter.ACCOUNT_NAME.name(), loginInput.getAccountName());
      }

      // Second Factor Authentication
      if (loginInput.isPasscodeInPassword()) {
        data.put(ClientAuthnParameter.EXT_AUTHN_DUO_METHOD.name(), "passcode");
      } else if (loginInput.getPasscode() != null) {
        data.put(ClientAuthnParameter.EXT_AUTHN_DUO_METHOD.name(), "passcode");
        data.put(ClientAuthnParameter.PASSCODE.name(), loginInput.getPasscode());
      } else {
        data.put(ClientAuthnParameter.EXT_AUTHN_DUO_METHOD.name(), "push");
      }

      data.put(ClientAuthnParameter.CLIENT_APP_VERSION.name(), loginInput.getAppVersion());
      ClientAuthnDTO authnData = new ClientAuthnDTO(data, loginInput.getInFlightCtx());
      String json = mapper.writeValueAsString(authnData);

      postRequest = new HttpPost(loginURI);

      // Add custom headers before adding common headers
      HttpUtil.applyAdditionalHeadersForSnowsight(
          postRequest, loginInput.getAdditionalHttpHeadersForSnowsight());

      // Add headers for driver name and version
      postRequest.addHeader(SF_HEADER_CLIENT_APP_ID, loginInput.getAppId());
      postRequest.addHeader(SF_HEADER_CLIENT_APP_VERSION, loginInput.getAppVersion());

      // attach the login info json body to the post request
      StringEntity input = new StringEntity(json, StandardCharsets.UTF_8);
      input.setContentType("application/json");
      postRequest.setEntity(input);

      postRequest.addHeader("accept", "application/json");
      postRequest.addHeader("Accept-Encoding", "");
      if (loginInput.isDPoPEnabled()) {
        new DPoPUtil(loginInput.getDPoPPublicKey()).addDPoPProofHeaderToRequest(postRequest, null);
      }

      /*
       * HttpClient should take authorization header from char[] instead of
       * String.
       */
      postRequest.setHeader(SFSession.SF_HEADER_AUTHORIZATION, SF_HEADER_BASIC_AUTHTYPE);

      setServiceNameHeader(loginInput, postRequest);

      String theString = null;

      int leftRetryTimeout = loginInput.getLoginTimeout();
      int leftsocketTimeout = loginInput.getSocketTimeoutInMillis();
      int retryCount = 0;

      Exception lastRestException = null;

      while (true) {
        try {
          theString =
              HttpUtil.executeGeneralRequest(
                  postRequest,
                  leftRetryTimeout,
                  loginInput.getAuthTimeout(),
                  leftsocketTimeout,
                  retryCount,
                  loginInput.getHttpClientSettingsKey());
        } catch (SnowflakeSQLException ex) {
          lastRestException = ex;
          if (ex.getErrorCode() == ErrorCode.AUTHENTICATOR_REQUEST_TIMEOUT.getMessageCode()) {
            if (authenticatorType == AuthenticatorType.SNOWFLAKE_JWT
                || authenticatorType == AuthenticatorType.OKTA) {

              if (authenticatorType == AuthenticatorType.SNOWFLAKE_JWT) {
                SessionUtilKeyPair s =
                    new SessionUtilKeyPair(
                        loginInput.getPrivateKey(),
                        loginInput.getPrivateKeyFile(),
                        loginInput.getPrivateKeyBase64(),
                        loginInput.getPrivateKeyPwd(),
                        loginInput.getAccountName(),
                        loginInput.getUserName());

                data.put(ClientAuthnParameter.TOKEN.name(), s.issueJwtToken());
              } else if (authenticatorType == AuthenticatorType.OKTA) {
                // TODO: there is no retry manager passed here for now - we still raise the
                // exception to retry in the old way
                logger.debug("Retrieve new token for Okta authentication.");
                // If we need to retry, we need to get a new Okta token
                tokenOrSamlResponse = getSamlResponseUsingOkta(loginInput);
                data.put(ClientAuthnParameter.RAW_SAML_RESPONSE.name(), tokenOrSamlResponse);
                ClientAuthnDTO updatedAuthnData =
                    new ClientAuthnDTO(data, loginInput.getInFlightCtx());
                String updatedJson = mapper.writeValueAsString(updatedAuthnData);

                StringEntity updatedInput = new StringEntity(updatedJson, StandardCharsets.UTF_8);
                updatedInput.setContentType("application/json");
                postRequest.setEntity(updatedInput);
              }

              long elapsedSeconds = ex.getElapsedSeconds();

              if (loginInput.getLoginTimeout() > 0) {
                if (leftRetryTimeout > elapsedSeconds) {
                  leftRetryTimeout -= elapsedSeconds;
                } else {
                  leftRetryTimeout = 1;
                }
              }

              // In RestRequest.execute(), socket timeout is replaced with auth timeout
              // so we can renew the request within auth timeout.
              // auth timeout within socket timeout is thrown without backoff,
              // and we need to update time remained in socket timeout here to control
              // the actual socket timeout from customer setting.
              if (loginInput.getSocketTimeoutInMillis() > 0) {
                if (ex.issocketTimeoutNoBackoff()) {
                  if (leftsocketTimeout > elapsedSeconds) {
                    leftsocketTimeout -= elapsedSeconds;
                  } else {
                    leftsocketTimeout = 1;
                  }
                } else {
                  // reset curl timeout for retry with backoff.
                  leftsocketTimeout = loginInput.getSocketTimeoutInMillis();
                }
              }

              // JWT or Okta renew should not count as a retry, so we pass back the current retry
              // count.
              retryCount = ex.getRetryCount();

              continue;
            }
          } else {
            throw ex;
          }
        } catch (Exception ex) {
          lastRestException = ex;
        }
        break;
      }

      handleEmptyAuthResponse(theString, loginInput, lastRestException);

      // general method, same as with data binding
      JsonNode jsonNode = mapper.readTree(theString);

      // check the success field first
      if (!jsonNode.path("success").asBoolean()) {
        logger.debug("Response: {}", theString);

        int errorCode = jsonNode.path("code").asInt();
        if (errorCode == Constants.ID_TOKEN_INVALID_LOGIN_REQUEST_GS_CODE) {
          // clean id_token first
          loginInput.setIdToken(null);
          deleteIdTokenCache(loginInput.getHostFromServerUrl(), loginInput.getUserName());

          logger.debug(
              "ID Token Expired / Not Applicable. Reauthenticating with ID Token cleared...: {}",
              errorCode);
          SnowflakeUtil.checkErrorAndThrowExceptionIncludingReauth(jsonNode);
        }

        if (errorCode == Constants.OAUTH_ACCESS_TOKEN_INVALID_GS_CODE) {
          logger.debug("OAuth Access Token Invalid: {}", errorCode);
          clearAccessTokenCache(loginInput);
        }

        if (errorCode == Constants.OAUTH_ACCESS_TOKEN_EXPIRED_GS_CODE) {
          clearAccessTokenCache(loginInput);

          logger.debug("OAuth Access Token Expired: {}", errorCode);
          SnowflakeUtil.checkErrorAndThrowExceptionIncludingReauth(jsonNode);
        }

        if (authenticatorType == AuthenticatorType.USERNAME_PASSWORD_MFA) {
          deleteMfaTokenCache(loginInput.getHostFromServerUrl(), loginInput.getUserName());
        }

        String errorMessage = jsonNode.path("message").asText();

        logger.error(
            "Failed to open new session for user: {}, host: {}. Error: {}",
            loginInput.getUserName(),
            loginInput.getHostFromServerUrl(),
            errorMessage);
        throw new SnowflakeSQLException(
            NO_QUERY_ID,
            errorMessage,
            SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
            errorCode);
      }

      // session token is in the data field of the returned json response
      sessionToken = jsonNode.path("data").path("token").asText();
      masterToken = jsonNode.path("data").path("masterToken").asText();
      idToken = nullStringAsEmptyString(jsonNode.path("data").path("idToken").asText());
      mfaToken = nullStringAsEmptyString(jsonNode.path("data").path("mfaToken").asText());
      masterTokenValidityInSeconds = jsonNode.path("data").path("masterValidityInSeconds").asLong();
      String serverVersion = jsonNode.path("data").path("serverVersion").asText();
      sessionId = jsonNode.path("data").path("sessionId").asText();

      JsonNode dbNode = jsonNode.path("data").path("sessionInfo").path("databaseName");
      sessionDatabase = dbNode.isNull() ? null : dbNode.asText();
      JsonNode schemaNode = jsonNode.path("data").path("sessionInfo").path("schemaName");
      sessionSchema = schemaNode.isNull() ? null : schemaNode.asText();
      JsonNode roleNode = jsonNode.path("data").path("sessionInfo").path("roleName");
      sessionRole = roleNode.isNull() ? null : roleNode.asText();
      JsonNode warehouseNode = jsonNode.path("data").path("sessionInfo").path("warehouseName");
      sessionWarehouse = warehouseNode.isNull() ? null : warehouseNode.asText();

      commonParams = SessionUtil.getCommonParams(jsonNode.path("data").path("parameters"));

      if (serverVersion != null) {
        logger.debug("Server version: {}", serverVersion);

        if (serverVersion.indexOf(" ") > 0) {
          databaseVersion = serverVersion.substring(0, serverVersion.indexOf(" "));
        } else {
          databaseVersion = serverVersion;
        }
      } else {
        logger.debug("Server version is null", false);
      }

      if (databaseVersion != null) {
        String[] components = databaseVersion.split("\\.");
        if (components.length >= 2) {
          try {
            databaseMajorVersion = Integer.parseInt(components[0]);
            databaseMinorVersion = Integer.parseInt(components[1]);
          } catch (Exception ex) {
            logger.error(
                "Exception encountered when parsing server " + "version: {} Exception: {}",
                databaseVersion,
                ex.getMessage());
          }
        }
      } else {
        logger.debug("database version is null", false);
      }

      if (!jsonNode.path("data").path("newClientForUpgrade").isNull()) {
        newClientForUpgrade = jsonNode.path("data").path("newClientForUpgrade").asText();

        logger.debug("New client: {}", newClientForUpgrade);
      }

      // get health check interval and adjust network timeouts if different
      int healthCheckIntervalFromGS = jsonNode.path("data").path("healthCheckInterval").asInt();

      logger.debug("Health check interval: {}", healthCheckIntervalFromGS);

      if (healthCheckIntervalFromGS > 0 && healthCheckIntervalFromGS != healthCheckInterval) {
        // add health check interval to socket timeout
        httpClientSocketTimeout =
            loginInput.getSocketTimeoutInMillis() + (healthCheckIntervalFromGS * 1000);

        final RequestConfig requestConfig =
            RequestConfig.copy(HttpUtil.getRequestConfigWithoutCookies())
                .setConnectTimeout(httpClientConnectionTimeout)
                .setSocketTimeout(httpClientSocketTimeout)
                .build();

        HttpUtil.setRequestConfig(requestConfig);

        logger.debug("Adjusted connection timeout to: {}", httpClientConnectionTimeout);

        logger.debug("Adjusted socket timeout to: {}", httpClientSocketTimeout);
      }
    } catch (SnowflakeSQLException ex) {
      throw ex; // must catch here to avoid Throwable to get the exception
    } catch (IOException ex) {
      logger.error("IOException when creating session: " + postRequest, ex);

      throw new SnowflakeSQLException(
          ex,
          SqlState.IO_ERROR,
          ErrorCode.NETWORK_ERROR.getMessageCode(),
          "Exception encountered when opening connection: " + ex.getMessage());
    } catch (Throwable ex) {
      logger.error("Exception when creating session: " + postRequest, ex);

      throw new SnowflakeSQLException(
          ex,
          SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
          ErrorCode.CONNECTION_ERROR.getMessageCode(),
          ErrorCode.CONNECTION_ERROR.getMessageCode(),
          ex.getMessage());
    }

    SFLoginOutput ret =
        new SFLoginOutput(
            sessionToken,
            masterToken,
            masterTokenValidityInSeconds,
            idToken,
            mfaToken,
            loginInput.getOauthAccessToken(),
            loginInput.getOauthRefreshToken(),
            databaseVersion,
            databaseMajorVersion,
            databaseMinorVersion,
            httpClientSocketTimeout,
            httpClientConnectionTimeout,
            sessionDatabase,
            sessionSchema,
            sessionRole,
            sessionWarehouse,
            sessionId,
            commonParams);

    if (asBoolean(loginInput.getSessionParameters().get(CLIENT_STORE_TEMPORARY_CREDENTIAL))) {
      if (consentCacheIdToken) {
        CredentialManager.writeIdToken(loginInput, ret.getIdToken());
      }
      if (loginInput.getOauthRefreshToken() != null) {
        CredentialManager.writeOAuthRefreshToken(loginInput);
      }
      if (loginInput.getDPoPPublicKey() != null
          && loginInput.getOauthAccessToken() != null
          && loginInput.isDPoPEnabled()) {
        CredentialManager.writeDPoPBundledAccessToken(loginInput);
      } else if (loginInput.getOauthAccessToken() != null) {
        CredentialManager.writeOAuthAccessToken(loginInput);
      }
    }

    if (asBoolean(loginInput.getSessionParameters().get(CLIENT_REQUEST_MFA_TOKEN))) {
      CredentialManager.writeMfaToken(loginInput, ret.getMfaToken());
    }

    stopwatch.stop();
    logger.debug(
        "User: {}, host: {} with authentication method: {} authenticated successfully in {} ms",
        loginInput.getUserName(),
        loginInput.getHostFromServerUrl(),
        authenticatorType,
        stopwatch.elapsedMillis());
    return ret;
  }

  private static void clearAccessTokenCache(SFLoginInput loginInput) throws SFException {
    loginInput.setOauthAccessToken(null);
    loginInput.setDPoPPublicKey(null);
    CredentialManager.deleteOAuthAccessTokenCacheEntry(loginInput);
    CredentialManager.deleteDPoPBundledAccessTokenCacheEntry(loginInput);
  }

  private static void setServiceNameHeader(SFLoginInput loginInput, HttpPost postRequest) {
    if (!isNullOrEmpty(loginInput.getServiceName())) {
      // service name is used to route a request to appropriate cluster.
      postRequest.setHeader(SF_HEADER_SERVICE_NAME, loginInput.getServiceName());
    }
  }

  private static String nullStringAsEmptyString(String value) {
    if (isNullOrEmpty(value) || "null".equals(value)) {
      return "";
    }
    return value;
  }

  /**
   * Delete the id token cache
   *
   * @param host The host string
   * @param user The user
   */
  public static void deleteIdTokenCache(String host, String user) {
    CredentialManager.deleteIdTokenCacheEntry(host, user);
  }

  /**
   * Delete the Oauth access token cache
   *
   * @param host The host string
   * @param user The user
   */
  @SnowflakeJdbcInternalApi
  public static void deleteOAuthAccessTokenCache(String host, String user) {
    CredentialManager.deleteOAuthAccessTokenCacheEntry(host, user);
  }

  /**
   * Delete the Oauth refresh token cache
   *
   * @param host The host string
   * @param user The user
   */
  @SnowflakeJdbcInternalApi
  public static void deleteOAuthRefreshTokenCache(String host, String user) {
    CredentialManager.deleteOAuthRefreshTokenCacheEntry(host, user);
  }

  /**
   * Delete the mfa token cache
   *
   * @param host The host string
   * @param user The user
   */
  public static void deleteMfaTokenCache(String host, String user) {
    CredentialManager.deleteMfaTokenCacheEntry(host, user);
  }

  /**
   * Renew a session.
   *
   * @param loginInput login information
   * @return login output
   * @throws SFException if unexpected uri information
   * @throws SnowflakeSQLException if failed to renew the session
   */
  static SFLoginOutput renewSession(SFLoginInput loginInput)
      throws SFException, SnowflakeSQLException {
    return renewTokenRequest(loginInput);
  }

  private static SFLoginOutput renewTokenRequest(SFLoginInput loginInput)
      throws SFException, SnowflakeSQLException {
    AssertUtil.assertTrue(loginInput.getServerUrl() != null, "missing server URL for tokenRequest");

    AssertUtil.assertTrue(
        loginInput.getMasterToken() != null, "missing master token for tokenRequest");
    AssertUtil.assertTrue(
        loginInput.getSessionToken() != null, "missing session token for tokenRequest");
    AssertUtil.assertTrue(
        loginInput.getLoginTimeout() >= 0, "negative login timeout for tokenRequest");

    // build URL for login request
    URIBuilder uriBuilder;
    HttpPost postRequest;
    String sessionToken;
    String masterToken;

    try {
      uriBuilder = new URIBuilder(loginInput.getServerUrl());
      uriBuilder.setPath(SF_PATH_TOKEN_REQUEST);

      uriBuilder.addParameter(SFSession.SF_QUERY_REQUEST_ID, UUIDUtils.getUUID().toString());

      postRequest = new HttpPost(uriBuilder.build());

      // Add headers for driver name and version
      postRequest.addHeader(SF_HEADER_CLIENT_APP_ID, loginInput.getAppId());
      postRequest.addHeader(SF_HEADER_CLIENT_APP_VERSION, loginInput.getAppVersion());

      // Add custom headers before adding common headers
      HttpUtil.applyAdditionalHeadersForSnowsight(
          postRequest, loginInput.getAdditionalHttpHeadersForSnowsight());
    } catch (URISyntaxException ex) {
      logger.error("Exception when creating http request", ex);

      throw new SFException(ex, ErrorCode.INTERNAL_ERROR, "unexpected URI syntax exception:3");
    }

    try {
      // input json with old session token and request type, notice the
      // session token needs to be quoted.
      Map<String, String> payload = new HashMap<>();
      String headerToken = loginInput.getMasterToken();
      payload.put("oldSessionToken", loginInput.getSessionToken());
      payload.put("requestType", TokenRequestType.RENEW.value);
      String json = mapper.writeValueAsString(payload);

      // attach the login info json body to the post request
      StringEntity input = new StringEntity(json, StandardCharsets.UTF_8);
      input.setContentType("application/json");
      postRequest.setEntity(input);

      postRequest.addHeader("accept", "application/json");

      postRequest.setHeader(
          SFSession.SF_HEADER_AUTHORIZATION,
          SFSession.SF_HEADER_SNOWFLAKE_AUTHTYPE
              + " "
              + SFSession.SF_HEADER_TOKEN_TAG
              + "=\""
              + headerToken
              + "\"");

      setServiceNameHeader(loginInput, postRequest);

      logger.debug(
          "Request type: {}, old session token: {}, " + "master token: {}",
          TokenRequestType.RENEW.value,
          (ArgSupplier) () -> loginInput.getSessionToken() != null ? "******" : null,
          (ArgSupplier) () -> loginInput.getMasterToken() != null ? "******" : null);

      String theString =
          HttpUtil.executeGeneralRequest(
              postRequest,
              loginInput.getLoginTimeout(),
              loginInput.getAuthTimeout(),
              loginInput.getSocketTimeoutInMillis(),
              0,
              loginInput.getHttpClientSettingsKey());

      // general method, same as with data binding
      JsonNode jsonNode = mapper.readTree(theString);

      // check the success field first
      if (!jsonNode.path("success").asBoolean()) {
        logger.debug("Response: {}", theString);

        String errorCode = jsonNode.path("code").asText();
        String message = jsonNode.path("message").asText();

        EventUtil.triggerBasicEvent(
            Event.EventType.NETWORK_ERROR,
            "SessionUtil:renewSession failure, error code=" + errorCode + ", message=" + message,
            true);

        SnowflakeUtil.checkErrorAndThrowExceptionIncludingReauth(jsonNode);
      }

      // session token is in the data field of the returned json response
      sessionToken = jsonNode.path("data").path("sessionToken").asText();
      masterToken = jsonNode.path("data").path("masterToken").asText();
    } catch (IOException ex) {
      logger.error("IOException when renewing session: " + postRequest, ex);

      // Any EventType.NETWORK_ERRORs should have been triggered before
      // exception was thrown.
      throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
    }

    SFLoginOutput loginOutput = new SFLoginOutput();
    loginOutput.setSessionToken(sessionToken).setMasterToken(masterToken);

    return loginOutput;
  }

  /**
   * Close a session
   *
   * @param loginInput login information
   * @throws SnowflakeSQLException if failed to close session
   * @throws SFException if failed to close session
   */
  static void closeSession(SFLoginInput loginInput) throws SFException, SnowflakeSQLException {
    logger.trace("void close() throws SFException");

    // assert the following inputs are valid
    AssertUtil.assertTrue(
        loginInput.getServerUrl() != null, "missing server URL for closing session");

    AssertUtil.assertTrue(
        loginInput.getSessionToken() != null, "missing session token for closing session");

    AssertUtil.assertTrue(
        loginInput.getLoginTimeout() >= 0, "missing login timeout for closing session");

    HttpPost postRequest = null;

    try {
      URIBuilder uriBuilder;

      uriBuilder = new URIBuilder(loginInput.getServerUrl());

      uriBuilder.addParameter(SF_QUERY_SESSION_DELETE, Boolean.TRUE.toString());
      uriBuilder.addParameter(SFSession.SF_QUERY_REQUEST_ID, UUIDUtils.getUUID().toString());

      uriBuilder.setPath(SF_PATH_SESSION);

      postRequest = new HttpPost(uriBuilder.build());

      // Add custom headers before adding common headers
      HttpUtil.applyAdditionalHeadersForSnowsight(
          postRequest, loginInput.getAdditionalHttpHeadersForSnowsight());

      postRequest.setHeader(
          SFSession.SF_HEADER_AUTHORIZATION,
          SFSession.SF_HEADER_SNOWFLAKE_AUTHTYPE
              + " "
              + SFSession.SF_HEADER_TOKEN_TAG
              + "=\""
              + loginInput.getSessionToken()
              + "\"");

      setServiceNameHeader(loginInput, postRequest);

      String theString =
          HttpUtil.executeGeneralRequest(
              postRequest,
              loginInput.getLoginTimeout(),
              loginInput.getAuthTimeout(),
              loginInput.getSocketTimeoutInMillis(),
              0,
              loginInput.getHttpClientSettingsKey());

      JsonNode rootNode;

      logger.debug("Connection close response: {}", theString);

      rootNode = mapper.readTree(theString);

      SnowflakeUtil.checkErrorAndThrowException(rootNode);
    } catch (URISyntaxException ex) {
      throw new RuntimeException("Unexpected URI syntax exception", ex);
    } catch (IOException ex) {
      logger.error("Unexpected IO exception for: " + postRequest, ex);
    } catch (SnowflakeSQLException ex) {
      // ignore exceptions for session expiration exceptions and for
      // sessions that no longer exist
      if (ex.getErrorCode() != Constants.SESSION_EXPIRED_GS_CODE
          && ex.getErrorCode() != Constants.SESSION_GONE) {
        throw ex;
      }
    }
  }

  /**
   * Given access token, query IDP URL snowflake app to get SAML response We also need to perform
   * important client side validation: validate the post back url come back with the SAML response
   * contains the same prefix as the Snowflake's server url, which is the intended destination url
   * to Snowflake. Explanation: This emulates the behavior of IDP initiated login flow in the user
   * browser where the IDP instructs the browser to POST the SAML assertion to the specific SP
   * endpoint. This is critical in preventing a SAML assertion issued to one SP from being sent to
   * another SP.
   *
   * @param loginInput Login Info for the request
   * @param ssoUrl URL to use for SSO
   * @param oneTimeTokenSupplier The function returning token used for SSO
   * @return The response in HTML form
   * @throws SnowflakeSQLException Will be thrown if the destination URL in the SAML assertion does
   *     not match
   */
  private static String federatedFlowStep4(
      SFLoginInput loginInput,
      String ssoUrl,
      ThrowingFunction<RetryContext, String, SnowflakeSQLException> oneTimeTokenSupplier)
      throws SnowflakeSQLException {
    // This call of the oneTimeTokenSupplier is a part of the basic federated flow (before any
    // retries). It is distinguished by a retrieval of a token without any RetryContext (passing
    // 'null'). We pass a RetryContext instance only when we are currently during retries process -
    // and we want to exchange information between the injected logic and the outer scope.
    String oneTimeToken = oneTimeTokenSupplier.apply(null);
    String responseHtml = "";

    try {
      RetryContextManager retryWithNewOTTManager =
          createFederatedFlowStep4RetryContext(ssoUrl, oneTimeTokenSupplier, loginInput);

      HttpGet httpGet = new HttpGet();
      prepareFederatedFlowStep4Request(httpGet, ssoUrl, oneTimeToken);

      responseHtml =
          HttpUtil.executeGeneralRequest(
              httpGet,
              loginInput.getLoginTimeout(),
              loginInput.getAuthTimeout(),
              loginInput.getSocketTimeoutInMillis(),
              0,
              loginInput.getHttpClientSettingsKey(),
              retryWithNewOTTManager);

      // step 5
      validateSAML(responseHtml, loginInput);
    } catch (IOException | URISyntaxException ex) {
      handleFederatedFlowError(loginInput, ex);
    }
    return responseHtml;
  }

  private static RetryContextManager createFederatedFlowStep4RetryContext(
      String ssoUrl,
      ThrowingFunction<RetryContext, String, SnowflakeSQLException> oneTimeTokenSupplier,
      SFLoginInput loginInput) {
    RetryContextManager retryWithNewOTTManager =
        new RetryContextManager(RetryContextManager.RetryHook.ALWAYS_BEFORE_RETRY);
    retryWithNewOTTManager.registerRetryCallback(
        (HttpRequestBase retrieveSamlRequest, RetryContext retryContext) -> {
          try {
            String newOneTimeToken = oneTimeTokenSupplier.apply(retryContext);
            prepareFederatedFlowStep4Request(retrieveSamlRequest, ssoUrl, newOneTimeToken);
          } catch (MalformedURLException | URISyntaxException ex) {
            handleFederatedFlowError(loginInput, ex);
          }
          return retryContext;
        });
    return retryWithNewOTTManager;
  }

  private static void validateSAML(String responseHtml, SFLoginInput loginInput)
      throws SnowflakeSQLException, MalformedURLException {
    if (!loginInput.getDisableSamlURLCheck()) {
      String postBackUrl = getPostBackUrlFromHTML(responseHtml);
      if (!isPrefixEqual(postBackUrl, loginInput.getServerUrl())) {
        URL idpDestinationUrl = new URL(postBackUrl);
        URL clientDestinationUrl = new URL(loginInput.getServerUrl());
        String idpDestinationHostName = idpDestinationUrl.getHost();
        String clientDestinationHostName = clientDestinationUrl.getHost();

        logger.error(
            "The Snowflake hostname specified in the client connection {} does not match "
                + "the destination hostname in the SAML response returned by the IdP: {}",
            clientDestinationHostName,
            idpDestinationHostName);

        // Session is in process of getting created, so exception constructor takes in null
        throw new SnowflakeSQLLoggedException(
            null,
            ErrorCode.IDP_INCORRECT_DESTINATION.getMessageCode(),
            SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION);
      }
    }
  }

  /**
   * Query IDP token url to authenticate and retrieve access token
   *
   * @param loginInput The login info for the request
   * @param tokenUrl The URL used to retrieve the access token
   * @return Returns the one time token
   * @throws SnowflakeSQLException Will be thrown if the execute request fails
   */
  private static String federatedFlowStep3(
      SFLoginInput loginInput, String tokenUrl, RetryContext retryContext)
      throws SnowflakeSQLException {

    String oneTimeToken = "";
    try {
      URL url = new URL(tokenUrl);
      URI tokenUri = url.toURI();
      final HttpPost postRequest = new HttpPost(tokenUri);
      setFederatedFlowStep3PostRequestAuthData(postRequest, loginInput);

      int retryTimeout;

      if (retryContext != null) {
        // This casting could be avoided if all execution methods from SessionUtil to RestRequest
        // shared the same data type (either long or int) for the retryTimeout parameter. Now they
        // are all cast to long at the end (in RestRequest's methods).
        retryTimeout = (int) retryContext.getRemainingRetryTimeoutInSeconds();
      } else {
        retryTimeout = loginInput.getLoginTimeout();
      }

      final String idpResponse =
          HttpUtil.executeRequestWithoutCookies(
              postRequest,
              retryTimeout,
              loginInput.getAuthTimeout(),
              loginInput.getSocketTimeoutInMillis(),
              0,
              0,
              null,
              loginInput.getHttpClientSettingsKey());

      logger.debug("User is authenticated against {}.", loginInput.getAuthenticator());

      // session token is in the data field of the returned json response
      final JsonNode jsonNode = mapper.readTree(idpResponse);
      oneTimeToken =
          jsonNode.get("sessionToken") != null
              ? jsonNode.get("sessionToken").asText()
              : jsonNode.get("cookieToken").asText();
    } catch (IOException | URISyntaxException ex) {
      handleFederatedFlowError(loginInput, ex);
    }
    return oneTimeToken;
  }

  /**
   * Perform important client side validation: validate both token url and sso url contains same
   * prefix (protocol + host + port) as the given authenticator url. Explanation: This provides a
   * way for the user to 'authenticate' the IDP it is sending his/her credentials to. Without such a
   * check, the user could be coerced to provide credentials to an IDP impersonator.
   *
   * @param loginInput The login info for the request
   * @param tokenUrl The token URL
   * @param ssoUrl The SSO URL
   * @throws SnowflakeSQLException Will be thrown if the prefix for the tokenUrl and ssoUrl do not
   *     match
   */
  private static void federatedFlowStep2(SFLoginInput loginInput, String tokenUrl, String ssoUrl)
      throws SnowflakeSQLException {
    try {
      if (!isPrefixEqual(loginInput.getAuthenticator(), tokenUrl)
          || !isPrefixEqual(loginInput.getAuthenticator(), ssoUrl)) {
        logger.debug(
            "The specified authenticator {} is not supported.", loginInput.getAuthenticator());
        // Session is in process of getting created, so exception constructor takes in null session
        // value
        throw new SnowflakeSQLLoggedException(
            null,
            ErrorCode.IDP_CONNECTION_ERROR.getMessageCode(),
            SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
            /* session= */ );
      }
    } catch (MalformedURLException ex) {
      handleFederatedFlowError(loginInput, ex);
    }
  }

  /**
   * Query Snowflake to obtain IDP token url and IDP SSO url
   *
   * @param loginInput The login info for the request
   * @throws SnowflakeSQLException Will be thrown if the execute request step fails
   */
  private static JsonNode federatedFlowStep1(SFLoginInput loginInput) throws SnowflakeSQLException {
    JsonNode dataNode = null;
    try {
      StringEntity requestInput = prepareFederatedFlowStep1RequestInput(loginInput);
      HttpPost postRequest = new HttpPost();
      prepareFederatedFlowStep1PostRequest(postRequest, loginInput, requestInput);

      final String gsResponse =
          HttpUtil.executeGeneralRequest(
              postRequest,
              loginInput.getLoginTimeout(),
              loginInput.getAuthTimeout(),
              loginInput.getSocketTimeoutInMillis(),
              0,
              loginInput.getHttpClientSettingsKey());

      logger.debug("Authenticator-request response: {}", gsResponse);
      JsonNode jsonNode = mapper.readTree(gsResponse);

      // check the success field first
      if (!jsonNode.path("success").asBoolean()) {
        logger.debug("Response: {}", gsResponse);
        int errorCode = jsonNode.path("code").asInt();

        throw new SnowflakeSQLException(
            NO_QUERY_ID,
            jsonNode.path("message").asText(),
            SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
            errorCode);
      }

      // session token is in the data field of the returned json response
      dataNode = jsonNode.path("data");
    } catch (IOException | URISyntaxException ex) {
      handleFederatedFlowError(loginInput, ex);
    }
    return dataNode;
  }

  /**
   * Logs an error generated during the federated authentication flow and re-throws it as a
   * SnowflakeSQLException. Note that we separate IOExceptions since those tend to be network
   * related.
   *
   * @param loginInput The login info from the request
   * @param ex The exception to process
   * @throws SnowflakeSQLException Will be thrown for all calls to this method
   */
  private static void handleFederatedFlowError(SFLoginInput loginInput, Exception ex)
      throws SnowflakeSQLException {

    if (ex instanceof IOException) {
      logger.error("IOException when authenticating with " + loginInput.getAuthenticator(), ex);
      throw new SnowflakeSQLException(
          ex,
          SqlState.IO_ERROR,
          ErrorCode.NETWORK_ERROR.getMessageCode(),
          "Exception encountered when opening connection: " + ex.getMessage());
    }
    logger.error("Exception when authenticating with " + loginInput.getAuthenticator(), ex);
    throw new SnowflakeSQLException(
        ex,
        SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
        ErrorCode.CONNECTION_ERROR.getMessageCode(),
        ErrorCode.CONNECTION_ERROR.getMessageCode(),
        ex.getMessage());
  }

  /**
   * FEDERATED FLOW See SNOW-27798 for additional details.
   *
   * @param loginInput The login info from the request
   * @return saml response
   * @throws SnowflakeSQLException Will be thrown if any of the federated steps fail
   */
  private static String getSamlResponseUsingOkta(SFLoginInput loginInput)
      throws SnowflakeSQLException {
    while (true) {
      try {
        JsonNode dataNode = federatedFlowStep1(loginInput);
        String tokenUrl = dataNode.path("tokenUrl").asText();
        String ssoUrl = dataNode.path("ssoUrl").asText();
        federatedFlowStep2(loginInput, tokenUrl, ssoUrl);
        ThrowingFunction<RetryContext, String, SnowflakeSQLException> oneTimeTokenSupplier =
            (RetryContext retryContext) -> federatedFlowStep3(loginInput, tokenUrl, retryContext);

        return federatedFlowStep4(loginInput, ssoUrl, oneTimeTokenSupplier);
      } catch (SnowflakeSQLException ex) {
        // This error gets thrown if the okta request encountered a retry-able error that
        // requires getting a new one-time token.
        if (ex.getErrorCode() == ErrorCode.AUTHENTICATOR_REQUEST_TIMEOUT.getMessageCode()) {
          logger.debug("Failed to get Okta SAML response. Retrying without changing retry count.");
        } else {
          throw ex;
        }
      }
    }
  }

  /**
   * Verify if two input urls have the same protocol, host, and port.
   *
   * @param aUrlStr a source URL string
   * @param bUrlStr a target URL string
   * @return true if matched otherwise false
   * @throws MalformedURLException raises if a URL string is not valid.
   */
  static boolean isPrefixEqual(String aUrlStr, String bUrlStr) throws MalformedURLException {
    URL aUrl = new URL(aUrlStr);
    URL bUrl = new URL(bUrlStr);
    int aPort = aUrl.getPort();
    int bPort = bUrl.getPort();
    if (aPort == -1 && "https".equals(aUrl.getProtocol())) {
      // default port number for HTTPS
      aPort = 443;
    }
    if (bPort == -1 && "https".equals(bUrl.getProtocol())) {
      // default port number for HTTPS
      bPort = 443;
    }
    // no default port number for HTTP is supported.
    return aUrl.getHost().equalsIgnoreCase(bUrl.getHost())
        && aUrl.getProtocol().equalsIgnoreCase(bUrl.getProtocol())
        && aPort == bPort;
  }

  /**
   * Extracts post back url from the HTML returned by the IDP
   *
   * @param html The HTML that we are parsing to find the post back url
   * @return The post back url
   */
  private static String getPostBackUrlFromHTML(String html) {
    Document doc = Jsoup.parse(html);
    Elements e1 = doc.getElementsByTag("body");
    Elements e2 = e1.get(0).getElementsByTag("form");
    return e2.first().attr("action");
  }

  /**
   * Helper function to parse a JsonNode from a GS response containing CommonParameters, emitting an
   * EnumMap of parameters
   *
   * @param paramsNode parameters in JSON form
   * @return map object including key and value pairs
   */
  public static Map<String, Object> getCommonParams(JsonNode paramsNode) {
    Map<String, Object> parameters = new HashMap<>();

    for (JsonNode child : paramsNode) {
      // If there isn't a name then the response from GS must be erroneous.
      if (!child.hasNonNull("name")) {
        logger.error("Common Parameter JsonNode encountered with " + "no parameter name!", false);
        continue;
      }

      // Look up the parameter based on the "name" attribute of the node.
      String paramName = child.path("name").asText();

      // What type of value is it and what's the value?
      if (!child.hasNonNull("value")) {
        logger.debug("No value found for Common Parameter: {}", child.path("name").asText());
        continue;
      }

      if (STRING_PARAMS.contains(paramName.toUpperCase())) {
        parameters.put(paramName, child.path("value").asText());
      } else if (INT_PARAMS.contains(paramName.toUpperCase())) {
        parameters.put(paramName, child.path("value").asInt());
      } else if (BOOLEAN_PARAMS.contains(paramName.toUpperCase())) {
        parameters.put(paramName, child.path("value").asBoolean());
      } else {
        try {
          // Value should only be boolean, int or string so we don't expect exceptions here.
          parameters.put(paramName, mapper.treeToValue(child.path("value"), Object.class));
        } catch (Exception e) {
          logger.debug(
              "Unknown Common Parameter Failed to Parse: {} -> {}. Exception: {}",
              paramName,
              child.path("value"),
              e.getMessage());
        }
        logger.debug("Unknown Common Parameter: {}", paramName);
      }

      logger.debug("Parameter {}: {}", paramName, child.path("value").asText());
    }

    return parameters;
  }

  static void updateSfDriverParamValues(Map<String, Object> parameters, SFBaseSession session) {
    if (parameters != null && !parameters.isEmpty()) {
      session.setCommonParameters(parameters);
    }
    for (Map.Entry<String, Object> entry : parameters.entrySet()) {
      logger.debug("Processing parameter {}", entry.getKey());

      if ("CLIENT_DISABLE_INCIDENTS".equalsIgnoreCase(entry.getKey())) {
        SnowflakeDriver.setDisableIncidents((Boolean) entry.getValue());
      } else if ("CLIENT_SESSION_KEEP_ALIVE".equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setEnableHeartbeat((Boolean) entry.getValue());
        }
      } else if (CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setHeartbeatFrequency((int) entry.getValue());
        }
      } else if ("CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS".equalsIgnoreCase(entry.getKey())) {
        boolean enableLogging = (Boolean) entry.getValue();
        if (session != null && session.getPreparedStatementLogging() != enableLogging) {
          session.setPreparedStatementLogging(enableLogging);
        }
      } else if ("AUTOCOMMIT".equalsIgnoreCase(entry.getKey())) {
        boolean autoCommit = (Boolean) entry.getValue();
        if (session != null && session.getAutoCommit() != autoCommit) {
          session.setAutoCommit(autoCommit);
        }
      } else if (JDBC_RS_COLUMN_CASE_INSENSITIVE.equalsIgnoreCase(entry.getKey())
          || CLIENT_RESULT_COLUMN_CASE_INSENSITIVE.equalsIgnoreCase(entry.getKey())) {
        if (session != null && !session.isResultColumnCaseInsensitive()) {
          session.setResultColumnCaseInsensitive((boolean) entry.getValue());
        }
      } else if (CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setMetadataRequestUseConnectionCtx((boolean) entry.getValue());
        }
      } else if (CLIENT_METADATA_USE_SESSION_DATABASE.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setMetadataRequestUseSessionDatabase((boolean) entry.getValue());
        }
      } else if (JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setTreatNTZAsUTC((boolean) entry.getValue());
        }
      } else if (JDBC_FORMAT_DATE_WITH_TIMEZONE.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setFormatDateWithTimezone((boolean) entry.getValue());
        }
      } else if (JDBC_USE_SESSION_TIMEZONE.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setUseSessionTimezone((boolean) entry.getValue());
        }
      } else if ("CLIENT_TIMESTAMP_TYPE_MAPPING".equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setTimestampMappedType(
              SnowflakeType.valueOf(((String) entry.getValue()).toUpperCase()));
        }
      } else if ("JDBC_TREAT_DECIMAL_AS_INT".equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setJdbcTreatDecimalAsInt((boolean) entry.getValue());
        }
      } else if ("JDBC_ENABLE_COMBINED_DESCRIBE".equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setEnableCombineDescribe((boolean) entry.getValue());
        }
      } else if (CLIENT_IN_BAND_TELEMETRY_ENABLED.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setClientTelemetryEnabled((boolean) entry.getValue());
        }
      } else if ("CLIENT_STAGE_ARRAY_BINDING_THRESHOLD".equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setArrayBindStageThreshold((int) entry.getValue());
        }
      } else if (CLIENT_STORE_TEMPORARY_CREDENTIAL.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setStoreTemporaryCredential((boolean) entry.getValue());
        }
      } else if (SERVICE_NAME.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setServiceName((String) entry.getValue());
        }
      } else if (CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setEnableConservativeMemoryUsage((boolean) entry.getValue());
        }
      } else if (CLIENT_CONSERVATIVE_MEMORY_ADJUST_STEP.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setConservativeMemoryAdjustStep((int) entry.getValue());
        }
      } else if (CLIENT_MEMORY_LIMIT.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setClientMemoryLimit((int) entry.getValue());
        }
      } else if (CLIENT_RESULT_CHUNK_SIZE.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setClientResultChunkSize((int) entry.getValue());
        }
      } else if (CLIENT_PREFETCH_THREADS.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setClientPrefetchThreads((int) entry.getValue());
        }
      } else if (CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED.equalsIgnoreCase(entry.getKey())) {
        // we ignore the parameter CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED
        // OOB telemetry is always disabled
        TelemetryService.disableOOBTelemetry();
      } else if (CLIENT_VALIDATE_DEFAULT_PARAMETERS.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setValidateDefaultParameters(SFLoginInput.getBooleanValue(entry.getValue()));
        }
      } else if (ENABLE_STAGE_S3_PRIVATELINK_FOR_US_EAST_1.equalsIgnoreCase((entry.getKey()))) {
        if (session != null) {
          session.setUseRegionalS3EndpointsForPresignedURL(
              SFLoginInput.getBooleanValue(entry.getValue()));
        }
      } else if (QUERY_CONTEXT_CACHE_SIZE.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setQueryContextCacheSize((int) entry.getValue());
        }
      } else if (JDBC_ENABLE_PUT_GET.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setJdbcEnablePutGet(SFLoginInput.getBooleanValue(entry.getValue()));
        }
      } else {
        if (session != null) {
          session.setOtherParameter(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  enum TokenRequestType {
    RENEW("RENEW"),
    CLONE("CLONE"),
    ISSUE("ISSUE");

    private String value;

    TokenRequestType(String value) {
      this.value = value;
    }
  }

  /**
   * Reset OCSP cache server if the snowflake server URL is for private link. If the URL is not for
   * private link, do nothing.
   *
   * @param serverUrl The Snowflake URL includes protocol such as "https://"
   * @throws IOException If exception encountered
   */
  public static void resetOCSPUrlIfNecessary(String serverUrl) throws IOException {
    setOCSPResponseCacheServerURL(serverUrl);
    if (PrivateLinkDetector.isPrivateLink(serverUrl)) {
      // Privatelink uses special OCSP Cache server
      URL url = new URL(serverUrl);
      String host = url.getHost();
      logger.debug("HOST: {}", host);
      String ocspCacheServerUrl =
          String.format("http://ocsp.%s/%s", host, SFTrustManager.CACHE_FILE_NAME);
      logger.debug("OCSP Cache Server for Privatelink: {}", ocspCacheServerUrl);
      resetOCSPResponseCacherServerURL(ocspCacheServerUrl);
    }
  }

  /**
   * Helper function to generate a JWT token
   *
   * @param privateKey private key
   * @param privateKeyFile path to private key file
   * @param privateKeyBase64 base64 encoded content of the private key file
   * @param privateKeyPwd password for private key file or base64 encoded private key
   * @param accountName account name
   * @param userName user name
   * @return JWT token
   * @throws SFException if Snowflake error occurs
   */
  public static String generateJWTToken(
      PrivateKey privateKey,
      String privateKeyFile,
      String privateKeyBase64,
      String privateKeyPwd,
      String accountName,
      String userName)
      throws SFException {
    SessionUtilKeyPair s =
        new SessionUtilKeyPair(
            privateKey, privateKeyFile, privateKeyBase64, privateKeyPwd, accountName, userName);
    return s.issueJwtToken();
  }

  /**
   * Helper function to generate a JWT token. Use {@link #generateJWTToken(PrivateKey, String,
   * String, String, String, String)}
   *
   * @param privateKey private key
   * @param privateKeyFile path to private key file
   * @param privateKeyFilePwd password for private key file
   * @param accountName account name
   * @param userName user name
   * @return JWT token
   * @throws SFException if Snowflake error occurs
   */
  @Deprecated
  public static String generateJWTToken(
      PrivateKey privateKey,
      String privateKeyFile,
      String privateKeyFilePwd,
      String accountName,
      String userName)
      throws SFException {
    return generateJWTToken(
        privateKey, privateKeyFile, null, privateKeyFilePwd, accountName, userName);
  }

  /**
   * Helper method to check if the request path is a login/auth request to use for retry strategy.
   *
   * @param request the post request
   * @return true if this is a login/auth request, false otherwise
   */
  public static boolean isNewRetryStrategyRequest(HttpRequestBase request) {
    URI requestURI = request.getURI();
    String requestPath = requestURI.getPath();
    if (requestPath != null) {
      return requestPath.equals(SF_PATH_LOGIN_REQUEST)
          || requestPath.equals(SF_PATH_AUTHENTICATOR_REQUEST)
          || requestPath.equals(SF_PATH_TOKEN_REQUEST)
          || requestPath.contains(SF_PATH_OKTA_TOKEN_REQUEST_SUFFIX)
          || requestPath.contains(SF_PATH_OKTA_SSO_REQUEST_SUFFIX);
    }
    return false;
  }

  /**
   * Prepares an HTTP POST request for the first step of the federated authentication flow.
   *
   * @param loginInput The login information for the request.
   * @param inputData The JSON input data to include in the request.
   * @throws URISyntaxException If the constructed URI is invalid.
   */
  private static void prepareFederatedFlowStep1PostRequest(
      HttpPost postRequest, SFLoginInput loginInput, StringEntity inputData)
      throws URISyntaxException {
    URIBuilder fedUriBuilder = new URIBuilder(loginInput.getServerUrl());
    // TODO: if loginInput.serverUrl contains port or additional segments - it will be ignored and
    // overwritten here - to be fixed in SNOW-1922872
    fedUriBuilder.setPath(SF_PATH_AUTHENTICATOR_REQUEST);
    URI fedUrlUri = fedUriBuilder.build();
    postRequest.setURI(fedUrlUri);

    postRequest.setEntity(inputData);
    postRequest.addHeader("accept", "application/json");

    postRequest.addHeader(SF_HEADER_CLIENT_APP_ID, loginInput.getAppId());
    postRequest.addHeader(SF_HEADER_CLIENT_APP_VERSION, loginInput.getAppVersion());
  }

  /**
   * Prepares the JSON input for the first step of the federated authentication flow.
   *
   * @param loginInput The login information for the request.
   * @return A {@link StringEntity} containing the JSON input for the request.
   * @throws JsonProcessingException If there is an error generating the JSON input.
   */
  private static StringEntity prepareFederatedFlowStep1RequestInput(SFLoginInput loginInput)
      throws JsonProcessingException {
    Map<String, Object> data = new HashMap<>();
    data.put(ClientAuthnParameter.ACCOUNT_NAME.name(), loginInput.getAccountName());
    data.put(ClientAuthnParameter.AUTHENTICATOR.name(), loginInput.getAuthenticator());
    data.put(ClientAuthnParameter.CLIENT_APP_ID.name(), loginInput.getAppId());
    data.put(ClientAuthnParameter.CLIENT_APP_VERSION.name(), loginInput.getAppVersion());

    ClientAuthnDTO authnData = new ClientAuthnDTO(data, null);
    String json = mapper.writeValueAsString(authnData);

    StringEntity input = new StringEntity(json, StandardCharsets.UTF_8);
    input.setContentType("application/json");
    return input;
  }

  /**
   * Sets the authentication data for the third step of the federated authentication flow.
   *
   * @param postRequest The {@link HttpPost} request to update with authentication data.
   * @param loginInput The login information for the request.
   * @throws SnowflakeSQLException If an error occurs while preparing the request.
   */
  private static void setFederatedFlowStep3PostRequestAuthData(
      HttpPost postRequest, SFLoginInput loginInput) throws SnowflakeSQLException {
    String userName =
        isNullOrEmpty(loginInput.getOKTAUserName())
            ? loginInput.getUserName()
            : loginInput.getOKTAUserName();
    try {
      StringEntity params =
          new StringEntity(
              "{\"username\":\""
                  + userName
                  + "\",\"password\":\""
                  + loginInput.getPassword()
                  + "\"}");
      postRequest.setEntity(params);

      HeaderGroup headers = new HeaderGroup();
      headers.addHeader(new BasicHeader(HttpHeaders.ACCEPT, "application/json"));
      headers.addHeader(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"));
      postRequest.setHeaders(headers.getAllHeaders());
    } catch (IOException ex) {
      handleFederatedFlowError(loginInput, ex);
    }
  }

  /**
   * Prepares an HTTP GET request for the fourth step of the federated authentication flow.
   *
   * @param retrieveSamlRequest The {@link HttpRequestBase} to update with the SAML request details.
   * @param ssoUrl The SSO URL to use for the request.
   * @param oneTimeToken The one-time token to include in the request.
   * @throws MalformedURLException If the SSO URL is malformed.
   * @throws URISyntaxException If the URI for the request cannot be built.
   */
  private static void prepareFederatedFlowStep4Request(
      HttpRequestBase retrieveSamlRequest, String ssoUrl, String oneTimeToken)
      throws MalformedURLException, URISyntaxException {
    final URL url = new URL(ssoUrl);
    URI oktaGetUri =
        new URIBuilder()
            .setScheme(url.getProtocol())
            .setHost(url.getHost())
            .setPort(url.getPort())
            .setPath(url.getPath())
            .setParameter("RelayState", "%2Fsome%2Fdeep%2Flink")
            .setParameter("onetimetoken", oneTimeToken)
            .build();
    retrieveSamlRequest.setURI(oktaGetUri);

    HeaderGroup headers = new HeaderGroup();
    headers.addHeader(new BasicHeader(HttpHeaders.ACCEPT, "*/*"));
    retrieveSamlRequest.setHeaders(headers.getAllHeaders());
  }

  private static void handleEmptyAuthResponse(
      String theString, SFLoginInput loginInput, Exception lastRestException)
      throws Exception, SFException {
    if (theString == null) {
      if (lastRestException != null) {
        logger.error(
            "Failed to open new session for user: {}, host: {}. Error: {}",
            loginInput.getUserName(),
            loginInput.getHostFromServerUrl(),
            lastRestException);
        throw lastRestException;
      } else {
        SnowflakeSQLException exception =
            new SnowflakeSQLException(
                NO_QUERY_ID,
                "empty authentication response",
                SqlState.CONNECTION_EXCEPTION,
                ErrorCode.CONNECTION_ERROR.getMessageCode());
        logger.error(
            "Failed to open new session for user: {}, host: {}. Error: {}",
            loginInput.getUserName(),
            loginInput.getHostFromServerUrl(),
            exception);
        throw exception;
      }
    }
  }
}
