/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.core.SFTrustManager.resetOCSPResponseCacherServerURL;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.client.jdbc.*;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.common.core.ClientAuthnDTO;
import net.snowflake.common.core.ClientAuthnParameter;
import net.snowflake.common.core.SqlState;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
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
  protected static final String SF_PATH_AUTHENTICATOR_REQUEST = "/session/authenticator-request";

  public static final String SF_QUERY_SESSION_DELETE = "delete";

  // Headers
  public static final String SF_HEADER_AUTHORIZATION = HttpHeaders.AUTHORIZATION;

  // Authentication type
  private static final String SF_HEADER_BASIC_AUTHTYPE = "Basic";
  private static final String SF_HEADER_SNOWFLAKE_AUTHTYPE = "Snowflake";
  private static final String SF_HEADER_TOKEN_TAG = "Token";
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
  private static final String CLIENT_RESULT_CHUNK_SIZE_JVM =
      "net.snowflake.jdbc.clientResultChunkSize";
  public static final String CLIENT_RESULT_CHUNK_SIZE = "CLIENT_RESULT_CHUNK_SIZE";
  public static final String CLIENT_MEMORY_LIMIT_JVM = "net.snowflake.jdbc.clientMemoryLimit";
  public static final String CLIENT_MEMORY_LIMIT = "CLIENT_MEMORY_LIMIT";
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

  static final String SF_HEADER_SERVICE_NAME = "X-Snowflake-Service";

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
              SERVICE_NAME));
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
              CLIENT_VALIDATE_DEFAULT_PARAMETERS));

  /**
   * Returns Authenticator type
   *
   * @param loginInput login information
   * @return Authenticator type
   */
  private static ClientAuthnDTO.AuthenticatorType getAuthenticator(SFLoginInput loginInput) {
    if (loginInput.getAuthenticator() != null) {
      if (loginInput
          .getAuthenticator()
          .equalsIgnoreCase(ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER.name())) {
        // SAML 2.0 compliant service/application
        return ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER;
      } else if (loginInput
          .getAuthenticator()
          .equalsIgnoreCase(ClientAuthnDTO.AuthenticatorType.OAUTH.name())) {
        // OAuth Authentication
        return ClientAuthnDTO.AuthenticatorType.OAUTH;
      } else if (loginInput
          .getAuthenticator()
          .equalsIgnoreCase(ClientAuthnDTO.AuthenticatorType.SNOWFLAKE_JWT.name())) {
        return ClientAuthnDTO.AuthenticatorType.SNOWFLAKE_JWT;
      } else if (loginInput
          .getAuthenticator()
          .equalsIgnoreCase(ClientAuthnDTO.AuthenticatorType.USERNAME_PASSWORD_MFA.name())) {
        return ClientAuthnDTO.AuthenticatorType.USERNAME_PASSWORD_MFA;
      } else if (!loginInput
          .getAuthenticator()
          .equalsIgnoreCase(ClientAuthnDTO.AuthenticatorType.SNOWFLAKE.name())) {
        // OKTA authenticator v1.
        return ClientAuthnDTO.AuthenticatorType.OKTA;
      }
    }

    // authenticator is null, then jdbc will decide authenticator depends on
    // if privateKey is specified or not. If yes, authenticator type will be
    // SNOWFLAKE_JWT, otherwise it will use SNOWFLAKE.
    return (loginInput.getPrivateKey() != null || loginInput.getPrivateKeyFile() != null)
        ? ClientAuthnDTO.AuthenticatorType.SNOWFLAKE_JWT
        : ClientAuthnDTO.AuthenticatorType.SNOWFLAKE;
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

    final ClientAuthnDTO.AuthenticatorType authenticator = getAuthenticator(loginInput);
    if (!authenticator.equals(ClientAuthnDTO.AuthenticatorType.OAUTH)) {
      // OAuth does not require a username
      AssertUtil.assertTrue(
          loginInput.getUserName() != null, "missing user name for opening session");
    }
    if (authenticator.equals(ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER)) {
      if (Constants.getOS() == Constants.OS.MAC || Constants.getOS() == Constants.OS.WINDOWS) {
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
      // TODO: patch for now. We should update mergeProperteis
      // to normalize all parameters using STRING_PARAMS, INT_PARAMS and
      // BOOLEAN_PARAMS.
      Object value = loginInput.getSessionParameters().get(CLIENT_STORE_TEMPORARY_CREDENTIAL);
      if (value != null) {
        loginInput.getSessionParameters().put(CLIENT_STORE_TEMPORARY_CREDENTIAL, asBoolean(value));
      }
    }

    if (authenticator.equals(ClientAuthnDTO.AuthenticatorType.USERNAME_PASSWORD_MFA)) {
      if (Constants.getOS() == Constants.OS.MAC || Constants.getOS() == Constants.OS.WINDOWS) {
        loginInput.getSessionParameters().put(CLIENT_REQUEST_MFA_TOKEN, true);
      }
    }

    preNewSession(loginInput);

    try {
      return newSession(loginInput, connectionPropertiesMap, tracingLevel);
    } catch (SnowflakeReauthenticationRequest ex) {
      // Id Token expired. We run newSession again with id_token cache cleared
      logger.debug("ID Token being used has expired. Reauthenticating with ID Token cleared...");
      return newSession(loginInput, connectionPropertiesMap, tracingLevel);
    }
  }

  private static void preNewSession(SFLoginInput loginInput) throws SFException {
    if (asBoolean(loginInput.getSessionParameters().get(CLIENT_STORE_TEMPORARY_CREDENTIAL))) {
      CredentialManager.getInstance().fillCachedIdToken(loginInput);
    }

    if (asBoolean(loginInput.getSessionParameters().get(CLIENT_REQUEST_MFA_TOKEN))) {
      CredentialManager.getInstance().fillCachedMfaToken(loginInput);
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

  private static SFLoginOutput newSession(
      SFLoginInput loginInput,
      Map<SFSessionProperty, Object> connectionPropertiesMap,
      String tracingLevel)
      throws SFException, SnowflakeSQLException {
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
    int httpClientSocketTimeout = loginInput.getSocketTimeout();
    final ClientAuthnDTO.AuthenticatorType authenticatorType = getAuthenticator(loginInput);
    Map<String, Object> commonParams;

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

      if (authenticatorType == ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER) {
        // try to reuse id_token if exists
        if (loginInput.getIdToken() == null) {
          // SAML 2.0 compliant service/application
          SessionUtilExternalBrowser s = SessionUtilExternalBrowser.createInstance(loginInput);
          s.authenticate();
          tokenOrSamlResponse = s.getToken();
          samlProofKey = s.getProofKey();
          consentCacheIdToken = s.isConsentCacheIdToken();
        }
      } else if (authenticatorType == ClientAuthnDTO.AuthenticatorType.OKTA) {
        // okta authenticator v1
        tokenOrSamlResponse = getSamlResponseUsingOkta(loginInput);
      } else if (authenticatorType == ClientAuthnDTO.AuthenticatorType.SNOWFLAKE_JWT) {
        SessionUtilKeyPair s =
            new SessionUtilKeyPair(
                loginInput.getPrivateKey(),
                loginInput.getPrivateKeyFile(),
                loginInput.getPrivateKeyFilePwd(),
                loginInput.getAccountName(),
                loginInput.getUserName());

        loginInput.setToken(s.issueJwtToken());
      }

      uriBuilder.addParameter(SFSession.SF_QUERY_REQUEST_ID, UUID.randomUUID().toString());

      uriBuilder.setPath(SF_PATH_LOGIN_REQUEST);
      loginURI = uriBuilder.build();
    } catch (URISyntaxException ex) {
      logger.error("Exception when building URL", ex);

      throw new SFException(ex, ErrorCode.INTERNAL_ERROR, "unexpected URI syntax exception:1");
    }

    try {
      // Adjust OCSP cache server if it is private link
      resetOCSPUrlIfNecessary(loginInput.getServerUrl());
    } catch (IOException ex) {
      throw new SFException(ex, ErrorCode.IO_ERROR, "unexpected URL syntax exception");
    }

    HttpPost postRequest = null;

    try {
      ClientAuthnDTO authnData = new ClientAuthnDTO();
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
      if (authenticatorType == ClientAuthnDTO.AuthenticatorType.SNOWFLAKE) {
        data.put(ClientAuthnParameter.PASSWORD.name(), loginInput.getPassword());
      } else if (authenticatorType == ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER) {
        if (loginInput.getIdToken() != null) {
          data.put(ClientAuthnParameter.AUTHENTICATOR.name(), ID_TOKEN_AUTHENTICATOR);
          data.put(ClientAuthnParameter.TOKEN.name(), loginInput.getIdToken());
        } else {
          data.put(
              ClientAuthnParameter.AUTHENTICATOR.name(),
              ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER.name());
          data.put(ClientAuthnParameter.PROOF_KEY.name(), samlProofKey);
          data.put(ClientAuthnParameter.TOKEN.name(), tokenOrSamlResponse);
        }
      } else if (authenticatorType == ClientAuthnDTO.AuthenticatorType.OKTA) {
        data.put(ClientAuthnParameter.RAW_SAML_RESPONSE.name(), tokenOrSamlResponse);
      } else if (authenticatorType == ClientAuthnDTO.AuthenticatorType.OAUTH
          || authenticatorType == ClientAuthnDTO.AuthenticatorType.SNOWFLAKE_JWT) {
        data.put(ClientAuthnParameter.AUTHENTICATOR.name(), authenticatorType.name());
        data.put(ClientAuthnParameter.TOKEN.name(), loginInput.getToken());
      } else if (authenticatorType == ClientAuthnDTO.AuthenticatorType.USERNAME_PASSWORD_MFA) {
        // No authenticator name should be added here, since this will be treated as snowflake
        // default authenticator by backend
        data.put(ClientAuthnParameter.PASSWORD.name(), loginInput.getPassword());
        if (loginInput.getMfaToken() != null) {
          data.put(ClientAuthnParameter.TOKEN.name(), loginInput.getMfaToken());
        }
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
      String clientInfoJSONStr = systemGetProperty("snowflake.client.info");
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

      authnData.setData(data);
      String json = mapper.writeValueAsString(authnData);

      postRequest = new HttpPost(loginURI);

      // attach the login info json body to the post request
      StringEntity input = new StringEntity(json, StandardCharsets.UTF_8);
      input.setContentType("application/json");
      postRequest.setEntity(input);

      postRequest.addHeader("accept", "application/json");

      /*
       * HttpClient should take authorization header from char[] instead of
       * String.
       */
      postRequest.setHeader(SF_HEADER_AUTHORIZATION, SF_HEADER_BASIC_AUTHTYPE);

      setServiceNameHeader(loginInput, postRequest);

      String theString =
          HttpUtil.executeGeneralRequest(
              postRequest, loginInput.getLoginTimeout(), loginInput.getOCSPMode());

      // general method, same as with data binding
      JsonNode jsonNode = mapper.readTree(theString);

      // check the success field first
      if (!jsonNode.path("success").asBoolean()) {
        logger.debug("response = {}", theString);

        int errorCode = jsonNode.path("code").asInt();
        if (errorCode == Constants.ID_TOKEN_INVALID_LOGIN_REQUEST_GS_CODE) {
          // clean id_token first
          loginInput.setIdToken(null);
          deleteIdTokenCache(loginInput.getHostFromServerUrl(), loginInput.getUserName());

          logger.debug(
              "ID Token Expired / Not Applicable. Reauthenticating without ID Token...: {}",
              errorCode);
          SnowflakeUtil.checkErrorAndThrowExceptionIncludingReauth(jsonNode);
        }

        if (authenticatorType == ClientAuthnDTO.AuthenticatorType.USERNAME_PASSWORD_MFA) {
          deleteMfaTokenCache(loginInput.getHostFromServerUrl(), loginInput.getUserName());
        }

        throw new SnowflakeSQLException(
            NO_QUERY_ID,
            jsonNode.path("message").asText(),
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
        logger.debug("server version = {}", serverVersion);

        if (serverVersion.indexOf(" ") > 0) {
          databaseVersion = serverVersion.substring(0, serverVersion.indexOf(" "));
        } else {
          databaseVersion = serverVersion;
        }
      } else {
        logger.debug("server version is null");
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
        logger.debug("database version is null");
      }

      if (!jsonNode.path("data").path("newClientForUpgrade").isNull()) {
        newClientForUpgrade = jsonNode.path("data").path("newClientForUpgrade").asText();

        logger.debug("new client: {}", newClientForUpgrade);
      }

      // get health check interval and adjust network timeouts if different
      int healthCheckIntervalFromGS = jsonNode.path("data").path("healthCheckInterval").asInt();

      logger.debug("health check interval = {}", healthCheckIntervalFromGS);

      if (healthCheckIntervalFromGS > 0 && healthCheckIntervalFromGS != healthCheckInterval) {
        // add health check interval to socket timeout
        httpClientSocketTimeout =
            loginInput.getSocketTimeout() + (healthCheckIntervalFromGS * 1000);

        final RequestConfig requestConfig =
            RequestConfig.copy(HttpUtil.getRequestConfigWithoutCookies())
                .setConnectTimeout(loginInput.getConnectionTimeout())
                .setSocketTimeout(httpClientSocketTimeout)
                .build();

        HttpUtil.setRequestConfig(requestConfig);

        logger.debug("adjusted connection timeout to = {}", loginInput.getConnectionTimeout());

        logger.debug("adjusted socket timeout to = {}", httpClientSocketTimeout);
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
            databaseVersion,
            databaseMajorVersion,
            databaseMinorVersion,
            httpClientSocketTimeout,
            sessionDatabase,
            sessionSchema,
            sessionRole,
            sessionWarehouse,
            sessionId,
            commonParams);

    if (consentCacheIdToken
        && asBoolean(loginInput.getSessionParameters().get(CLIENT_STORE_TEMPORARY_CREDENTIAL))) {
      CredentialManager.getInstance().writeIdToken(loginInput, ret);
    }

    if (asBoolean(loginInput.getSessionParameters().get(CLIENT_REQUEST_MFA_TOKEN))) {
      CredentialManager.getInstance().writeMfaToken(loginInput, ret);
    }

    return ret;
  }

  private static void setServiceNameHeader(SFLoginInput loginInput, HttpPost postRequest) {
    if (!Strings.isNullOrEmpty(loginInput.getServiceName())) {
      // service name is used to route a request to appropriate cluster.
      postRequest.setHeader(SF_HEADER_SERVICE_NAME, loginInput.getServiceName());
    }
  }

  private static String nullStringAsEmptyString(String value) {
    if (Strings.isNullOrEmpty(value) || "null".equals(value)) {
      return "";
    }
    return value;
  }

  /** Delete the id token cache */
  public static void deleteIdTokenCache(String host, String user) {
    CredentialManager.getInstance().deleteIdTokenCache(host, user);
  }

  public static void deleteMfaTokenCache(String host, String user) {
    CredentialManager.getInstance().deleteMfaTokenCache(host, user);
  }

  /**
   * Renew a session.
   *
   * <p>Use cases: - Session and Master tokens are provided. No Id token: - succeed in getting a new
   * Session token. - fail and raise SnowflakeReauthenticationRequest because Master token expires.
   * Since no id token exists, the exception is thrown to the upstream. - Session and Id tokens are
   * provided. No Master token: - fail and raise SnowflakeReauthenticationRequest and issue a new
   * Session token - fail and raise SnowflakeReauthenticationRequest and fail to issue a new Session
   * token as the
   *
   * @param loginInput login information
   * @return login output
   * @throws SFException if unexpected uri information
   * @throws SnowflakeSQLException if failed to renew the session
   */
  static SFLoginOutput renewSession(SFLoginInput loginInput)
      throws SFException, SnowflakeSQLException {
    return tokenRequest(loginInput, TokenRequestType.RENEW);
  }

  private static SFLoginOutput tokenRequest(SFLoginInput loginInput, TokenRequestType requestType)
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

      uriBuilder.addParameter(SFSession.SF_QUERY_REQUEST_ID, UUID.randomUUID().toString());

      postRequest = new HttpPost(uriBuilder.build());
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
      payload.put("requestType", requestType.value);
      String json = mapper.writeValueAsString(payload);

      // attach the login info json body to the post request
      StringEntity input = new StringEntity(json, StandardCharsets.UTF_8);
      input.setContentType("application/json");
      postRequest.setEntity(input);

      postRequest.addHeader("accept", "application/json");

      postRequest.setHeader(
          SF_HEADER_AUTHORIZATION,
          SF_HEADER_SNOWFLAKE_AUTHTYPE + " " + SF_HEADER_TOKEN_TAG + "=\"" + headerToken + "\"");

      setServiceNameHeader(loginInput, postRequest);

      logger.debug(
          "request type: {}, old session token: {}, " + "master token: {}",
          requestType.value,
          (ArgSupplier) () -> loginInput.getSessionToken() != null ? "******" : null,
          (ArgSupplier) () -> loginInput.getMasterToken() != null ? "******" : null);

      String theString =
          HttpUtil.executeGeneralRequest(
              postRequest, loginInput.getLoginTimeout(), loginInput.getOCSPMode());

      // general method, same as with data binding
      JsonNode jsonNode = mapper.readTree(theString);

      // check the success field first
      if (!jsonNode.path("success").asBoolean()) {
        logger.debug("response = {}", theString);

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
    logger.debug(" public void close() throws SFException");

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
      uriBuilder.addParameter(SFSession.SF_QUERY_REQUEST_ID, UUID.randomUUID().toString());

      uriBuilder.setPath(SF_PATH_SESSION);

      postRequest = new HttpPost(uriBuilder.build());

      postRequest.setHeader(
          SF_HEADER_AUTHORIZATION,
          SF_HEADER_SNOWFLAKE_AUTHTYPE
              + " "
              + SF_HEADER_TOKEN_TAG
              + "=\""
              + loginInput.getSessionToken()
              + "\"");

      setServiceNameHeader(loginInput, postRequest);

      String theString =
          HttpUtil.executeGeneralRequest(
              postRequest, loginInput.getLoginTimeout(), loginInput.getOCSPMode());

      JsonNode rootNode;

      logger.debug("connection close response: {}", theString);

      rootNode = mapper.readTree(theString);

      SnowflakeUtil.checkErrorAndThrowException(rootNode);
    } catch (URISyntaxException ex) {
      throw new RuntimeException("unexpected URI syntax exception", ex);
    } catch (IOException ex) {
      logger.error("unexpected IO exception for: " + postRequest, ex);
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
   * @param oneTimeToken The token used for SSO
   * @return The response in HTML form
   * @throws SnowflakeSQLException Will be thrown if the destination URL in the SAML assertion does
   *     not match
   */
  private static String federatedFlowStep4(
      SFLoginInput loginInput, String ssoUrl, String oneTimeToken) throws SnowflakeSQLException {
    String responseHtml = "";
    try {

      final URL url = new URL(ssoUrl);
      URI oktaGetUri =
          new URIBuilder()
              .setScheme(url.getProtocol())
              .setHost(url.getHost())
              .setPath(url.getPath())
              .setParameter("RelayState", "%2Fsome%2Fdeep%2Flink")
              .setParameter("onetimetoken", oneTimeToken)
              .build();
      HttpGet httpGet = new HttpGet(oktaGetUri);

      HeaderGroup headers = new HeaderGroup();
      headers.addHeader(new BasicHeader(HttpHeaders.ACCEPT, "*/*"));
      httpGet.setHeaders(headers.getAllHeaders());

      responseHtml =
          HttpUtil.executeGeneralRequest(
              httpGet, loginInput.getLoginTimeout(), loginInput.getOCSPMode());

      // step 5
      String postBackUrl = getPostBackUrlFromHTML(responseHtml);
      if (!isPrefixEqual(postBackUrl, loginInput.getServerUrl())) {
        logger.debug(
            "The specified authenticator {} and the destination URL "
                + "in the SAML assertion {} do not match.",
            loginInput.getAuthenticator(),
            postBackUrl);
        // Session is in process of getting created, so exception constructor takes in null session
        // value
        throw new SnowflakeSQLLoggedException(
            null,
            ErrorCode.IDP_INCORRECT_DESTINATION.getMessageCode(),
            SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
            /* session = */ );
      }
    } catch (IOException | URISyntaxException ex) {
      handleFederatedFlowError(loginInput, ex);
    }
    return responseHtml;
  }

  /**
   * Query IDP token url to authenticate and retrieve access token
   *
   * @param loginInput The login info for the request
   * @param tokenUrl The URL used to retrieve the access token
   * @return Returns the one time token
   * @throws SnowflakeSQLException Will be thrown if the execute request fails
   */
  private static String federatedFlowStep3(SFLoginInput loginInput, String tokenUrl)
      throws SnowflakeSQLException {

    String oneTimeToken = "";
    try {
      URL url = new URL(tokenUrl);
      URI tokenUri = url.toURI();
      final HttpPost postRequest = new HttpPost(tokenUri);

      String userName;
      if (Strings.isNullOrEmpty(loginInput.getOKTAUserName())) {
        userName = loginInput.getUserName();
      } else {
        userName = loginInput.getOKTAUserName();
      }
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

      final String idpResponse =
          HttpUtil.executeRequestWithoutCookies(
              postRequest, loginInput.getLoginTimeout(), 0, null, loginInput.getOCSPMode());

      logger.debug("user is authenticated against {}.", loginInput.getAuthenticator());

      // session token is in the data field of the returned json response
      final JsonNode jsonNode = mapper.readTree(idpResponse);
      oneTimeToken = jsonNode.get("cookieToken").asText();
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
            /* session = */ );
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
      URIBuilder fedUriBuilder = new URIBuilder(loginInput.getServerUrl());
      fedUriBuilder.setPath(SF_PATH_AUTHENTICATOR_REQUEST);
      URI fedUrlUri = fedUriBuilder.build();

      Map<String, Object> data = new HashMap<>();
      data.put(ClientAuthnParameter.ACCOUNT_NAME.name(), loginInput.getAccountName());
      data.put(ClientAuthnParameter.AUTHENTICATOR.name(), loginInput.getAuthenticator());
      data.put(ClientAuthnParameter.CLIENT_APP_ID.name(), loginInput.getAppId());
      data.put(ClientAuthnParameter.CLIENT_APP_VERSION.name(), loginInput.getAppVersion());

      ClientAuthnDTO authnData = new ClientAuthnDTO();
      authnData.setData(data);
      String json = mapper.writeValueAsString(authnData);

      // attach the login info json body to the post request
      StringEntity input = new StringEntity(json, StandardCharsets.UTF_8);
      input.setContentType("application/json");
      HttpPost postRequest = new HttpPost(fedUrlUri);
      postRequest.setEntity(input);
      postRequest.addHeader("accept", "application/json");

      final String gsResponse =
          HttpUtil.executeGeneralRequest(
              postRequest, loginInput.getLoginTimeout(), loginInput.getOCSPMode());
      logger.debug("authenticator-request response: {}", gsResponse);
      JsonNode jsonNode = mapper.readTree(gsResponse);

      // check the success field first
      if (!jsonNode.path("success").asBoolean()) {
        logger.debug("response = {}", gsResponse);
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
   * SnowflakeSQLException. Note that we seperate IOExceptions since those tend to be network
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
    JsonNode dataNode = federatedFlowStep1(loginInput);
    String tokenUrl = dataNode.path("tokenUrl").asText();
    String ssoUrl = dataNode.path("ssoUrl").asText();
    federatedFlowStep2(loginInput, tokenUrl, ssoUrl);
    final String oneTimeToken = federatedFlowStep3(loginInput, tokenUrl);
    return federatedFlowStep4(loginInput, ssoUrl, oneTimeToken);
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
        logger.error("Common Parameter JsonNode encountered with " + "no parameter name!");
        continue;
      }

      // Look up the parameter based on the "name" attribute of the node.
      String paramName = child.path("name").asText();

      // What type of value is it and what's the value?
      if (!child.hasNonNull("value")) {
        logger.debug("No value found for Common Parameter {}", child.path("name").asText());
        continue;
      }

      if (STRING_PARAMS.contains(paramName.toUpperCase())) {
        parameters.put(paramName, child.path("value").asText());
      } else if (INT_PARAMS.contains(paramName.toUpperCase())) {
        parameters.put(paramName, child.path("value").asInt());
      } else if (BOOLEAN_PARAMS.contains(paramName.toUpperCase())) {
        parameters.put(paramName, child.path("value").asBoolean());
      } else {
        logger.debug("Unknown Common Parameter: {}", paramName);
      }

      logger.debug("Parameter {}: {}", paramName, child.path("value").asText());
    }

    return parameters;
  }

  static void updateSfDriverParamValues(Map<String, Object> parameters, SFBaseSession session) {
    for (Map.Entry<String, Object> entry : parameters.entrySet()) {
      logger.debug("processing parameter {}", entry.getKey());

      if ("CLIENT_DISABLE_INCIDENTS".equalsIgnoreCase(entry.getKey())) {
        SnowflakeDriver.setDisableIncidents((Boolean) entry.getValue());
      } else if ("CLIENT_SESSION_KEEP_ALIVE".equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setEnableHeartbeat((Boolean) entry.getValue());
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
        if ((boolean) entry.getValue()) {
          TelemetryService.enable();
        } else {
          TelemetryService.disable();
        }
      } else if (CLIENT_VALIDATE_DEFAULT_PARAMETERS.equalsIgnoreCase(entry.getKey())) {
        if (session != null) {
          session.setValidateDefaultParameters(SFLoginInput.getBooleanValue(entry.getValue()));
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
   */
  public static void resetOCSPUrlIfNecessary(String serverUrl) throws IOException {
    if (serverUrl.indexOf(".privatelink.snowflakecomputing.com") > 0) {
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
}
