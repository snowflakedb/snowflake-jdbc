package net.snowflake.client.core;

import java.security.PrivateKey;
import java.util.regex.Pattern;
import net.snowflake.client.jdbc.ErrorCode;

/** session properties accepted for opening a new session. */
public enum SFSessionProperty {
  SERVER_URL("serverURL", true, String.class),
  USER("user", false, String.class),
  PASSWORD("password", false, String.class),
  ACCOUNT("account", true, String.class),
  DATABASE("database", false, String.class, "db"),
  SCHEMA("schema", false, String.class),
  PASSCODE_IN_PASSWORD("passcodeInPassword", false, Boolean.class),
  PASSCODE("passcode", false, String.class),
  TOKEN("token", false, String.class),
  ID_TOKEN_PASSWORD("id_token_password", false, String.class),
  ROLE("role", false, String.class),
  AUTHENTICATOR("authenticator", false, String.class),
  OKTA_USERNAME("oktausername", false, String.class),
  PRIVATE_KEY("privateKey", false, PrivateKey.class),
  OAUTH_REDIRECT_URI("oauthRedirectUri", false, String.class),
  OAUTH_CLIENT_ID("oauthClientID", false, String.class),
  OAUTH_CLIENT_SECRET("oauthClientSecret", false, String.class),
  OAUTH_SCOPE("oauthScope", false, String.class),
  OAUTH_AUTHORIZATION_URL("oauthAuthorizationUrl", false, String.class),
  OAUTH_TOKEN_REQUEST_URL("oauthTokenRequestUrl", false, String.class),
  WORKLOAD_IDENTITY_PROVIDER("workloadIdentityProvider", false, String.class),
  WORKLOAD_IDENTITY_ENTRA_RESOURCE("workloadIdentityEntraResource", false, String.class),
  WAREHOUSE("warehouse", false, String.class),
  LOGIN_TIMEOUT("loginTimeout", false, Integer.class),
  NETWORK_TIMEOUT("networkTimeout", false, Integer.class),
  INJECT_SOCKET_TIMEOUT("injectSocketTimeout", false, Integer.class),
  INJECT_CLIENT_PAUSE("injectClientPause", false, Integer.class),
  APP_ID("appId", false, String.class),
  APP_VERSION("appVersion", false, String.class),
  OCSP_FAIL_OPEN("ocspFailOpen", false, Boolean.class),
  /**
   * @deprecated Use {@link #DISABLE_OCSP_CHECKS} for clarity. This configuration option is used to
   *     disable OCSP verification.
   */
  @Deprecated
  INSECURE_MODE("insecureMode", false, Boolean.class),
  DISABLE_OCSP_CHECKS("disableOCSPChecks", false, Boolean.class),
  QUERY_TIMEOUT("queryTimeout", false, Integer.class),
  STRINGS_QUOTED("stringsQuotedForColumnDef", false, Boolean.class),
  APPLICATION("application", false, String.class),
  TRACING("tracing", false, String.class),
  DISABLE_SOCKS_PROXY("disableSocksProxy", false, Boolean.class),
  // connection proxy
  USE_PROXY("useProxy", false, Boolean.class),
  PROXY_HOST("proxyHost", false, String.class),
  PROXY_PORT("proxyPort", false, String.class),
  PROXY_USER("proxyUser", false, String.class),
  PROXY_PASSWORD("proxyPassword", false, String.class),
  NON_PROXY_HOSTS("nonProxyHosts", false, String.class),
  PROXY_PROTOCOL("proxyProtocol", false, String.class),
  VALIDATE_DEFAULT_PARAMETERS("validateDefaultParameters", false, Boolean.class),
  INJECT_WAIT_IN_PUT("inject_wait_in_put", false, Integer.class),
  PRIVATE_KEY_FILE("private_key_file", false, String.class),
  PRIVATE_KEY_BASE64("private_key_base64", false, String.class),
  /**
   * @deprecated Use {@link #PRIVATE_KEY_PWD} for clarity. The given password will be used to
   *     decrypt the private key value independent of whether that value is supplied as a file or
   *     base64 string
   */
  @Deprecated
  PRIVATE_KEY_FILE_PWD("private_key_file_pwd", false, String.class),
  PRIVATE_KEY_PWD("private_key_pwd", false, String.class),
  CLIENT_INFO("snowflakeClientInfo", false, String.class),
  ALLOW_UNDERSCORES_IN_HOST("allowUnderscoresInHost", false, Boolean.class),

  // Adds a suffix to the user agent header in the http requests made by the jdbc driver
  USER_AGENT_SUFFIX("user_agent_suffix", false, String.class),

  CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED(
      "CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED", false, Boolean.class),
  GZIP_DISABLED("gzipDisabled", false, Boolean.class),
  DISABLE_QUERY_CONTEXT_CACHE("disableQueryContextCache", false, Boolean.class),
  HTAP_OOB_TELEMETRY_ENABLED("htapOOBTelemetryEnabled", false, Boolean.class),

  CLIENT_CONFIG_FILE("client_config_file", false, String.class),

  MAX_HTTP_RETRIES("maxHttpRetries", false, Integer.class),

  ENABLE_PUT_GET("enablePutGet", false, Boolean.class),
  DISABLE_CONSOLE_LOGIN("disableConsoleLogin", false, Boolean.class),

  PUT_GET_MAX_RETRIES("putGetMaxRetries", false, Integer.class),

  RETRY_TIMEOUT("retryTimeout", false, Integer.class),
  ENABLE_DIAGNOSTICS("ENABLE_DIAGNOSTICS", false, Boolean.class),
  DIAGNOSTICS_ALLOWLIST_FILE("DIAGNOSTICS_ALLOWLIST_FILE", false, String.class),

  ENABLE_PATTERN_SEARCH("enablePatternSearch", false, Boolean.class),
  ENABLE_EXACT_SCHEMA_SEARCH_ENABLED("ENABLE_EXACT_SCHEMA_SEARCH_ENABLED", false, Boolean.class),

  DISABLE_GCS_DEFAULT_CREDENTIALS("disableGcsDefaultCredentials", false, Boolean.class),

  JDBC_ARROW_TREAT_DECIMAL_AS_INT("JDBC_ARROW_TREAT_DECIMAL_AS_INT", false, Boolean.class),

  DISABLE_SAML_URL_CHECK("disableSamlURLCheck", false, Boolean.class),

  // Used to determine whether to use the previously hardcoded value for the formatter (for
  // backwards compatibility) or use the value of JDBC_FORMAT_DATE_WITH_TIMEZONE
  JDBC_DEFAULT_FORMAT_DATE_WITH_TIMEZONE(
      "JDBC_DEFAULT_FORMAT_DATE_WITH_TIMEZONE", false, Boolean.class),

  // Used as a fix for issue SNOW-354859. Remove with snowflake-jdbc version 4.x with BCR changes.
  JDBC_GET_DATE_USE_NULL_TIMEZONE("JDBC_GET_DATE_USE_NULL_TIMEZONE", false, Boolean.class),

  BROWSER_RESPONSE_TIMEOUT("BROWSER_RESPONSE_TIMEOUT", false, Integer.class),

  ENABLE_CLIENT_STORE_TEMPORARY_CREDENTIAL("clientStoreTemporaryCredential", false, Boolean.class),

  ENABLE_CLIENT_REQUEST_MFA_TOKEN("clientRequestMfaToken", false, Boolean.class),

  HTTP_CLIENT_CONNECTION_TIMEOUT("HTTP_CLIENT_CONNECTION_TIMEOUT", false, Integer.class),

  HTTP_CLIENT_SOCKET_TIMEOUT("HTTP_CLIENT_SOCKET_TIMEOUT", false, Integer.class),

  JAVA_LOGGING_CONSOLE_STD_OUT("JAVA_LOGGING_CONSOLE_STD_OUT", false, Boolean.class),

  JAVA_LOGGING_CONSOLE_STD_OUT_THRESHOLD(
      "JAVA_LOGGING_CONSOLE_STD_OUT_THRESHOLD", false, String.class),

  IMPLICIT_SERVER_SIDE_QUERY_TIMEOUT("IMPLICIT_SERVER_SIDE_QUERY_TIMEOUT", false, Boolean.class),

  CLEAR_BATCH_ONLY_AFTER_SUCCESSFUL_EXECUTION(
      "CLEAR_BATCH_ONLY_AFTER_SUCCESSFUL_EXECUTION", false, Boolean.class);

  // property key in string
  private String propertyKey;

  // if required  when establishing connection
  private boolean required;

  // value type
  private Class<?> valueType;

  // alias to property key
  private String[] aliases;

  // application name matcher
  public static Pattern APPLICATION_REGEX = Pattern.compile("^[A-Za-z][A-Za-z0-9\\.\\-_]{1,50}$");

  public boolean isRequired() {
    return required;
  }

  public String getPropertyKey() {
    return propertyKey;
  }

  public Class<?> getValueType() {
    return valueType;
  }

  SFSessionProperty(String propertyKey, boolean required, Class<?> valueType, String... aliases) {
    this.propertyKey = propertyKey;
    this.required = required;
    this.valueType = valueType;
    this.aliases = aliases;
  }

  static SFSessionProperty lookupByKey(String propertyKey) {
    for (SFSessionProperty property : SFSessionProperty.values()) {
      if (property.propertyKey.equalsIgnoreCase(propertyKey)) {
        return property;
      } else {
        for (String alias : property.aliases) {
          if (alias.equalsIgnoreCase(propertyKey)) {
            return property;
          }
        }
      }
    }
    return null;
  }

  /**
   * Check if property value is desired class. Convert if possible
   *
   * @param property The session property to check
   * @param propertyValue The property value to check
   * @return The checked property value
   * @throws SFException Will be thrown if an invalid property value is passed in
   */
  static Object checkPropertyValue(SFSessionProperty property, Object propertyValue)
      throws SFException {
    if (propertyValue == null) {
      return null;
    }

    if (property.getValueType().isAssignableFrom(propertyValue.getClass())) {
      switch (property) {
        case APPLICATION:
          if (APPLICATION_REGEX.matcher((String) propertyValue).find()) {
            return propertyValue;
          } else {
            throw new SFException(ErrorCode.INVALID_PARAMETER_VALUE, propertyValue, property);
          }
        default:
          return propertyValue;
      }
    } else {
      if (property.getValueType() == Boolean.class && propertyValue instanceof String) {
        return SFLoginInput.getBooleanValue(propertyValue);
      } else if (property.getValueType() == Integer.class && propertyValue instanceof String) {
        try {
          return Integer.valueOf((String) propertyValue);
        } catch (NumberFormatException e) {
          throw new SFException(
              ErrorCode.INVALID_PARAMETER_VALUE,
              propertyValue.getClass().getName(),
              property.getValueType().getName());
        }
      }
    }

    throw new SFException(
        ErrorCode.INVALID_PARAMETER_TYPE,
        propertyValue.getClass().getName(),
        property.getValueType().getName());
  }
}
