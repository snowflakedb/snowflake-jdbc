package net.snowflake.client.authentication;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;

import java.util.Properties;

public class AuthConnectionParameters {

  static final String SSO_USER = systemGetEnv("SNOWFLAKE_AUTH_TEST_BROWSER_USER");
  static final String SNOWFLAKE_USER = systemGetEnv("SNOWFLAKE_AUTH_TEST_SNOWFLAKE_USER");
  static final String HOST = systemGetEnv("SNOWFLAKE_AUTH_TEST_HOST");
  static final String SSO_PASSWORD = systemGetEnv("SNOWFLAKE_AUTH_TEST_OKTA_PASS");
  static final String OKTA = systemGetEnv("SNOWFLAKE_AUTH_TEST_OKTA_NAME");
  static final String OAUTH_PASSWORD =
      systemGetEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_USER_PASSWORD");
  static final String SNOWFLAKE_INTERNAL_ROLE =
      systemGetEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_ROLE");

  static Properties getBaseConnectionParameters() {
    Properties properties = new Properties();
    properties.put("host", HOST);
    properties.put("port", systemGetEnv("SNOWFLAKE_AUTH_TEST_PORT"));
    properties.put("role", systemGetEnv("SNOWFLAKE_AUTH_TEST_ROLE"));
    properties.put("account", systemGetEnv("SNOWFLAKE_AUTH_TEST_ACCOUNT"));
    properties.put("db", systemGetEnv("SNOWFLAKE_AUTH_TEST_DATABASE"));
    properties.put("schema", systemGetEnv("SNOWFLAKE_AUTH_TEST_SCHEMA"));
    properties.put("warehouse", systemGetEnv("SNOWFLAKE_AUTH_TEST_WAREHOUSE"));
    properties.put("CLIENT_STORE_TEMPORARY_CREDENTIAL", false);
    return properties;
  }

  static Properties getExternalBrowserConnectionParameters() {
    Properties properties = getBaseConnectionParameters();
    properties.put("user", SSO_USER);
    properties.put("authenticator", "externalbrowser");
    return properties;
  }

  static Properties getStoreIDTokenConnectionParameters() {
    Properties properties = getExternalBrowserConnectionParameters();
    properties.put("CLIENT_STORE_TEMPORARY_CREDENTIAL", true);
    return properties;
  }

  static Properties getOktaConnectionParameters() {
    Properties properties = getBaseConnectionParameters();
    properties.put("user", SSO_USER);
    properties.put("password", SSO_PASSWORD);
    properties.put("authenticator", systemGetEnv("SNOWFLAKE_AUTH_TEST_OAUTH_URL"));
    return properties;
  }

  static Properties getOauthConnectionParameters(String token) {
    Properties properties = getBaseConnectionParameters();
    properties.put("user", SSO_USER);
    properties.put("authenticator", "OAUTH");
    properties.put("token", token);
    return properties;
  }

  static Properties getOAuthExternalAuthorizationCodeConnectionParameters() {
    Properties properties = getBaseConnectionParameters();
    properties.put("authenticator", "OAUTH_AUTHORIZATION_CODE");
    properties.put(
        "oauthClientId", systemGetEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID"));
    properties.put(
        "oauthClientSecret", systemGetEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_SECRET"));
    properties.put(
        "oauthRedirectURI", systemGetEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_REDIRECT_URI"));
    properties.put(
        "oauthAuthorizationUrl", systemGetEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_AUTH_URL"));
    properties.put(
        "oauthTokenRequestUrl", systemGetEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_TOKEN"));
    properties.put("user", SSO_USER);

    return properties;
  }

  static Properties getOAuthSnowflakeAuthorizationCodeConnectionParameters() {
    Properties properties = getBaseConnectionParameters();
    properties.put("authenticator", "OAUTH_AUTHORIZATION_CODE");
    properties.put(
        "oauthClientId", systemGetEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_CLIENT_ID"));
    properties.put(
        "oauthClientSecret",
        systemGetEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_CLIENT_SECRET"));
    properties.put(
        "oauthRedirectURI",
        systemGetEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_REDIRECT_URI"));
    properties.put("role", systemGetEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_ROLE"));
    properties.put("user", systemGetEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID"));
    return properties;
  }

  static Properties getOAuthSnowflakeWildcardsAuthorizationCodeConnectionParameters() {
    Properties properties = getBaseConnectionParameters();
    properties.put("authenticator", "OAUTH_AUTHORIZATION_CODE");
    properties.put(
        "oauthClientId",
        systemGetEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_WILDCARDS_CLIENT_ID"));
    properties.put(
        "oauthClientSecret",
        systemGetEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_WILDCARDS_CLIENT_SECRET"));
    properties.put("role", systemGetEnv("SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_ROLE"));
    properties.put("user", systemGetEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID"));
    return properties;
  }

  static Properties getOAuthOktaClientCredentialParameters() {
    Properties properties = getBaseConnectionParameters();
    properties.put("authenticator", "OAUTH_CLIENT_CREDENTIALS");
    properties.put(
        "oauthClientId", systemGetEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID"));
    properties.put(
        "oauthClientSecret", systemGetEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_SECRET"));
    properties.put(
        "oauthTokenRequestUrl", systemGetEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_TOKEN"));
    properties.put("user", systemGetEnv("SNOWFLAKE_AUTH_TEST_EXTERNAL_OAUTH_OKTA_CLIENT_ID"));
    return properties;
  }

  static Properties getPATConnectionParameters() {
    Properties properties = getBaseConnectionParameters();
    properties.put("user", SSO_USER);
    properties.put("authenticator", "PROGRAMMATIC_ACCESS_TOKEN");
    return properties;
  }
}
