package net.snowflake.client.authentication;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;

import java.util.Properties;

public class AuthConnectionParameters {

  static final String SSO_USER = systemGetEnv("SNOWFLAKE_AUTH_TEST_BROWSER_USER");
  static final String HOST = systemGetEnv("SNOWFLAKE_AUTH_TEST_HOST");
  static final String SSO_PASSWORD = systemGetEnv("SNOWFLAKE_AUTH_TEST_OKTA_PASS");

  static Properties getBaseConnectionParameters() {
    Properties properties = new Properties();
    properties.put("host", HOST);
    properties.put("port", systemGetEnv("SNOWFLAKE_AUTH_TEST_PORT"));
    properties.put("role", systemGetEnv("SNOWFLAKE_AUTH_TEST_ROLE"));
    properties.put("account", systemGetEnv("SNOWFLAKE_AUTH_TEST_ACCOUNT"));
    properties.put("db", systemGetEnv("SNOWFLAKE_AUTH_TEST_DATABASE"));
    properties.put("schema", systemGetEnv("SNOWFLAKE_AUTH_TEST_SCHEMA"));
    properties.put("warehouse", systemGetEnv("SNOWFLAKE_AUTH_TEST_WAREHOUSE"));
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
}
