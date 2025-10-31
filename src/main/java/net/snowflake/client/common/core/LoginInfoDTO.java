package net.snowflake.client.common.core;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * Login Information Data Transfer Object. Used by Snowflake clients to supply auxiliary login info
 * like the client software type and version.
 *
 * @author jhuang
 */
@SnowflakeJdbcInternalApi
public class LoginInfoDTO {
  // Application identifier.
  String clientAppId;

  // Application version.
  String clientAppVersion;

  // [Optional] When instructed, the client can send the user's newly selected
  // password.
  String chosenNewPassword;

  // Flag that user has accepted license
  boolean licenseAccepted;

  public static final String SF_ODBC_APP_ID = "ODBC";
  public static final String SF_JDBC_APP_ID = "JDBC";
  public static final String SF_CONSOLE_APP_ID = "Snowflake UI";
  public static final String SF_SNOWSQL_APP_ID = "SnowSQL";
  public static final String SF_PYTHON_APP_ID = "PythonConnector";
  public static final String SF_SNOWPARK_APP_ID = "Snowpark";

  private static final ResourceBundleManager versionResourceBundleManager =
      ResourceBundleManager.getSingleton("net.snowflake.client.common.version");

  public String getClientAppId() {
    return clientAppId;
  }

  public void setClientAppId(String clientAppId) {
    this.clientAppId = clientAppId;
  }

  public String getClientAppVersion() {
    return clientAppVersion;
  }

  public void setClientAppVersion(String clientAppVersion) {
    this.clientAppVersion = clientAppVersion;
  }

  public String getChosenNewPassword() {
    return chosenNewPassword;
  }

  public void setChosenNewPassword(String chosenNewPassword) {
    this.chosenNewPassword = chosenNewPassword;
  }

  public static String getLatestJDBCAppVersion() {
    return versionResourceBundleManager.getLocalizedMessage("jdbc.version");
  }

  public static String getLatestODBCAppVersion() {
    return versionResourceBundleManager.getLocalizedMessage("odbc.version");
  }

  public static String getLatestPythonAppVersion() {
    return versionResourceBundleManager.getLocalizedMessage("python.version");
  }

  public static String getLatestNodeAppVersion() {
    return versionResourceBundleManager.getLocalizedMessage("node-js.version");
  }

  public static String getLatestSparkConnectorVersion() {
    return versionResourceBundleManager.getLocalizedMessage("spark-snowflakedb.version");
  }

  public static String getLatestSnowsqlAppVersion() {
    return versionResourceBundleManager.getLocalizedMessage("snowsql.version");
  }

  public boolean isLicenseAccepted() {
    return this.licenseAccepted;
  }
}
