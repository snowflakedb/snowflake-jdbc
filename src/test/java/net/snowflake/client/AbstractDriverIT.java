package net.snowflake.client;

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.base.Strings;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/** Base test class with common constants, data structures and methods */
public class AbstractDriverIT {

  public static final String DRIVER_CLASS = "net.snowflake.client.jdbc.SnowflakeDriver";
  public static final String DRIVER_CLASS_COM = "com.snowflake.client.jdbc.SnowflakeDriver";
  public static final int DONT_INJECT_SOCKET_TIMEOUT = 0;

  // data files
  protected static final String TEST_DATA_FILE = "orders_100.csv";
  protected static final String TEST_DATA_FILE_2 = "orders_101.csv";

  protected static final String[] fileNames = {TEST_DATA_FILE, TEST_DATA_FILE_2};

  private static Logger logger = Logger.getLogger(AbstractDriverIT.class.getName());

  protected final int ERROR_CODE_BIND_VARIABLE_NOT_ALLOWED_IN_VIEW_OR_UDF_DEF = 2210;

  protected final int ERROR_CODE_DOMAIN_OBJECT_DOES_NOT_EXIST = 2003;

  private static String getConnPropKeyFromEnv(String connectionType, String propKey) {
    String envKey = String.format("SNOWFLAKE_%s_%s", connectionType, propKey);
    return envKey;
  }

  private static String getConnPropValueFromEnv(String connectionType, String propKey) {
    String envKey = String.format("SNOWFLAKE_%s_%s", connectionType, propKey);
    return TestUtil.systemGetEnv(envKey);
  }

  public static Map<String, String> getConnectionParameters(String accountName) {
    return getConnectionParameters(accountName, "TEST");
  }

  /**
   * getConnectionParameters is to obtain connection params from Env
   *
   * @param accountName the connection could be different with different accounts
   * @param connectionType use connectionType is either "TEST"(default) or "ORG"
   * @return properties' key-value map -- In the connection json files the parameters' format is
   *     like below and these key/values have been flattened to a bunch of env variables of these
   *     junit tests.
   *     <p>"testconnection": { "SNOWFLAKE_TEST_ACCOUNT": "...", ... "SNOWFLAKE_TEST_ROLE": ".." },
   *     "orgconnection": { "SNOWFLAKE_ORG_ACCOUNT": "...", ... "SNOWFLAKE_ORG_PORT": "443" } }
   */
  public static Map<String, String> getConnectionParameters(
      String accountName, String connectionType) {
    Map<String, String> params = new HashMap<>();
    String account;
    String host;

    if (accountName == null) {
      account = getConnPropValueFromEnv(connectionType, "ACCOUNT");
      host = getConnPropValueFromEnv(connectionType, "HOST");
    } else {
      account = accountName;
      // By default, the test will run against reg deployment.
      // If developer needs to run in IntelliJ, you can set this env as ".dev.local"
      String deployment = getConnPropValueFromEnv(connectionType, "DEPLOYMENT");
      if (Strings.isNullOrEmpty(deployment)) {
        deployment = ".reg.local";
      }
      host = accountName.trim() + deployment;
    }
    assertThat(
        "set SNOWFLAKE_TEST_ACCOUNT environment variable to the account name.",
        !Strings.isNullOrEmpty(account));
    params.put("account", account);

    if (Strings.isNullOrEmpty(host)) {
      host = account + ".snowflakecomputing.com";
    }

    assertThat(
        "set SNOWFLAKE_TEST_HOST environment variable to the host name.",
        !Strings.isNullOrEmpty(host));
    params.put("host", host);

    String protocol = getConnPropValueFromEnv(connectionType, "PROTOCOL");
    String ssl;
    if ("http".equals(protocol)) {
      ssl = "off";
    } else {
      ssl = "on";
    }
    params.put("ssl", ssl);

    String user = getConnPropValueFromEnv(connectionType, "USER");
    assertThat("set SNOWFLAKE_TEST_USER environment variable.", !Strings.isNullOrEmpty(user));
    params.put("user", user);

    String password = getConnPropValueFromEnv(connectionType, "PASSWORD");
    assertThat(
        "set SNOWFLAKE_TEST_PASSWORD environment variable.", !Strings.isNullOrEmpty(password));
    params.put("password", password);

    String port = getConnPropValueFromEnv(connectionType, "PORT");
    if (Strings.isNullOrEmpty(port)) {
      if ("on".equals(ssl)) {
        port = "443";
      } else {
        port = "80";
      }
    }
    assertThat("set SNOWFLAKE_TEST_PORT environment variable.", !Strings.isNullOrEmpty(port));
    params.put("port", port);

    String database = getConnPropValueFromEnv(connectionType, "DATABASE");
    assertThat(
        "set SNOWFLAKE_TEST_DATABASE environment variable.", !Strings.isNullOrEmpty(database));
    params.put("database", database);

    String schema = getConnPropValueFromEnv(connectionType, "SCHEMA");
    assertThat("set SNOWFLAKE_TEST_SCHEMA environment variable.", !Strings.isNullOrEmpty(schema));
    params.put("schema", schema);

    String role = getConnPropValueFromEnv(connectionType, "ROLE");
    assertThat("set SNOWFLAKE_TEST_ROLE environment variable.", !Strings.isNullOrEmpty(role));
    params.put("role", role);

    String warehouse = getConnPropValueFromEnv(connectionType, "WAREHOUSE");
    assertThat(
        "set SNOWFLAKE_TEST_WAREHOUSE environment variable.", !Strings.isNullOrEmpty(warehouse));
    params.put("warehouse", warehouse);

    params.put("uri", String.format("jdbc:snowflake://%s:%s", host, port));

    String adminUser = getConnPropValueFromEnv(connectionType, "ADMIN_USER");
    params.put("adminUser", adminUser);

    String adminPassword = getConnPropValueFromEnv(connectionType, "ADMIN_PASSWORD");
    params.put("adminPassword", adminPassword);

    String ssoUser = getConnPropValueFromEnv(connectionType, "SSO_USER");
    params.put("ssoUser", ssoUser);

    String ssoPassword = getConnPropValueFromEnv(connectionType, "SSO_PASSWORD");
    params.put("ssoPassword", ssoPassword);

    return params;
  }

  public static Map<String, String> getConnectionParameters() {
    return getConnectionParameters(null);
  }

  /**
   * Gets a connection with default session parameter settings, but tunable query api version and
   * socket timeout setting
   *
   * @param paramProperties connection properties
   * @return Connection a database connection
   * @throws SQLException raised if any error occurs
   */
  public static Connection getConnection(Properties paramProperties) throws SQLException {
    return getConnection(DONT_INJECT_SOCKET_TIMEOUT, paramProperties, false, false);
  }

  /**
   * Gets a connection with custom account name, but otherwise default settings
   *
   * @return Connection a database connection
   * @throws SQLException raised if any error occurs
   */
  public static Connection getConnection(String accountName) throws SQLException {
    return getConnection(DONT_INJECT_SOCKET_TIMEOUT, null, false, false, accountName);
  }

  /**
   * Gets a connection with custom account name and some property set, useful for testing property
   * on specific account
   *
   * @return Connection a database connection
   * @throws SQLException raised if any error occurs
   */
  public static Connection getConnection(String accountName, Properties paramProperties)
      throws SQLException {
    return getConnection(DONT_INJECT_SOCKET_TIMEOUT, paramProperties, false, false, accountName);
  }

  /**
   * Gets a connection with default settings
   *
   * @return Connection a database connection
   * @throws SQLException raised if any error occurs
   */
  public static Connection getConnection() throws SQLException {
    return getConnection(DONT_INJECT_SOCKET_TIMEOUT, null, false, false);
  }

  /**
   * Gets a connection with default session parameter settings, but tunable query api version and
   * socket timeout setting
   *
   * @param injectSocketTimeout number of seconds to inject in connection
   * @return Connection a database connection
   * @throws SQLException raised if any error occurs
   */
  public static Connection getConnection(int injectSocketTimeout) throws SQLException {
    return getConnection(injectSocketTimeout, null, false, false);
  }

  /**
   * Gets a connection with Snowflake admin
   *
   * @return Connection a database connection
   * @throws SQLException raised if any error occurs
   */
  protected static Connection getSnowflakeAdminConnection() throws SQLException {
    return getConnection(DONT_INJECT_SOCKET_TIMEOUT, null, true, false);
  }

  /**
   * Gets a connection with Snowflake admin
   *
   * @param paramProperties connection properties
   * @return Connection a database connection
   * @throws SQLException raised if any error occurs
   */
  protected static Connection getSnowflakeAdminConnection(Properties paramProperties)
      throws SQLException {
    return getConnection(DONT_INJECT_SOCKET_TIMEOUT, paramProperties, true, false);
  }

  /**
   * Gets a connection in same way as function below but with default account (gotten from
   * environment variables)
   *
   * @param injectSocketTimeout
   * @param paramProperties
   * @param isAdmin
   * @param usesCom
   * @return
   * @throws SQLException
   */
  public static Connection getConnection(
      int injectSocketTimeout, Properties paramProperties, boolean isAdmin, boolean usesCom)
      throws SQLException {
    return getConnection(injectSocketTimeout, paramProperties, isAdmin, usesCom, null);
  }

  /**
   * Gets a connection for the custom session parameter settings and tunable query api version and
   * socket timeout setting
   *
   * @param injectSocketTimeout number of seconds to inject in connection
   * @param paramProperties connection properties
   * @param isAdmin is Snowflake admin user?
   * @param usesCom uses com.snowflake instead of net.snowflake?
   * @return Connection database connection
   * @throws SQLException raised if any error occurs
   */
  public static Connection getConnection(
      int injectSocketTimeout,
      @Nullable Properties paramProperties,
      boolean isAdmin,
      boolean usesCom,
      String accountName)
      throws SQLException {
    // Load Snowflake JDBC class
    String driverClass = DRIVER_CLASS;
    if (usesCom) {
      driverClass = DRIVER_CLASS_COM;
    }
    try {
      Class.forName(driverClass);
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Cannot find Driver", e);
      throw new RuntimeException(e.getCause());
    }
    Map<String, String> params = getConnectionParameters(accountName);

    // build connection properties
    Properties properties = new Properties();
    if (isAdmin) {
      assertThat(
          "set SNOWFLAKE_TEST_ADMIN_USER environment variable.",
          !Strings.isNullOrEmpty(params.get("adminUser")));
      assertThat(
          "set SNOWFLAKE_TEST_ADMIN_PASSWORD environment variable.",
          !Strings.isNullOrEmpty(params.get("adminPassword")));

      properties.put("user", params.get("adminUser"));
      properties.put("password", params.get("adminPassword"));
      properties.put("role", "accountadmin");
      properties.put("account", "snowflake");
    } else {
      properties.put("user", params.get("user"));
      properties.put("password", params.get("password"));
      properties.put("role", params.get("role"));
      properties.put("account", params.get("account"));
    }
    properties.put("db", params.get("database"));
    properties.put("schema", params.get("schema"));
    properties.put("warehouse", params.get("warehouse"));
    properties.put("ssl", params.get("ssl"));

    properties.put("internal", Boolean.TRUE.toString()); // TODO: do we need this?
    properties.put("insecureMode", false); // use OCSP for all tests.

    if (injectSocketTimeout > 0) {
      properties.put("injectSocketTimeout", String.valueOf(injectSocketTimeout));
    }

    // Set the session parameter properties
    if (paramProperties != null) {
      for (Map.Entry<?, ?> entry : paramProperties.entrySet()) {
        properties.put(entry.getKey(), entry.getValue());
      }
    }
    return DriverManager.getConnection(params.get("uri"), properties);
  }

  /**
   * Close SQL Objects
   *
   * @param resultSet a result set object
   * @param statement a statement object
   * @param connection a connection
   * @throws SQLException raised if any error occurs
   */
  public void closeSQLObjects(ResultSet resultSet, Statement statement, Connection connection)
      throws SQLException {
    if (resultSet != null) {
      resultSet.close();
    }
    if (statement != null) {
      statement.close();
    }
    if (connection != null) {
      connection.close();
    }
  }

  /**
   * Close SQL Objects
   *
   * @param statement a statement object
   * @param connection a connection
   * @throws SQLException raised if any error occurs
   */
  public void closeSQLObjects(Statement statement, Connection connection) throws SQLException {
    if (statement != null) {
      statement.close();
    }
    if (connection != null) {
      connection.close();
    }
  }

  /**
   * Get a full path of the file in Resource
   *
   * @param fileName a file name
   * @return a full path name of the file
   */
  public static String getFullPathFileInResource(String fileName) {
    ClassLoader classLoader = AbstractDriverIT.class.getClassLoader();
    URL url = classLoader.getResource(fileName);
    if (url != null) {
      try {
        return Paths.get(url.toURI()).toAbsolutePath().toString();
      } catch (URISyntaxException ex) {
        throw new RuntimeException("Unable to get absolute path: " + fileName);
      }
    } else {
      throw new RuntimeException("No file is found: " + fileName);
    }
  }

  protected static Timestamp buildTimestamp(
      int year, int month, int day, int hour, int minute, int second, int fractionInNanoseconds) {
    Calendar cal = Calendar.getInstance();
    cal.set(year, month, day, hour, minute, second);
    Timestamp ts = new Timestamp(cal.getTime().getTime());
    ts.setNanos(fractionInNanoseconds);
    return ts;
  }

  protected static Date buildDate(int year, int month, int day) {
    Calendar cal = Calendar.getInstance();
    cal.set(year, month, day, 0, 0, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return new Date(cal.getTime().getTime());
  }

  protected static Date buildDateWithTZ(int year, int month, int day, TimeZone tz) {
    Calendar cal = Calendar.getInstance();
    cal.setTimeZone(tz);
    cal.set(year, month, day, 0, 0, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return new Date(cal.getTime().getTime());
  }
}
