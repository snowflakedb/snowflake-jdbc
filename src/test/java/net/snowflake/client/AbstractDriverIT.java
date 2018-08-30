/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client;

import com.google.common.base.Strings;
import org.junit.Rule;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Base test class with common constants, data structures and methods
 *
 * @author jhuang
 */
public class AbstractDriverIT
{
  public static final String DRIVER_CLASS = "net.snowflake.client.jdbc.SnowflakeDriver";
  static final int DONT_INJECT_SOCKET_TIMEOUT = 0;

  private static Logger logger =
      Logger.getLogger(AbstractDriverIT.class.getName());

  static
  {
    // Load Snowflake JDBC class
    try
    {
      Class.forName(DRIVER_CLASS);
    } catch (Exception e)
    {
      logger.log(Level.SEVERE, "Cannot find Driver", e);
      throw new RuntimeException(e.getCause());
    }
  }

  protected final int ERROR_CODE_UNACCEPTABLE_PREPARED_STATEMENT = 7;

  protected final int ERROR_CODE_BIND_VARIABLE_NOT_ALLOWED_IN_VIEW_OR_UDF_DEF
      = 2210;

  protected final int ERROR_CODE_DOMAIN_OBJECT_DOES_NOT_EXIST = 2003;

  @Rule
  public ConditionalIgnoreRule rule = new ConditionalIgnoreRule();

  public static Map<String, String> getConnectionParameters()
      throws SQLException
  {
    Map<String, String> params = new HashMap<>();
    String account = System.getenv("SNOWFLAKE_TEST_ACCOUNT");
    assertThat("set SNOWFLAKE_TEST_ACCOUNT environment variable.",
        !Strings.isNullOrEmpty(account));
    params.put("account", account);

    String user = System.getenv("SNOWFLAKE_TEST_USER");
    assertThat("set SNOWFLAKE_TEST_USER environment variable.",
        !Strings.isNullOrEmpty(user));
    params.put("user", user);

    String password = System.getenv("SNOWFLAKE_TEST_PASSWORD");
    assertThat("set SNOWFLAKE_TEST_PASSWORD environment variable.",
        !Strings.isNullOrEmpty(password));
    params.put("password", password);

    String host = System.getenv("SNOWFLAKE_TEST_HOST");
    assertThat("set SNOWFLAKE_TEST_HOST environment variable.",
        !Strings.isNullOrEmpty(host));
    params.put("host", host);

    String port = System.getenv("SNOWFLAKE_TEST_PORT");
    assertThat("set SNOWFLAKE_TEST_PORT environment variable.",
        !Strings.isNullOrEmpty(port));
    params.put("port", port);

    String database = System.getenv("SNOWFLAKE_TEST_DATABASE");
    assertThat("set SNOWFLAKE_TEST_DATABASE environment variable.",
        !Strings.isNullOrEmpty(database));
    params.put("database", database);

    String schema = System.getenv("SNOWFLAKE_TEST_SCHEMA");
    assertThat("set SNOWFLAKE_TEST_SCHEMA environment variable.",
        !Strings.isNullOrEmpty(schema));
    params.put("schema", schema);

    String role = System.getenv("SNOWFLAKE_TEST_ROLE");
    assertThat("set SNOWFLAKE_TEST_ROLE environment variable.",
        !Strings.isNullOrEmpty(role));
    params.put("role", role);

    String warehouse = System.getenv("SNOWFLAKE_TEST_WAREHOUSE");
    assertThat("set SNOWFLAKE_TEST_WAREHOUSE environment variable.",
        !Strings.isNullOrEmpty(role));
    params.put("warehouse", warehouse);

    String protocol = System.getenv("SNOWFLAKE_TEST_PROTOCOL");
    String ssl;
    if (Strings.isNullOrEmpty(protocol) || "http".equals(protocol))
    {
      ssl = "off";
    } else
    {
      ssl = "on";
    }
    params.put("ssl", ssl);

    params.put("uri", String.format("jdbc:snowflake://%s:%s", host, port));

    String adminUser = System.getenv("SNOWFLAKE_TEST_ADMIN_USER");
    params.put("adminUser", adminUser);

    String adminPassword = System.getenv("SNOWFLAKE_TEST_ADMIN_PASSWORD");
    params.put("adminPassword", adminPassword);

    String ssoUser = System.getenv("SNOWFLAKE_TEST_SSO_USER");
    params.put("ssoUser", ssoUser);

    return params;
  }

  /**
   * Gets a connection with default session parameter settings,
   * but tunable query api version and socket timeout setting
   *
   * @param paramProperties connection properties
   * @return Connection a database connection
   * @throws SQLException raised if any error occurs
   */

  public static Connection getConnection(Properties paramProperties)
      throws SQLException
  {
    return getConnection(DONT_INJECT_SOCKET_TIMEOUT, paramProperties, false);
  }

  /**
   * Gets a connection with default settings
   *
   * @return Connection a database connection
   * @throws SQLException raised if any error occurs
   */
  public static Connection getConnection()
      throws SQLException
  {
    return getConnection(DONT_INJECT_SOCKET_TIMEOUT, null, false);
  }

  /**
   * Gets a connection with default session parameter settings,
   * but tunable query api version and socket timeout setting
   *
   * @param injectSocketTimeout number of seconds to inject in connection
   * @return Connection a database connection
   * @throws SQLException raised if any error occurs
   */
  public static Connection getConnection(int injectSocketTimeout)
      throws SQLException
  {
    return getConnection(injectSocketTimeout, null, false);
  }

  /**
   * Gets a connection with Snowflake admin
   *
   * @return Connection a database connection
   * @throws SQLException raised if any error occurs
   */
  public static Connection getSnowflakeAdminConnection()
      throws SQLException
  {
    return getConnection(DONT_INJECT_SOCKET_TIMEOUT, null, true);
  }

  /**
   * Gets a connection with Snowflake admin
   *
   * @param paramProperties connection properties
   * @return Connection a database connection
   * @throws SQLException raised if any error occurs
   */
  public static Connection getSnowflakeAdminConnection(Properties paramProperties)
      throws SQLException
  {
    return getConnection(DONT_INJECT_SOCKET_TIMEOUT, paramProperties, true);
  }

  /**
   * Gets a connection for the custom session parameter settings and
   * tunable query api version and socket timeout setting
   *
   * @param injectSocketTimeout number of seconds to inject in connection
   * @param paramProperties     connection properties
   * @param isAdmin             is Snowflake admin user?
   * @return Connectiona database connection
   * @throws SQLException raised if any error occurs
   */
  public static Connection getConnection(
      int injectSocketTimeout, Properties paramProperties, boolean isAdmin)
      throws SQLException
  {
    Map<String, String> params = getConnectionParameters();

    // build connection properties
    Properties properties = new Properties();
    if (isAdmin)
    {
      assertThat("set SNOWFLAKE_TEST_ADMIN_USER environment variable.",
          !Strings.isNullOrEmpty(params.get("adminUser")));
      assertThat("set SNOWFLAKE_TEST_ADMIN_PASSWORD environment variable.",
          !Strings.isNullOrEmpty(params.get("adminPassword")));

      properties.put("user", params.get("adminUser"));
      properties.put("password", params.get("adminPassword"));
      properties.put("role", "accountadmin");
      properties.put("account", "snowflake");
    } else
    {
      properties.put("user", params.get("user"));
      properties.put("password", params.get("password"));
      properties.put("role", params.get("role"));
      properties.put("account", params.get("account"));
    }
    properties.put("db", params.get("database"));
    properties.put("schema", params.get("schema"));
    properties.put("warehouse", params.get("warehouse"));
    properties.put("ssl", params.get("ssl"));

    properties.put("internal", "true"); // TODO: do we need this?

    properties.put("insecureMode", false); // use OCSP for all tests.

    if (injectSocketTimeout > 0)
    {
      properties.put(
          "injectSocketTimeout", String.valueOf(injectSocketTimeout));
    }

    // Set the session parameter properties
    if (paramProperties != null)
    {
      for (Map.Entry entry : paramProperties.entrySet())
      {
        properties.put(entry.getKey(), entry.getValue());
      }
    }
    return DriverManager.getConnection(params.get("uri"), properties);
  }

  /**
   * Close SQL Objects
   *
   * @param resultSet  a result set object
   * @param statement  a statement object
   * @param connection a connection
   * @throws SQLException raised if any error occurs
   */
  public void closeSQLObjects(ResultSet resultSet, Statement statement,
                              Connection connection) throws SQLException
  {
    if (resultSet != null)
    {
      resultSet.close();
    }
    if (statement != null)
    {
      statement.close();
    }
    if (connection != null)
    {
      connection.close();
    }
  }

  protected static String getSFProjectRoot() throws UnsupportedEncodingException
  {
    URL location =
        AbstractDriverIT.class.getProtectionDomain().getCodeSource().getLocation();
    String testDir = URLDecoder.decode(location.getPath(), "UTF-8");

    return testDir.substring(0, testDir.indexOf("Client"));
  }
}
