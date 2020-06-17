package net.snowflake.client.jdbc;

import net.snowflake.client.core.SessionUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Connection Manual Tests
 * <p>
 * This test can run from the command line.
 * Intentionally Junit/Hamcrest matcher is not used to simplify the
 * commandline option.
 * <p>
 * - Prerequisite:
 * 1. Create a shell script.
 * <p>
 * #!/bin/bash -e
 * #
 * # Run ConnectionManaulTest using IntelliJ remote debug
 * #
 * export SNOWFLAKE_TEST_ACCOUNT=ACCOUNT
 * export SNOWFLAKE_TEST_SSO_USER=USER
 * export SNOWFLAKE_TEST_PORT=PORT
 * export SNOWFLAKE_TEST_SSL=SSL
 * export SNOWFLAKE_TEST_DATABASE=DATABASE
 * export SNOWFLAKE_TEST_SCHEMA=SCHEMA
 * export SNOWFLAKE_TEST_SCHEMA2=SCHEMA2
 * export SNOWFLAKE_TEST_ADMIN_ACCOUNT=ADMIN_ACCOUNT
 * export SNOWFLAKE_TEST_ADMIN_USER=ADMIN_USER
 * export SNOWFLAKE_TEST_ADMIN_PASSWORD=ADMIN_PASSWORD
 * <p>
 * # This delete step doesn't need to be done in the future.
 * # Test code will take care of it.
 * # echo "[INFO] Deleting id token cache"
 * # rm -f ~/.cache/snowflake/temporary_credential.json
 * <p>
 * java -ea \
 * -agentlib:jdwp=transport=dt_socket,server=n,address=localhost:5005,suspend=y \
 * -cp lib/snowflake-jdbc-3.6.6.jar:target/test-classes \
 * net.snowflake.client.jdbc.ConnectionManual
 * <p>
 * 2. Open Run/Debug Configuration and create a Remote configuration
 * as Listen mode using 5005 and named it "ConnectionManual".
 * <p>
 * - Debugging
 * <p>
 * 1. Set breakpoints on IntelliJ.
 * 2. Run mvn install to build a new JDBC jar, which is required to locate
 * Snowflake JDBC driver from the driver manager.
 * 3. Start the "ConnectionManual" remote
 * 4. Run the script.
 * ./run.sh
 * 5. Enjoy debugging!
 */
public class ConnectionManual
{
  public static void main(String args[]) throws Throwable
  {
    ConnectionManual test = new ConnectionManual();
    test.setTokenValidityForTest();
    try
    {
      test.testSSO();
    }
    finally
    {
      test.resetTokenValidity();
    }

  }

  private Properties getProperties()
  {
    String account = System.getenv("SNOWFLAKE_TEST_ACCOUNT");
    String ssoUser = System.getenv("SNOWFLAKE_TEST_SSO_USER");
    String ssl = System.getenv("SNOWFLAKE_TEST_SSL");

    Properties properties = new Properties();
    properties.put("user", ssoUser);
    properties.put("account", account);
    properties.put("ssl", ssl);
    properties.put("tracing", "FINEST");
    properties.put("authenticator", "externalbrowser");
    properties.put("CLIENT_STORE_TEMPORARY_CREDENTIAL", true);
    return properties;
  }

  private String getUrl()
  {
    String account = System.getenv("SNOWFLAKE_TEST_ACCOUNT");
    String port = System.getenv("SNOWFLAKE_TEST_PORT");
    return String.format(
        "jdbc:snowflake://%s.reg.snowflakecomputing.com:%s", account, port);
  }

  private Connection getAdminConnection() throws Throwable
  {
    String adminAccount = System.getenv("SNOWFLAKE_TEST_ADMIN_ACCOUNT");
    if (adminAccount == null)
    {
      adminAccount = "snowflake";
    }
    String adminUser = System.getenv("SNOWFLAKE_TEST_ADMIN_USER");
    String adminPassword = System.getenv("SNOWFLAKE_TEST_ADMIN_PASSWORD");
    String port = System.getenv("SNOWFLAKE_TEST_PORT");
    String ssl = System.getenv("SNOWFLAKE_TEST_SSL");
    if (ssl == null)
    {
      ssl = "on";
    }

    Properties properties = new Properties();
    properties.put("user", adminUser);
    properties.put("password", adminPassword);
    properties.put("account", adminAccount);
    properties.put("ssl", ssl);

    // connect url
    String url = String.format(
        "jdbc:snowflake://%s.reg.snowflakecomputing.com:%s", adminAccount, port);
    return DriverManager.getConnection(url, properties);
  }

  private void setTokenValidityForTest() throws Throwable
  {
    Connection con = getAdminConnection();
    con.createStatement().execute(
        "alter system set " +
        "MASTER_TOKEN_VALIDITY=60, " +
        "SESSION_TOKEN_VALIDITY=30, " +
        "ID_TOKEN_VALIDITY=60");

    // ALLOW_UNPROTECTED_ID_TOKEN will be deprecated in the future
    //con.createStatement().execute("alter account testaccount set " +
    //                                "ALLOW_UNPROTECTED_ID_TOKEN=true;");
    con.createStatement().execute("alter account testaccount set ID_TOKEN_FEATURE_ENABLED=true;");
    con.createStatement().execute("alter account testaccount set ALLOW_ID_TOKEN=true;");
  }

  private void resetTokenValidity() throws Throwable
  {
    Connection con = getAdminConnection();
    con.createStatement().execute(
        "alter system set " +
        "MASTER_TOKEN_VALIDITY=default, " +
        "SESSION_TOKEN_VALIDITY=default, " +
        "ID_TOKEN_VALIDITY=default");
  }

  private void testSSO() throws Throwable
  {
    String database = System.getenv("SNOWFLAKE_TEST_DATABASE");
    String schema1 = System.getenv("SNOWFLAKE_TEST_SCHEMA");
    String schema2 = System.getenv("SNOWFLAKE_TEST_SCHEMA2");

    Properties properties = getProperties();
    String url = getUrl();

    SessionUtil.deleteIdTokenCache(String.format("%s.reg.snowflakecomputing.com", properties.getProperty("account")), properties.getProperty("user"));

    System.out.println(
        "[INFO] 1st connection gets id token and stores in the cache file. " +
        "This popup a browser to SSO login");
    Connection con1 = DriverManager.getConnection(url, properties);
    assert database.equalsIgnoreCase(con1.getCatalog());
    assert schema1.equalsIgnoreCase(con1.getSchema());
    con1.close();

    System.out.println(
        "[INFO] 2nd connection reads the cache file and uses the id token. " +
        "This should not popups a browser.");
    properties.setProperty("database", database);
    properties.setProperty("schema", schema1);
    Connection con2 = DriverManager.getConnection(url, properties);
    assert database.equalsIgnoreCase(con2.getCatalog());
    assert schema1.equalsIgnoreCase(con2.getSchema());

    System.out.println("[INFO] Running a statement... 10 seconds");
    con2.createStatement().execute(
        "select seq8() from table(generator(timelimit=>10))");
    assert database.equalsIgnoreCase(con2.getCatalog());
    assert schema1.equalsIgnoreCase(con2.getSchema());

    System.out.println("[INFO] Running a statement... 1 second");
    con2.createStatement().execute(
        "select seq8() from table(generator(timelimit=>1))");
    assert database.equalsIgnoreCase(con2.getCatalog());
    assert schema1.equalsIgnoreCase(con2.getSchema());

    System.out.println("[INFO] Running a statement... 90 seconds");
    con2.createStatement().execute(
        "select seq8() from table(generator(timelimit=>90))");
    assert database.equalsIgnoreCase(con2.getCatalog());
    assert schema1.equalsIgnoreCase(con2.getSchema());
    con2.close();

    System.out.println(
        "[INFO] 3rd connection reads the cache file and uses the id token. " +
        "This should work even after closing the previous connections. " +
        "A specified schema should be set in the connection object.");
    properties.setProperty("schema", schema2);
    Connection con3 = DriverManager.getConnection(url, properties);
    assert database.equalsIgnoreCase(con3.getCatalog());
    assert schema2.equalsIgnoreCase(con3.getSchema());
    con3.close();

    System.out.println(
        "[INFO] 4th connection reads the cache file and tries to use the id token. " +
        "However we manually banned IdToken by set some parameters. " +
        "So a web browser pop up is expected to show up here.");
    Connection conAdmin = getAdminConnection();
    // ALLOW_UNPROTECTED_ID_TOKEN will be deprecated in the future
    //conAdmin.createStatement().execute("alter account testaccount set " +
    //                                "ALLOW_UNPROTECTED_ID_TOKEN=false;");
    conAdmin.createStatement().execute("alter account testaccount set ID_TOKEN_FEATURE_ENABLED=false;");
    conAdmin.createStatement().execute("alter account testaccount set ALLOW_ID_TOKEN=false;");
    Connection con4 = DriverManager.getConnection(url, properties);

    System.out.println("Finished. You might need to close login page in web browser to exit this test.");
  }
}
