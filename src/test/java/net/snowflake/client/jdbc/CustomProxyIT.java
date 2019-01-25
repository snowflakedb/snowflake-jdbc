package net.snowflake.client.jdbc;

import org.junit.Ignore;
import org.junit.Test;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class CustomProxyIT
{
  @Test
  @Ignore
  public void testCorrectProxySettingFromConnectionString()
      throws ClassNotFoundException, SQLException
  {

    String connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL" +
            "&proxyHost=localhost&proxyPort=3128" +
            "&proxyUser=testuser1&proxyPassword=test" +
            "&nonProxyHosts=*.foo.com|localhost&useProxy=true";
    // should finish correctly
    runProxyConnection(connectionUrl);

    connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL" +
            "&proxyHost=localhost&proxyPort=3128" +
            "&proxyUser=testuser1&proxyPassword=test" +
            "&useProxy=true";
    // should finish correctly
    runProxyConnection(connectionUrl);
  }

  @Test
  @Ignore
  public void testWrongProxyPortSettingFromConnectionString()
      throws ClassNotFoundException, SQLException
  {

    String connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL" +
            "&proxyHost=localhost&proxyPort=31281" +
            "&proxyUser=testuser1&proxyPassword=test" +
            "&nonProxyHosts=*.foo.com|localhost&useProxy=true";
    // should show warning for null response for the requests
    runProxyConnection(connectionUrl);
  }

  @Test
  @Ignore
  public void testWrongProxyPasswordSettingFromConnectionString()
      throws ClassNotFoundException, SQLException
  {

    String connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL" +
            "&proxyHost=localhost&proxyPort=3128" +
            "&proxyUser=testuser2&proxyPassword=test111" +
            "&nonProxyHosts=*.foo.com|localhost&useProxy=true";
    // should show warning for null response for the requests
    try
    {
      runProxyConnection(connectionUrl);
    }
    catch (SQLException e)
    {
      assertThat("JDBC driver encountered communication error", e.getErrorCode(),
          equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));

    }
  }

  @Test
  @Ignore
  public void testInvalidProxyPortFromConnectionString()
      throws ClassNotFoundException, SQLException
  {

    String connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL" +
            "&proxyHost=localhost" +
            "&proxyUser=testuser1&proxyPassword=test" +
            "&nonProxyHosts=*.foo.com|localhost&useProxy=true";
    // should throw SnowflakeSQLException: 200051
    try
    {
      runProxyConnection(connectionUrl);
    }
    catch (SQLException e)
    {
      assertThat("invalid proxy error", e.getErrorCode(),
          equalTo(ErrorCode.INVALID_PROXY_PROPERTIES.getMessageCode()));

    }
  }

  @Test
  @Ignore
  public void testNonProxyHostsFromConnectionString()
      throws ClassNotFoundException, SQLException
  {

    String connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL" +
            "&proxyHost=localhost&proxyPort=31281" +
            "&proxyUser=testuser1&proxyPassword=test" +
            "&nonProxyHosts=*.snowflakecomputing.com|localhost&useProxy=true";
    // should finish correctly
    runProxyConnection(connectionUrl);
  }

  @Test
  @Ignore
  public void testWrongNonProxyHostsFromConnectionString()
      throws ClassNotFoundException, SQLException
  {

    String connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL" +
            "&proxyHost=localhost&proxyPort=31281" +
            "&proxyUser=testuser1&proxyPassword=test" +
            "&nonProxyHosts=*.foo.com|localhost&useProxy=true";
    // should fail to connect
    runProxyConnection(connectionUrl);
  }

  public void runProxyConnection(String connectionUrl)
      throws ClassNotFoundException, SQLException
  {
    Authenticator.setDefault(
        new Authenticator() {
          @Override
          public PasswordAuthentication getPasswordAuthentication() {
            System.out.println("RequestorType: " + getRequestorType());
            System.out.println("Protocol: " + getRequestingProtocol().toLowerCase());
            return new PasswordAuthentication(
                System.getProperty("http.proxyUser"),
                System.getProperty("http.proxyPassword").toCharArray());
          }
        }
    );

    // SET USER AND PASSWORD FIRST
    String user = "USER";
    String passwd = "PASSWORD";
    Properties _connectionProperties = new Properties();
    _connectionProperties.put("user", user);
    _connectionProperties.put("password", passwd);

    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
    long counter = 0;
    while(true) {
      Connection con = DriverManager.getConnection(connectionUrl, _connectionProperties);
      Statement stmt = con.createStatement();
      stmt.execute("use warehouse TINY_WAREHOUSE");
      String sql = "select $1 from values(1),(3),(5),(7)";
      ResultSet res = stmt.executeQuery(sql);
      while (res.next()) {
        System.out.println("value: " + res.getInt(1));
      }
      System.out.println("OK - " + counter);
      con.close();
      counter++;
      break;
    }
  }

}
