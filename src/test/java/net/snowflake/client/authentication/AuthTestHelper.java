package net.snowflake.client.authentication;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.core.SessionUtil;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;

public class AuthTestHelper {

  private Exception exception;
  private String idToken;
  private final boolean runAuthTestsManually;

  public AuthTestHelper() {
    this.runAuthTestsManually = Boolean.parseBoolean(System.getenv("RUN_AUTH_TESTS_MANUALLY"));
  }

  public Thread getConnectAndExecuteSimpleQueryThread(Properties props, String sessionParameters) {
    return new Thread(() -> connectAndExecuteSimpleQuery(props, sessionParameters));
  }

  public Thread getConnectAndExecuteSimpleQueryThread(Properties props) {
    return new Thread(() -> connectAndExecuteSimpleQuery(props, null));
  }

  public void verifyExceptionIsThrown(String message) {
    assertThat("Expected exception not thrown", this.exception.getMessage(), is(message));
  }

  public void verifyExceptionIsNotThrown() {
    assertThat("Unexpected exception thrown", this.exception, nullValue());
  }

  public void connectAndProvideCredentials(Thread provideCredentialsThread, Thread connectThread)
      throws InterruptedException {
    if (runAuthTestsManually) {
      connectThread.start();
      connectThread.join();
    } else {
      provideCredentialsThread.start();
      connectThread.start();
      provideCredentialsThread.join();
      connectThread.join();
    }
  }

  public void provideCredentials(String scenario, String login, String password) {
    try {
      String provideBrowserCredentialsPath = "/externalbrowser/provideBrowserCredentials.js";
      ProcessBuilder processBuilder =
          new ProcessBuilder("node", provideBrowserCredentialsPath, scenario, login, password);
      Process process = processBuilder.start();
      process.waitFor(15, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void cleanBrowserProcesses() {
    if (!runAuthTestsManually) {
      String cleanBrowserProcessesPath = "/externalbrowser/cleanBrowserProcesses.js";
      ProcessBuilder processBuilder = new ProcessBuilder("node", cleanBrowserProcessesPath);
      try {
        Process process = processBuilder.start();
        process.waitFor(15, TimeUnit.SECONDS);
      } catch (InterruptedException | IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void deleteIdToken() {
    SessionUtil.deleteIdTokenCache(
        AuthConnectionParameters.HOST, AuthConnectionParameters.SSO_USER);
  }

  public void connectAndExecuteSimpleQuery(Properties props, String sessionParameters) {
    String url = String.format("jdbc:snowflake://%s:%s", props.get("host"), props.get("port"));
    if (sessionParameters != null) {
      url += "?" + sessionParameters;
    }
    try (Connection con = DriverManager.getConnection(url, props);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      saveToken(con);
    } catch (SQLException e) {
      this.exception = e;
    }
  }

  private void saveToken(Connection con) throws SnowflakeSQLException {
    SnowflakeConnectionV1 sfcon = (SnowflakeConnectionV1) con;
    this.idToken = sfcon.getSfSession().getIdToken();
  }

  public String getIdToken() {
    return idToken;
  }
}
