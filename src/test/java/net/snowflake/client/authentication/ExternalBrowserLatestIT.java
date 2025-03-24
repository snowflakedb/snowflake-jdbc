package net.snowflake.client.authentication;

import static net.snowflake.client.authentication.AuthConnectionParameters.getExternalBrowserConnectionParameters;

import java.io.IOException;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.AUTHENTICATION)
class ExternalBrowserLatestIT {

  String login = AuthConnectionParameters.SSO_USER;
  String password = AuthConnectionParameters.SSO_PASSWORD;
  AuthTestHelper authTestHelper = new AuthTestHelper();
  Properties properties;

  @BeforeEach
  public void setUp() throws IOException {
    AuthTestHelper.deleteIdToken();
    properties = getExternalBrowserConnectionParameters();
  }

  @AfterEach
  public void tearDown() {
    authTestHelper.cleanBrowserProcesses();
    AuthTestHelper.deleteIdToken();
  }

  @Test
  void shouldAuthenticateUsingExternalBrowser() throws InterruptedException {
    Thread provideCredentialsThread =
        new Thread(() -> authTestHelper.provideCredentials("success", login, password));
    Thread connectThread = authTestHelper.getConnectAndExecuteSimpleQueryThread(properties);

    authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTestHelper.verifyExceptionIsNotThrown();
  }

  @Test
  void shouldThrowErrorForMismatchedUsername() throws InterruptedException {
    properties.put("user", "differentUsername");
    Thread provideCredentialsThread =
        new Thread(() -> authTestHelper.provideCredentials("success", login, password));
    Thread connectThread = authTestHelper.getConnectAndExecuteSimpleQueryThread(properties);

    authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTestHelper.verifyExceptionIsThrown(
        "The user you were trying to authenticate as differs from the user currently logged in at the IDP.");
  }

  @Test
  void shouldThrowErrorForWrongCredentials() throws InterruptedException {
    String login = "itsnotanaccount.com";
    String password = "fakepassword";
    Thread provideCredentialsThread =
        new Thread(() -> authTestHelper.provideCredentials("fail", login, password));
    Thread connectThread =
        authTestHelper.getConnectAndExecuteSimpleQueryThread(
            properties, "BROWSER_RESPONSE_TIMEOUT=10");

    authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTestHelper.verifyExceptionIsThrown(
        "JDBC driver encountered communication error. Message: External browser authentication failed within timeout of 10000 milliseconds.");
  }

  @Test
  void shouldThrowErrorForBrowserTimeout() throws InterruptedException {
    Thread provideCredentialsThread =
        new Thread(() -> authTestHelper.provideCredentials("timeout", login, password));
    Thread connectThread =
        authTestHelper.getConnectAndExecuteSimpleQueryThread(
            properties, "BROWSER_RESPONSE_TIMEOUT=1");

    authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTestHelper.verifyExceptionIsThrown(
        "JDBC driver encountered communication error. Message: External browser authentication failed within timeout of 1000 milliseconds.");
  }
}
