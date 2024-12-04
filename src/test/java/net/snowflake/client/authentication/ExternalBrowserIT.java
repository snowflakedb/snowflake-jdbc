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
class ExternalBrowserIT {

  String login = AuthConnectionParameters.SSO_USER;
  String password = AuthConnectionParameters.SSO_PASSWORD;
  AuthTest authTest;

  @BeforeEach
  public void setUp() throws IOException {
    authTest = new AuthTest();
    AuthTest.deleteIdToken();
  }

  @AfterEach
  public void tearDown() {
    authTest.cleanBrowserProcesses();
    AuthTest.deleteIdToken();
  }

  @Test
  void shouldAuthenticateUsingExternalBrowser() throws InterruptedException {
    Thread provideCredentialsThread =
        new Thread(() -> authTest.provideCredentials("success", login, password));
    Thread connectThread =
        authTest.getConnectAndExecuteSimpleQueryThread(getExternalBrowserConnectionParameters());

    authTest.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTest.verifyExceptionIsNotThrown();
  }

  @Test
  void shouldThrowErrorForMismatchedUsername() throws InterruptedException {
    Properties properties = getExternalBrowserConnectionParameters();
    properties.put("user", "differentUsername");
    Thread provideCredentialsThread =
        new Thread(() -> authTest.provideCredentials("success", login, password));
    Thread connectThread = authTest.getConnectAndExecuteSimpleQueryThread(properties);

    authTest.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTest.verifyExceptionIsThrown(
        "The user you were trying to authenticate as differs from the user currently logged in at the IDP.");
  }

  @Test
  void shouldThrowErrorForWrongCredentials() throws InterruptedException {
    String login = "itsnotanaccount.com";
    String password = "fakepassword";
    Thread provideCredentialsThread =
        new Thread(() -> authTest.provideCredentials("fail", login, password));
    Thread connectThread =
        authTest.getConnectAndExecuteSimpleQueryThread(
            getExternalBrowserConnectionParameters(), "BROWSER_RESPONSE_TIMEOUT=10");

    authTest.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTest.verifyExceptionIsThrown(
        "JDBC driver encountered communication error. Message: External browser authentication failed within timeout of 10000 milliseconds.");
  }

  @Test
  void shouldThrowErrorForBrowserTimeout() throws InterruptedException {
    Thread provideCredentialsThread =
        new Thread(() -> authTest.provideCredentials("timeout", login, password));
    Thread connectThread =
        authTest.getConnectAndExecuteSimpleQueryThread(
            getExternalBrowserConnectionParameters(), "BROWSER_RESPONSE_TIMEOUT=1");

    authTest.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTest.verifyExceptionIsThrown(
        "JDBC driver encountered communication error. Message: External browser authentication failed within timeout of 1000 milliseconds.");
  }
}
