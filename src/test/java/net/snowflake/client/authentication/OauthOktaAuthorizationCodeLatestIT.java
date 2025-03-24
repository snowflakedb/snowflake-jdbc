package net.snowflake.client.authentication;

import static net.snowflake.client.authentication.AuthConnectionParameters.getOAuthExternalAuthorizationCodeConnectionParameters;

import java.io.IOException;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.AUTHENTICATION)
public class OauthOktaAuthorizationCodeLatestIT {
  String login = AuthConnectionParameters.SSO_USER;
  String password = AuthConnectionParameters.SSO_PASSWORD;
  AuthTestHelper authTestHelper = new AuthTestHelper();
  Properties properties;

  @BeforeEach
  public void setUp() throws IOException {
    AuthTestHelper.deleteIdToken();
    AuthTestHelper.deleteOauthToken();
    properties = getOAuthExternalAuthorizationCodeConnectionParameters();
  }

  @AfterEach
  public void cleanUp() {
    authTestHelper.cleanBrowserProcesses();
  }

  @AfterAll
  public static void tearDown() {
    AuthTestHelper.deleteIdToken();
    AuthTestHelper.deleteOauthToken();
  }

  @Test
  void shouldAuthenticateUsingExternalOauthOktaAuthorizationCode() throws InterruptedException {
    Thread provideCredentialsThread =
        new Thread(
            () -> authTestHelper.provideCredentials("externalOauthOktaSuccess", login, password));
    Thread connectThread =
        new Thread(() -> authTestHelper.connectAndExecuteSimpleQuery(properties, null));

    authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTestHelper.verifyExceptionIsNotThrown();
  }

  @Test
  void shouldThrowErrorForMismatchedOauthOktaUsername() throws InterruptedException {
    properties.setProperty("user", "invalidUser@snowflake.com");
    Thread provideCredentialsThread =
        new Thread(
            () -> authTestHelper.provideCredentials("externalOauthOktaSuccess", login, password));
    Thread connectThread = authTestHelper.getConnectAndExecuteSimpleQueryThread(properties, null);

    authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTestHelper.verifyExceptionIsThrown(
        "The user you were trying to authenticate as differs from the user tied to the access token.");
  }

  @Test
  void shouldThrowErrorForOauthOktaTimeout() throws InterruptedException {
    properties.put("BROWSER_RESPONSE_TIMEOUT", "0");
    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsThrown(
        "Error during OAuth Authorization Code authentication: Authorization request timed out. "
            + "Snowflake driver did not receive authorization code back to the redirect URI. Verify your security integration and driver configuration.");
  }

  @Test
  void shouldAuthenticateUsingTokenCacheForOauthOkta() throws InterruptedException {
    properties.put("CLIENT_STORE_TEMPORARY_CREDENTIAL", true);

    Thread provideCredentialsThread =
        new Thread(
            () -> authTestHelper.provideCredentials("externalOauthOktaSuccess", login, password));
    Thread connectThread = authTestHelper.getConnectAndExecuteSimpleQueryThread(properties, null);

    authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTestHelper.verifyExceptionIsNotThrown();
    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsNotThrown();
  }
}
