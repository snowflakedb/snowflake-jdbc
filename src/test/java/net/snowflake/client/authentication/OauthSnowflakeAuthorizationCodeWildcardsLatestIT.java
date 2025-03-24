package net.snowflake.client.authentication;

import static net.snowflake.client.authentication.AuthConnectionParameters.OAUTH_PASSWORD;
import static net.snowflake.client.authentication.AuthConnectionParameters.getOAuthSnowflakeWildcardsAuthorizationCodeConnectionParameters;

import java.io.IOException;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.AUTHENTICATION)
public class OauthSnowflakeAuthorizationCodeWildcardsLatestIT {

  Properties properties = getOAuthSnowflakeWildcardsAuthorizationCodeConnectionParameters();

  String login = properties.getProperty("user");
  String password = OAUTH_PASSWORD;

  AuthTestHelper authTestHelper = new AuthTestHelper();

  @BeforeEach
  public void setUp() throws IOException {
    authTestHelper.cleanBrowserProcesses();
    AuthTestHelper.deleteIdToken(AuthConnectionParameters.HOST, login);
    AuthTestHelper.deleteOauthToken(AuthConnectionParameters.HOST, login);
    AuthTestHelper.deleteOauthRefreshToken(AuthConnectionParameters.HOST, login);
    properties = getOAuthSnowflakeWildcardsAuthorizationCodeConnectionParameters();
  }

  @AfterEach
  public void cleanUp() {
    authTestHelper.cleanBrowserProcesses();
  }

  @AfterAll
  public static void tearDown() {
    Properties properties = getOAuthSnowflakeWildcardsAuthorizationCodeConnectionParameters();
    AuthTestHelper.deleteIdToken(AuthConnectionParameters.HOST, properties.getProperty("user"));
    AuthTestHelper.deleteOauthToken(AuthConnectionParameters.HOST, properties.getProperty("user"));
    AuthTestHelper.deleteOauthRefreshToken(
        AuthConnectionParameters.HOST, properties.getProperty("user"));
  }

  @Test
  void shouldAuthenticateUsingSnowflakeOauthAuthorizationCode() throws InterruptedException {
    Thread provideCredentialsThread =
        new Thread(
            () ->
                authTestHelper.provideCredentials(
                    "internalOauthSnowflakeSuccess", login, password));
    Thread connectThread =
        new Thread(() -> authTestHelper.connectAndExecuteSimpleQuery(properties, null));

    authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTestHelper.verifyExceptionIsNotThrown();
  }

  @Test
  void shouldThrowErrorForMismatchedOauthSnowflakeUsername() throws InterruptedException {
    properties.setProperty("user", "invalidUser@snowflake.com");
    Thread provideCredentialsThread =
        new Thread(
            () ->
                authTestHelper.provideCredentials(
                    "internalOauthSnowflakeSuccess", login, password));
    Thread connectThread =
        new Thread(() -> authTestHelper.connectAndExecuteSimpleQuery(properties, null));

    authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTestHelper.verifyExceptionIsThrown(
        "The user you were trying to authenticate as differs from the user tied to the access token.");
  }

  @Test
  void shouldThrowErrorForOauthSnowflakeTimeout() throws InterruptedException {
    properties.put("BROWSER_RESPONSE_TIMEOUT", "0");
    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsThrown(
        "Error during OAuth Authorization Code authentication: Authorization request timed out. "
            + "Snowflake driver did not receive authorization code back to the redirect URI. Verify your security integration and driver configuration.");
  }

  @Test
  void shouldAuthenticateUsingTokenCacheOauthSnowflake() throws InterruptedException {
    properties.put("CLIENT_STORE_TEMPORARY_CREDENTIAL", true);
    Thread provideCredentialsThread =
        new Thread(
            () ->
                authTestHelper.provideCredentials(
                    "internalOauthSnowflakeSuccess", login, password));
    Thread connectThread = authTestHelper.getConnectAndExecuteSimpleQueryThread(properties, null);

    authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTestHelper.verifyExceptionIsNotThrown();
    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsNotThrown();
  }
}
