package net.snowflake.client.authentication;
import static net.snowflake.client.authentication.AuthConnectionParameters.*;
import static net.snowflake.client.authentication.AuthConnectionParameters.getOAuthSnowflakeAuthorizationCodeConnectionParameters;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.Properties;

import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.*;


@Tag(TestTags.TESTING)
public class OauthSnowflakeAuthorizationCodeLatestIT {

    Properties properties = getOAuthSnowflakeAuthorizationCodeConnectionParameters();

    String login = properties.getProperty("user");
    String password = OAUTH_PASSWORD;

    AuthTestHelper authTestHelper = new AuthTestHelper();

    @BeforeEach
    public void setUp() throws IOException {
        authTestHelper.cleanBrowserProcesses();
        AuthTestHelper.deleteIdToken(AuthConnectionParameters.HOST, login);
        AuthTestHelper.deleteOauthToken(AuthConnectionParameters.HOST, login);
        AuthTestHelper.deleteOauthRefreshToken(AuthConnectionParameters.HOST, login);
    }

    @AfterEach
    public void cleanUp() {
        authTestHelper.cleanBrowserProcesses();
    }

    @AfterAll
    public static void tearDown() {
        Properties properties = getOAuthSnowflakeAuthorizationCodeConnectionParameters();
        AuthTestHelper.deleteIdToken(AuthConnectionParameters.HOST, properties.getProperty("user"));
        AuthTestHelper.deleteOauthToken(AuthConnectionParameters.HOST, properties.getProperty("user"));
        AuthTestHelper.deleteOauthRefreshToken(AuthConnectionParameters.HOST, properties.getProperty("user"));
    }


    @Test
    @Order(1)

    void shouldAuthenticateUsingSnowflakeOauthAuthorizationCode() throws InterruptedException {
        Properties properties = getOAuthSnowflakeAuthorizationCodeConnectionParameters();
        Thread provideCredentialsThread =
                new Thread(() -> authTestHelper.provideCredentials("internalOauthSnowflakeSuccess", login, password));
        Thread connectThread =
                new Thread(() -> authTestHelper.connectAndExecuteSimpleQuery(properties, null));

        authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.verifyExceptionIsNotThrown();
    }


    @Test
    @Order(2)
    void shouldThrowErrorForMismatchedOauthSnowflakeUsername() throws InterruptedException {
        Properties properties = getOAuthSnowflakeAuthorizationCodeConnectionParameters();
        properties.setProperty("user", "invalidUser@snowflake.com");
        Thread provideCredentialsThread =
                new Thread(() -> authTestHelper.provideCredentials("internalOauthSnowflakeSuccess", login, password));
        Thread connectThread =
                new Thread(() -> authTestHelper.connectAndExecuteSimpleQuery(properties, null));

        authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.verifyExceptionIsThrown("The user you were trying to authenticate as differs from the user tied to the access token.");
    }

    @Test
    @Order(3)
    void shouldThrowErrorForOauthSnowflakeTimeout() throws InterruptedException {
        Properties properties = getOAuthSnowflakeAuthorizationCodeConnectionParameters();
        properties.put("BROWSER_RESPONSE_TIMEOUT", "0");
        authTestHelper.connectAndExecuteSimpleQuery(properties, null);
        authTestHelper.verifyExceptionIsThrown("Error during OAuth Authorization Code authentication: Authorization request timed out. " +
                "Snowflake driver did not receive authorization code back to the redirect URI. Verify your security integration and driver configuration.");
    }

    @Test
    @Order(4)
    void shouldAuthenticateUsingTokenCacheOauthSnowflake() throws InterruptedException {
        Properties properties = getOAuthSnowflakeAuthorizationCodeConnectionParameters();
        properties.put("CLIENT_STORE_TEMPORARY_CREDENTIAL", true);
        Thread provideCredentialsThread =
                new Thread(() -> authTestHelper.provideCredentials("internalOauthSnowflakeSuccess", login, password));
        Thread connectThread = authTestHelper.getConnectAndExecuteSimpleQueryThread(properties, null);

        authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.verifyExceptionIsNotThrown();
        authTestHelper.connectAndExecuteSimpleQuery(properties, null);
        authTestHelper.verifyExceptionIsNotThrown();
    }

    @Test
    void shouldNotSaveSnowflakeAuthorizationAccessToken() throws InterruptedException {

        Properties properties = getOAuthSnowflakeAuthorizationCodeConnectionParameters();
        properties.put("CLIENT_STORE_TEMPORARY_CREDENTIAL", false);

        String accessToken = authTestHelper.getAccessToken();
        System.out.println(accessToken);  //!!!!!!

        assertNull(accessToken, "Access token should be empty");

        Thread provideCredentialsThread =
                new Thread(() -> authTestHelper.provideCredentials("externalOauthOktaSuccess", login, password));
        Thread connectThread = authTestHelper.getConnectAndExecuteSimpleQueryThread(properties, null);

        authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.verifyExceptionIsNotThrown();

        accessToken = authTestHelper.getAccessToken();
        System.out.println(accessToken);  //!!!!!!

        assertNull(accessToken, "Access token should be empty");
    }
}
