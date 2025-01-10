package net.snowflake.client.authentication;
import static net.snowflake.client.authentication.AuthConnectionParameters.*;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import java.io.IOException;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.*;

@Tag(TestTags.AUTHENTICATION)
public class OauthSnowflakeClientCredentialsLatestIT {
    Properties properties = getOAuthSnowflakeAuthorizationCodeConnectionParameters();
    String login = properties.getProperty("user");
    AuthTestHelper authTestHelper = new AuthTestHelper();

    @BeforeEach
    public void setUp() throws IOException {
        AuthTestHelper.deleteIdToken(AuthConnectionParameters.HOST, login);
        AuthTestHelper.deleteOauthToken(OKTA, login);
    }

    @AfterAll
    public static void tearDown() {
        Properties properties = getOAuthSnowflakeAuthorizationCodeConnectionParameters();
        AuthTestHelper.deleteIdToken(AuthConnectionParameters.HOST, properties.getProperty("user"));
        AuthTestHelper.deleteOauthToken(OKTA, properties.getProperty("user"));
    }

    @Test
    void shouldAuthenticateUsingSnowflakeOauthClientCredentials() {
        Properties properties = getOAuthSnowflakeClientCredentialParameters();
        authTestHelper.connectAndExecuteSimpleQuery(properties, null);
        authTestHelper.verifyExceptionIsNotThrown();
    }

    @Test
    void shouldThrowErrorForClientCredentialsMismatchedUsername() throws InterruptedException {
        Properties properties = getOAuthSnowflakeClientCredentialParameters();
        properties.put("user", "invalidUser@snowflake.com");

        authTestHelper.connectAndExecuteSimpleQuery(properties, null);
        authTestHelper.verifyExceptionIsThrown("The user you were trying to authenticate as differs from the user tied to the access token.");
    }

    @Test
    void shouldThrowErrorForUnauthorizedClientCredentials() throws InterruptedException, SnowflakeSQLException {
        Properties properties = getOAuthSnowflakeClientCredentialParameters();
        properties.put("clientId", "invalidClientId");

        authTestHelper.connectAndExecuteSimpleQuery(properties, null);
        authTestHelper.verifyExceptionIsThrown("Error during OAuth Client Credentials authentication: JDBC driver encountered communication error. Message: HTTP status=401.");
    }
    @Test
    void shouldSaveClientCredentialAccessToken() throws InterruptedException {
        Properties properties = getOAuthSnowflakeClientCredentialParameters();
        properties.put("CLIENT_STORE_TEMPORARY_CREDENTIAL", false);

        String accessToken = authTestHelper.getAccessToken();
        assertNull(accessToken, "Access token should be empty");

        authTestHelper.connectAndExecuteSimpleQuery(properties, null);
        authTestHelper.verifyExceptionIsNotThrown();

        accessToken = authTestHelper.getAccessToken();
        assertNotNull(accessToken, "Access token should not be empty");

    }
}
