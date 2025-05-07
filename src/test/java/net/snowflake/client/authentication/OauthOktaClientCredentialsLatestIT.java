package net.snowflake.client.authentication;

import static net.snowflake.client.authentication.AuthConnectionParameters.OKTA;
import static net.snowflake.client.authentication.AuthConnectionParameters.getOAuthOktaClientCredentialParameters;

import java.io.IOException;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.AUTHENTICATION)
public class OauthOktaClientCredentialsLatestIT {
  Properties properties = getOAuthOktaClientCredentialParameters();
  String login = properties.getProperty("user");
  AuthTestHelper authTestHelper = new AuthTestHelper();

  @BeforeEach
  public void setUp() throws IOException {
    AuthTestHelper.deleteIdToken(AuthConnectionParameters.HOST, login);
    AuthTestHelper.deleteOauthToken(OKTA, login);
    properties = getOAuthOktaClientCredentialParameters();
  }

  @AfterAll
  public static void tearDown() {
    Properties properties = getOAuthOktaClientCredentialParameters();
    AuthTestHelper.deleteIdToken(AuthConnectionParameters.HOST, properties.getProperty("user"));
    AuthTestHelper.deleteOauthToken(OKTA, properties.getProperty("user"));
  }

  @Test
  void shouldAuthenticateUsingSnowflakeOauthClientCredentials() {
    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsNotThrown();
  }

  @Test
  void shouldThrowErrorForClientCredentialsMismatchedUsername() throws InterruptedException {
    properties.put("user", "invalidUser@snowflake.com");

    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsThrown(
        "The user you were trying to authenticate as differs from the user tied to the access token.");
  }

  @Test
  void shouldThrowErrorForUnauthorizedClientCredentials()
      throws InterruptedException, SnowflakeSQLException {
    properties.put("oauthClientId", "invalidClientId");

    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsThrown(
        "Error during OAuth Client Credentials authentication: JDBC driver encountered communication error. Message: HTTP status=401.");
  }
}
