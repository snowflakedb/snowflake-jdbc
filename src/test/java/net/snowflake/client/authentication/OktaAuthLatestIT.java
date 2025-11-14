package net.snowflake.client.authentication;

import static net.snowflake.client.authentication.AuthConnectionParameters.SSO_USER;
import static net.snowflake.client.authentication.AuthConnectionParameters.getOktaConnectionParameters;

import java.io.IOException;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.AUTHENTICATION)
class OktaAuthLatestIT {

  AuthTestHelper authTestHelper;
  Properties properties;

  @BeforeEach
  public void setUp() throws IOException {
    authTestHelper = new AuthTestHelper();
    properties = getOktaConnectionParameters();
  }

  @Test
  void shouldAuthenticateUsingOkta() {
    authTestHelper.connectAndExecuteSimpleQuery(getOktaConnectionParameters(), null);
    authTestHelper.verifyExceptionIsNotThrown();
  }

  @Test
  void shouldAuthenticateUsingOktaWithOktaUsernameParam() {
    properties.replace("user", "differentUsername");
    authTestHelper.connectAndExecuteSimpleQuery(properties, "oktausername=" + SSO_USER);
    authTestHelper.verifyExceptionIsNotThrown();
  }

  @Test
  void shouldThrowErrorForWrongOktaCredentials() {
    properties.put("user", "invalidUsername");
    properties.put("password", "fakepassword");
    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsThrown(
        "JDBC driver encountered communication error. Message: HTTP status=401.");
  }

  @Test
  void shouldThrowErrorForWrongOktaCredentialsInOktaUsernameParam() {
    properties.replace("user", "differentUsername");
    authTestHelper.connectAndExecuteSimpleQuery(properties, "oktausername=invalidUser");
    authTestHelper.verifyExceptionIsThrown(
        "JDBC driver encountered communication error. Message: HTTP status=401.");
  }

  @Test
  void shouldThrowErrorForWrongOktaUrl() {
    properties.put("authenticator", "https://invalid.okta.com/");
    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsThrown(
        "The specified authenticator is not accepted by your Snowflake account configuration.  Please contact your local system administrator to get the correct URL to use.");
  }

  @Test
  @Disabled // todo SNOW-1852279 implement error handling for invalid URL
  void shouldThrowErrorForWrongUrlWithoutOktaPath() {
    properties.put("authenticator", "https://invalid.abc.com/");
    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsThrown("todo");
  }
}
