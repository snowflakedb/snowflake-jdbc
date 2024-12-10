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

  AuthTest authTest;

  @BeforeEach
  public void setUp() throws IOException {
    authTest = new AuthTest();
  }

  @Test
  void shouldAuthenticateUsingOkta() {
    authTest.connectAndExecuteSimpleQuery(getOktaConnectionParameters(), null);
    authTest.verifyExceptionIsNotThrown();
  }

  @Test
  void shouldAuthenticateUsingOktaWithOktaUsernameParam() {
    Properties properties = getOktaConnectionParameters();
    properties.replace("user", "differentUsername");
    authTest.connectAndExecuteSimpleQuery(properties, "oktausername=" + SSO_USER);
    authTest.verifyExceptionIsNotThrown();
  }

  @Test
  void shouldThrowErrorForWrongOktaCredentials() {
    Properties properties = getOktaConnectionParameters();
    properties.put("user", "invalidUsername");
    properties.put("password", "fakepassword");
    authTest.connectAndExecuteSimpleQuery(properties, null);
    authTest.verifyExceptionIsThrown(
        "JDBC driver encountered communication error. Message: HTTP status=401.");
  }

  @Test
  void shouldThrowErrorForWrongOktaCredentialsInOktaUsernameParam() {
    Properties properties = getOktaConnectionParameters();
    properties.replace("user", "differentUsername");
    authTest.connectAndExecuteSimpleQuery(properties, "oktausername=invalidUser");
    authTest.verifyExceptionIsThrown(
        "JDBC driver encountered communication error. Message: HTTP status=401.");
  }

  @Test
  void shouldThrowErrorForWrongOktaUrl() {
    Properties properties = getOktaConnectionParameters();
    properties.put("authenticator", "https://invalid.okta.com/");
    authTest.connectAndExecuteSimpleQuery(properties, null);
    authTest.verifyExceptionIsThrown(
        "The specified authenticator is not accepted by your Snowflake account configuration.  Please contact your local system administrator to get the correct URL to use.");
  }

  @Test
  @Disabled // todo SNOW-1852279 implement error handling for invalid URL
  void shouldThrowErrorForWrongUrlWithoutOktaPath() {
    Properties properties = getOktaConnectionParameters();
    properties.put("authenticator", "https://invalid.abc.com/");
    authTest.connectAndExecuteSimpleQuery(properties, null);
    authTest.verifyExceptionIsThrown("todo");
  }
}
