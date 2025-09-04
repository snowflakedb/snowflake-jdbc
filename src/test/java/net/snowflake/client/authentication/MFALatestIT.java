package net.snowflake.client.authentication;

import static net.snowflake.client.authentication.AuthConnectionParameters.getMfaConnectionParameters;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.AUTHENTICATION)
public class MFALatestIT {

  AuthTestHelper authTestHelper;

  @BeforeEach
  public void setUp() throws IOException {
    authTestHelper = new AuthTestHelper();
  }

  @Test
  void testMfaSuccessful() {
    Properties connectionParameters = getMfaConnectionParameters();
    connectionParameters.put("CLIENT_REQUEST_MFA_TOKEN", true);

    List<String> totpCodes = authTestHelper.getTotp();
    assertTrue(totpCodes.size() > 0, "Expected to get TOTP codes but got none");

    boolean connectionSuccess =
        authTestHelper.connectAndExecuteSimpleQueryWithMfaToken(connectionParameters, totpCodes);

    assertTrue(
        connectionSuccess, "Failed to connect with any of the " + totpCodes.size() + " TOTP codes");

    authTestHelper.verifyExceptionIsNotThrown();

    connectionParameters.remove("passcode");
    AuthTestHelper cacheTestHelper = new AuthTestHelper();
    cacheTestHelper.connectAndExecuteSimpleQuery(connectionParameters, null);

    cacheTestHelper.verifyExceptionIsNotThrown();
  }
}
