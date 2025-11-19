package net.snowflake.client.authentication;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Strings;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.stream.Stream;
import net.snowflake.client.TestUtil;
import net.snowflake.client.core.SecurityUtil;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class PrivateKeyAuthenticationExceptionHandlingTest extends BaseJDBCTest {

  static Stream<String> jwtTimeoutProvider() {
    return Stream.of("10", "100", null);
  }

  /**
   * Tests the authentication exception and retry JWT renew functionality when retrying login
   * requests. To run, update environment variables to use connect with JWT authentication.
   */
  @Disabled
  @ParameterizedTest
  @MethodSource("jwtTimeoutProvider")
  public void testPrivateKeyAuthTimeout(String jwtTimeout) {
    System.setProperty(SecurityUtil.ENABLE_BOUNCYCASTLE_PROVIDER_JVM, "true");
    if (jwtTimeout != null) {
      System.setProperty("JWT_AUTH_TIMEOUT", jwtTimeout);
    }

    Properties props = new Properties();
    props.put("user", "testUser");
    props.put("account", "testaccount");

    String privateKeyFile = TestUtil.systemGetEnv("SNOWFLAKE_TEST_PRIVATE_KEY_FILE");
    if (!Strings.isNullOrEmpty(privateKeyFile)) {
      String workspace = System.getenv("WORKSPACE");
      if (workspace != null) {
        props.put(
            "private_key_file", java.nio.file.Paths.get(workspace, privateKeyFile).toString());
      } else {
        props.put("private_key_file", privateKeyFile);
      }
      props.put("authenticator", "SNOWFLAKE_JWT");

      String privateKeyPwd = TestUtil.systemGetEnv("SNOWFLAKE_TEST_PRIVATE_KEY_PWD");
      if (!Strings.isNullOrEmpty(privateKeyPwd)) {
        props.put("private_key_pwd", privateKeyPwd);
      }
    } else {
      String password = TestUtil.systemGetEnv("SNOWFLAKE_TEST_PASSWORD");
      if (!Strings.isNullOrEmpty(password)) {
        props.put("password", password);
      } else {
        throw new IllegalStateException(
            "Neither SNOWFLAKE_TEST_PRIVATE_KEY_FILE nor SNOWFLAKE_TEST_PASSWORD environment variable is set. Please configure one of them for authentication.");
      }
    }
    try {
      Exception ex =
          assertThrows(
              SnowflakeSQLException.class,
              () ->
                  DriverManager.getConnection(
                      "jdbc:snowflake://localhost:8443/?db=TEST_DB&schema=PUBLIC", props));
      assertTrue(ex.getMessage().contains("JDBC driver encountered communication error"));
    } finally {
      System.setProperty(SecurityUtil.ENABLE_BOUNCYCASTLE_PROVIDER_JVM, "false");
      System.setProperty("JWT_AUTH_TIMEOUT", "0");
    }
  }
}
