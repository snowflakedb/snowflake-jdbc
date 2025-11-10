package net.snowflake.client.authentication;

import static net.snowflake.client.TestUtil.systemGetEnv;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.util.Properties;
import java.util.stream.Stream;
import net.snowflake.client.core.SecurityUtil;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class PrivateKeyAuthenticationExceptionHandlingTest extends BaseJDBCTest {

  static Stream<String> jwtTimeoutProvider() {
    return Stream.of("10", "100", null);
  }

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
    props.put("private_key_file", systemGetEnv("SNOWFLAKE_TEST_PRIVATE_KEY_FILE"));
    props.put("private_key_file_pwd", systemGetEnv("SNOWFLAKE_TEST_PRIVATE_KEY_PWD"));
    props.put("authenticator", "SNOWFLAKE_JWT");

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
