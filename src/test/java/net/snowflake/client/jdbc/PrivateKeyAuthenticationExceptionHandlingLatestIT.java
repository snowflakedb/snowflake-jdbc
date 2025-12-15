package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Stream;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SecurityUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag(TestTags.CONNECTION)
public class PrivateKeyAuthenticationExceptionHandlingLatestIT {

  static Stream<String> jwtTimeoutProvider() {
    return Stream.of("10", "100", null);
  }

  static Stream<String> timeOutSettings() {
    return Stream.of("HTTP_CLIENT_CONNECTION_TIMEOUT", "HTTP_CLIENT_SOCKET_TIMEOUT");
  }

  @AfterEach
  void cleanup() {
    HttpUtil.reset();
  }

  /**
   * Tests the authentication exception and retry JWT renew functionality when retrying login
   * requests
   */
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

    String privateKeyFile = AbstractDriverIT.getFullPathFileInResource("rsa_key.p8");
    props.put("private_key_file", privateKeyFile);
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

  /**
   * Test that Connection timeout and socket timout are applied to the httpclient.
   *
   * @throws SQLException if any SQL error occurs
   */
  @ParameterizedTest
  @MethodSource("timeOutSettings")
  public void testConnectionAndSocketTimeout(String timeoutType) throws SQLException {
    Properties props = new Properties();
    props.put("user", "testUser");
    props.put("account", "testaccount");
    props.put("password", "testPasswordt");
    props.put(timeoutType, "10");
    long startLoginTime = System.currentTimeMillis();
    SQLException e =
        assertThrows(
            SQLException.class,
            () -> {
              DriverManager.getConnection(
                  "jdbc:snowflake://fakeaccount.snowflakecomputing.com", props);
            });
    assertThat(e.getErrorCode(), is(ErrorCode.NETWORK_ERROR.getMessageCode()));
    long endLoginTime = System.currentTimeMillis();
    // Time lapsed should be less than default socket timeout.
    assertTrue(endLoginTime - startLoginTime < 30000);
  }
}
