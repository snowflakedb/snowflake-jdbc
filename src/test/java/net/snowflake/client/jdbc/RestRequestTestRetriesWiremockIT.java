/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class RestRequestTestRetriesWiremockIT extends BaseWiremockTest {

  private static final String SCENARIOS_BASE_DIR = "/wiremock/mappings";

  @BeforeEach
  public void setUp() throws IOException {
    resetWiremock();
  }

  @Test
  public void testRetryWhen503Code() {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/response503.json");
    Properties props = new Properties();
    props.setProperty("maxHttpRetries", "3");
    SnowflakeSQLException thrown =
        assertThrows(SnowflakeSQLException.class, () -> executeServerRequest(props));
    verifyCount(4, "/queries/v1/query-request.*");

    assertTrue(
        thrown
            .getMessage()
            .contains("JDBC driver encountered communication error. Message: HTTP status=503."));
  }

  @Test
  public void testHttpClientFailedAfterFiveRetries() {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/six_malformed_and_correct.json");
    Properties props = new Properties();
    props.setProperty("maxHttpRetries", "5");

    SnowflakeSQLException thrown =
        assertThrows(SnowflakeSQLException.class, () -> executeServerRequest(props));
    assertTrue(thrown.getMessage().contains("Bad chunk header"));
    verifyCount(6, "/queries/v1/query-request.*");
  }

  @Test
  public void testHttpClientSuccessAfterFiveRetries() {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/six_malformed_and_correct.json");
    try {
      Properties props = new Properties();
      props.setProperty("maxHttpRetries", "7");
      //      setScenarioState("malformed_response_retry", "MALFORMED_RETRY_2");
      executeServerRequest(props);
      verifyCount(7, "/queries/v1/query-request.*");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      System.clearProperty("maxHttpRetries");
    }
  }

  @Test
  public void testHttpClientSuccessWithoutRetries() {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/correct_response.json");
    try {
      executeServerRequest(null);
      verifyCount(1, "/queries/v1/query-request.*");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static void executeServerRequest(Properties properties) throws SQLException {
    Properties props = properties != null ? properties : new Properties();
    SnowflakeUtil.systemSetEnv("SNOWFLAKE_TEST_HOST", WIREMOCK_HOST);
    System.setProperty("JAVA_LOGGING_CONSOLE_STD_OUT", "true");
    SnowflakeUtil.systemSetEnv("SNOWFLAKE_TEST_PROTOCOL", "http");
    SnowflakeUtil.systemSetEnv("SNOWFLAKE_TEST_PORT", String.valueOf(wiremockHttpPort));
    Connection conn = BaseJDBCTest.getConnection(properties);
    Statement stmt = conn.createStatement();
    stmt.executeQuery("SELECT 1");
  }
}
