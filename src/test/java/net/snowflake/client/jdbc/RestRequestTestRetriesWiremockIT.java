/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static net.snowflake.client.core.HttpUtil.JDBC_MALFORMED_RESPONSE_MAX_RETRY_COUNT_PROPERTY;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
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
  public void testHttpClientFailedAfterDefaultThreeRetries() throws IOException {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/six_malformed_and_correct.json");
    //    WireMock.setScenarioState("malformed_response_retry", "MALFORMED_RETRY_3");
    SnowflakeSQLException thrown =
        assertThrows(SnowflakeSQLException.class, () -> executeServerRequest());
    assertTrue(thrown.getMessage().contains("Bad chunk header"));
    WireMock.verify(4, postRequestedFor(urlMatching("/queries/v1/query-request.*")));
  }

  @Test
  public void testHttpClientFailedAfterFiveRetries() throws IOException {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/six_malformed_and_correct.json");
    System.setProperty(JDBC_MALFORMED_RESPONSE_MAX_RETRY_COUNT_PROPERTY, "5");
    SnowflakeSQLException thrown =
        assertThrows(SnowflakeSQLException.class, () -> executeServerRequest());
    assertTrue(thrown.getMessage().contains("Bad chunk header"));
    WireMock.verify(6, postRequestedFor(urlMatching("/queries/v1/query-request.*")));
  }

  @Test
  public void testHttpClientSuccessAfterFiveRetries() throws IOException {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/six_malformed_and_correct.json");
    try {
      System.setProperty(JDBC_MALFORMED_RESPONSE_MAX_RETRY_COUNT_PROPERTY, "5");
      WireMock.setScenarioState("malformed_response_retry", "MALFORMED_RETRY_2");
      executeServerRequest();
      WireMock.verify(6, postRequestedFor(urlMatching("/queries/v1/query-request.*")));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      System.clearProperty(JDBC_MALFORMED_RESPONSE_MAX_RETRY_COUNT_PROPERTY);
    }
  }

  @Test
  public void testHttpClientSuccessWithoutRetries() throws IOException {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/six_malformed_and_correct.json");
    try {
      WireMock.setScenarioState("malformed_response_retry", "CORRECT");
      executeServerRequest();
      WireMock.verify(1, postRequestedFor(urlMatching("/queries/v1/query-request.*")));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static void executeServerRequest() throws SQLException {
    Properties properties = new Properties();
    SnowflakeUtil.systemSetEnv("SNOWFLAKE_TEST_HOST", WIREMOCK_HOST);
    SnowflakeUtil.systemSetEnv("SNOWFLAKE_TEST_PORT", String.valueOf(wiremockHttpPort));
    Connection conn = BaseJDBCTest.getConnection(properties);
    Statement stmt = conn.createStatement();
    stmt.executeQuery("SELECT 1");
  }
}
