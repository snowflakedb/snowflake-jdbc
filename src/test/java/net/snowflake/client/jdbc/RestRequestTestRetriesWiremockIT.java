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
import net.snowflake.client.util.Stopwatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class RestRequestTestRetriesWiremockIT extends BaseWiremockTest {

  private static final String SCENARIOS_BASE_DIR = "/wiremock/mappings/restrequest";

  @BeforeEach
  public void setUp() throws IOException {
    resetWiremock();
  }

  @Test
  public void testRetryWhen503Code() {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/response503.json");
    Properties props = getWiremockProps();
    props.setProperty("maxHttpRetries", "3");
    SnowflakeSQLException thrown =
        assertThrows(SnowflakeSQLException.class, () -> executeServerRequest(props));
    verifyRequestCount(4, "/queries/v1/query-request.*");

    assertTrue(
        thrown
            .getMessage()
            .contains("JDBC driver encountered communication error. Message: HTTP status=503."));
  }

  private static Properties getWiremockProps() {
    Properties props = new Properties();
    props.put("protocol", "http://");
    props.put("host", WIREMOCK_HOST);
    props.put("port", String.valueOf(wiremockHttpPort));
    return props;
  }

  @Test
  public void testHttpClientFailedAfterFiveRetries() {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/six_malformed_and_correct.json");
    Properties props = getWiremockProps();
    props.setProperty("maxHttpRetries", "5");

    SnowflakeSQLException thrown =
        assertThrows(SnowflakeSQLException.class, () -> executeServerRequest(props));
    assertTrue(thrown.getMessage().contains("Bad chunk header"));
    verifyRequestCount(6, "/queries/v1/query-request.*");
  }

  @Test
  public void testHttpClientSuccessAfterFiveRetries() {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/six_malformed_and_correct.json");
    try {
      Properties props = getWiremockProps();
      props.setProperty("maxHttpRetries", "7");
      executeServerRequest(props);
      verifyRequestCount(7, "/queries/v1/query-request.*");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      System.clearProperty("maxHttpRetries");
    }
  }

  @Test
  public void testHttpClientSuccessAfter307Retry() {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/http_307_retry.json");
    try {
      Properties props = getWiremockProps();
      props.setProperty("injectSocketTimeout", "200");
      executeServerRequest(props);
      verifyRequestCount(2, "/queries/v1/query-request.*");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      System.clearProperty("injectSocketTimeout");
    }
  }

  @Test
  public void testHttpClientSuccessAfter308Retry() {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/http_308_retry.json");
    try {
      Properties props = getWiremockProps();
      props.setProperty("injectSocketTimeout", "200");
      executeServerRequest(props);
      verifyRequestCount(2, "/queries/v1/query-request.*");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      System.clearProperty("injectSocketTimeout");
    }
  }

  @Test
  public void testHttpClientSuccessWithoutRetries() {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/correct_response.json");
    try {
      executeServerRequest(getWiremockProps());
      verifyRequestCount(1, "/queries/v1/query-request.*");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testElapsedTimeoutExceeded() {
    importMappingFromResources(SCENARIOS_BASE_DIR + "/six_malformed_and_correct.json");
    Properties props = getWiremockProps();
    props.setProperty("networkTimeout", "1000");
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    assertThrows(SnowflakeSQLException.class, () -> executeServerRequest(props));
    stopwatch.stop();
    assertTrue(stopwatch.elapsedMillis() > 1000);
    assertTrue(stopwatch.elapsedMillis() < 3000);
  }

  private static void executeServerRequest(Properties properties) throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(properties);
    Statement stmt = conn.createStatement();
    stmt.executeQuery("SELECT 1");
  }
}
