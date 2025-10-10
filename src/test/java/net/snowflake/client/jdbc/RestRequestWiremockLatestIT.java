package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.ExecTimeTelemetryData;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.OTHERS)
public class RestRequestWiremockLatestIT extends BaseWiremockTest {

  String connectionResetByPeerScenario =
      "{\n"
          + "    \"mappings\": [\n"
          + "        {\n"
          + "            \"scenarioName\": \"Connection reset by peer\",\n"
          + "            \"requiredScenarioState\": \"Started\",\n"
          + "            \"newScenarioState\": \"Connection is stable\",\n"
          + "            \"request\": {\n"
          + "                \"method\": \"GET\",\n"
          + "                \"url\": \"/endpoint\"\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"fault\": \"CONNECTION_RESET_BY_PEER\"\n"
          + "            }\n"
          + "        },\n"
          + "        {\n"
          + "            \"scenarioName\": \"Connection reset by peer\",\n"
          + "            \"requiredScenarioState\": \"Connection is stable\",\n"
          + "            \"request\": {\n"
          + "                \"method\": \"GET\",\n"
          + "                \"url\": \"/endpoint\"\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"status\": 200\n"
          + "            }\n"
          + "        }\n"
          + "    ],\n"
          + "    \"importOptions\": {\n"
          + "        \"duplicatePolicy\": \"IGNORE\",\n"
          + "        \"deleteAllNotInImport\": true\n"
          + "    }"
          + "}";

  @Test
  public void testProxyIsUsedWhenSetInProperties() throws Exception {
    importMapping(connectionResetByPeerScenario);
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create().disableAutomaticRetries();
    try (CloseableHttpClient httpClient = httpClientBuilder.build()) {
      HttpGet request =
          new HttpGet(String.format("http://%s:%d/endpoint", WIREMOCK_HOST, wiremockHttpPort));
      RestRequest.executeWithRetries(
          httpClient,
          request,
          0,
          0,
          0,
          0,
          0,
          new AtomicBoolean(false),
          false,
          false,
          false,
          false,
          false,
          new ExecTimeTelemetryData(),
          null,
          null,
          null,
          false);

      CloseableHttpResponse response = httpClient.execute(request);
      assert (response.getStatusLine().getStatusCode() == 200);
    }
  }

  @Test
  public void testStickyHeaderPropagatedFromLogin() throws Exception {
    importMappingFromResources("/wiremock/mappings/restrequest/sticky_header_from_login.json");
    Properties props = getWiremockProps();

    try {
      String connectStr = String.format("jdbc:snowflake://%s:%s", WIREMOCK_HOST, wiremockHttpPort);
      Connection conn = DriverManager.getConnection(connectStr, props);
      Statement stmt = conn.createStatement();
      // Execute first query - should have sticky header from login
      stmt.executeQuery("SELECT 1");
      // Execute second query - should also have sticky header from login
      stmt.executeQuery("SELECT 1");

      verifyRequestCount(1, "/session/v1/login-request.*");
      verifyRequestCount(2, "/queries/v1/query-request.*");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testStickyHeaderPropagatedFromFirstQuery() throws Exception {
    importMappingFromResources(
        "/wiremock/mappings/restrequest/sticky_header_from_first_query.json");
    Properties props = getWiremockProps();

    try {
      String connectStr = String.format("jdbc:snowflake://%s:%s", WIREMOCK_HOST, wiremockHttpPort);
      Connection conn = DriverManager.getConnection(connectStr, props);
      Statement stmt = conn.createStatement();
      // Execute first query - should get the sticky header from response
      stmt.executeQuery("SELECT 1");
      // Execute second query - should send the sticky header in request
      stmt.executeQuery("SELECT 1");

      // Verify login request was made
      verifyRequestCount(1, "/session/v1/login-request.*");
      // Verify two query requests were made, second one with the sticky header
      verifyRequestCount(2, "/queries/v1/query-request.*");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testStickyHeaderValueUpdated() throws Exception {
    importMappingFromResources("/wiremock/mappings/restrequest/sticky_header_updated.json");
    Properties props = getWiremockProps();

    try {
      String connectStr = String.format("jdbc:snowflake://%s:%s", WIREMOCK_HOST, wiremockHttpPort);
      Connection conn = DriverManager.getConnection(connectStr, props);
      Statement stmt = conn.createStatement();
      // Execute first query - should send session-ABC from login
      stmt.executeQuery("SELECT 1");
      // Execute second query - should send session-ABC, receive session-XYZ
      stmt.executeQuery("SELECT 1");
      // Execute third query - should send updated session-XYZ
      stmt.executeQuery("SELECT 1");

      verifyRequestCount(1, "/session/v1/login-request.*");
      verifyRequestCount(3, "/queries/v1/query-request.*");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static Properties getWiremockProps() {
    Properties props = new Properties();
    props.put("account", "testaccount");
    props.put("user", "testuser");
    props.put("password", "testpassword");
    props.put("warehouse", "testwh");
    props.put("database", "testdb");
    props.put("schema", "testschema");
    props.put("ssl", "off");
    props.put("insecureMode", "true");
    return props;
  }

  private static void executeServerRequest(Properties properties) throws SQLException {
    String connectStr = String.format("jdbc:snowflake://%s:%s", WIREMOCK_HOST, wiremockHttpPort);
    Connection conn = DriverManager.getConnection(connectStr, properties);
    Statement stmt = conn.createStatement();
    stmt.executeQuery("SELECT 1");
  }
}
