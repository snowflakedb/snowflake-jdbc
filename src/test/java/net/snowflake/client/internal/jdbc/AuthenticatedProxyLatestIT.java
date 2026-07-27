package net.snowflake.client.internal.jdbc;

import static net.snowflake.client.AbstractDriverIT.connectAndVerifySimpleQuery;
import static net.snowflake.client.AbstractDriverIT.getFullPathFileInResource;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import net.snowflake.client.annotations.RunOnGCP;
import net.snowflake.client.category.TestTags;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests verifying that Snowflake JDBC operations work through an authenticated proxy.
 *
 * <p>Uses WireMock in forward proxy mode with {@code --proxy-pass-through true} so that all proxied
 * requests (both Snowflake API and cloud storage) are forwarded to their original destinations. The
 * driver is configured with {@code proxyUser}/{@code proxyPassword} to exercise the authenticated
 * proxy code path.
 */
@Tag(TestTags.OTHERS)
@RunOnGCP
public class AuthenticatedProxyLatestIT extends BaseWiremockTest {

  private static final String PROXY_USER = "testUser";
  private static final String PROXY_PASSWORD = "testPassword";
  private static final String TEST_DATA_FILE = "orders_100.csv";

  @AfterEach
  public void tearDown() {
    super.tearDown();
  }

  private void addAuthenticatedProxyProperties(Properties props) {
    String proxyProtocol = getProxyProtocol(props);
    props.put("useProxy", "true");
    props.put("proxyProtocol", proxyProtocol);
    props.put("proxyHost", WIREMOCK_HOST);
    props.put("proxyPort", String.valueOf(getProxyPort(proxyProtocol)));
    props.put("proxyUser", PROXY_USER);
    props.put("proxyPassword", PROXY_PASSWORD);
  }

  /**
   * Sets up WireMock to reject all requests with 407 Proxy Authentication Required. Used for
   * negative tests where we expect the connection to fail.
   */
  private void rejectAllWith407() {
    String mapping =
        "{\n"
            + "  \"request\": {\n"
            + "    \"method\": \"ANY\",\n"
            + "    \"urlPattern\": \".*\"\n"
            + "  },\n"
            + "  \"response\": {\n"
            + "    \"status\": 407,\n"
            + "    \"headers\": {\n"
            + "      \"Proxy-Authenticate\": \"Basic realm=\\\"WireMock Proxy\\\"\"\n"
            + "    }\n"
            + "  }\n"
            + "}";
    addMapping(mapping);
  }

  private void verifyRequestToProxy(String pathPattern, int minExpectedCount) {
    String body = String.format("{ \"method\":\"POST\",\"urlPattern\": \".*%s.*\" }", pathPattern);
    HttpPost postRequest = createWiremockPostRequest(body, "/__admin/requests/count");
    try (CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = client.execute(postRequest)) {
      String responseString = EntityUtils.toString(response.getEntity());
      ObjectMapper mapper = new ObjectMapper();
      JsonNode json = mapper.readTree(responseString);
      int actualCount = json.get("count").asInt();
      assertTrue(
          actualCount >= minExpectedCount,
          String.format(
              "expected at least %d requests for pattern '%s', but found %d",
              minExpectedCount, pathPattern, actualCount));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void verifyProxyWasUsed() {
    verifyRequestToProxy(".*login.*", 1);
    verifyRequestToProxy(".*query.*", 1);
  }

  @Override
  protected Properties getProperties() {
    Properties props = super.getProperties();
    // Disable telemetry to avoid race conditions between tests
    props.put("CLIENT_TELEMETRY_ENABLED", "false");
    return props;
  }

  @Test
  public void testSimpleQueryThroughAuthenticatedProxy() throws SQLException {
    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    addAuthenticatedProxyProperties(props);

    connectAndVerifySimpleQuery(props);
    verifyProxyWasUsed();
  }

  @Test
  public void testCreateTemporaryStageAndPutThroughAuthenticatedProxy() throws SQLException {
    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    addAuthenticatedProxyProperties(props);

    String jdbcUrl = String.format("jdbc:snowflake://%s:%s", props.get("host"), props.get("port"));
    try (Connection con = DriverManager.getConnection(jdbcUrl, props);
        Statement stmt = con.createStatement()) {
      stmt.execute("CREATE TEMPORARY STAGE testAuthProxy_stage");

      String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);
      assertTrue(
          stmt.execute("PUT file://" + sourceFilePath + " @testAuthProxy_stage"),
          "Failed to put a file via authenticated proxy");

      try (ResultSet rs = stmt.executeQuery("LS @testAuthProxy_stage")) {
        assertTrue(rs.next(), "Stage should contain the uploaded file");
      }
    }

    verifyProxyWasUsed();
  }

  @Test
  public void testConnectionFailsWhenProxyReturns407() {
    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    String proxyProtocol = getProxyProtocol(props);
    props.put("useProxy", "true");
    props.put("proxyProtocol", proxyProtocol);
    props.put("proxyHost", WIREMOCK_HOST);
    props.put("proxyPort", String.valueOf(getProxyPort(proxyProtocol)));
    // No proxyUser/proxyPassword — all requests will be rejected with 407

    rejectAllWith407();

    assertThrows(SQLException.class, () -> connectAndVerifySimpleQuery(props));
  }
}
