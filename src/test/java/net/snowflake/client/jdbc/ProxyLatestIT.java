package net.snowflake.client.jdbc;

import static net.snowflake.client.AbstractDriverIT.connectAndVerifySimpleQuery;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.OTHERS)
public class ProxyLatestIT extends BaseWiremockTest {

  @AfterEach
  public void tearDown() {
    super.tearDown();
    unsetJvmProperties();
  }

  private String getProxyProtocol(Properties props) {
    return props.get("ssl").toString().equals("on") ? "https" : "http";
  }

  private int getProxyPort(String proxyProtocol) {
    if (Objects.equals(proxyProtocol, "http")) {
      return wiremockHttpPort;
    } else {
      return wiremockHttpsPort;
    }
  }

  private void addProxyProperties(Properties props) {
    String proxyProtocol = getProxyProtocol(props);
    props.put("useProxy", "true");
    props.put("proxyProtocol", proxyProtocol);
    props.put("proxyHost", WIREMOCK_HOST);
    props.put("proxyPort", String.valueOf(getProxyPort(proxyProtocol)));
  }

  private void setJvmProperties(Properties props) {
    String proxyProtocol = getProxyProtocol(props);
    System.setProperty("http.useProxy", "true");
    System.setProperty("http.proxyProtocol", proxyProtocol);
    if (Objects.equals(proxyProtocol, "http")) {
      System.setProperty("http.proxyHost", WIREMOCK_HOST);
      System.setProperty("http.proxyPort", String.valueOf(getProxyPort(proxyProtocol)));
    } else {
      System.setProperty("https.proxyHost", WIREMOCK_HOST);
      System.setProperty("https.proxyPort", String.valueOf(getProxyPort(proxyProtocol)));
    }
  }

  private void unsetJvmProperties() {
    System.clearProperty("http.useProxy");
    System.clearProperty("http.proxyProtocol");
    System.clearProperty("http.proxyHost");
    System.clearProperty("http.proxyPort");
    System.clearProperty("https.proxyHost");
    System.clearProperty("https.proxyPort");
  }

  private String getSnowflakeUrl(Properties props) {
    String protocol = getProxyProtocol(props);
    return String.format("%s://%s:%s", protocol, props.get("host"), props.get("port"));
  }

  private void proxyAll(Properties props) {
    String template =
        "{\n"
            + "  \"request\": {\n"
            + "    \"method\": \"ANY\",\n"
            + "    \"urlPattern\": \".*\"\n"
            + "  }\n,"
            + "  \"response\": { \"proxyBaseUrl\": \"%s\" }\n"
            + "}";
    String body = String.format(template, getSnowflakeUrl(props));
    addMapping(body);
  }

  private void verifyProxyWasUsed() {
    verifyRequestToProxy(".*login.*", 1);
    verifyRequestToProxy(".*query.*", 1);
  }

  private void verifyProxyNotUsed() {
    verifyRequestToProxy(".*", 0);
  }

  private void verifyRequestToProxy(String pathPattern, int expectedCount) {
    String body = String.format("{ \"method\":\"POST\",\"urlPattern\": \".*%s.*\" }", pathPattern);
    HttpPost postRequest = createWiremockPostRequest(body, "/__admin/requests/count");
    try (CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = client.execute(postRequest)) {
      String responseString = EntityUtils.toString(response.getEntity());
      ObjectMapper mapper = new ObjectMapper();
      JsonNode json = mapper.readTree(responseString);
      assertEquals(
          expectedCount,
          json.get("count").asInt(),
          "expected request count not matched for pattern: " + pathPattern);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testProxyIsUsedWhenSetInProperties() throws SQLException {
    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    addProxyProperties(props);
    proxyAll(props);

    connectAndVerifySimpleQuery(props);
    verifyProxyWasUsed();
  }

  @Test
  public void testProxyIsUsedWhenSetInJVMParams() throws SQLException {
    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    setJvmProperties(props);
    proxyAll(props);

    connectAndVerifySimpleQuery(props);
    verifyProxyWasUsed();
  }

  @Test
  public void testProxyNotUsedWhenNonProxyHostsMatchingInProperties() throws SQLException {
    Properties props = getProperties();
    addProxyProperties(props);
    props.put("nonProxyHosts", "*");
    proxyAll(props);

    connectAndVerifySimpleQuery(props);
    verifyProxyNotUsed();
  }

  @Test
  public void testProxyIsUsedWhenNonProxyHostsNotMatchingInProperties() throws SQLException {
    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    addProxyProperties(props);
    props.put("nonProxyHosts", "notMatchingHost");
    proxyAll(props);

    connectAndVerifySimpleQuery(props);
    verifyProxyWasUsed();
  }

  @Test
  public void testProxyNotUsedWhenNonProxyHostsMatchingInJVMParams() throws SQLException {
    Properties props = getProperties();
    setJvmProperties(props);
    System.setProperty("http.nonProxyHosts", "*");
    proxyAll(props);

    connectAndVerifySimpleQuery(props);
    verifyProxyNotUsed();
  }

  @Test
  public void testProxyUsedWhenNonProxyHostsNotMatchingInJVMParams() throws SQLException {
    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    setJvmProperties(props);
    System.setProperty("http.nonProxyHosts", "notMatchingHost");
    proxyAll(props);

    connectAndVerifySimpleQuery(props);
    verifyProxyWasUsed();
  }

  @Override
  protected Properties getProperties() {
    Properties props = super.getProperties();
    // disable telemetry because it introduces race condition which leads to sending requests
    // outside of current test proxy settings
    // Scenario:
    // testProxyIsUsed*** -> success -> telemetry is scheduled for sending
    // tearDown() resets properties and wiremock
    // telemetry is sent using HttpClient with configured proxy from the previous test
    // testProxyIsNotUsed*** -> fails because telemetry request is counted
    props.put("CLIENT_TELEMETRY_ENABLED", "false");
    return props;
  }
}
