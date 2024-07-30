package net.snowflake.client.jdbc;

import static junit.framework.TestCase.assertEquals;
import static net.snowflake.client.AbstractDriverIT.getConnectionParameters;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import net.snowflake.client.RunningNotOnGithubActionsMac;
import net.snowflake.client.RunningNotOnJava21;
import net.snowflake.client.RunningNotOnJava8;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryOthers.class)
public class ProxyLatestIT {

  private static final SFLogger logger = SFLoggerFactory.getLogger(ProxyLatestIT.class);
  private static final String WIREMOCK_HOME_DIR = ".wiremock";
  private static final String WIREMOCK_M2_PATH =
      "/.m2/repository/org/wiremock/wiremock-standalone/3.8.0/wiremock-standalone-3.8.0.jar";
  private static final String WIREMOCK_HOST = "localhost";
  private static final String TRUST_STORE_PROPERTY = "javax.net.ssl.trustStore";
  private static int httpProxyPort;
  private static int httpsProxyPort;
  private static String originalTrustStorePath;
  private static Process wiremockStandalone;

  @BeforeClass
  public static void setUpClass() {
    assumeFalse(RunningNotOnJava8.isRunningOnJava8());
    assumeFalse(RunningNotOnJava21.isRunningOnJava21());
    assumeFalse(
        RunningNotOnGithubActionsMac
            .isRunningOnGithubActionsMac()); // disabled until issue with access to localhost
    // (https://github.com/snowflakedb/snowflake-jdbc/pull/1807#discussion_r1686229430) is fixed on
    // github actions mac image. Ticket to enable when fixed: SNOW-1555950
    originalTrustStorePath = systemGetProperty(TRUST_STORE_PROPERTY);
    startWiremockStandAlone();
  }

  @After
  public void tearDown() {
    restoreTrustStorePathProperty();
    unsetJvmProperties();
    resetWiremock();
    HttpUtil.httpClient.clear();
  }

  @AfterClass
  public static void tearDownClass() {
    stopWiremockStandAlone();
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

  private static void startWiremockStandAlone() {
    // retrying in case of fail in port bindings
    await()
        .alias("wait for wiremock responding")
        .atMost(Duration.ofSeconds(10))
        .until(
            () -> {
              try {
                httpProxyPort = findFreePort();
                httpsProxyPort = findFreePort();
                wiremockStandalone =
                    new ProcessBuilder(
                            "java",
                            "-jar",
                            getWiremockStandAlonePath(),
                            "--root-dir",
                            System.getProperty("user.dir")
                                + File.separator
                                + WIREMOCK_HOME_DIR
                                + File.separator,
                            "--enable-browser-proxying", // work as forward proxy
                            "--proxy-pass-through",
                            "false", // pass through only matched requests
                            "--port",
                            String.valueOf(httpProxyPort),
                            "--https-port",
                            String.valueOf(httpsProxyPort),
                            "--https-keystore",
                            getResourceURL("wiremock" + File.separator + "ca-cert.jks"),
                            "--ca-keystore",
                            getResourceURL("wiremock" + File.separator + "ca-cert.jks"))
                        .inheritIO()
                        .start();
                waitForWiremock();
                return true;
              } catch (Exception e) {
                logger.warn("Failed to start wiremock, retrying: ", e);
                return false;
              }
            });
  }

  private void resetWiremock() {
    HttpPost postRequest;
    postRequest = new HttpPost("http://" + WIREMOCK_HOST + ":" + getAdminPort() + "/__admin/reset");
    try (CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = client.execute(postRequest)) {
      assertEquals(200, response.getStatusLine().getStatusCode());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getWiremockStandAlonePath() {
    return System.getProperty("user.home") + WIREMOCK_M2_PATH;
  }

  private static void waitForWiremock() {
    await()
        .pollDelay(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(3))
        .until(ProxyLatestIT::isWiremockResponding);
  }

  private static boolean isWiremockResponding() {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet request =
          new HttpGet(String.format("http://%s:%d/__admin/mappings", WIREMOCK_HOST, httpProxyPort));
      CloseableHttpResponse response = httpClient.execute(request);
      return response.getStatusLine().getStatusCode() == 200;
    } catch (Exception e) {
      logger.warn("Waiting for wiremock to respond: ", e);
    }
    return false;
  }

  private static void stopWiremockStandAlone() {
    if (wiremockStandalone != null) {
      wiremockStandalone.destroyForcibly();
      await()
          .alias("stop wiremock")
          .atMost(Duration.ofSeconds(10))
          .until(() -> !wiremockStandalone.isAlive());
    }
  }

  private static int findFreePort() {
    try {
      ServerSocket socket = new ServerSocket(0);
      int port = socket.getLocalPort();
      socket.close();
      return port;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Properties getProperties() {
    Map<String, String> params = getConnectionParameters();
    Properties props = new Properties();
    props.put("host", params.get("host"));
    props.put("port", params.get("port"));
    props.put("account", params.get("account"));
    props.put("user", params.get("user"));
    props.put("role", params.get("role"));
    props.put("password", params.get("password"));
    props.put("warehouse", params.get("warehouse"));
    props.put("db", params.get("database"));
    props.put("ssl", params.get("ssl"));
    props.put("insecureMode", true); // OCSP disabled for wiremock proxy tests
    return props;
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

  private String getProxyProtocol(Properties props) {
    return props.get("ssl").toString().equals("on") ? "https" : "http";
  }

  private int getProxyPort(String proxyProtocol) {
    if (Objects.equals(proxyProtocol, "http")) {
      return httpProxyPort;
    } else {
      return httpsProxyPort;
    }
  }

  private void verifyProxyWasUsed() {
    verifyRequestToProxy(".*login.*", 1);
    verifyRequestToProxy(".*query.*", 1);
  }

  private void verifyProxyNotUsed() {
    verifyRequestToProxy(".*", 0);
  }

  private void proxyAll(Properties props) {
    String body =
        String.format(
            "{\"request\": {\"method\": \"ANY\", \"urlPattern\": \".*\"},\"response\": {\"proxyBaseUrl\": \"%s\"}}",
            getSnowflakeUrl(props));
    HttpPost postRequest = createWiremockPostRequest(body, "/__admin/mappings");
    try (CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = client.execute(postRequest)) {
      assertEquals(201, response.getStatusLine().getStatusCode());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void connectAndVerifySimpleQuery(Properties props) throws SQLException {
    try (Connection con =
            DriverManager.getConnection(
                String.format("jdbc:snowflake://%s:%s", props.get("host"), props.get("port")),
                props);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
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
          "expected request count not matched for pattern: " + pathPattern,
          expectedCount,
          json.get("count").asInt());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private HttpPost createWiremockPostRequest(String body, String path) {
    HttpPost postRequest = new HttpPost("http://" + WIREMOCK_HOST + ":" + getAdminPort() + path);
    final StringEntity entity;
    try {
      entity = new StringEntity(body);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    postRequest.setEntity(entity);
    postRequest.setHeader("Accept", "application/json");
    postRequest.setHeader("Content-type", "application/json");
    return postRequest;
  }

  private static void restoreTrustStorePathProperty() {
    if (originalTrustStorePath != null) {
      System.setProperty(TRUST_STORE_PROPERTY, originalTrustStorePath);
    } else {
      System.clearProperty(TRUST_STORE_PROPERTY);
    }
  }

  private int getAdminPort() {
    return httpProxyPort;
  }

  private String getSnowflakeUrl(Properties props) {
    String protocol = getProxyProtocol(props);
    return String.format("%s://%s:%s", protocol, props.get("host"), props.get("port"));
  }

  private static String getResourceURL(String relativePath) {
    return Paths.get(systemGetProperty("user.dir"), "src", "test", "resources", relativePath)
        .toAbsolutePath()
        .toString();
  }

  private void setCustomTrustStorePropertyPath() {
    System.setProperty(
        TRUST_STORE_PROPERTY, getResourceURL("wiremock" + File.separator + "ca-cert.jks"));
  }
}
