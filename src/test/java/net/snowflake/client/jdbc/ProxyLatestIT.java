package net.snowflake.client.jdbc;

import static junit.framework.TestCase.assertEquals;
import static net.snowflake.client.AbstractDriverIT.getConnectionParameters;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import net.snowflake.client.category.TestCategoryOthers;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

@Category(TestCategoryOthers.class)
@RunWith(Parameterized.class)
public class ProxyLatestIT {

  private static final String TRUST_STORE_PROPERTY = "javax.net.ssl.trustStore";
  private String originalTrustStorePath;
  private final String proxyProtocol;

  public ProxyLatestIT(String proxyProtocol) {
    this.proxyProtocol = proxyProtocol;
  }

  @BeforeClass
  public static void setUpGlobal() {
    Testcontainers.exposeHostPorts(8082);
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    originalTrustStorePath = System.getProperty(TRUST_STORE_PROPERTY);
    System.setProperty(
        TRUST_STORE_PROPERTY,
        System.getProperty("user.dir") + "/src/test/resources/wiremock/truststore.jks");
    System.out.println(wiremockServer.getMappedPort(8080));
    System.out.println(wiremockServer.getMappedPort(8443));
  }

  @After
  public void tearDown() {
    restoreTrustStorePathProperty();
  }

  @Rule
  public GenericContainer wiremockServer =
      new GenericContainer<>("wiremock/wiremock:3.6.0")
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("wiremock/ca-cert.jks"),
              "/home/wiremock/ca-cert.jks")
          .withCommand(
              "--enable-browser-proxying "
                  + "--port 8080 --https-port 8443 "
                  + "--https-keystore /home/wiremock/ca-cert.jks "
                  + "--keystore-type JKS "
                  + "--ca-keystore /home/wiremock/ca-cert.jks "
                  + "--proxy-pass-through false")
          //          .withAccessToHost(true)
          //              .withExtraHost("host.docker.internal", "host-gateway")
          .withExposedPorts(8080, 8443);

  @Parameterized.Parameters(name = "protocol: {0}")
  public static Object[][] data() {
    return new Object[][] {{"http"}, {"https"}};
  }

  @Test
  public void testProxyIsUsedWhenSetInProperties() throws SQLException {
    Properties props = getProperties();
    addProxyProperties(props);
    proxyAll(props);

    connectAndVerifySimpleQuery(props);
    verifyProxyWasUsed();
  }

  @Test
  public void testProxyIsUsedWhenSetInJVMParams() throws SQLException {
    Properties props = getProperties();
    setJvmProperties(proxyProtocol);
    proxyAll(props);

    connectAndVerifySimpleQuery(props);
    verifyProxyWasUsed();
  }

  @Test
  public void testProxyNotUsedWhenNonProxyHostsMatchingInProperties() throws SQLException {
    restoreTrustStorePathProperty();
    Properties props = getProperties();
    addProxyProperties(props);
    props.put("nonProxyHosts", "*");
    proxyAll(props);

    connectAndVerifySimpleQuery(props);
    verifyProxyNotUsed();
  }

  @Test
  public void testProxyIsUsedWhenNonProxyHostsNotMatchingInProperties() throws SQLException {
    Properties props = getProperties();
    addProxyProperties(props);
    props.put("nonProxyHosts", "notMatchingHost");
    proxyAll(props);

    connectAndVerifySimpleQuery(props);
    verifyProxyWasUsed();
  }

  @Test
  public void testProxyNotUsedWhenNonProxyHostsMatchingInJVMParams() throws SQLException {
    restoreTrustStorePathProperty();
    Properties props = getProperties();
    setJvmProperties(proxyProtocol);
    System.setProperty("http.nonProxyHosts", "*");
    proxyAll(props);

    connectAndVerifySimpleQuery(props);
    verifyProxyNotUsed();
  }

  @Test
  public void testProxyUsedWhenNonProxyHostsNotMatchingInJVMParams() throws SQLException {
    Properties props = getProperties();
    setJvmProperties(proxyProtocol);
    System.setProperty("http.nonProxyHosts", "notMatchingHost");
    proxyAll(props);

    connectAndVerifySimpleQuery(props);
    verifyProxyWasUsed();
  }

  private void verifyProxyWasUsed() {
    verifyRequestToProxy(".*login.*", 1);
    verifyRequestToProxy(".*query.*", 1);
  }

  private void verifyProxyNotUsed() {
    verifyRequestToProxy(".*", 0);
  }

  private void restoreTrustStorePathProperty() {
    if (originalTrustStorePath != null) {
      System.setProperty(TRUST_STORE_PROPERTY, originalTrustStorePath);
    } else {
      System.clearProperty(TRUST_STORE_PROPERTY);
    }
  }

  private int getProxyPort(String proxyProtocol) {
    if (Objects.equals(proxyProtocol, "http")) {
      return wiremockServer.getMappedPort(8080);
    } else {
      return wiremockServer.getMappedPort(8443);
    }
  }

  private void addProxyProperties(Properties props) {
    props.put("useProxy", "true");
    props.put("proxyProtocol", proxyProtocol);
    props.put("proxyHost", wiremockServer.getHost());
    props.put("proxyPort", String.valueOf(getProxyPort(proxyProtocol)));
  }

  private void setJvmProperties(String proxyProtocol) {
    System.setProperty("http.useProxy", "true");
    System.setProperty("http.proxyProtocol", proxyProtocol);
    if (Objects.equals(proxyProtocol, "http")) {
      System.setProperty("http.proxyHost", wiremockServer.getHost());
      System.setProperty("http.proxyPort", String.valueOf(getProxyPort(proxyProtocol)));
    } else {
      System.setProperty("https.proxyHost", wiremockServer.getHost());
      System.setProperty("https.proxyPort", String.valueOf(getProxyPort(proxyProtocol)));
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

  private void verifyRequestToProxy(String pathPattern, int expectedCount) {
    String body = String.format("{ \"method\":\"POST\",\"urlPattern\": \".*%s.*\" }", pathPattern);
    HttpPost postRequest = createPostRequest(body, "/__admin/requests/count");
    try (CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = client.execute(postRequest)) {
      String responseString = EntityUtils.toString(response.getEntity());
      ObjectMapper mapper = new ObjectMapper();
      JsonNode json = mapper.readTree(responseString);
      assertEquals(expectedCount, json.get("count").asInt());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void proxyAll(Properties props) {
    String body =
        String.format(
            "{\"request\": {\"method\": \"ANY\", \"urlPattern\": \".*\"},\"response\": {\"proxyBaseUrl\": \"%s\"}}",
            getSnowflakeUrl(props));
    HttpPost postRequest = createPostRequest(body, "/__admin/mappings");
    try (CloseableHttpClient client = HttpClients.createDefault();
         CloseableHttpResponse response = client.execute(postRequest)) {
      assertEquals(201, response.getStatusLine().getStatusCode());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private HttpPost createPostRequest(String body, String path) {
    HttpPost postRequest =
        new HttpPost("http://" + wiremockServer.getHost() + ":" + getAdminPort() + path);
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

  private int getAdminPort() {
    return wiremockServer.getMappedPort(8080);
  }

  private String getSnowflakeUrl(Properties props) {
    String protocol = props.get("ssl").equals("on") ? "https" : "http";
    String host = props.get("ssl").equals("on") ? props.get("host").toString() : "host.testcontainers.internal";
    System.out.println(host);
    return String.format("%s://%s:%s", protocol, host, props.get("port"));
  }
}
