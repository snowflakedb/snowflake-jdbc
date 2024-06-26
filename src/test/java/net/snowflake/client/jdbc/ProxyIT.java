package net.snowflake.client.jdbc;

import static junit.framework.TestCase.assertEquals;
import static net.snowflake.client.AbstractDriverIT.getConnectionParameters;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.cert.CertificateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.client.core.QueryStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(TestCategoryOthers.class)
@RunWith(Parameterized.class)
public class ProxyIT {

  private static final String TRUST_STORE_PROPERTY = "javax.net.ssl.trustStore";
  private static String originalTrustStorePath;
  private final String proxyProtocol;

  private static Process wiremockStandalone;

  public ProxyIT(String proxyProtocol) {
    this.proxyProtocol = proxyProtocol;
  }

  @BeforeClass
  public static void setupGlobal()
      throws IOException {
    wiremockStandalone =
            Runtime.getRuntime()
                    .exec(
                            String.format(
                                    "~/Library/Java/JavaVirtualMachines/corretto-11.0.23/Contents/Home/bin/java -Djavax.net.ssl.trustStore=%s -jar %s --enable-browser-proxying --port 8080 --https-port 8443 --https-keystore %s --ca-keystore %s",
                                    System.getProperty("user.dir") + "/src/test/resources/wiremock/truststore.jks",
                                    getResourceURL("wiremock/wiremock-standalone-3.7.0.jar"),
                                    System.getProperty("user.dir") + "/src/test/resources/wiremock/ca-cert.jks",
                                    System.getProperty("user.dir") + "/src/test/resources/wiremock/ca-cert.jks"
                            ));
    waitForWiremock();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    originalTrustStorePath = System.getProperty(TRUST_STORE_PROPERTY);
    System.out.println(originalTrustStorePath);
    System.setProperty(
        TRUST_STORE_PROPERTY,
        System.getProperty("user.dir") + "/src/test/resources/wiremock/truststore.jks");
  }

  @After
  public void tearDown() {
    resetWiremock();
  }

  @AfterClass
  public static void tearDownGlobal() {
    restoreTrustStorePathProperty();
    wiremockStandalone.destroy();
  }

  @Parameterized.Parameters(name = "protocol: {0}")
  public static Object[][] data() {
    return new Object[][] {{"http"}, {"https"}};
  }

  @Test
  public void testProxyIsUsedWhenSetInProperties() throws SQLException {
    Properties props = getProperties();
    addProxyProperties(props);
    proxyAll(props);

    System.out.println(props.get("ssl"));
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

  private static void restoreTrustStorePathProperty() {
      System.clearProperty(TRUST_STORE_PROPERTY);
  }

  private int getProxyPort(String proxyProtocol) {
    if (Objects.equals(proxyProtocol, "http")) {
      return 8080;
    } else {
      return 8443;
    }
  }

  private void addProxyProperties(Properties props) {
    props.put("useProxy", "true");
    props.put("proxyProtocol", proxyProtocol);
    props.put("proxyHost", getWiremockHost());
    props.put("proxyPort", String.valueOf(getProxyPort(proxyProtocol)));
  }

  private void setJvmProperties(String proxyProtocol) {
    System.setProperty("http.useProxy", "true");
    System.setProperty("http.proxyProtocol", proxyProtocol);
    if (Objects.equals(proxyProtocol, "http")) {
      System.setProperty("http.proxyHost", getWiremockHost());
      System.setProperty("http.proxyPort", String.valueOf(getProxyPort(proxyProtocol)));
    } else {
      System.setProperty("https.proxyHost", getWiremockHost());
      System.setProperty("https.proxyPort", String.valueOf(getProxyPort(proxyProtocol)));
    }
  }

  private void connectAndVerifySimpleQuery(Properties props) throws SQLException {
    System.out.println(String.format("jdbc:snowflake://%s:%s", props.get("host"), props.get("port")));
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
    System.out.println("getSnowflakeUrl: " + getSnowflakeUrl(props));
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
    HttpPost postRequest;
    postRequest = new HttpPost("http://" + getWiremockHost() + ":" + getAdminPort() + path);
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

  private void resetWiremock() {
    HttpPost postRequest;
    postRequest = new HttpPost("http://" + getWiremockHost() + ":" + getAdminPort() + "/__admin/reset");
    try (CloseableHttpClient client = HttpClients.createDefault();
         CloseableHttpResponse response = client.execute(postRequest)) {
      assertEquals(200, response.getStatusLine().getStatusCode());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private int getAdminPort() {
    return 8080;
  }

  private static String getWiremockHost() {
    return "localhost";
  }

  private String getSnowflakeUrl(Properties props) {
    String protocol = props.get("ssl").equals("on") ? "https" : "http";
    return String.format("%s://%s:%s", protocol, props.get("host"), props.get("port"));
  }

  private static String getResourceURL(String relativePath) {
    return ProxyIT.class
            .getClassLoader()
            .getResource(relativePath).getPath();
  }

  private static void waitForWiremock() {
    await()
            .atMost(Duration.ofSeconds(5))
            .until(
                    () -> {
                        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                            HttpGet request = new HttpGet(String.format("http://%s:8080/__admin/mappings", getWiremockHost()));
                            CloseableHttpResponse response = httpClient.execute(request);
                          return response.getStatusLine().getStatusCode() == 200;
                        } catch (Exception e) {}
                        return false;
                    });
  }

  public static KeyStore loadDefaultTrustStore() throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException {
    Path location = null;
    String type = null;
    String password = null;

    String locationProperty = System.getProperty("javax.net.ssl.trustStore");
    if ((null != locationProperty) && (locationProperty.length() > 0)) {
      Path p = Paths.get(locationProperty);
      File f = p.toFile();
      if (f.exists() && f.isFile() && f.canRead()) {
        location = p;
      }
    } else {
      String javaHome = System.getProperty("java.home");
      location = Paths.get(javaHome, "lib", "security", "jssecacerts");
      if (!location.toFile().exists()) {
        location = Paths.get(javaHome, "lib", "security", "cacerts");
      }
    }

    String passwordProperty = System.getProperty("javax.net.ssl.trustStorePassword");
    if ((null != passwordProperty) && (passwordProperty.length() > 0)) {
      password = passwordProperty;
    } else {
      password = "changeit";
    }

    String typeProperty = System.getProperty("javax.net.ssl.trustStoreType");
    if ((null != typeProperty) && (typeProperty.length() > 0)) {
      type = passwordProperty;
    } else {
      type = KeyStore.getDefaultType();
    }

    KeyStore trustStore = null;
    try {
      trustStore = KeyStore.getInstance(type, Security.getProvider("SUN"));
    } catch (KeyStoreException e) {
      throw new RuntimeException(e);
    }

    try (InputStream is = Files.newInputStream(location)) {
      trustStore.load(is, password.toCharArray());
    } catch (IOException
             | CertificateException
             | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    FileOutputStream out = new FileOutputStream(System.getProperty("user.dir") + "/src/test/resources/wiremock/test.jks");
    trustStore.store(out, password.toCharArray());
      try {
          out.close();
      } catch (IOException e) {
          throw new RuntimeException(e);
      }
      return trustStore;
  }
}
