package net.snowflake.client.jdbc;

import static junit.framework.TestCase.assertEquals;
import static net.snowflake.client.AbstractDriverIT.getConnectionParameters;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.awaitility.Awaitility.await;
import static org.junit.Assume.*;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.nio.file.Paths;
import java.sql.*;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.RunningNotOnGithubActionsMac;
import net.snowflake.client.RunningNotOnJava21;
import net.snowflake.client.RunningNotOnJava8;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class BaseWiremockTest {

  protected static final SFLogger logger = SFLoggerFactory.getLogger(BaseWiremockTest.class);
  protected static final String WIREMOCK_HOME_DIR = ".wiremock";
  protected static final String WIREMOCK_M2_PATH =
      "/.m2/repository/org/wiremock/wiremock-standalone/3.8.0/wiremock-standalone-3.8.0.jar";
  protected static final String WIREMOCK_HOST = "localhost";
  protected static final String TRUST_STORE_PROPERTY = "javax.net.ssl.trustStore";
  protected static int httpProxyPort;
  protected static int httpsProxyPort;
  private static String originalTrustStorePath;
  protected static Process wiremockStandalone;

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
    resetWiremock();
    HttpUtil.httpClient.clear();
  }

  @AfterClass
  public static void tearDownClass() {
    stopWiremockStandAlone();
  }

  protected static void startWiremockStandAlone() {
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

  protected void resetWiremock() {
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
        .until(BaseWiremockTest::isWiremockResponding);
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

  protected static void stopWiremockStandAlone() {
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

  protected Properties getProperties() {
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

  protected HttpPost createWiremockPostRequest(String body, String path) {
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

  protected static void restoreTrustStorePathProperty() {
    if (originalTrustStorePath != null) {
      System.setProperty(TRUST_STORE_PROPERTY, originalTrustStorePath);
    } else {
      System.clearProperty(TRUST_STORE_PROPERTY);
    }
  }

  private int getAdminPort() {
    return httpProxyPort;
  }

  private static String getResourceURL(String relativePath) {
    return Paths.get(systemGetProperty("user.dir"), "src", "test", "resources", relativePath)
        .toAbsolutePath()
        .toString();
  }

  protected void setCustomTrustStorePropertyPath() {
    System.setProperty(
        TRUST_STORE_PROPERTY, getResourceURL("wiremock" + File.separator + "ca-cert.jks"));
  }

  protected void importMapping(String mappingImport) {
    HttpPost request = createWiremockPostRequest(mappingImport, "/__admin/mappings/import");
    try (CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = httpClient.execute(request)) {
      assumeTrue(response.getStatusLine().getStatusCode() == 200);
    } catch (Exception e) {
      logger.error("Importing mapping failed", e);
      assumeNoException(e);
    }
  }

  protected void addMapping(String mapping) {
    HttpPost postRequest = createWiremockPostRequest(mapping, "/__admin/mappings");
    try (CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = client.execute(postRequest)) {
      assertEquals(201, response.getStatusLine().getStatusCode());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
