package net.snowflake.client.jdbc;

import static net.snowflake.client.AbstractDriverIT.getConnectionParameters;
import static net.snowflake.client.AssumptionUtils.assumeNotRunningOnGithubActionsMac;
import static net.snowflake.client.AssumptionUtils.assumeNotRunningOnJava21;
import static net.snowflake.client.AssumptionUtils.assumeNotRunningOnJava8;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;

public abstract class BaseWiremockTest {

  protected static final SFLogger logger = SFLoggerFactory.getLogger(BaseWiremockTest.class);
  protected static final String WIREMOCK_HOME_DIR = ".wiremock";
  protected static final String WIREMOCK_M2_PATH =
      "/.m2/repository/org/wiremock/wiremock-standalone/3.8.0/wiremock-standalone-3.8.0.jar";
  protected static final String WIREMOCK_HOST = "localhost";
  protected static final String TRUST_STORE_PROPERTY = "javax.net.ssl.trustStore";
  protected static final String MAPPINGS_BASE_DIR = "/wiremock/mappings";
  protected static int wiremockHttpPort;
  protected static int wiremockHttpsPort;
  private static String originalTrustStorePath;
  protected static Process wiremockStandalone;

  @BeforeAll
  public static void setUpClass() {
    assumeNotRunningOnJava8();
    assumeNotRunningOnJava21();
    assumeNotRunningOnGithubActionsMac(); // disabled until issue with access to localhost
    // (https://github.com/snowflakedb/snowflake-jdbc/pull/1807#discussion_r1686229430) is fixed on
    // github actions mac image. Ticket to enable when fixed: SNOW-1555950
    originalTrustStorePath = systemGetProperty(TRUST_STORE_PROPERTY);
    startWiremockStandAlone();
  }

  @AfterEach
  public void tearDown() {
    restoreTrustStorePathProperty();
    resetWiremock();
    HttpUtil.httpClient.clear();
  }

  @AfterAll
  public static void tearDownClass() {
    stopWiremockStandAlone(wiremockStandalone);
  }

  protected static void startWiremockStandAlone() {
    // retrying in case of fail in port bindings
    await()
        .alias("wait for wiremock responding")
        .atMost(Duration.ofSeconds(10))
        .until(
            () -> {
              try {
                wiremockHttpPort = findFreePort();
                wiremockHttpsPort = findFreePort();
                wiremockStandalone = startWiremockProcess(wiremockHttpPort, wiremockHttpsPort);
                waitForWiremockOnPort(wiremockHttpPort);
                return true;
              } catch (Exception e) {
                logger.warn("Failed to start wiremock, retrying: ", e);
                return false;
              }
            });
  }

  protected static Process startWiremockProcess(int wiremockHttpPort, int wiremockHttpsPort)
      throws IOException {
    String javaExecutable =
        System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    return new ProcessBuilder(
            javaExecutable,
            "-jar",
            getWiremockStandAlonePath(),
            "--root-dir",
            System.getProperty("user.dir") + File.separator + WIREMOCK_HOME_DIR + File.separator,
            "--enable-browser-proxying", // work as forward proxy
            "--proxy-pass-through",
            "false", // pass through only matched requests
            "--port",
            String.valueOf(wiremockHttpPort),
            "--https-port",
            String.valueOf(wiremockHttpsPort),
            "--https-keystore",
            getResourceURL("wiremock" + File.separator + "ca-cert.jks"),
            "--ca-keystore",
            getResourceURL("wiremock" + File.separator + "ca-cert.jks"))
        .inheritIO()
        .start();
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

  protected static void waitForWiremockOnPort(int port) {
    await()
        .pollDelay(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(3))
        .until(() -> isWiremockResponding(port));
  }

  private static boolean isWiremockResponding(int port) {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet request =
          new HttpGet(String.format("http://%s:%d/__admin/mappings", WIREMOCK_HOST, port));
      CloseableHttpResponse response = httpClient.execute(request);
      return response.getStatusLine().getStatusCode() == 200;
    } catch (Exception e) {
      logger.warn("Waiting for wiremock to respond: ", e);
    }
    return false;
  }

  protected static void stopWiremockStandAlone(Process wiremockStandalone) {
    if (wiremockStandalone != null) {
      wiremockStandalone.destroyForcibly();
      await()
          .alias("stop wiremock")
          .atMost(Duration.ofSeconds(10))
          .until(() -> !wiremockStandalone.isAlive());
    }
  }

  protected static int findFreePort() {
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

    // Handle authentication - prioritize private key, fallback to password
    if (params.get("private_key_file") != null) {
      props.put("private_key_file", params.get("private_key_file"));
      props.put("authenticator", params.get("authenticator"));
      if (params.get("private_key_pwd") != null) {
        props.put("private_key_pwd", params.get("private_key_pwd"));
      }
    } else if (params.get("password") != null) {
      props.put("password", params.get("password"));
    }
    props.put("warehouse", params.get("warehouse"));
    props.put("db", params.get("database"));
    props.put("schema", params.get("schema"));
    props.put("ssl", params.get("ssl"));
    props.put("insecureMode", true); // OCSP disabled for wiremock proxy tests
    return props;
  }

  protected HttpPost createWiremockPostRequest(String body, String path) {
    return createWiremockPostRequest(body, path, getAdminPort());
  }

  protected HttpPost createWiremockPostRequest(String body, String path, int adminPort) {
    HttpPost postRequest = new HttpPost("http://" + WIREMOCK_HOST + ":" + adminPort + path);
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
    return wiremockHttpPort;
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
      Assumptions.assumeTrue(response.getStatusLine().getStatusCode() == 200);
    } catch (Exception e) {
      logger.error("Importing mapping failed", e);
      Assumptions.abort("Importing mapping failed");
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

  /**
   * Reads JSON content from a file, supporting both absolute paths and classpath resources.
   *
   * @param filePath The file path (absolute or from the resources directory).
   * @return JSON content as a String.
   * @throws IOException If an error occurs while reading the file.
   */
  private String readJSONFromFile(String filePath) throws IOException {
    // Check if the file exists as an absolute file path
    Path sourceFilePath = Paths.get(filePath);
    if (Files.exists(sourceFilePath)) {
      return new String(Files.readAllBytes(sourceFilePath), StandardCharsets.UTF_8);
    }

    // If not found, attempt to read from the classpath (resources directory).
    // Has to start from '/' followed by a subdirectory of the resources directory.
    try (InputStream inputStream = getClass().getResourceAsStream(filePath)) {
      if (inputStream == null) {
        throw new IllegalStateException(
            "Could not find file under the specified path: " + filePath);
      }
      try (InputStreamReader isr = new InputStreamReader(inputStream);
          BufferedReader br = new BufferedReader(isr)) {
        return br.lines().collect(Collectors.joining("\n"));
      }
    }
  }

  /**
   * Reads JSON content from a file and replaces placeholders dynamically.
   *
   * @param mappingPath The path to the JSON file in the classpath.
   * @param placeholdersMappings A map of placeholders to be replaced in the JSON content.
   * @return The JSON content as a String with placeholders replaced.
   * @throws IOException If an error occurs while reading the file.
   */
  protected String getWireMockMappingFromFile(
      String mappingPath, Map<String, Object> placeholdersMappings) throws IOException {
    String jsonContent = readJSONFromFile(mappingPath);

    // Replace placeholders with actual values
    for (Map.Entry<String, Object> entry : placeholdersMappings.entrySet()) {
      jsonContent = jsonContent.replace(entry.getKey(), String.valueOf(entry.getValue()));
    }
    return jsonContent;
  }

  /** A minimal POJO representing a serve event from WireMock. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class MinimalServeEvent {
    private MinimalRequest request;

    public MinimalRequest getRequest() {
      return request;
    }

    public void setRequest(MinimalRequest request) {
      this.request = request;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MinimalRequest {
      private String url;
      private Date loggedDate;
      private Map<String, MinimalQueryParameter> queryParams;

      public String getUrl() {
        return url;
      }

      public void setUrl(String url) {
        this.url = url;
      }

      public Date getLoggedDate() {
        return loggedDate;
      }

      public void setLoggedDate(Date loggedDate) {
        this.loggedDate = loggedDate;
      }

      public Map<String, MinimalQueryParameter> getQueryParams() {
        return queryParams;
      }

      public void setQueryParams(Map<String, MinimalQueryParameter> queryParams) {
        this.queryParams = queryParams;
      }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MinimalQueryParameter {
      private List<String> values;

      public List<String> getValues() {
        return values;
      }

      public void setValues(List<String> values) {
        this.values = values;
      }

      /** Returns the first value in the query parameter list, or null if no values are present. */
      public String firstValue() {
        return (values != null && !values.isEmpty()) ? values.get(0) : null;
      }
    }
  }

  /**
   * A wrapper for the serve events JSON structure returned by WireMock. The JSON has a top-level
   * "requests" field which is an array of serve events.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ServeEventsWrapper {
    private List<MinimalServeEvent> requests = new ArrayList<>();

    public List<MinimalServeEvent> getRequests() {
      return requests;
    }

    public void setRequests(List<MinimalServeEvent> requests) {
      this.requests = requests;
    }
  }

  /**
   * Retrieves all serve events recorded by WireMock by querying the admin endpoint. This
   * implementation uses our minimal POJOs to avoid deserialization issues.
   *
   * <p>We have to use wiremock api endpoints to retrieve those events, because we are unable to
   * import wiremock.stubbing.ServeEvent - it would cause tests to fail on Java8 - since it would be
   * still imported during compilation.
   *
   * @return A list of MinimalServeEvent objects representing the requests WireMock has recorded.
   */
  protected List<MinimalServeEvent> getAllServeEvents() {
    return getAllServeEvents(getAdminPort());
  }

  protected List<MinimalServeEvent> getAllServeEvents(int port) {
    String url = "http://" + WIREMOCK_HOST + ":" + port + "/__admin/requests";
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      HttpGet request = new HttpGet(url);
      try (CloseableHttpResponse response = client.execute(request)) {
        String json = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        ObjectMapper mapper = new ObjectMapper();
        ServeEventsWrapper wrapper = mapper.readValue(json, ServeEventsWrapper.class);
        return wrapper.getRequests();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to get serve events from WireMock", e);
    }
  }

  protected void importMappingFromResources(String relativePath) {
    try (InputStream is = BaseWiremockTest.class.getResourceAsStream(relativePath)) {
      String scenario = IOUtils.toString(Objects.requireNonNull(is), StandardCharsets.UTF_8.name());
      importMapping(scenario);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Verifies the count of requests matching a given URL pattern.
   *
   * @param expectedCount The expected number of requests
   * @param urlPattern The URL pattern to match against (supports regex)
   */
  protected void verifyRequestCount(int expectedCount, String urlPattern) {
    String requestBody =
        String.format("{\"method\": \"POST\", \"urlPattern\": \"%s\"}", urlPattern);

    HttpPost postRequest = createWiremockPostRequest(requestBody, "/__admin/requests/count");
    try (CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = client.execute(postRequest)) {
      String json = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
      ObjectMapper mapper = new ObjectMapper();
      Map<String, String> countResponse =
          mapper.readValue(json, new TypeReference<Map<String, String>>() {});
      int actualCount = Integer.parseInt(countResponse.get("count"));
      assertEquals(
          expectedCount,
          actualCount,
          String.format(
              "Expected %d requests matching pattern '%s', but found %d",
              expectedCount, urlPattern, actualCount));
    } catch (Exception e) {
      throw new RuntimeException("Failed to verify request count from WireMock", e);
    }
  }
}
