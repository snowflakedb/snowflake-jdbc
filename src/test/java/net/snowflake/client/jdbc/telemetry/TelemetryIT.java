package net.snowflake.client.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SessionUtil;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class TelemetryIT extends AbstractDriverIT {
  private Connection connection = null;
  private static final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  public void init() throws SQLException, IOException {
    this.connection = getConnection();
  }

  @Test
  public void testTelemetry() throws Exception {
    TelemetryClient telemetry = (TelemetryClient) TelemetryClient.createTelemetry(connection, 100);
    testTelemetryInternal(telemetry);
  }

  @Disabled
  @Test
  @DontRunOnGithubActions
  public void testSessionlessTelemetry() throws Exception, SFException {
    testTelemetryInternal(createSessionlessTelemetry());
  }

  @Disabled
  @Test
  @DontRunOnGithubActions
  public void testJWTSessionlessTelemetry() throws Exception, SFException {
    testTelemetryInternal(createJWTSessionlessTelemetry());
  }

  @Disabled
  @Test
  @DontRunOnGithubActions
  public void testOAuthSessionlessTelemetry() throws Exception, SFException {
    testTelemetryInternal(createOAuthSessionlessTelemetry());
  }

  private void testTelemetryInternal(TelemetryClient telemetry) throws Exception {
    ObjectNode node1 = mapper.createObjectNode();
    node1.put("type", "query");
    node1.put("query_id", "sdasdasdasdasds");
    ObjectNode node2 = mapper.createObjectNode();
    node2.put("type", "query");
    node2.put("query_id", "eqweqweqweqwe");
    telemetry.addLogToBatch(node1, 1234567);
    telemetry.addLogToBatch(new TelemetryData(node2, 22345678));
    assertEquals(telemetry.bufferSize(), 2);

    assertTrue(telemetry.sendBatchAsync().get());
    assertEquals(telemetry.bufferSize(), 0);

    assertTrue(telemetry.sendLog(node1, 1234567));
    assertEquals(telemetry.bufferSize(), 0);

    assertTrue(telemetry.sendLog(new TelemetryData(node2, 22345678)));
    assertEquals(telemetry.bufferSize(), 0);

    // reach flush threshold
    for (int i = 0; i < 99; i++) {
      telemetry.addLogToBatch(node1, 1111);
    }
    assertEquals(telemetry.bufferSize(), 99);
    telemetry.addLogToBatch(node1, 222);

    // flush is async, sleep some time and then check buffer size
    Thread.sleep(1000);
    assertEquals(telemetry.bufferSize(), 0);

    telemetry.addLogToBatch(node1, 111);
    assertEquals(telemetry.bufferSize(), 1);

    assertFalse(telemetry.isClosed());
    telemetry.close();
    assertTrue(telemetry.isClosed());
    // close function sends the metrics to the server
    assertEquals(telemetry.bufferSize(), 0);
  }

  @Test
  public void close1() {
    TelemetryClient telemetry = (TelemetryClient) TelemetryClient.createTelemetry(connection);
    telemetry.close();
    ObjectNode node = mapper.createObjectNode();
    node.put("type", "query");
    node.put("query_id", "sdasdasdasdasds");
    telemetry.addLogToBatch(node, 1234567);
  }

  @Test
  public void close2() {
    TelemetryClient telemetry = (TelemetryClient) TelemetryClient.createTelemetry(connection);
    telemetry.close();
    ObjectNode node = mapper.createObjectNode();
    node.put("type", "query");
    node.put("query_id", "sdasdasdasdasds");
    telemetry.addLogToBatch(new TelemetryData(node, 1234567));
  }

  @Test
  public void close3() {
    TelemetryClient telemetry = (TelemetryClient) TelemetryClient.createTelemetry(connection);
    telemetry.close();
    telemetry.close();
  }

  @Test
  public void testDisableTelemetry() throws Exception {
    TelemetryClient telemetry = (TelemetryClient) TelemetryClient.createTelemetry(connection, 100);
    testDisableTelemetryInternal(telemetry);
  }

  @Test
  @DontRunOnGithubActions
  public void testDisableJWTSessionlessTelemetry() throws Exception, SFException {
    testDisableTelemetryInternal(createJWTSessionlessTelemetry());
  }

  @Test
  @DontRunOnGithubActions
  public void testDisableOAuthSessionlessTelemetry() throws Exception, SFException {
    testDisableTelemetryInternal(createOAuthSessionlessTelemetry());
  }

  public void testDisableTelemetryInternal(TelemetryClient telemetry) throws Exception {
    ObjectNode node1 = mapper.createObjectNode();
    node1.put("type", "query");
    node1.put("query_id", "sdasdasdasdasds");
    ObjectNode node2 = mapper.createObjectNode();
    node2.put("type", "query");
    node2.put("query_id", "eqweqweqweqwe");
    telemetry.addLogToBatch(node1, 1234567);

    assertEquals(telemetry.bufferSize(), 1);
    telemetry.disableTelemetry();
    assertFalse(telemetry.isTelemetryEnabled());

    telemetry.addLogToBatch(new TelemetryData(node2, 22345678));
    assertEquals(telemetry.bufferSize(), 1);

    assertFalse(telemetry.sendBatchAsync().get());
    assertEquals(telemetry.bufferSize(), 1);

    assertFalse(telemetry.sendLog(node1, 1234567));
    assertEquals(telemetry.bufferSize(), 1);

    assertFalse(telemetry.sendLog(new TelemetryData(node2, 22345678)));
    assertEquals(telemetry.bufferSize(), 1);
  }

  @Test
  @DontRunOnGithubActions
  public void testClosedJWTSessionlessTelemetry() throws Exception, SFException {
    TelemetryClient telemetry = createJWTSessionlessTelemetry();
    telemetry.close();
    ObjectNode node = mapper.createObjectNode();
    node.put("type", "query");
    node.put("query_id", "sdasdasdasdasds");
    telemetry.addLogToBatch(node, 1234567);
    assertFalse(telemetry.sendBatchAsync().get());
  }

  @Test
  @DontRunOnGithubActions
  public void testClosedOAuthSessionlessTelemetry() throws Exception, SFException {
    TelemetryClient telemetry = createOAuthSessionlessTelemetry();
    telemetry.close();
    ObjectNode node = mapper.createObjectNode();
    node.put("type", "query");
    node.put("query_id", "sdasdasdasdasds");
    telemetry.addLogToBatch(node, 1234567);
    assertFalse(telemetry.sendBatchAsync().get());
  }

  // Helper function to create a sessionless telemetry
  // using createSessionlessTelemetry(CloseableHttpClient httpClient, String serverUrl)
  private TelemetryClient createSessionlessTelemetry()
      throws SFException, SQLException, IOException {
    setUpPublicKey();
    String privateKeyLocation = getFullPathFileInResource("rsa_key.p8");
    Map<String, String> parameters = getConnectionParameters();
    String jwtToken =
        SessionUtil.generateJWTToken(
            null,
            privateKeyLocation,
            null,
            null,
            parameters.get("account"),
            parameters.get("user"));

    CloseableHttpClient httpClient = HttpUtil.buildHttpClient(null, null, false);
    TelemetryClient telemetry =
        (TelemetryClient)
            TelemetryClient.createSessionlessTelemetry(
                httpClient, String.format("%s:%s", parameters.get("host"), parameters.get("port")));
    telemetry.refreshToken(jwtToken);
    return telemetry;
  }

  // Helper function to create a sessionless telemetry using keypair JWT
  private TelemetryClient createJWTSessionlessTelemetry()
      throws SFException, SQLException, IOException {
    setUpPublicKey();
    String privateKeyLocation = getFullPathFileInResource("rsa_key.p8");
    Map<String, String> parameters = getConnectionParameters();
    String jwtToken =
        SessionUtil.generateJWTToken(
            null,
            privateKeyLocation,
            null,
            null,
            parameters.get("account"),
            parameters.get("user"));

    CloseableHttpClient httpClient = HttpUtil.buildHttpClient(null, null, false);
    TelemetryClient telemetry =
        (TelemetryClient)
            TelemetryClient.createSessionlessTelemetry(
                httpClient,
                String.format("%s:%s", parameters.get("host"), parameters.get("port")),
                "KEYPAIR_JWT");
    telemetry.refreshToken(jwtToken);
    return telemetry;
  }

  // Helper function to set up the public key
  private void setUpPublicKey() throws SQLException, IOException {
    Map<String, String> parameters = getConnectionParameters();
    String testUser = parameters.get("user");
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("use role accountadmin");
      String pathfile = getFullPathFileInResource("rsa_key.pub");
      String pubKey = new String(Files.readAllBytes(Paths.get(pathfile)));
      pubKey = pubKey.replace("-----BEGIN PUBLIC KEY-----", "");
      pubKey = pubKey.replace("-----END PUBLIC KEY-----", "");
      statement.execute(String.format("alter user %s set rsa_public_key='%s'", testUser, pubKey));
    }
  }

  // Helper function to create a sessionless telemetry using OAuth
  private TelemetryClient createOAuthSessionlessTelemetry()
      throws SFException, SQLException, IOException {
    String oAuthToken = getOAuthToken();
    Map<String, String> parameters = getConnectionParameters();
    CloseableHttpClient httpClient = HttpUtil.buildHttpClient(null, null, false);
    TelemetryClient telemetry =
        (TelemetryClient)
            TelemetryClient.createSessionlessTelemetry(
                httpClient,
                String.format("%s:%s", parameters.get("host"), parameters.get("port")),
                "OAUTH");
    telemetry.refreshToken(oAuthToken);
    return telemetry;
  }

  // Helper function to set up and get OAuth token
  private String getOAuthToken() throws SQLException {
    Map<String, String> parameters = getConnectionParameters();
    String token = null;
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("use role accountadmin");
      statement.execute(
          "create or replace security integration telemetry_oauth_integration\n"
              + "  type=oauth\n"
              + "  oauth_client=CUSTOM\n"
              + "  oauth_client_type=CONFIDENTIAL\n"
              + "  oauth_redirect_uri='https://localhost.com/oauth'\n"
              + "  oauth_issue_refresh_tokens=true\n"
              + "  enabled=true oauth_refresh_token_validity=86400;");
      String role = parameters.get("role");
      try (ResultSet resultSet =
          statement.executeQuery(
              "select system$it('create_oauth_access_token', 'TELEMETRY_OAUTH_INTEGRATION', '"
                  + role
                  + "')")) {
        assertTrue(resultSet.next());
        token = resultSet.getString(1);
      }
    }
    return token;
  }
}
