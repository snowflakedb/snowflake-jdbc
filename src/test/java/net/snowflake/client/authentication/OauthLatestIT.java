package net.snowflake.client.authentication;

import static net.snowflake.client.authentication.AuthConnectionParameters.getOauthConnectionParameters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.HttpUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.AUTHENTICATION)
public class OauthLatestIT {

  AuthTest authTest;

  @BeforeEach
  public void setUp() throws IOException {
    authTest = new AuthTest();
  }

  @AfterEach
  public void tearDown() {
    HttpUtil.httpClient.clear();
  }

  @Test
  void shouldAuthenticateUsingOauth() throws IOException, InterruptedException {
    authTest.connectAndExecuteSimpleQuery(getOauthConnectionParameters(getToken()), null);
    authTest.verifyExceptionIsNotThrown();
  }

  @Test
  void shouldThrowErrorForInvalidToken() {
    authTest.connectAndExecuteSimpleQuery(getOauthConnectionParameters("invalidToken"), null);
    authTest.verifyExceptionIsThrown("Invalid OAuth access token. ");
  }

  @Test
  void shouldThrowErrorForMismatchedOauthUsername() throws IOException, InterruptedException {
    Properties properties = getOauthConnectionParameters(getToken());
    properties.put("user", "differentUsername");
    authTest.connectAndExecuteSimpleQuery(properties, null);
    authTest.verifyExceptionIsThrown(
        "The user you were trying to authenticate as differs from the user tied to the access token.");
  }

  private String getToken() throws IOException, InterruptedException {
    List<String> data =
        Stream.of(
                "username=" + System.getenv("SNOWFLAKE_AUTH_TEST_OKTA_USER"),
                "password=" + System.getenv("SNOWFLAKE_AUTH_TEST_OKTA_PASS"),
                "grant_type=password",
                "scope=session:role:" + System.getenv("SNOWFLAKE_AUTH_TEST_ROLE").toLowerCase())
            .collect(Collectors.toList());

    String auth =
        System.getenv("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_ID")
            + ":"
            + System.getenv("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_SECRET");
    String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));

    HttpClient httpClient = HttpClient.newHttpClient();
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(System.getenv("SNOWFLAKE_AUTH_TEST_OAUTH_URL")))
            .header("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
            .header("Authorization", "Basic " + encodedAuth)
            .POST(HttpRequest.BodyPublishers.ofString(String.join("&", data)))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new IOException("Failed to get access token, response code: " + response.statusCode());
    }

    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree(response.body());
    return jsonNode.get("access_token").asText();
  }
}
