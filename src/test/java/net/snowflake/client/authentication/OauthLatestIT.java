package net.snowflake.client.authentication;

import static net.snowflake.client.authentication.AuthConnectionParameters.getOauthConnectionParameters;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.AUTHENTICATION)
public class OauthLatestIT {

  AuthTestHelper authTestHelper;

  @BeforeEach
  public void setUp() throws IOException {
    authTestHelper = new AuthTestHelper();
  }

  @Test
  void shouldAuthenticateUsingOauth() throws IOException {
    authTestHelper.connectAndExecuteSimpleQuery(getOauthConnectionParameters(getToken()), null);
    authTestHelper.verifyExceptionIsNotThrown();
  }

  @Test
  void shouldThrowErrorForInvalidToken() {
    authTestHelper.connectAndExecuteSimpleQuery(getOauthConnectionParameters("invalidToken"), null);
    authTestHelper.verifyExceptionIsThrown("Invalid OAuth access token. ");
  }

  @Test
  void shouldThrowErrorForMismatchedOauthUsername() throws IOException {
    Properties properties = getOauthConnectionParameters(getToken());
    properties.put("user", "differentUsername");
    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsThrown(
        "The user you were trying to authenticate as differs from the user tied to the access token.");
  }

  private String getToken() throws IOException {
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

    URL url = new URL(System.getenv("SNOWFLAKE_AUTH_TEST_OAUTH_URL"));
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty(
        "Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
    connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
    connection.setDoOutput(true);

    try (DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.writeBytes(String.join("&", data));
      out.flush();
    }

    int responseCode = connection.getResponseCode();
    assertThat("Failed to get access token, response code: " + responseCode, responseCode, is(200));

    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode;
    try (InputStream inputStream = connection.getInputStream()) {
      jsonNode = mapper.readTree(inputStream);
    }
    return jsonNode.get("access_token").asText();
  }
}
