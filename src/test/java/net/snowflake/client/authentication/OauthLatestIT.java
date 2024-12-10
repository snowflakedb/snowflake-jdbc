package net.snowflake.client.authentication;

import static net.snowflake.client.authentication.AuthConnectionParameters.getOauthConnectionParameters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.client.category.TestTags;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
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

  @Test
  void shouldAuthenticateUsingOauth() throws IOException {
    authTest.connectAndExecuteSimpleQuery(getOauthConnectionParameters(getToken()), null);
    authTest.verifyExceptionIsNotThrown();
  }

  @Test
  void shouldThrowErrorForInvalidToken() {
    authTest.connectAndExecuteSimpleQuery(getOauthConnectionParameters("invalidToken"), null);
    authTest.verifyExceptionIsThrown("Invalid OAuth access token. ");
  }

  @Test
  void shouldThrowErrorForMismatchedOauthUsername() throws IOException {
    Properties properties = getOauthConnectionParameters(getToken());
    properties.put("user", "differentUsername");
    authTest.connectAndExecuteSimpleQuery(properties, null);
    authTest.verifyExceptionIsThrown(
        "The user you were trying to authenticate as differs from the user tied to the access token.");
  }

  private String getToken() throws IOException {
    List<BasicNameValuePair> data =
        Stream.of(
                new BasicNameValuePair("username", System.getenv("SNOWFLAKE_AUTH_TEST_OKTA_USER")),
                new BasicNameValuePair("password", System.getenv("SNOWFLAKE_AUTH_TEST_OKTA_PASS")),
                new BasicNameValuePair("grant_type", "password"),
                new BasicNameValuePair(
                    "scope",
                    "session:role:" + System.getenv("SNOWFLAKE_AUTH_TEST_ROLE").toLowerCase()))
            .collect(Collectors.toList());

    String auth =
        System.getenv("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_ID")
            + ":"
            + System.getenv("SNOWFLAKE_AUTH_TEST_OAUTH_CLIENT_SECRET");
    String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));

    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpPost httpPost = new HttpPost(System.getenv("SNOWFLAKE_AUTH_TEST_OAUTH_URL"));
      httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
      httpPost.setHeader("Authorization", "Basic " + encodedAuth);
      httpPost.setEntity(new UrlEncodedFormEntity(data, StandardCharsets.UTF_8));

      try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
        if (response.getStatusLine().getStatusCode() != 200) {
          throw new IOException(
              "Failed to get access token, response code: "
                  + response.getStatusLine().getStatusCode());
        }

        String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(responseBody);
        return jsonNode.get("access_token").asText();
      }
    }
  }
}
