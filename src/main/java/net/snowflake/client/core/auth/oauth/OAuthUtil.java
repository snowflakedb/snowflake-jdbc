package net.snowflake.client.core.auth.oauth;

import static net.snowflake.client.core.auth.oauth.OAuthAuthorizationCodeAccessTokenProvider.DEFAULT_REDIRECT_HOST;
import static net.snowflake.client.core.auth.oauth.OAuthAuthorizationCodeAccessTokenProvider.DEFAULT_REDIRECT_URI_ENDPOINT;

import com.amazonaws.util.StringUtils;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import net.snowflake.client.core.SFOauthLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

@SnowflakeJdbcInternalApi
public class OAuthUtil {

  private static final String SNOWFLAKE_AUTHORIZE_ENDPOINT = "/oauth/authorize";
  private static final String SNOWFLAKE_TOKEN_REQUEST_ENDPOINT = "/oauth/token-request";

  private static final String DEFAULT_SESSION_ROLE_SCOPE_PREFIX = "session:role:";

  @SnowflakeJdbcInternalApi
  public static URI getTokenRequestUrl(SFOauthLoginInput oauthLoginInput, String serverUrl) {
    URI uri =
        !StringUtils.isNullOrEmpty(oauthLoginInput.getTokenRequestUrl())
            ? URI.create(oauthLoginInput.getTokenRequestUrl())
            : URI.create(serverUrl + SNOWFLAKE_TOKEN_REQUEST_ENDPOINT);
    return uri.normalize();
  }

  static HttpRequestBase convertToBaseAuthorizationRequest(HTTPRequest request) {
    HttpPost baseRequest = new HttpPost(request.getURI());
    baseRequest.setEntity(new StringEntity(request.getBody(), StandardCharsets.UTF_8));
    request
        .getHeaderMap()
        .forEach((key, values) -> values.forEach(value -> baseRequest.addHeader(key, value)));
    return baseRequest;
  }

  static URI getAuthorizationUrl(SFOauthLoginInput oauthLoginInput, String serverUrl) {
    URI uri =
        !StringUtils.isNullOrEmpty(oauthLoginInput.getAuthorizationUrl())
            ? URI.create(oauthLoginInput.getAuthorizationUrl())
            : URI.create(serverUrl + SNOWFLAKE_AUTHORIZE_ENDPOINT);
    return uri.normalize();
  }

  static String getScope(SFOauthLoginInput oauthLoginInput, String role) {
    return (!StringUtils.isNullOrEmpty(oauthLoginInput.getScope()))
        ? oauthLoginInput.getScope()
        : DEFAULT_SESSION_ROLE_SCOPE_PREFIX + role;
  }

  static URI buildRedirectUri(SFOauthLoginInput oauthLoginInput) throws IOException {
    String redirectUri =
        !StringUtils.isNullOrEmpty(oauthLoginInput.getRedirectUri())
            ? oauthLoginInput.getRedirectUri()
            : createDefaultRedirectUri();
    return URI.create(redirectUri);
  }

  private static String createDefaultRedirectUri() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return String.format(
          "%s:%s%s", DEFAULT_REDIRECT_HOST, socket.getLocalPort(), DEFAULT_REDIRECT_URI_ENDPOINT);
    }
  }
}
