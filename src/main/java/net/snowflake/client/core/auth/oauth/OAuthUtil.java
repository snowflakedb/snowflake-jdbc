package net.snowflake.client.core.auth.oauth;

import static net.snowflake.client.core.auth.oauth.OAuthAuthorizationCodeAccessTokenProvider.DEFAULT_REDIRECT_HOST;
import static net.snowflake.client.core.auth.oauth.OAuthAuthorizationCodeAccessTokenProvider.DEFAULT_REDIRECT_URI_ENDPOINT;
import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SFOauthLoginInput;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

class OAuthUtil {

  private static final SFLogger logger = SFLoggerFactory.getLogger(OAuthUtil.class);

  private static final String SNOWFLAKE_AUTHORIZE_ENDPOINT = "/oauth/authorize";
  private static final String SNOWFLAKE_TOKEN_REQUEST_ENDPOINT = "/oauth/token-request";

  private static final String DEFAULT_SESSION_ROLE_SCOPE_PREFIX = "session:role:";

  static URI getTokenRequestUrl(SFOauthLoginInput oauthLoginInput, String serverUrl) {
    URI uri =
        !isNullOrEmpty(oauthLoginInput.getTokenRequestUrl())
            ? URI.create(oauthLoginInput.getTokenRequestUrl())
            : URI.create(serverUrl + SNOWFLAKE_TOKEN_REQUEST_ENDPOINT);
    return uri.normalize();
  }

  static HttpRequestBase convertToBaseRequest(HTTPRequest request) {
    HttpPost baseRequest = new HttpPost(request.getURI());
    baseRequest.setEntity(new StringEntity(request.getBody(), StandardCharsets.UTF_8));
    request
        .getHeaderMap()
        .forEach((key, values) -> values.forEach(value -> baseRequest.addHeader(key, value)));
    return baseRequest;
  }

  static URI getAuthorizationUrl(SFOauthLoginInput oauthLoginInput, String serverUrl) {
    URI uri =
        !isNullOrEmpty(oauthLoginInput.getAuthorizationUrl())
            ? URI.create(oauthLoginInput.getAuthorizationUrl())
            : URI.create(serverUrl + SNOWFLAKE_AUTHORIZE_ENDPOINT);
    return uri.normalize();
  }

  static String getScope(SFOauthLoginInput oauthLoginInput, String role) {
    return (!isNullOrEmpty(oauthLoginInput.getScope()))
        ? oauthLoginInput.getScope()
        : DEFAULT_SESSION_ROLE_SCOPE_PREFIX + role;
  }

  static URI buildRedirectUri(SFOauthLoginInput oauthLoginInput) throws IOException {
    String redirectUri =
        !isNullOrEmpty(oauthLoginInput.getRedirectUri())
            ? oauthLoginInput.getRedirectUri()
            : createDefaultRedirectUri();
    return URI.create(redirectUri);
  }

  static TokenResponseDTO sendTokenRequest(HttpRequestBase request, SFLoginInput loginInput)
      throws SnowflakeSQLException, IOException {
    URI requestUri = request.getURI();
    logger.debug(
        "Requesting OAuth access token from: {}", requestUri.getAuthority() + requestUri.getPath());
    String tokenResponse =
        HttpUtil.executeGeneralRequest(
            request,
            loginInput.getLoginTimeout(),
            loginInput.getAuthTimeout(),
            loginInput.getSocketTimeoutInMillis(),
            0,
            loginInput.getHttpClientSettingsKey());
    ObjectMapper objectMapper = new ObjectMapper();
    TokenResponseDTO tokenResponseDTO =
        objectMapper.readValue(tokenResponse, TokenResponseDTO.class);
    logger.debug(
        "Received OAuth access token of type \"{}\" from: {}{}",
        tokenResponseDTO.getTokenType(),
        requestUri.getAuthority(),
        requestUri.getPath());
    return tokenResponseDTO;
  }

  private static String createDefaultRedirectUri() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return String.format(
          "%s:%s%s", DEFAULT_REDIRECT_HOST, socket.getLocalPort(), DEFAULT_REDIRECT_URI_ENDPOINT);
    }
  }
}
