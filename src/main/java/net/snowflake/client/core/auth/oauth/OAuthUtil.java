package net.snowflake.client.core.auth.oauth;

import com.amazonaws.util.StringUtils;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import net.snowflake.client.core.SFLoginInput;
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

  public static HttpRequestBase convertToBaseRequest(HTTPRequest request) {
    HttpPost baseRequest = new HttpPost(request.getURI());
    baseRequest.setEntity(new StringEntity(request.getBody(), StandardCharsets.UTF_8));
    request.getHeaderMap().forEach((key, values) -> baseRequest.addHeader(key, values.get(0)));
    return baseRequest;
  }

  public static URI getAuthorizationUrl(SFOauthLoginInput oauthLoginInput, String serverUrl) {
    return !StringUtils.isNullOrEmpty(oauthLoginInput.getExternalAuthorizationUrl())
        ? URI.create(oauthLoginInput.getExternalAuthorizationUrl())
        : URI.create(serverUrl + SNOWFLAKE_AUTHORIZE_ENDPOINT);
  }

  public static URI getTokenRequestUrl(SFOauthLoginInput oauthLoginInput, String serverUrl) {
    return !StringUtils.isNullOrEmpty(oauthLoginInput.getExternalTokenRequestUrl())
        ? URI.create(oauthLoginInput.getExternalTokenRequestUrl())
        : URI.create(serverUrl + SNOWFLAKE_TOKEN_REQUEST_ENDPOINT);
  }

  public static String getScope(SFOauthLoginInput oauthLoginInput, String role) {
    return (!StringUtils.isNullOrEmpty(oauthLoginInput.getScope()))
        ? oauthLoginInput.getScope()
        : DEFAULT_SESSION_ROLE_SCOPE_PREFIX + role;
  }
}
