package net.snowflake.client.internal.core.auth.oauth;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.HttpClientSettingsKey;
import net.snowflake.client.internal.core.HttpUtil;
import net.snowflake.client.internal.core.OCSPMode;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

/**
 * Internal facade that performs the OAuth token-exchange call used to mint a Streamlit embed URL.
 *
 * <p>This lives in the internal subtree because it depends on internal HTTP plumbing ({@link
 * HttpUtil}, {@link HttpClientSettingsKey}). The public-facing {@code EmbeddedStreamlit} builder
 * delegates the network call to this class so that the public class can stay free of internal
 * imports and so that the HTTP layer can be stubbed in unit tests via {@link HttpSender}.
 *
 * <p>The token-exchange request follows the verified wire contract:
 *
 * <pre>
 *   POST https://&lt;account&gt;.snowflakecomputing.com/oauth/token
 *   Content-Type: application/x-www-form-urlencoded; charset=UTF-8
 *   Accept: application/json
 *   (no Authorization header, no client identity)
 *
 *   grant_type=urn:ietf:params:oauth:grant-type:token-exchange
 *   subject_token=&lt;credential&gt;
 *   subject_token_type=&lt;urn&gt;
 *   scope=session:streamlit:&lt;db.schema.app&gt;
 * </pre>
 */
public final class StreamlitEmbedTokenExchange {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(StreamlitEmbedTokenExchange.class);

  /** OAuth 2.0 Token Exchange grant type (RFC 8693). */
  public static final String TOKEN_EXCHANGE_GRANT_TYPE =
      "urn:ietf:params:oauth:grant-type:token-exchange";

  /** Default token-exchange endpoint path appended to the account host. */
  public static final String DEFAULT_TOKEN_ENDPOINT_PATH = "/oauth/token";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private StreamlitEmbedTokenExchange() {}

  /**
   * Seam over the HTTP layer so unit tests can stub the network call. The default implementation
   * routes through {@link HttpUtil#executeGeneralRequestOmitSnowflakeHeaders} (no Snowflake/auth
   * headers are added, matching the wire contract).
   */
  @FunctionalInterface
  public interface HttpSender {
    String send(HttpPost request, HttpClientSettingsKey clientKey)
        throws SnowflakeSQLException, IOException;
  }

  private static final HttpSender DEFAULT_SENDER =
      (request, clientKey) ->
          HttpUtil.executeGeneralRequestOmitSnowflakeHeaders(
              request,
              /* retryTimeout= */ 0,
              /* authTimeout= */ 0,
              /* socketTimeout= */ 0,
              /* retryCount= */ 0,
              clientKey,
              /* sfSession= */ null);

  /** Returns the default HTTP sender backed by {@link HttpUtil}. */
  public static HttpSender defaultSender() {
    return DEFAULT_SENDER;
  }

  /**
   * Performs the token-exchange POST and parses the response.
   *
   * @param tokenEndpoint the absolute token endpoint URI (e.g. {@code
   *     https://acct.snowflakecomputing.com/oauth/token})
   * @param subjectToken the service credential value (secret)
   * @param subjectTokenType the {@code subject_token_type} URN
   * @param scope the {@code session:streamlit:<fqn>} scope string
   * @param sender the HTTP seam (use {@link #defaultSender()} in production)
   * @return the parsed response carrying {@code redirect_uri} and {@code expires_in}
   * @throws SnowflakeSQLException on transport errors, non-2xx responses, or an unparseable /
   *     redirect_uri-less response
   */
  public static StreamlitEmbedTokenResponseDTO exchange(
      URI tokenEndpoint,
      String subjectToken,
      String subjectTokenType,
      String scope,
      HttpSender sender)
      throws SnowflakeSQLException {
    if (tokenEndpoint.getScheme() != null && tokenEndpoint.getScheme().equalsIgnoreCase("http")) {
      logger.warn("Streamlit embed token endpoint uses insecure HTTP protocol: {}", tokenEndpoint);
    }
    HttpPost request = buildRequest(tokenEndpoint, subjectToken, subjectTokenType, scope);
    // The token-exchange endpoint has no OAuth client identity; default to FAIL_OPEN OCSP.
    HttpClientSettingsKey clientKey = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);

    String responseBody;
    try {
      logger.debug(
          "Requesting Streamlit embed authorization code from: {}{}",
          tokenEndpoint.getAuthority(),
          tokenEndpoint.getPath());
      responseBody = sender.send(request, clientKey);
    } catch (SnowflakeSQLException e) {
      throw e;
    } catch (IOException e) {
      throw new SnowflakeSQLException(e, ErrorCode.NETWORK_ERROR, e.getMessage());
    }

    StreamlitEmbedTokenResponseDTO dto;
    try {
      dto = OBJECT_MAPPER.readValue(responseBody, StreamlitEmbedTokenResponseDTO.class);
    } catch (IOException e) {
      // Do not echo responseBody: it may carry the redirect_uri with the single-use code (secret).
      throw new SnowflakeSQLException(
          e,
          ErrorCode.STREAMLIT_EMBED_URL_ERROR,
          "could not parse the token-exchange response from the server");
    }
    if (dto.getRedirectUri() == null || dto.getRedirectUri().trim().isEmpty()) {
      throw new SnowflakeSQLException(
          ErrorCode.STREAMLIT_EMBED_URL_ERROR,
          "the token-exchange response did not contain a redirect_uri");
    }
    return dto;
  }

  private static HttpPost buildRequest(
      URI tokenEndpoint, String subjectToken, String subjectTokenType, String scope) {
    // Use a deterministic ordering for stable, testable bodies.
    Map<String, String> params = new LinkedHashMap<>();
    params.put("grant_type", TOKEN_EXCHANGE_GRANT_TYPE);
    params.put("subject_token", subjectToken);
    params.put("subject_token_type", subjectTokenType);
    params.put("scope", scope);

    HttpPost request = new HttpPost(tokenEndpoint);
    request.setHeader("Accept", "application/json");
    request.setEntity(
        new StringEntity(
            urlEncodeForm(params),
            ContentType.create("application/x-www-form-urlencoded", StandardCharsets.UTF_8)));
    return request;
  }

  /** Builds an {@code application/x-www-form-urlencoded} body from the given parameters. */
  static String urlEncodeForm(Map<String, String> params) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : params.entrySet()) {
      if (sb.length() > 0) {
        sb.append('&');
      }
      sb.append(urlEncode(entry.getKey())).append('=').append(urlEncode(entry.getValue()));
    }
    return sb.toString();
  }

  private static String urlEncode(String value) {
    try {
      return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8.name());
    } catch (java.io.UnsupportedEncodingException e) {
      // UTF-8 is always supported.
      throw new IllegalStateException(e);
    }
  }

  /**
   * Extracts the live session token from an established Snowflake connection so it can be used as
   * the {@code subject_token} for the token-exchange. Lives here (internal subtree) because the
   * public {@code SnowflakeConnection} interface does not expose the session token.
   *
   * @param connection a {@link net.snowflake.client.api.connection.SnowflakeConnection}
   * @return the session token
   * @throws SnowflakeSQLException if the connection type is unsupported or has no active session
   */
  public static String extractSessionToken(
      net.snowflake.client.api.connection.SnowflakeConnection connection)
      throws SnowflakeSQLException {
    return resolveSession(connection).getSessionToken();
  }

  /**
   * Extracts the server URL (e.g. {@code https://acct.snowflakecomputing.com}) from an established
   * Snowflake connection, used to derive the default token endpoint when no explicit host/account
   * is supplied.
   *
   * @param connection a {@link net.snowflake.client.api.connection.SnowflakeConnection}
   * @return the server URL, or null if it is not set on the session
   * @throws SnowflakeSQLException if the connection type is unsupported or has no active session
   */
  public static String extractServerUrl(
      net.snowflake.client.api.connection.SnowflakeConnection connection)
      throws SnowflakeSQLException {
    return resolveSession(connection).getServerUrl();
  }

  private static net.snowflake.client.internal.core.SFSession resolveSession(
      net.snowflake.client.api.connection.SnowflakeConnection connection)
      throws SnowflakeSQLException {
    if (connection
        instanceof
        net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl) {
      return ((net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl)
              connection)
          .getSfSession();
    }
    throw new SnowflakeSQLException(
        ErrorCode.INVALID_PARAMETER_VALUE,
        connection.getClass().getName(),
        "connection (a SnowflakeConnectionImpl is required for the session-token mode)");
  }
}
