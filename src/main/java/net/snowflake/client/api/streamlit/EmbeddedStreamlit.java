package net.snowflake.client.api.streamlit;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import net.snowflake.client.api.connection.SnowflakeConnection;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.auth.oauth.StreamlitEmbedTokenExchange;
import net.snowflake.client.internal.core.auth.oauth.StreamlitEmbedTokenResponseDTO;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

/**
 * Generates a Streamlit-in-Snowflake (SiS) embed URL for embedding a SiS app in a 3rd-party web
 * page, without needing an interactive SQL session.
 *
 * <p>This is the native-driver replacement for the {@code SYSTEM$STREAMLIT_GENERATE_EMBED_URL} SQL
 * system function. It calls Snowflake's OAuth 2.0 token-exchange endpoint ({@code POST
 * /oauth/token}) with a service credential and the {@code session:streamlit:<fqn>} scope, then
 * assembles the final embed URL from the returned single-use authorization code.
 *
 * <p><b>This is a credential-only side-channel utility:</b> it is intended for customer backends
 * (for example {@code analytics.example.com}) that mint a short-lived embed URL using a service
 * credential (PAT / key-pair / session token) and hand it to a browser. It deliberately does not
 * require a JDBC {@code Connection} (although one may be supplied for the session-token mode).
 *
 * <p><b>Example usage:</b>
 *
 * <pre>{@code
 * String embedUrl =
 *     EmbeddedStreamlit.forApp("MY_DB.MY_SCHEMA.MY_APP", "https://analytics.example.com")
 *         .withAccount("myaccount")
 *         .withPat(programmaticAccessToken)
 *         .getEmbedUrl();
 * }</pre>
 *
 * <p><b>Security:</b> the supplied credential and the authorization code are secrets. They are
 * never logged at info level.
 *
 * <p><b>Status:</b> DRAFT. Depends on the Global Services server-side token-exchange handler for
 * the {@code session:streamlit:<fqn>} scope, which is not yet in production.
 */
public final class EmbeddedStreamlit {

  private static final SFLogger logger = SFLoggerFactory.getLogger(EmbeddedStreamlit.class);

  private static final String SCOPE_PREFIX = "session:streamlit:";
  private static final String EMBEDDED_APP_PARAM = "__embeddedApp";
  private static final String PARENT_ORIGIN_PARAM = "__parentOrigin";

  /** {@code subject_token_type} for a PAT (Programmatic Access Token). */
  public static final String SUBJECT_TOKEN_TYPE_PAT = "programmatic_access_token";

  /** {@code subject_token_type} for a Snowflake session token. */
  public static final String SUBJECT_TOKEN_TYPE_SESSION = "urn:snowflake:token-type:session";

  /**
   * Provisional {@code subject_token_type} for a key-pair JWT. There is no GS enum value for this
   * yet; this default is overridable via {@link #withSubjectTokenType(String)} pending the GS
   * keypair extension.
   */
  public static final String SUBJECT_TOKEN_TYPE_KEYPAIR_JWT_PROVISIONAL =
      "urn:ietf:params:oauth:token-type:jwt";

  private static final String SNOWFLAKE_HOST_SUFFIX = ".snowflakecomputing.com";

  private final String streamlitId;
  private final String parentOrigin;

  // Credential state (at most one credential mode is expected to be set).
  private String subjectToken;
  private String subjectTokenType;

  private String account;
  private String host;
  private String tokenEndpoint;

  private StreamlitEmbedTokenExchange.HttpSender httpSender =
      StreamlitEmbedTokenExchange.defaultSender();

  private StreamlitEmbedTokenResponseDTO lastResponse;

  private EmbeddedStreamlit(String streamlitId, String parentOrigin) {
    this.streamlitId = streamlitId;
    this.parentOrigin = parentOrigin;
  }

  /**
   * Creates a builder for the given Streamlit app.
   *
   * @param streamlitId the fully-qualified app name ({@code db.schema.app}); dots only, must not
   *     contain a {@code ':'}
   * @param parentOrigin the origin of the embedding page (e.g. {@code
   *     https://analytics.example.com})
   * @return a new builder
   */
  public static EmbeddedStreamlit forApp(String streamlitId, String parentOrigin) {
    return new EmbeddedStreamlit(streamlitId, parentOrigin);
  }

  // ---------------------------------------------------------------------------
  // Credential modes
  // ---------------------------------------------------------------------------

  /**
   * Use a Programmatic Access Token (PAT) as the service credential.
   *
   * @param pat the PAT value (secret)
   * @return this builder
   */
  public EmbeddedStreamlit withPat(String pat) {
    this.subjectToken = pat;
    if (this.subjectTokenType == null) {
      this.subjectTokenType = SUBJECT_TOKEN_TYPE_PAT;
    }
    return this;
  }

  /**
   * Use the session token of an established Snowflake connection as the service credential. The
   * server URL of the connection is also used to derive the default token endpoint when no explicit
   * host/account is supplied.
   *
   * @param conn an established {@link SnowflakeConnection}
   * @return this builder
   * @throws SQLException if the session token or server URL cannot be read from the connection
   */
  public EmbeddedStreamlit withConnection(SnowflakeConnection conn) throws SQLException {
    this.subjectToken = StreamlitEmbedTokenExchange.extractSessionToken(conn);
    if (this.subjectTokenType == null) {
      this.subjectTokenType = SUBJECT_TOKEN_TYPE_SESSION;
    }
    if (this.host == null && this.account == null && this.tokenEndpoint == null) {
      String serverUrl = StreamlitEmbedTokenExchange.extractServerUrl(conn);
      if (serverUrl != null) {
        this.host = serverUrl;
      }
    }
    return this;
  }

  /**
   * Use a pre-signed key-pair JWT as the service credential.
   *
   * <p>The driver does not sign the JWT here; pass a JWT already issued for the target account/user
   * (see {@code SessionUtilKeyPair#issueJwtToken}). The {@code subject_token_type} defaults to the
   * provisional {@link #SUBJECT_TOKEN_TYPE_KEYPAIR_JWT_PROVISIONAL} and may be overridden via
   * {@link #withSubjectTokenType(String)}.
   *
   * @param jwt the pre-signed JWT (secret)
   * @return this builder
   */
  public EmbeddedStreamlit withKeyPairJwt(String jwt) {
    this.subjectToken = jwt;
    if (this.subjectTokenType == null) {
      this.subjectTokenType = SUBJECT_TOKEN_TYPE_KEYPAIR_JWT_PROVISIONAL;
    }
    return this;
  }

  // ---------------------------------------------------------------------------
  // Endpoint / token-type configuration
  // ---------------------------------------------------------------------------

  /**
   * Sets the Snowflake account (used to derive the default token endpoint host as {@code
   * <account>.snowflakecomputing.com}). Ignored if {@link #withHost(String)} or {@link
   * #withTokenEndpoint(String)} is supplied.
   *
   * @param account the account identifier
   * @return this builder
   */
  public EmbeddedStreamlit withAccount(String account) {
    this.account = account;
    return this;
  }

  /**
   * Sets the full Snowflake host (e.g. {@code https://acct.snowflakecomputing.com} or {@code
   * acct.snowflakecomputing.com}). Takes precedence over {@link #withAccount(String)}.
   *
   * @param host the host
   * @return this builder
   */
  public EmbeddedStreamlit withHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Overrides the {@code subject_token_type} URN. Useful for the key-pair mode (whose default is
   * provisional) and for forward-compatibility (pass an explicit {@code subject_token} + {@code
   * subject_token_type} pair).
   *
   * @param subjectTokenType the {@code subject_token_type} URN
   * @return this builder
   */
  public EmbeddedStreamlit withSubjectTokenType(String subjectTokenType) {
    this.subjectTokenType = subjectTokenType;
    return this;
  }

  /**
   * Escape hatch: set an explicit {@code subject_token} together with its {@code
   * subject_token_type}. Use this for forward-compatibility with credential types the builder does
   * not model directly.
   *
   * @param subjectToken the credential value (secret)
   * @param subjectTokenType the {@code subject_token_type} URN
   * @return this builder
   */
  public EmbeddedStreamlit withSubjectToken(String subjectToken, String subjectTokenType) {
    this.subjectToken = subjectToken;
    this.subjectTokenType = subjectTokenType;
    return this;
  }

  /**
   * Overrides the absolute token endpoint URL. Primarily for tests (inject a fake server) and for
   * non-standard deployments. When unset, the endpoint is derived from the host/account.
   *
   * @param tokenEndpoint the absolute token endpoint URL
   * @return this builder
   */
  public EmbeddedStreamlit withTokenEndpoint(String tokenEndpoint) {
    this.tokenEndpoint = tokenEndpoint;
    return this;
  }

  /**
   * Injects the HTTP sender used to perform the token-exchange call. Intended for unit tests.
   *
   * @param httpSender the HTTP seam
   * @return this builder
   */
  EmbeddedStreamlit withHttpSender(StreamlitEmbedTokenExchange.HttpSender httpSender) {
    this.httpSender = httpSender;
    return this;
  }

  // ---------------------------------------------------------------------------
  // Terminal operations
  // ---------------------------------------------------------------------------

  /**
   * Performs the token-exchange and returns the assembled embed URL.
   *
   * @return the final embed URL
   * @throws SQLException if configuration is invalid, the server call fails, or the response is
   *     malformed
   */
  public String getEmbedUrl() throws SQLException {
    validate();
    String scope = buildScope(streamlitId);
    URI endpoint = resolveTokenEndpoint();

    StreamlitEmbedTokenResponseDTO response =
        StreamlitEmbedTokenExchange.exchange(
            endpoint, subjectToken, subjectTokenType, scope, httpSender);
    this.lastResponse = response;

    String code = extractAuthorizationCode(response.getRedirectUri());
    String embedUrl = assembleEmbedUrl(response.getRedirectUri(), code, parentOrigin);
    logger.info("Generated Streamlit embed URL for app: {}", streamlitId);
    return embedUrl;
  }

  /**
   * The validity window (in seconds) of the most recently generated embed URL.
   *
   * <p>{@link #getEmbedUrl()} must be called first (it performs the token-exchange that returns the
   * {@code expires_in} value). Calling this before {@code getEmbedUrl()} throws {@link
   * IllegalStateException}.
   *
   * @return seconds until the generated embed URL expires
   * @throws IllegalStateException if {@link #getEmbedUrl()} has not been called yet
   */
  public long getExpiresInSeconds() {
    if (lastResponse == null) {
      throw new IllegalStateException("getEmbedUrl() must be called before getExpiresInSeconds()");
    }
    return lastResponse.getExpiresIn();
  }

  private void validate() throws SnowflakeSQLException {
    if (isBlank(streamlitId)) {
      throw new SnowflakeSQLException(
          ErrorCode.INVALID_PARAMETER_VALUE, "<missing>", "streamlitId");
    }
    if (streamlitId.indexOf(':') >= 0) {
      // The FQN is not a credential, so it is safe to surface its value.
      throw new SnowflakeSQLException(
          ErrorCode.INVALID_PARAMETER_VALUE,
          streamlitId,
          "streamlitId (must be a dotted FQN without ':')");
    }
    if (isBlank(parentOrigin)) {
      throw new SnowflakeSQLException(
          ErrorCode.INVALID_PARAMETER_VALUE, "<missing>", "parentOrigin");
    }
    if (isBlank(subjectToken)) {
      // Never echo the credential value; report only that it is missing.
      throw new SnowflakeSQLException(
          ErrorCode.STREAMLIT_EMBED_URL_ERROR,
          "a credential is required: call withPat / withConnection / withKeyPairJwt / withSubjectToken");
    }
    if (isBlank(subjectTokenType)) {
      throw new SnowflakeSQLException(
          ErrorCode.INVALID_PARAMETER_VALUE, "<missing>", "subject_token_type");
    }
  }

  /** Builds the {@code session:streamlit:<fqn>} scope string. */
  static String buildScope(String streamlitId) {
    return SCOPE_PREFIX + streamlitId;
  }

  private URI resolveTokenEndpoint() throws SnowflakeSQLException {
    String raw;
    if (!isBlank(tokenEndpoint)) {
      raw = tokenEndpoint;
    } else if (!isBlank(host)) {
      raw = normalizeHost(host) + StreamlitEmbedTokenExchange.DEFAULT_TOKEN_ENDPOINT_PATH;
    } else if (!isBlank(account)) {
      raw =
          "https://"
              + account
              + SNOWFLAKE_HOST_SUFFIX
              + StreamlitEmbedTokenExchange.DEFAULT_TOKEN_ENDPOINT_PATH;
    } else {
      throw new SnowflakeSQLException(
          ErrorCode.INVALID_PARAMETER_VALUE,
          "<none>",
          "tokenEndpoint (supply withTokenEndpoint, withHost, withAccount, or withConnection)");
    }
    try {
      return new URI(raw);
    } catch (URISyntaxException e) {
      throw new SnowflakeSQLException(
          e, ErrorCode.STREAMLIT_EMBED_URL_ERROR, "invalid token endpoint URI: " + raw);
    }
  }

  /** Ensures the host carries a scheme; defaults to https when none is present. */
  private static String normalizeHost(String host) {
    String trimmed = host.trim();
    if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
      // Strip any trailing slash so the endpoint path joins cleanly.
      return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }
    return "https://"
        + (trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed);
  }

  /**
   * Extracts the single-use authorization code from the redirect URI. Per the wire contract, the
   * code may be carried as a fragment ({@code #code=...}) or as a query parameter ({@code
   * ?code=...} / {@code &code=...}). The fragment form is checked first.
   *
   * @param redirectUri the redirect URI returned by the server
   * @return the authorization code (secret)
   * @throws SnowflakeSQLException if no code can be located
   */
  static String extractAuthorizationCode(String redirectUri) throws SnowflakeSQLException {
    URI uri;
    try {
      uri = new URI(redirectUri);
    } catch (URISyntaxException e) {
      // Do not echo redirect_uri: it may carry the single-use authorization code (a secret).
      throw new SnowflakeSQLException(
          e,
          ErrorCode.STREAMLIT_EMBED_URL_ERROR,
          "redirect_uri returned by the server was not a valid URI");
    }

    String code = findParam(uri.getRawFragment(), "code");
    if (code == null) {
      code = findParam(uri.getRawQuery(), "code");
    }
    if (isBlank(code)) {
      throw new SnowflakeSQLException(
          ErrorCode.STREAMLIT_EMBED_URL_ERROR,
          "redirect_uri returned by the server did not contain an authorization code");
    }
    return code;
  }

  /**
   * Finds {@code key}'s value within an {@code &}-separated parameter string (raw, not decoded).
   */
  private static String findParam(String paramString, String key) {
    if (isBlank(paramString)) {
      return null;
    }
    for (String pair : paramString.split("&")) {
      int eq = pair.indexOf('=');
      if (eq < 0) {
        continue;
      }
      if (pair.substring(0, eq).equals(key)) {
        return pair.substring(eq + 1);
      }
    }
    return null;
  }

  /**
   * Reproduces the proven {@code SYSTEM$STREAMLIT_GENERATE_EMBED_URL} output format:
   *
   * <pre>
   *   base + "?__parentOrigin=" + urlencode(parentOrigin) + "&__embeddedApp=true" + "#code=" + code
   * </pre>
   *
   * <p>where {@code base} is {@code scheme://host[:port]/path} of the redirect URI with the code
   * and any pre-existing {@code __embeddedApp} / {@code __parentOrigin} params removed. If the base
   * already carries query params, the new params are appended with {@code &}.
   */
  static String assembleEmbedUrl(String redirectUri, String code, String parentOrigin)
      throws SnowflakeSQLException {
    URI uri;
    try {
      uri = new URI(redirectUri);
    } catch (URISyntaxException e) {
      // Do not echo redirect_uri: it may carry the single-use authorization code (a secret).
      throw new SnowflakeSQLException(
          e,
          ErrorCode.STREAMLIT_EMBED_URL_ERROR,
          "redirect_uri returned by the server was not a valid URI");
    }

    StringBuilder base = new StringBuilder();
    if (uri.getScheme() != null) {
      base.append(uri.getScheme()).append("://");
    }
    if (uri.getRawAuthority() != null) {
      base.append(uri.getRawAuthority());
    }
    if (uri.getRawPath() != null) {
      base.append(uri.getRawPath());
    }

    String retainedQuery =
        stripParams(uri.getRawQuery(), "code", EMBEDDED_APP_PARAM, PARENT_ORIGIN_PARAM);

    StringBuilder url = new StringBuilder(base);
    if (!isBlank(retainedQuery)) {
      url.append('?').append(retainedQuery).append('&');
    } else {
      url.append('?');
    }
    url.append(PARENT_ORIGIN_PARAM)
        .append('=')
        .append(urlEncode(parentOrigin))
        .append('&')
        .append(EMBEDDED_APP_PARAM)
        .append("=true")
        .append("#code=")
        .append(code);
    return url.toString();
  }

  /** Removes the given keys from an {@code &}-separated raw query string, preserving order. */
  private static String stripParams(String rawQuery, String... keysToRemove) {
    if (isBlank(rawQuery)) {
      return null;
    }
    StringBuilder kept = new StringBuilder();
    outer:
    for (String pair : rawQuery.split("&")) {
      int eq = pair.indexOf('=');
      String key = eq < 0 ? pair : pair.substring(0, eq);
      for (String remove : keysToRemove) {
        if (key.equals(remove)) {
          continue outer;
        }
      }
      if (kept.length() > 0) {
        kept.append('&');
      }
      kept.append(pair);
    }
    return kept.length() == 0 ? null : kept.toString();
  }

  private static String urlEncode(String value) {
    try {
      return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8.name());
    } catch (java.io.UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  private static boolean isBlank(String s) {
    return s == null || s.trim().isEmpty();
  }
}
