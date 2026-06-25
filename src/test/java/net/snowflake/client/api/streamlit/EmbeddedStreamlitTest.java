package net.snowflake.client.api.streamlit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.TreeMap;
import net.snowflake.client.api.connection.SnowflakeConnection;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl;
import net.snowflake.client.internal.core.HttpClientSettingsKey;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.core.auth.oauth.StreamlitEmbedTokenExchange;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link EmbeddedStreamlit}. The HTTP layer is stubbed via {@link
 * StreamlitEmbedTokenExchange.HttpSender} so no live server is required.
 */
public class EmbeddedStreamlitTest {

  private static final String FQN = "MY_DB.MY_SCHEMA.MY_APP";
  private static final String PARENT_ORIGIN = "https://analytics.example.com";
  private static final String ENDPOINT = "https://acct.snowflakecomputing.com/oauth/token";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** A stub sender that records the request and returns a canned JSON body. */
  private static final class RecordingSender implements StreamlitEmbedTokenExchange.HttpSender {
    private final String responseBody;
    HttpPost capturedRequest;
    String capturedBody;

    RecordingSender(String responseBody) {
      this.responseBody = responseBody;
    }

    @Override
    public String send(HttpPost request, HttpClientSettingsKey clientKey)
        throws SnowflakeSQLException {
      this.capturedRequest = request;
      try {
        this.capturedBody = EntityUtils.toString(request.getEntity());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return responseBody;
    }
  }

  private static String responseJson(String redirectUri, long expiresIn) {
    return "{\"redirect_uri\":\"" + redirectUri + "\",\"expires_in\":" + expiresIn + "}";
  }

  /** Parses the captured x-www-form-urlencoded body into a map (values URL-decoded). */
  private static Map<String, String> parseForm(String body) throws Exception {
    Map<String, String> result = new TreeMap<>();
    for (String pair : body.split("&")) {
      int eq = pair.indexOf('=');
      String k = java.net.URLDecoder.decode(pair.substring(0, eq), "UTF-8");
      String v = java.net.URLDecoder.decode(pair.substring(eq + 1), "UTF-8");
      result.put(k, v);
    }
    return result;
  }

  // ---------------------------------------------------------------------------
  // Credential modes -> subject_token / subject_token_type mapping + scope
  // ---------------------------------------------------------------------------

  @Test
  public void patModeSendsCorrectSubjectTokenTypeAndScope() throws Exception {
    RecordingSender sender =
        new RecordingSender(
            responseJson("https://acct.snowflakecomputing.com/app#code=AZCODE", 60));

    EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN)
        .withTokenEndpoint(ENDPOINT)
        .withPat("my-pat-secret")
        .withHttpSender(sender)
        .getEmbedUrl();

    Map<String, String> form = parseForm(sender.capturedBody);
    assertEquals("urn:ietf:params:oauth:grant-type:token-exchange", form.get("grant_type"));
    assertEquals("my-pat-secret", form.get("subject_token"));
    assertEquals("programmatic_access_token", form.get("subject_token_type"));
    assertEquals("session:streamlit:MY_DB.MY_SCHEMA.MY_APP", form.get("scope"));
    assertEquals(ENDPOINT, sender.capturedRequest.getURI().toString());
    assertEquals("application/json", sender.capturedRequest.getFirstHeader("Accept").getValue());
    // Content-Type must carry the UTF-8 charset per the wire contract.
    assertEquals(
        "application/x-www-form-urlencoded; charset=UTF-8",
        sender.capturedRequest.getEntity().getContentType().getValue());
    // The streamlit embed token-exchange has no OAuth client identity: no Authorization header.
    assertNull(sender.capturedRequest.getFirstHeader("Authorization"));
  }

  @Test
  public void keyPairModeUsesProvisionalJwtTokenTypeByDefault() throws Exception {
    RecordingSender sender =
        new RecordingSender(responseJson("https://acct.snowflakecomputing.com/app#code=AZ", 60));

    EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN)
        .withTokenEndpoint(ENDPOINT)
        .withKeyPairJwt("signed.jwt.token")
        .withHttpSender(sender)
        .getEmbedUrl();

    Map<String, String> form = parseForm(sender.capturedBody);
    assertEquals("signed.jwt.token", form.get("subject_token"));
    assertEquals("urn:ietf:params:oauth:token-type:jwt", form.get("subject_token_type"));
  }

  @Test
  public void subjectTokenTypeIsOverridable() throws Exception {
    RecordingSender sender =
        new RecordingSender(responseJson("https://acct.snowflakecomputing.com/app#code=AZ", 60));

    EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN)
        .withTokenEndpoint(ENDPOINT)
        .withSubjectTokenType("urn:snowflake:token-type:wif")
        .withPat("ignored-type-but-token-used")
        .withHttpSender(sender)
        .getEmbedUrl();

    Map<String, String> form = parseForm(sender.capturedBody);
    assertEquals("urn:snowflake:token-type:wif", form.get("subject_token_type"));
  }

  @Test
  public void explicitSubjectTokenEscapeHatch() throws Exception {
    RecordingSender sender =
        new RecordingSender(responseJson("https://acct.snowflakecomputing.com/app#code=AZ", 60));

    EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN)
        .withTokenEndpoint(ENDPOINT)
        .withSubjectToken("raw-cred", "some:future:type")
        .withHttpSender(sender)
        .getEmbedUrl();

    Map<String, String> form = parseForm(sender.capturedBody);
    assertEquals("raw-cred", form.get("subject_token"));
    assertEquals("some:future:type", form.get("subject_token_type"));
  }

  @Test
  public void sessionTokenTypeConstantMapping() {
    assertEquals("urn:snowflake:token-type:session", EmbeddedStreamlit.SUBJECT_TOKEN_TYPE_SESSION);
    assertEquals("programmatic_access_token", EmbeddedStreamlit.SUBJECT_TOKEN_TYPE_PAT);
  }

  @Test
  public void scopeStringIsBuiltFromFqn() {
    assertEquals("session:streamlit:MY_DB.MY_SCHEMA.MY_APP", EmbeddedStreamlit.buildScope(FQN));
  }

  // ---------------------------------------------------------------------------
  // Authorization-code extraction: fragment form AND query form
  // ---------------------------------------------------------------------------

  @Test
  public void extractsCodeFromFragment() throws Exception {
    assertEquals(
        "AZCODE123",
        EmbeddedStreamlit.extractAuthorizationCode(
            "https://acct.snowflakecomputing.com/app/path#code=AZCODE123"));
  }

  @Test
  public void extractsCodeFromQuery() throws Exception {
    assertEquals(
        "AZCODE456",
        EmbeddedStreamlit.extractAuthorizationCode(
            "https://acct.snowflakecomputing.com/app/path?code=AZCODE456"));
  }

  @Test
  public void extractsCodeFromQueryWhenNotFirstParam() throws Exception {
    assertEquals(
        "AZCODE789",
        EmbeddedStreamlit.extractAuthorizationCode(
            "https://acct.snowflakecomputing.com/app/path?foo=bar&code=AZCODE789"));
  }

  @Test
  public void fragmentTakesPrecedenceOverQuery() throws Exception {
    assertEquals(
        "FROM_FRAGMENT",
        EmbeddedStreamlit.extractAuthorizationCode(
            "https://acct.snowflakecomputing.com/app?code=FROM_QUERY#code=FROM_FRAGMENT"));
  }

  @Test
  public void missingCodeThrows() {
    SnowflakeSQLException ex =
        assertThrows(
            SnowflakeSQLException.class,
            () ->
                EmbeddedStreamlit.extractAuthorizationCode(
                    "https://acct.snowflakecomputing.com/app/path"));
    assertTrue(ex.getMessage().toLowerCase().contains("authorization code"));
  }

  // ---------------------------------------------------------------------------
  // URL assembly
  // ---------------------------------------------------------------------------

  @Test
  public void assemblesEmbedUrlFromCleanBase() throws Exception {
    String url =
        EmbeddedStreamlit.assembleEmbedUrl(
            "https://acct.snowflakecomputing.com/app/path#code=AZ", "AZ", PARENT_ORIGIN);
    assertEquals(
        "https://acct.snowflakecomputing.com/app/path"
            + "?__parentOrigin=https%3A%2F%2Fanalytics.example.com"
            + "&__embeddedApp=true#code=AZ",
        url);
  }

  @Test
  public void assembleStripsCodeFromQueryBase() throws Exception {
    // redirect_uri carries the code in the query; base must drop it.
    String url =
        EmbeddedStreamlit.assembleEmbedUrl(
            "https://acct.snowflakecomputing.com/app?code=AZ", "AZ", PARENT_ORIGIN);
    assertEquals(
        "https://acct.snowflakecomputing.com/app"
            + "?__parentOrigin=https%3A%2F%2Fanalytics.example.com"
            + "&__embeddedApp=true#code=AZ",
        url);
  }

  @Test
  public void assemblePreservesPreexistingQueryAndStripsManagedParams() throws Exception {
    // Base already has a query param (keep), plus stale managed params (drop), plus code (drop).
    String url =
        EmbeddedStreamlit.assembleEmbedUrl(
            "https://acct.snowflakecomputing.com/app?foo=bar&__embeddedApp=true&__parentOrigin=old&code=AZ",
            "AZ",
            PARENT_ORIGIN);
    assertEquals(
        "https://acct.snowflakecomputing.com/app"
            + "?foo=bar"
            + "&__parentOrigin=https%3A%2F%2Fanalytics.example.com"
            + "&__embeddedApp=true#code=AZ",
        url);
  }

  @Test
  public void endToEndProducesEmbedUrlAndExpiry() throws Exception {
    RecordingSender sender =
        new RecordingSender(
            responseJson("https://acct.snowflakecomputing.com/app#code=THECODE", 120));

    EmbeddedStreamlit builder =
        EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN)
            .withTokenEndpoint(ENDPOINT)
            .withPat("pat")
            .withHttpSender(sender);
    String url = builder.getEmbedUrl();

    assertEquals(
        "https://acct.snowflakecomputing.com/app"
            + "?__parentOrigin=https%3A%2F%2Fanalytics.example.com"
            + "&__embeddedApp=true#code=THECODE",
        url);
    assertEquals(120L, builder.getExpiresInSeconds());
  }

  // ---------------------------------------------------------------------------
  // Endpoint derivation from account / host
  // ---------------------------------------------------------------------------

  @Test
  public void endpointDerivedFromAccount() throws Exception {
    RecordingSender sender =
        new RecordingSender(responseJson("https://x.snowflakecomputing.com/a#code=C", 60));
    EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN)
        .withAccount("myaccount")
        .withPat("pat")
        .withHttpSender(sender)
        .getEmbedUrl();
    assertEquals(
        "https://myaccount.snowflakecomputing.com/oauth/token",
        sender.capturedRequest.getURI().toString());
  }

  @Test
  public void endpointDerivedFromHostWithoutScheme() throws Exception {
    RecordingSender sender =
        new RecordingSender(responseJson("https://x.snowflakecomputing.com/a#code=C", 60));
    EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN)
        .withHost("myaccount.snowflakecomputing.com")
        .withPat("pat")
        .withHttpSender(sender)
        .getEmbedUrl();
    assertEquals(
        "https://myaccount.snowflakecomputing.com/oauth/token",
        sender.capturedRequest.getURI().toString());
  }

  // ---------------------------------------------------------------------------
  // Error cases
  // ---------------------------------------------------------------------------

  @Test
  public void missingCredentialThrows() {
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN).withTokenEndpoint(ENDPOINT).getEmbedUrl());
  }

  @Test
  public void missingStreamlitIdThrows() {
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            EmbeddedStreamlit.forApp("", PARENT_ORIGIN)
                .withTokenEndpoint(ENDPOINT)
                .withPat("pat")
                .getEmbedUrl());
  }

  @Test
  public void colonInStreamlitIdThrows() {
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            EmbeddedStreamlit.forApp("db.schema.session:streamlit:bad", PARENT_ORIGIN)
                .withTokenEndpoint(ENDPOINT)
                .withPat("pat")
                .getEmbedUrl());
  }

  @Test
  public void missingParentOriginThrows() {
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            EmbeddedStreamlit.forApp(FQN, "")
                .withTokenEndpoint(ENDPOINT)
                .withPat("pat")
                .getEmbedUrl());
  }

  @Test
  public void noEndpointSourceThrows() {
    assertThrows(
        SnowflakeSQLException.class,
        () -> EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN).withPat("pat").getEmbedUrl());
  }

  @Test
  public void nonOkResponseSurfacesAsSqlException() {
    StreamlitEmbedTokenExchange.HttpSender failing =
        (req, key) -> {
          throw new SnowflakeSQLException("bad request", "08000", 1);
        };
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN)
                .withTokenEndpoint(ENDPOINT)
                .withPat("pat")
                .withHttpSender(failing)
                .getEmbedUrl());
  }

  @Test
  public void missingRedirectUriInResponseThrows() {
    RecordingSender sender = new RecordingSender("{\"expires_in\":60}");
    SnowflakeSQLException ex =
        assertThrows(
            SnowflakeSQLException.class,
            () ->
                EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN)
                    .withTokenEndpoint(ENDPOINT)
                    .withPat("pat")
                    .withHttpSender(sender)
                    .getEmbedUrl());
    assertTrue(ex.getMessage().toLowerCase().contains("redirect_uri"));
  }

  @Test
  public void unparseableResponseThrows() {
    RecordingSender sender = new RecordingSender("not-json");
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN)
                .withTokenEndpoint(ENDPOINT)
                .withPat("pat")
                .withHttpSender(sender)
                .getEmbedUrl());
  }

  @Test
  public void dtoParsesRedirectUriAndExpiresIn() throws Exception {
    net.snowflake.client.internal.core.auth.oauth.StreamlitEmbedTokenResponseDTO dto =
        MAPPER.readValue(
            responseJson("https://acct.snowflakecomputing.com/app#code=C", 300),
            net.snowflake.client.internal.core.auth.oauth.StreamlitEmbedTokenResponseDTO.class);
    assertEquals("https://acct.snowflakecomputing.com/app#code=C", dto.getRedirectUri());
    assertEquals(300L, dto.getExpiresIn());
  }

  // ---------------------------------------------------------------------------
  // getExpiresInSeconds() lifecycle
  // ---------------------------------------------------------------------------

  @Test
  public void getExpiresInSecondsBeforeGetEmbedUrlThrows() {
    EmbeddedStreamlit builder =
        EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN).withTokenEndpoint(ENDPOINT).withPat("pat");
    assertThrows(IllegalStateException.class, builder::getExpiresInSeconds);
  }

  // ---------------------------------------------------------------------------
  // Insecure-http warn branch (still proceeds with the call)
  // ---------------------------------------------------------------------------

  @Test
  public void insecureHttpEndpointStillSendsRequest() throws Exception {
    RecordingSender sender =
        new RecordingSender(responseJson("https://acct.snowflakecomputing.com/app#code=AZ", 60));
    EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN)
        .withTokenEndpoint("http://localhost:8080/oauth/token")
        .withPat("pat")
        .withHttpSender(sender)
        .getEmbedUrl();
    assertEquals("http://localhost:8080/oauth/token", sender.capturedRequest.getURI().toString());
  }

  // ---------------------------------------------------------------------------
  // withConnection: session token extraction (Impl path) + non-Impl rejection
  // ---------------------------------------------------------------------------

  @Test
  public void withConnectionUsesSessionTokenAndServerUrl() throws Exception {
    SFSession session = mock(SFSession.class);
    when(session.getSessionToken()).thenReturn("session-secret-token");
    when(session.getServerUrl()).thenReturn("https://fromconn.snowflakecomputing.com");

    SnowflakeConnectionImpl conn = mock(SnowflakeConnectionImpl.class);
    when(conn.getSfSession()).thenReturn(session);

    RecordingSender sender =
        new RecordingSender(responseJson("https://acct.snowflakecomputing.com/app#code=AZ", 60));

    EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN)
        .withConnection(conn)
        .withHttpSender(sender)
        .getEmbedUrl();

    Map<String, String> form = parseForm(sender.capturedBody);
    assertEquals("session-secret-token", form.get("subject_token"));
    assertEquals("urn:snowflake:token-type:session", form.get("subject_token_type"));
    // The endpoint was derived from the connection's server URL (no explicit host/account given).
    assertEquals(
        "https://fromconn.snowflakecomputing.com/oauth/token",
        sender.capturedRequest.getURI().toString());
  }

  @Test
  public void withConnectionRejectsNonImplConnection() {
    SnowflakeConnection conn = mock(SnowflakeConnection.class);
    assertThrows(
        SnowflakeSQLException.class,
        () -> EmbeddedStreamlit.forApp(FQN, PARENT_ORIGIN).withConnection(conn));
  }
}
