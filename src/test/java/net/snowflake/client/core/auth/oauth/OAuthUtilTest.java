package net.snowflake.client.core.auth.oauth;

import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import net.snowflake.client.core.SFOauthLoginInput;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OAuthUtilTest {

  private static final String BASE_SERVER_URL_FROM_LOGIN_INPUT = "http://some.snowflake.server.com";
  public static final String ROLE_FROM_LOGIN_INPUT = "ANALYST";

  @Test
  public void shouldCreateDefaultAuthorizationUrl() {
    SFOauthLoginInput loginInput = createLoginInputStub(null, null, null, null);
    URI authorizationUrl =
        OAuthUtil.getAuthorizationUrl(loginInput, BASE_SERVER_URL_FROM_LOGIN_INPUT);
    Assertions.assertNotNull(authorizationUrl);
    Assertions.assertEquals(
        "http://some.snowflake.server.com/oauth/authorize", authorizationUrl.toString());
  }

  @Test
  public void shouldCreateUserSuppliedAuthorizationUrl() {
    SFOauthLoginInput loginInput =
        createLoginInputStub("http://some.external.authorization.url.com/authz", null, null, null);
    URI tokenRequestUrl =
        OAuthUtil.getAuthorizationUrl(loginInput, BASE_SERVER_URL_FROM_LOGIN_INPUT);
    Assertions.assertNotNull(tokenRequestUrl);
    Assertions.assertEquals(
        "http://some.external.authorization.url.com/authz", tokenRequestUrl.toString());
  }

  @Test
  public void shouldCreateDefaultTokenRequestUrl() {
    SFOauthLoginInput loginInput = createLoginInputStub(null, null, null, null);
    URI tokenRequestUrl =
        OAuthUtil.getTokenRequestUrl(loginInput, BASE_SERVER_URL_FROM_LOGIN_INPUT);
    Assertions.assertNotNull(tokenRequestUrl);
    Assertions.assertEquals(
        "http://some.snowflake.server.com/oauth/token-request", tokenRequestUrl.toString());
  }

  @Test
  public void shouldCreateUserSuppliedTokenRequestUrl() {
    SFOauthLoginInput loginInput =
        createLoginInputStub(
            null, "http://some.external.authorization.url.com/token-request", null, null);
    URI tokenRequestUrl =
        OAuthUtil.getTokenRequestUrl(loginInput, BASE_SERVER_URL_FROM_LOGIN_INPUT);
    Assertions.assertNotNull(tokenRequestUrl);
    Assertions.assertEquals(
        "http://some.external.authorization.url.com/token-request", tokenRequestUrl.toString());
  }

  @Test
  public void shouldCreateDefaultScope() {
    SFOauthLoginInput loginInput = createLoginInputStub(null, null, null, null);
    String scope = OAuthUtil.getScope(loginInput, ROLE_FROM_LOGIN_INPUT);
    Assertions.assertNotNull(scope);
    Assertions.assertEquals("session:role:ANALYST", scope);
  }

  @Test
  public void shouldCreateUserSuppliedScope() {
    SFOauthLoginInput loginInput = createLoginInputStub(null, null, "some:custom:SCOPE", null);
    String scope = OAuthUtil.getScope(loginInput, ROLE_FROM_LOGIN_INPUT);
    Assertions.assertNotNull(scope);
    Assertions.assertEquals("some:custom:SCOPE", scope);
  }

  @Test
  public void shouldCreateDefaultRedirectUri() throws IOException {
    SFOauthLoginInput loginInput = createLoginInputStub(null, null, null, null);
    URI redirectUri = OAuthUtil.buildRedirectUri(loginInput);
    Assertions.assertNotNull(redirectUri);
    Assertions.assertTrue(
        redirectUri.toString().matches("^http://127.0.0.1:([0-9]*)/"),
        "Invalid redirect URI: " + redirectUri);
  }

  @Test
  public void shouldCreateCustomRedirectUri() throws IOException {
    SFOauthLoginInput loginInput =
        createLoginInputStub(null, null, null, "http://localhost:8989/some-endpoint");
    URI redirectUri = OAuthUtil.buildRedirectUri(loginInput);
    Assertions.assertNotNull(redirectUri);
    Assertions.assertEquals("http://localhost:8989/some-endpoint", redirectUri.toString());
  }

  @Test
  public void shouldConvertToBaseAuthorizationRequest() throws IOException {
    HTTPRequest httpRequest =
        new HTTPRequest(
            HTTPRequest.Method.POST, URI.create("https://some.snowflake.server.com/oauth/token"));
    httpRequest.setAccept("application/json");
    httpRequest.setAuthorization("Bearer some-token");
    httpRequest.setBody("{\"grant_type\":\"authorization_code\",\"code\":\"some-code\"}");

    HttpRequestBase httpRequestBase = OAuthUtil.convertToBaseRequest(httpRequest);
    Assertions.assertNotNull(httpRequestBase);
    Assertions.assertEquals(HttpPost.class, httpRequestBase.getClass());
    Assertions.assertEquals(
        "https://some.snowflake.server.com/oauth/token", httpRequestBase.getURI().toString());
    Header[] allHeaders = httpRequestBase.getAllHeaders();
    Assertions.assertNotNull(allHeaders);
    Assertions.assertEquals(2, allHeaders.length);
    Assertions.assertEquals("Accept", allHeaders[0].getName());
    Assertions.assertEquals("application/json", allHeaders[0].getValue());
    Assertions.assertEquals("Authorization", allHeaders[1].getName());
    Assertions.assertEquals("Bearer some-token", allHeaders[1].getValue());

    StringWriter writer = new StringWriter();
    try (InputStream ins = ((HttpPost) httpRequestBase).getEntity().getContent()) {
      IOUtils.copy(ins, writer, "UTF-8");
    }
    Assertions.assertEquals(
        "{\"grant_type\":\"authorization_code\",\"code\":\"some-code\"}", writer.toString());
  }

  private SFOauthLoginInput createLoginInputStub(
      String oauthAuthorizationUrl, String oauthTokenRequestUrl, String scope, String redirectUri) {
    return new SFOauthLoginInput(
        null, null, redirectUri, oauthAuthorizationUrl, oauthTokenRequestUrl, scope);
  }
}
