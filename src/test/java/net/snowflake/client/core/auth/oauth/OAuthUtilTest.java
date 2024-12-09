package net.snowflake.client.core.auth.oauth;

import java.net.URI;
import net.snowflake.client.core.SFOauthLoginInput;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OAuthUtilTest {

  private static final String BASE_SERVER_URL_FROM_LOGIN_INPUT = "http://some.snowflake.server.com";
  public static final String ROLE_FROM_LOGIN_INPUT = "ANALYST";

  @Test
  public void shouldCreateDefaultAuthorizationUrl() {
    SFOauthLoginInput loginInput = createLoginInputStub(null, null, null);
    URI authorizationUrl =
        OAuthUtil.getAuthorizationUrl(loginInput, BASE_SERVER_URL_FROM_LOGIN_INPUT);
    Assertions.assertNotNull(authorizationUrl);
    Assertions.assertEquals(
        "http://some.snowflake.server.com/oauth/authorize", authorizationUrl.toString());
  }

  @Test
  public void shouldCreateUserSuppliedAuthorizationUrl() {
    SFOauthLoginInput loginInput =
        createLoginInputStub("http://some.external.authorization.url.com/authz", null, null);
    URI tokenRequestUrl =
        OAuthUtil.getAuthorizationUrl(loginInput, BASE_SERVER_URL_FROM_LOGIN_INPUT);
    Assertions.assertNotNull(tokenRequestUrl);
    Assertions.assertEquals(
        "http://some.external.authorization.url.com/authz", tokenRequestUrl.toString());
  }

  @Test
  public void shouldCreateDefaultTokenRequestUrl() {
    SFOauthLoginInput loginInput = createLoginInputStub(null, null, null);
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
            null, "http://some.external.authorization.url.com/token-request", null);
    URI tokenRequestUrl =
        OAuthUtil.getTokenRequestUrl(loginInput, BASE_SERVER_URL_FROM_LOGIN_INPUT);
    Assertions.assertNotNull(tokenRequestUrl);
    Assertions.assertEquals(
        "http://some.external.authorization.url.com/token-request", tokenRequestUrl.toString());
  }

  @Test
  public void shouldCreateDefaultScope() {
    SFOauthLoginInput loginInput = createLoginInputStub(null, null, null);
    String scope = OAuthUtil.getScope(loginInput, ROLE_FROM_LOGIN_INPUT);
    Assertions.assertNotNull(scope);
    Assertions.assertEquals("session:role:ANALYST", scope);
  }

  @Test
  public void shouldCreateUserSuppliedScope() {
    SFOauthLoginInput loginInput = createLoginInputStub(null, null, "some:custom:SCOPE");
    String scope = OAuthUtil.getScope(loginInput, ROLE_FROM_LOGIN_INPUT);
    Assertions.assertNotNull(scope);
    Assertions.assertEquals("some:custom:SCOPE", scope);
  }

  private SFOauthLoginInput createLoginInputStub(
      String externalAuthorizationUrl, String externalTokenRequestUrl, String scope) {
    return new SFOauthLoginInput(
        null, null, null, externalAuthorizationUrl, externalTokenRequestUrl, scope);
  }
}
