package net.snowflake.client.core.auth.oauth;

import java.util.Arrays;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SFOauthLoginInput;
import net.snowflake.client.core.auth.AuthenticatorType;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class OAuthAccessTokenProviderFactoryTest {

  private final OAuthAccessTokenProviderFactory providerFactory =
      new OAuthAccessTokenProviderFactory(null, 30);

  @Test
  public void shouldProperlyReturnIfAuthenticatorIsEligible() {
    Arrays.stream(AuthenticatorType.values())
        .forEach(
            authenticatorType -> {
              if (authenticatorType == AuthenticatorType.OAUTH_CLIENT_CREDENTIALS
                  || authenticatorType.equals(AuthenticatorType.OAUTH_AUTHORIZATION_CODE)) {
                Assertions.assertTrue(
                    OAuthAccessTokenProviderFactory.isEligible(authenticatorType));
              } else {
                Assertions.assertFalse(
                    OAuthAccessTokenProviderFactory.isEligible(authenticatorType));
              }
            });
  }

  @Test
  public void shouldProperlyCreateClientCredentialsAccessTokenProvider() throws SFException {
    SFLoginInput loginInput = createLoginInputStub("123", "123", null, "some/url", null);
    AccessTokenProvider provider =
        providerFactory.createAccessTokenProvider(
            AuthenticatorType.OAUTH_CLIENT_CREDENTIALS, loginInput);
    Assertions.assertNotNull(provider);
    Assertions.assertInstanceOf(OAuthClientCredentialsAccessTokenProvider.class, provider);
  }

  @Test
  public void shouldProperlyCreateAuthzCodeAccessTokenProvider() throws SFException {
    SFLoginInput loginInput = createLoginInputStub("123", "123", null, null, null);
    AccessTokenProvider provider =
        providerFactory.createAccessTokenProvider(
            AuthenticatorType.OAUTH_AUTHORIZATION_CODE, loginInput);
    Assertions.assertNotNull(provider);
    Assertions.assertInstanceOf(OAuthAuthorizationCodeAccessTokenProvider.class, provider);
  }

  @Test
  public void shouldProperlyCreateAuthzCodeAccessTokenProviderForExternalIdp() throws SFException {
    SFLoginInput loginInput =
        createLoginInputStub(
            "123",
            "123",
            "https://some.ext.idp.com/authz",
            "https://some.ext.idp.com/token",
            "http://localhost:12345/authz-code");
    AccessTokenProvider provider =
        providerFactory.createAccessTokenProvider(
            AuthenticatorType.OAUTH_AUTHORIZATION_CODE, loginInput);
    Assertions.assertNotNull(provider);
    Assertions.assertInstanceOf(OAuthAuthorizationCodeAccessTokenProvider.class, provider);
  }

  @Test
  public void shouldFailToCreateClientCredentialsAccessTokenProviderWithoutClientId() {
    SFLoginInput loginInput = createLoginInputStub(null, "123", null, "some/url", null);
    SFException e =
        Assertions.assertThrows(
            SFException.class,
            () ->
                providerFactory.createAccessTokenProvider(
                    AuthenticatorType.OAUTH_CLIENT_CREDENTIALS, loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "passing oauthClientId is required for OAUTH_CLIENT_CREDENTIALS authentication."));
  }

  @Test
  public void shouldFailToCreateClientCredentialsAccessTokenProviderWithoutClientSecret() {
    SFLoginInput loginInput = createLoginInputStub("123", null, null, "some/url", null);
    SFException e =
        Assertions.assertThrows(
            SFException.class,
            () ->
                providerFactory.createAccessTokenProvider(
                    AuthenticatorType.OAUTH_CLIENT_CREDENTIALS, loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "passing oauthClientSecret is required for OAUTH_CLIENT_CREDENTIALS authentication."));
  }

  @Test
  public void shouldFailToCreateClientCredentialsAccessTokenProviderWithoutExtTokenUrl() {
    SFLoginInput loginInput = createLoginInputStub("123", "123", null, null, null);
    SFException e =
        Assertions.assertThrows(
            SFException.class,
            () ->
                providerFactory.createAccessTokenProvider(
                    AuthenticatorType.OAUTH_CLIENT_CREDENTIALS, loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "passing oauthTokenRequestUrl is required for OAUTH_CLIENT_CREDENTIALS authentication."));
  }

  @Test
  public void shouldProperlyCreateAuthorizationCodeAccessTokenProvider() throws SFException {
    SFLoginInput loginInput = createLoginInputStub("123", "123", null, null, null);
    AccessTokenProvider provider =
        providerFactory.createAccessTokenProvider(
            AuthenticatorType.OAUTH_AUTHORIZATION_CODE, loginInput);
    Assertions.assertNotNull(provider);
    Assertions.assertInstanceOf(OAuthAuthorizationCodeAccessTokenProvider.class, provider);
  }

  @Test
  public void shouldFailToCreateAuthzCodeAccessTokenProviderWithoutClientId() {
    SFLoginInput loginInput = createLoginInputStub(null, "123", "some/url", "some/url", null);
    SFException e =
        Assertions.assertThrows(
            SFException.class,
            () ->
                providerFactory.createAccessTokenProvider(
                    AuthenticatorType.OAUTH_AUTHORIZATION_CODE, loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "passing oauthClientId is required for OAUTH_AUTHORIZATION_CODE authentication."));
  }

  @Test
  public void shouldFailToCreateAuthzCodeAccessTokenProviderWithoutClientSecret() {
    SFLoginInput loginInput = createLoginInputStub("123", null, null, null, null);
    SFException e =
        Assertions.assertThrows(
            SFException.class,
            () ->
                providerFactory.createAccessTokenProvider(
                    AuthenticatorType.OAUTH_AUTHORIZATION_CODE, loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "passing oauthClientSecret is required for OAUTH_AUTHORIZATION_CODE authentication."));
  }

  @Test
  public void shouldFailToCreateAuthzCodeAccessTokenProviderWithHttpsRedirectUri() {
    SFLoginInput loginInput =
        createLoginInputStub("123", "123", null, null, "https://localhost:1234/");
    SFException e =
        Assertions.assertThrows(
            SFException.class,
            () ->
                providerFactory.createAccessTokenProvider(
                    AuthenticatorType.OAUTH_AUTHORIZATION_CODE, loginInput));
    Assertions.assertTrue(
        e.getMessage().contains("provided redirect URI should start with \"http\", not \"https\""));
  }

  @Test
  public void shouldFailToCreateAuthzCodeAccessTokenProviderWithJustExtAuthzUrl() {
    SFLoginInput loginInput =
        createLoginInputStub(
            "123", "123", "https://some.ext.idp.com/authz", null, "http://localhost:1234/");
    SFException e =
        Assertions.assertThrows(
            SFException.class,
            () ->
                providerFactory.createAccessTokenProvider(
                    AuthenticatorType.OAUTH_AUTHORIZATION_CODE, loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Error during OAuth Authorization Code authentication: For OAUTH_AUTHORIZATION_CODE authentication with external IdP, both oauthAuthorizationUrl and oauthTokenRequestUrl must be specified"));
  }

  @Test
  public void shouldFailToCreateAuthzCodeAccessTokenProviderWithJustExtTokenUrl() {
    SFLoginInput loginInput =
        createLoginInputStub(
            "123", "123", null, "https://some.ext.idp.com/token", "http://localhost:1234/");
    SFException e =
        Assertions.assertThrows(
            SFException.class,
            () ->
                providerFactory.createAccessTokenProvider(
                    AuthenticatorType.OAUTH_AUTHORIZATION_CODE, loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Error during OAuth Authorization Code authentication: For OAUTH_AUTHORIZATION_CODE authentication with external IdP, both oauthAuthorizationUrl and oauthTokenRequestUrl must be specified"));
  }

  @Test
  public void shouldFailToCreateAuthzCodeAccessTokenProviderWithInvalidAuthzUrl() {
    SFLoginInput loginInput =
        createLoginInputStub(
            "123",
            "123",
            "invalid/url/format",
            "https://some.ext.idp.com/token",
            "http://localhost:1234/");
    SFException e =
        Assertions.assertThrows(
            SFException.class,
            () ->
                providerFactory.createAccessTokenProvider(
                    AuthenticatorType.OAUTH_AUTHORIZATION_CODE, loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Error during OAuth Authorization Code authentication: OAuth authorization URL and token URL must be specified in proper format; oauthAuthorizationUrl=invalid/url/format oauthTokenRequestUrl=https://some.ext.idp.com/token"));
  }

  @Test
  public void shouldFailToCreateAuthzCodeAccessTokenProviderWithInvalidTokenUrl() {
    SFLoginInput loginInput =
        createLoginInputStub(
            "123",
            "123",
            "https://some.ext.idp.com/authz",
            "invalid-token-format",
            "http://localhost:1234/");
    SFException e =
        Assertions.assertThrows(
            SFException.class,
            () ->
                providerFactory.createAccessTokenProvider(
                    AuthenticatorType.OAUTH_AUTHORIZATION_CODE, loginInput));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Error during OAuth Authorization Code authentication: OAuth authorization URL and token URL must be specified in proper format; oauthAuthorizationUrl=https://some.ext.idp.com/authz oauthTokenRequestUrl=invalid-token-format"));
  }

  @Test
  public void shouldFailToCreateAuthzCodeAccessTokenProviderWithDifferentUrlDomains()
      throws SFException {
    SFLoginInput loginInput =
        createLoginInputStub(
            "123",
            "123",
            "https://malicious.ext.idp.com/authz-url",
            "https://some.ext.idp.com/token-url",
            "http://localhost:1234/");
    SFLogger loggerMock = Mockito.mock(SFLogger.class);
    try (MockedStatic<SFLoggerFactory> loggerFactoryMockedStatic =
        Mockito.mockStatic(SFLoggerFactory.class)) {
      loggerFactoryMockedStatic
          .when(() -> SFLoggerFactory.getLogger(OAuthAccessTokenProviderFactory.class))
          .thenReturn(loggerMock);
      new OAuthAccessTokenProviderFactory(null, 30)
          .createAccessTokenProvider(AuthenticatorType.OAUTH_AUTHORIZATION_CODE, loginInput);
      Mockito.verify(loggerMock)
          .warn(
              "Both oauthAuthorizationUrl and oauthTokenRequestUrl should belong to the same host; oauthAuthorizationUrl=https://malicious.ext.idp.com/authz-url oauthTokenRequestUrl=https://some.ext.idp.com/token-url");
    }
  }

  private SFLoginInput createLoginInputStub(
      String clientId,
      String clientSecret,
      String authorizationUrl,
      String tokenUrl,
      String redirectUri) {
    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setOauthLoginInput(
        new SFOauthLoginInput(
            clientId, clientSecret, redirectUri, authorizationUrl, tokenUrl, null));
    return loginInput;
  }
}
