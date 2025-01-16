/*
 * Copyright (c) 2024-2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.auth.oauth;

import java.util.Arrays;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SFOauthLoginInput;
import net.snowflake.client.core.auth.AuthenticatorType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
            .contains("passing clientId is required for OAUTH_CLIENT_CREDENTIALS authentication."));
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
                "passing clientSecret is required for OAUTH_CLIENT_CREDENTIALS authentication."));
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
                "passing externalTokenRequestUrl is required for OAUTH_CLIENT_CREDENTIALS authentication."));
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
            .contains("passing clientId is required for OAUTH_AUTHORIZATION_CODE authentication."));
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
                "passing clientSecret is required for OAUTH_AUTHORIZATION_CODE authentication."));
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
                "Error during OAuth Authorization Code authentication: For OAUTH_AUTHORIZATION_CODE authentication with external IdP, both externalAuthorizationUrl and externalTokenRequestUrl must be specified"));
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
                "Error during OAuth Authorization Code authentication: For OAUTH_AUTHORIZATION_CODE authentication with external IdP, both externalAuthorizationUrl and externalTokenRequestUrl must be specified"));
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
                "Error during OAuth Authorization Code authentication: Both externalAuthorizationUrl and externalTokenRequestUrl must belong to the same host"));
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
                "Both externalAuthorizationUrl and externalTokenRequestUrl must belong to the same host; externalAuthorizationUrl=https://some.ext.idp.com/authz externalTokenRequestUrl=invalid-token-format"));
  }

  private SFLoginInput createLoginInputStub(
      String clientId,
      String clientSecret,
      String externalAuthorizationUrl,
      String externalTokenUrl,
      String redirectUri) {
    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setOauthLoginInput(
        new SFOauthLoginInput(
            clientId, clientSecret, redirectUri, externalAuthorizationUrl, externalTokenUrl, null));
    return loginInput;
  }
}
