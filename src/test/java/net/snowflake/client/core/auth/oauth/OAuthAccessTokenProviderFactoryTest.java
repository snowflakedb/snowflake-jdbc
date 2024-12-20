/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
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
    SFLoginInput loginInput = createLoginInputStub("123", "123", "some/url");
    AccessTokenProvider provider =
        providerFactory.createAccessTokenProvider(
            AuthenticatorType.OAUTH_CLIENT_CREDENTIALS, loginInput);
    Assertions.assertNotNull(provider);
    Assertions.assertInstanceOf(OAuthClientCredentialsAccessTokenProvider.class, provider);
  }

  @Test
  public void shouldFailToCreateClientCredentialsAccessTokenProviderWithoutClientId() {
    SFLoginInput loginInput = createLoginInputStub(null, "123", "some/url");
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
    SFLoginInput loginInput = createLoginInputStub("123", null, "some/url");
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
  public void shouldFailToCreateClientCredentialsAccessTokenProviderWithoutClientAuthzUrl() {
    SFLoginInput loginInput = createLoginInputStub("123", "123", null);
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
    SFLoginInput loginInput = createLoginInputStub("123", "123", "");
    AccessTokenProvider provider =
        providerFactory.createAccessTokenProvider(
            AuthenticatorType.OAUTH_AUTHORIZATION_CODE, loginInput);
    Assertions.assertNotNull(provider);
    Assertions.assertInstanceOf(OAuthAuthorizationCodeAccessTokenProvider.class, provider);
  }

  @Test
  public void shouldFailToCreateAuthzCodeAccessTokenProviderWithoutClientId() {
    SFLoginInput loginInput = createLoginInputStub(null, "123", "some/url");
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
    SFLoginInput loginInput = createLoginInputStub("123", null, null);
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

  private SFLoginInput createLoginInputStub(
      String clientId, String clientSecret, String externalTokenUrl) {
    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setOauthLoginInput(
        new SFOauthLoginInput(clientId, clientSecret, null, null, externalTokenUrl, null));
    return loginInput;
  }
}
