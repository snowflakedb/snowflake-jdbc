/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.auth.oauth;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import net.snowflake.client.core.AssertUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SessionUtilExternalBrowser;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.auth.AuthenticatorType;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class OAuthAccessTokenProviderFactory {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(OAuthAccessTokenProviderFactory.class);
  private static final Set<AuthenticatorType> ELIGIBLE_AUTH_TYPES =
      new HashSet<>(
          Arrays.asList(
              AuthenticatorType.OAUTH_AUTHORIZATION_CODE,
              AuthenticatorType.OAUTH_CLIENT_CREDENTIALS));

  private final SessionUtilExternalBrowser.AuthExternalBrowserHandlers browserHandler;
  private final long browserAuthorizationTimeoutSeconds;

  public OAuthAccessTokenProviderFactory(
      SessionUtilExternalBrowser.AuthExternalBrowserHandlers browserHandler,
      long browserAuthorizationTimeoutSeconds) {
    this.browserHandler = browserHandler;
    this.browserAuthorizationTimeoutSeconds = browserAuthorizationTimeoutSeconds;
  }

  public AccessTokenProvider createAccessTokenProvider(
      AuthenticatorType authenticatorType, SFLoginInput loginInput) throws SFException {
    switch (authenticatorType) {
      case OAUTH_AUTHORIZATION_CODE:
        assertContainsClientCredentials(loginInput, authenticatorType);
        validateHttpRedirectUriIfSpecified(loginInput);
        return new OAuthAuthorizationCodeAccessTokenProvider(
            browserHandler, new RandomStateProvider(), browserAuthorizationTimeoutSeconds);
      case OAUTH_CLIENT_CREDENTIALS:
        assertContainsClientCredentials(loginInput, authenticatorType);
        AssertUtil.assertTrue(
            loginInput.getOauthLoginInput().getExternalTokenRequestUrl() != null,
            "passing externalTokenRequestUrl is required for OAUTH_CLIENT_CREDENTIALS authentication");
        return new OAuthClientCredentialsAccessTokenProvider();
      default:
        logger.error("Unsupported authenticator type: " + authenticatorType);
        throw new SFException(ErrorCode.INTERNAL_ERROR);
    }
  }

  private void validateHttpRedirectUriIfSpecified(SFLoginInput loginInput) throws SFException {
    String redirectUri = loginInput.getOauthLoginInput().getRedirectUri();
    if (redirectUri != null) {
      AssertUtil.assertTrue(
          !redirectUri.startsWith("https"),
          "provided redirect URI should start with \"http\", not \"https\"");
    }
  }

  public static boolean isEligible(AuthenticatorType authenticatorType) {
    return getEligible().contains(authenticatorType);
  }

  private static Set<AuthenticatorType> getEligible() {
    return ELIGIBLE_AUTH_TYPES;
  }

  private void assertContainsClientCredentials(
      SFLoginInput loginInput, AuthenticatorType authenticatorType) throws SFException {
    AssertUtil.assertTrue(
        loginInput.getOauthLoginInput().getClientId() != null,
        String.format(
            "passing clientId is required for %s authentication", authenticatorType.name()));
    AssertUtil.assertTrue(
        loginInput.getOauthLoginInput().getClientSecret() != null,
        String.format(
            "passing clientSecret is required for %s authentication", authenticatorType.name()));
  }
}
