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
public class AccessTokenProviderFactory {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(AccessTokenProviderFactory.class);
  private static final AuthenticatorType[] ELIGIBLE_AUTH_TYPES = {
    AuthenticatorType.OAUTH_AUTHORIZATION_CODE, AuthenticatorType.OAUTH_CLIENT_CREDENTIALS
  };

  private final SessionUtilExternalBrowser.AuthExternalBrowserHandlers browserHandler;
  private final int browserAuthorizationTimeoutSeconds;

  public AccessTokenProviderFactory(
      SessionUtilExternalBrowser.AuthExternalBrowserHandlers browserHandler,
      int browserAuthorizationTimeoutSeconds) {
    this.browserHandler = browserHandler;
    this.browserAuthorizationTimeoutSeconds = browserAuthorizationTimeoutSeconds;
  }

  public AccessTokenProvider createAccessTokenProvider(
      AuthenticatorType authenticatorType, SFLoginInput loginInput) throws SFException {
    switch (authenticatorType) {
      case OAUTH_AUTHORIZATION_CODE:
        assertContainsClientCredentials(loginInput);
        return new OAuthAuthorizationCodeAccessTokenProvider(
            browserHandler, browserAuthorizationTimeoutSeconds);
      case OAUTH_CLIENT_CREDENTIALS:
        assertContainsClientCredentials(loginInput);
        return new OAuthClientCredentialsAccessTokenProvider();
      default:
        logger.error("Unsupported authenticator type: " + authenticatorType);
        throw new SFException(ErrorCode.INTERNAL_ERROR);
    }
  }

  public static Set<AuthenticatorType> getEligible() {
    return new HashSet<>(Arrays.asList(ELIGIBLE_AUTH_TYPES));
  }

  public static boolean isEligible(AuthenticatorType authenticatorType) {
    return getEligible().contains(authenticatorType);
  }

  private void assertContainsClientCredentials(SFLoginInput loginInput) throws SFException {
    AssertUtil.assertTrue(
        loginInput.getOauthLoginInput().getClientId() != null,
        "passing clientId is required for OAUTH_AUTHORIZATION_CODE_FLOW authentication");
    AssertUtil.assertTrue(
        loginInput.getOauthLoginInput().getClientSecret() != null,
        "passing clientSecret is required for OAUTH_AUTHORIZATION_CODE_FLOW authentication");
  }
}
