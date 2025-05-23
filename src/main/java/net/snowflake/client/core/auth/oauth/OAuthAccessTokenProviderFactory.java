package net.snowflake.client.core.auth.oauth;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import net.snowflake.client.core.AssertUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SFOauthLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.auth.AuthenticatorType;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class OAuthAccessTokenProviderFactory {

  private static final String SNOWFLAKE_DOMAIN = "snowflakecomputing.com";

  private final SFLogger logger = SFLoggerFactory.getLogger(OAuthAccessTokenProviderFactory.class);
  private static final Set<AuthenticatorType> ELIGIBLE_AUTH_TYPES =
      new HashSet<>(
          Arrays.asList(
              AuthenticatorType.OAUTH_AUTHORIZATION_CODE,
              AuthenticatorType.OAUTH_CLIENT_CREDENTIALS));

  public AccessTokenProvider createAccessTokenProvider(
      AuthenticatorType authenticatorType, SFLoginInput loginInput) throws SFException {
    switch (authenticatorType) {
      case OAUTH_AUTHORIZATION_CODE:
        if (isEligibleForDefaultClientCredentials(loginInput.getOauthLoginInput())) {
          loginInput.getOauthLoginInput().setLocalApplicationClientCredential();
        }
        assertContainsClientCredentials(loginInput, authenticatorType);
        validateHttpRedirectUriIfSpecified(loginInput);
        validateAuthorizationAndTokenEndpointsIfSpecified(loginInput);
        return new OAuthAuthorizationCodeAccessTokenProvider(
            loginInput.getBrowserHandler(),
            new RandomStateProvider(),
            loginInput.getBrowserResponseTimeout().getSeconds());
      case OAUTH_CLIENT_CREDENTIALS:
        assertContainsClientCredentials(loginInput, authenticatorType);
        AssertUtil.assertTrue(
            loginInput.getOauthLoginInput().getTokenRequestUrl() != null,
            "passing oauthTokenRequestUrl is required for OAUTH_CLIENT_CREDENTIALS authentication");
        return new OAuthClientCredentialsAccessTokenProvider();
      default:
        String message = "Unsupported authenticator type: " + authenticatorType;
        logger.error(message);
        throw new SFException(ErrorCode.INTERNAL_ERROR, message);
    }
  }

  static boolean isEligibleForDefaultClientCredentials(SFOauthLoginInput oauthLoginInput) {
    return areClientCredentialsNotSupplied(oauthLoginInput) && isSnowflakeAsIdP(oauthLoginInput);
  }

  private static boolean areClientCredentialsNotSupplied(SFOauthLoginInput oauthLoginInput) {
    return (oauthLoginInput.getClientId() == null && oauthLoginInput.getClientSecret() == null);
  }

  private static boolean isSnowflakeAsIdP(SFOauthLoginInput oauthLoginInput) {
    return ((oauthLoginInput.getAuthorizationUrl() == null
            || oauthLoginInput.getAuthorizationUrl().contains(SNOWFLAKE_DOMAIN))
        && (oauthLoginInput.getTokenRequestUrl() == null
            || oauthLoginInput.getTokenRequestUrl().contains(SNOWFLAKE_DOMAIN)));
  }

  private void validateAuthorizationAndTokenEndpointsIfSpecified(SFLoginInput loginInput)
      throws SFException {
    String authorizationEndpoint = loginInput.getOauthLoginInput().getAuthorizationUrl();
    String tokenEndpoint = loginInput.getOauthLoginInput().getTokenRequestUrl();
    if ((!isNullOrEmpty(authorizationEndpoint) && isNullOrEmpty(tokenEndpoint))
        || (isNullOrEmpty(authorizationEndpoint) && !isNullOrEmpty(tokenEndpoint))) {
      throw new SFException(
          ErrorCode.OAUTH_AUTHORIZATION_CODE_FLOW_ERROR,
          "For OAUTH_AUTHORIZATION_CODE authentication with external IdP, both oauthAuthorizationUrl and oauthTokenRequestUrl must be specified");
    } else if (!isNullOrEmpty(authorizationEndpoint) && !isNullOrEmpty(tokenEndpoint)) {
      URI authorizationUrl = URI.create(authorizationEndpoint);
      URI tokenUrl = URI.create(tokenEndpoint);
      if (isNullOrEmpty(authorizationUrl.getHost()) || isNullOrEmpty(tokenUrl.getHost())) {
        throw new SFException(
            ErrorCode.OAUTH_AUTHORIZATION_CODE_FLOW_ERROR,
            String.format(
                "OAuth authorization URL and token URL must be specified in proper format; oauthAuthorizationUrl=%s oauthTokenRequestUrl=%s",
                authorizationUrl, tokenUrl));
      }
      if (!authorizationUrl.getHost().equals(tokenUrl.getHost())) {
        logger.warn(
            String.format(
                "Both oauthAuthorizationUrl and oauthTokenRequestUrl should belong to the same host; oauthAuthorizationUrl=%s oauthTokenRequestUrl=%s",
                authorizationUrl, tokenUrl));
      }
    }
  }

  private static void validateHttpRedirectUriIfSpecified(SFLoginInput loginInput)
      throws SFException {
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

  private static void assertContainsClientCredentials(
      SFLoginInput loginInput, AuthenticatorType authenticatorType) throws SFException {
    AssertUtil.assertTrue(
        loginInput.getOauthLoginInput().getClientId() != null,
        String.format(
            "passing oauthClientId is required for %s authentication", authenticatorType.name()));
    AssertUtil.assertTrue(
        loginInput.getOauthLoginInput().getClientSecret() != null,
        String.format(
            "passing oauthClientSecret is required for %s authentication",
            authenticatorType.name()));
  }
}
