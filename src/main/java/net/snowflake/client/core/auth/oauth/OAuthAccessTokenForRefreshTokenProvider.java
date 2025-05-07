package net.snowflake.client.core.auth.oauth;

import com.nimbusds.oauth2.sdk.RefreshTokenGrant;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import java.net.URI;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeUseDPoPNonceException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpRequestBase;

@SnowflakeJdbcInternalApi
public class OAuthAccessTokenForRefreshTokenProvider implements AccessTokenProvider {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(OAuthClientCredentialsAccessTokenProvider.class);

  private final DPoPUtil dPoPUtil;

  public OAuthAccessTokenForRefreshTokenProvider() throws SFException {
    this.dPoPUtil = new DPoPUtil();
  }

  @Override
  public TokenResponseDTO getAccessToken(SFLoginInput loginInput) throws SFException {
    return exchangeRefreshTokenForAccessToken(loginInput, null, false);
  }

  @Override
  public String getDPoPPublicKey() {
    return dPoPUtil.getPublicKey();
  }

  private TokenResponseDTO exchangeRefreshTokenForAccessToken(
      SFLoginInput loginInput, String dpopNonce, boolean retried) throws SFException {
    try {
      logger.info("Obtaining new OAuth access token using refresh token...");
      HttpRequestBase tokenRequest = buildTokenRequest(loginInput, dpopNonce);
      return OAuthUtil.sendTokenRequest(tokenRequest, loginInput);
    } catch (SnowflakeUseDPoPNonceException e) {
      logger.debug("Received \"use_dpop_nonce\" error from IdP while performing token request");
      if (!retried) {
        logger.debug("Retrying token request with DPoP nonce included...");
        return exchangeRefreshTokenForAccessToken(loginInput, e.getNonce(), true);
      } else {
        logger.debug("Skipping DPoP nonce retry as it has been already retried");
        throw e;
      }
    } catch (Exception e) {
      logger.error("Error during OAuth refresh token flow.", e);
      throw new SFException(e, ErrorCode.OAUTH_REFRESH_TOKEN_FLOW_ERROR, e.getMessage());
    }
  }

  private HttpRequestBase buildTokenRequest(SFLoginInput loginInput, String dpopNonce)
      throws SFException {
    URI tokenRequestUrl =
        OAuthUtil.getTokenRequestUrl(loginInput.getOauthLoginInput(), loginInput.getServerUrl());
    ClientAuthentication clientAuthentication =
        new ClientSecretBasic(
            new ClientID(loginInput.getOauthLoginInput().getClientId()),
            new Secret(loginInput.getOauthLoginInput().getClientSecret()));
    Scope scope =
        new Scope(OAuthUtil.getScope(loginInput.getOauthLoginInput(), loginInput.getRole()));
    RefreshToken refreshToken = new RefreshToken(loginInput.getOauthRefreshToken());
    TokenRequest tokenRequest =
        new TokenRequest(
            tokenRequestUrl, clientAuthentication, new RefreshTokenGrant(refreshToken), scope);
    HTTPRequest tokenHttpRequest = tokenRequest.toHTTPRequest();
    HttpRequestBase convertedTokenRequest = OAuthUtil.convertToBaseRequest(tokenHttpRequest);

    if (loginInput.isDPoPEnabled()) {
      dPoPUtil.addDPoPProofHeaderToRequest(convertedTokenRequest, dpopNonce);
    }

    return convertedTokenRequest;
  }
}
