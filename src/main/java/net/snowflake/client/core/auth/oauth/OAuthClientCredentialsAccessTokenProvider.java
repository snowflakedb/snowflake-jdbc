package net.snowflake.client.core.auth.oauth;

import com.nimbusds.oauth2.sdk.ClientCredentialsGrant;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.id.ClientID;
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
public class OAuthClientCredentialsAccessTokenProvider implements AccessTokenProvider {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(OAuthClientCredentialsAccessTokenProvider.class);

  private final DPoPUtil dpopUtil;

  public OAuthClientCredentialsAccessTokenProvider() throws SFException {
    this.dpopUtil = new DPoPUtil();
  }

  @Override
  public TokenResponseDTO getAccessToken(SFLoginInput loginInput) throws SFException {
    return exchangeClientCredentialsForAccessToken(loginInput, null, false);
  }

  @Override
  public String getDPoPPublicKey() {
    return dpopUtil.getPublicKey();
  }

  private TokenResponseDTO exchangeClientCredentialsForAccessToken(
      SFLoginInput loginInput, String dpopNonce, boolean retried) throws SFException {
    try {
      logger.info("Starting OAuth client credentials authentication flow...");
      HttpRequestBase tokenRequest = buildTokenRequest(loginInput, dpopNonce);
      return OAuthUtil.sendTokenRequest(tokenRequest, loginInput);
    } catch (SnowflakeUseDPoPNonceException e) {
      logger.debug("Received \"use_dpop_nonce\" error from IdP while performing token request");
      if (!retried) {
        logger.debug("Retrying token request with DPoP nonce included...");
        return exchangeClientCredentialsForAccessToken(loginInput, e.getNonce(), true);
      } else {
        logger.debug("Skipping DPoP nonce retry as it has been already retried");
        throw e;
      }
    } catch (Exception | SFException e) {
      logger.error(
          "Error during OAuth client credentials flow. Verify configuration passed to driver and IdP (URLs, grant types, scope, etc.)",
          e);
      throw new SFException(e, ErrorCode.OAUTH_CLIENT_CREDENTIALS_FLOW_ERROR, e.getMessage());
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
    TokenRequest tokenRequest =
        new TokenRequest(
            tokenRequestUrl, clientAuthentication, new ClientCredentialsGrant(), scope);
    HTTPRequest tokenHttpRequest = tokenRequest.toHTTPRequest();
    HttpRequestBase convertedTokenRequest = OAuthUtil.convertToBaseRequest(tokenHttpRequest);

    if (loginInput.isDPoPEnabled()) {
      dpopUtil.addDPoPProofHeaderToRequest(convertedTokenRequest, dpopNonce);
    }

    return convertedTokenRequest;
  }
}
