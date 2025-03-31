package net.snowflake.client.core.auth.oauth;

import static net.snowflake.client.core.SessionUtilExternalBrowser.AuthExternalBrowserHandlers;

import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.AuthorizationRequest;
import com.nimbusds.oauth2.sdk.ResponseType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import com.nimbusds.oauth2.sdk.pkce.CodeVerifier;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SFOauthLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeUseDPoPNonceException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URLEncodedUtils;

@SnowflakeJdbcInternalApi
public class OAuthAuthorizationCodeAccessTokenProvider implements AccessTokenProvider {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(OAuthAuthorizationCodeAccessTokenProvider.class);

  static final String DEFAULT_REDIRECT_HOST = "http://127.0.0.1";
  static final String DEFAULT_REDIRECT_URI_ENDPOINT = "/";

  private final AuthExternalBrowserHandlers browserHandler;
  private final StateProvider<String> stateProvider;
  private final DPoPUtil dpopUtil;
  private final long browserAuthorizationTimeoutSeconds;

  public OAuthAuthorizationCodeAccessTokenProvider(
      AuthExternalBrowserHandlers browserHandler,
      StateProvider<String> stateProvider,
      long browserAuthorizationTimeoutSeconds)
      throws SFException {
    this.browserHandler = browserHandler;
    this.stateProvider = stateProvider;
    this.dpopUtil = new DPoPUtil();
    this.browserAuthorizationTimeoutSeconds = browserAuthorizationTimeoutSeconds;
  }

  @Override
  public TokenResponseDTO getAccessToken(SFLoginInput loginInput) throws SFException {
    try {
      logger.info("Starting OAuth authorization code authentication flow...");
      CodeVerifier pkceVerifier = new CodeVerifier();
      URI redirectUri = OAuthUtil.buildRedirectUri(loginInput.getOauthLoginInput());
      AuthorizationCode authorizationCode =
          requestAuthorizationCode(loginInput, pkceVerifier, redirectUri);
      return exchangeAuthorizationCodeForAccessToken(
          loginInput, authorizationCode, pkceVerifier, redirectUri, null, false);
    } catch (Exception e) {
      logger.error(
          "Error during OAuth authorization code flow. Verify configuration passed to driver and IdP (URLs, grant types, scope, etc.)",
          e);
      throw new SFException(e, ErrorCode.OAUTH_AUTHORIZATION_CODE_FLOW_ERROR, e.getMessage());
    }
  }

  @Override
  public String getDPoPPublicKey() {
    return dpopUtil.getPublicKey();
  }

  private AuthorizationCode requestAuthorizationCode(
      SFLoginInput loginInput, CodeVerifier pkceVerifier, URI redirectUri)
      throws SFException, IOException {
    State state = new State(stateProvider.getState());
    AuthorizationRequest request =
        buildAuthorizationRequest(loginInput, pkceVerifier, state, redirectUri);
    URI authorizeRequestURI = request.toURI();
    HttpServer httpServer = createHttpServer(redirectUri);
    CompletableFuture<String> codeFuture =
        setupRedirectURIServerForAuthorizationCode(httpServer, state);
    logger.debug("Waiting for authorization code redirection to {}...", redirectUri);
    return letUserAuthorize(authorizeRequestURI, codeFuture, httpServer);
  }

  private TokenResponseDTO exchangeAuthorizationCodeForAccessToken(
      SFLoginInput loginInput,
      AuthorizationCode authorizationCode,
      CodeVerifier pkceVerifier,
      URI redirectUri,
      String dpopNonce,
      boolean retried)
      throws SFException {
    try {
      HttpRequestBase request =
          buildTokenRequest(loginInput, authorizationCode, pkceVerifier, redirectUri, dpopNonce);
      return OAuthUtil.sendTokenRequest(request, loginInput);
    } catch (SnowflakeUseDPoPNonceException e) {
      logger.debug("Received \"use_dpop_nonce\" error from IdP while performing token request");
      if (!retried) {
        logger.debug("Retrying token request with DPoP nonce included...");
        return exchangeAuthorizationCodeForAccessToken(
            loginInput, authorizationCode, pkceVerifier, redirectUri, e.getNonce(), true);
      } else {
        logger.debug("Skipping DPoP nonce retry as it has been already retried");
        throw e;
      }
    } catch (Exception e) {
      logger.error("Error during making OAuth access token request", e);
      throw new SFException(e, ErrorCode.OAUTH_AUTHORIZATION_CODE_FLOW_ERROR, e.getMessage());
    }
  }

  private AuthorizationCode letUserAuthorize(
      URI authorizeRequestURI, CompletableFuture<String> codeFuture, HttpServer httpServer)
      throws SFException {
    try {
      logger.debug(
          "Opening browser for authorization code request to: {}",
          authorizeRequestURI.getAuthority() + authorizeRequestURI.getPath());
      browserHandler.openBrowser(authorizeRequestURI.toString());
      String code = codeFuture.get(this.browserAuthorizationTimeoutSeconds, TimeUnit.SECONDS);
      return new AuthorizationCode(code);
    } catch (TimeoutException e) {
      throw new SFException(
          e,
          ErrorCode.OAUTH_AUTHORIZATION_CODE_FLOW_ERROR,
          "Authorization request timed out. Snowflake driver did not receive authorization code back to the redirect URI. Verify your security integration and driver configuration.");
    } catch (Exception e) {
      throw new SFException(e, ErrorCode.OAUTH_AUTHORIZATION_CODE_FLOW_ERROR, e.getMessage());
    } finally {
      logger.debug("Stopping OAuth redirect URI server @ {}", httpServer.getAddress());
      httpServer.stop(1);
    }
  }

  private static CompletableFuture<String> setupRedirectURIServerForAuthorizationCode(
      HttpServer httpServer, State expectedState) {
    CompletableFuture<String> authorizationCodeFuture = new CompletableFuture<>();
    httpServer.createContext(
        DEFAULT_REDIRECT_URI_ENDPOINT,
        exchange -> {
          Map<String, String> urlParams =
              URLEncodedUtils.parse(exchange.getRequestURI(), StandardCharsets.UTF_8).stream()
                  .collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue));
          String response =
              AuthorizationCodeRedirectRequestHandler.handleRedirectRequest(
                  urlParams, authorizationCodeFuture, expectedState);
          exchange.sendResponseHeaders(200, response.length());
          exchange.getResponseBody().write(response.getBytes(StandardCharsets.UTF_8));
          exchange.getResponseBody().close();
        });
    logger.debug("Starting OAuth redirect URI server @ {}", httpServer.getAddress());
    httpServer.start();
    return authorizationCodeFuture;
  }

  private static HttpServer createHttpServer(URI redirectUri) throws IOException {
    return HttpServer.create(
        new InetSocketAddress(redirectUri.getHost(), redirectUri.getPort()), 0);
  }

  private AuthorizationRequest buildAuthorizationRequest(
      SFLoginInput loginInput, CodeVerifier pkceVerifier, State state, URI redirectUri)
      throws SFException {
    SFOauthLoginInput oauthLoginInput = loginInput.getOauthLoginInput();
    ClientID clientID = new ClientID(oauthLoginInput.getClientId());
    String scope = OAuthUtil.getScope(loginInput.getOauthLoginInput(), loginInput.getRole());
    AuthorizationRequest.Builder builder =
        new AuthorizationRequest.Builder(new ResponseType(ResponseType.Value.CODE), clientID)
            .scope(new Scope(scope))
            .state(state)
            .redirectionURI(redirectUri)
            .codeChallenge(pkceVerifier, CodeChallengeMethod.S256)
            .endpointURI(
                OAuthUtil.getAuthorizationUrl(
                    loginInput.getOauthLoginInput(), loginInput.getServerUrl()));

    if (loginInput.isDPoPEnabled()) {
      builder.dPoPJWKThumbprintConfirmation(new DPoPUtil().getThumbprint());
    }
    return builder.build();
  }

  private HttpRequestBase buildTokenRequest(
      SFLoginInput loginInput,
      AuthorizationCode authorizationCode,
      CodeVerifier pkceVerifier,
      URI redirectUri,
      String dpopNonce)
      throws SFException {
    AuthorizationGrant codeGrant =
        new AuthorizationCodeGrant(authorizationCode, redirectUri, pkceVerifier);
    ClientAuthentication clientAuthentication =
        new ClientSecretBasic(
            new ClientID(loginInput.getOauthLoginInput().getClientId()),
            new Secret(loginInput.getOauthLoginInput().getClientSecret()));
    Scope scope =
        new Scope(OAuthUtil.getScope(loginInput.getOauthLoginInput(), loginInput.getRole()));
    TokenRequest tokenRequest =
        new TokenRequest(
            OAuthUtil.getTokenRequestUrl(
                loginInput.getOauthLoginInput(), loginInput.getServerUrl()),
            clientAuthentication,
            codeGrant,
            scope);
    HTTPRequest tokenHttpRequest = tokenRequest.toHTTPRequest();
    HttpRequestBase convertedTokenRequest = OAuthUtil.convertToBaseRequest(tokenHttpRequest);

    if (loginInput.isDPoPEnabled()) {
      dpopUtil.addDPoPProofHeaderToRequest(convertedTokenRequest, dpopNonce);
    }

    return convertedTokenRequest;
  }
}
