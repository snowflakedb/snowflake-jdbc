package net.snowflake.client.core.auth.oauth;

import static net.snowflake.client.core.SessionUtilExternalBrowser.AuthExternalBrowserHandlers;

import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SFOauthLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

@SnowflakeJdbcInternalApi
public class OAuthAuthorizationCodeAccessTokenProvider implements AccessTokenProvider {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(OAuthAuthorizationCodeAccessTokenProvider.class);

  private static final String DEFAULT_REDIRECT_HOST = "http://localhost:8001";
  private static final String REDIRECT_URI_ENDPOINT = "/snowflake/oauth-redirect";
  private static final String DEFAULT_REDIRECT_URI = DEFAULT_REDIRECT_HOST + REDIRECT_URI_ENDPOINT;

  private final AuthExternalBrowserHandlers browserHandler;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final int browserAuthorizationTimeoutSeconds;

  public OAuthAuthorizationCodeAccessTokenProvider(
      AuthExternalBrowserHandlers browserHandler, int browserAuthorizationTimeoutSeconds) {
    this.browserHandler = browserHandler;
    this.browserAuthorizationTimeoutSeconds = browserAuthorizationTimeoutSeconds;
  }

  @Override
  public String getAccessToken(SFLoginInput loginInput) throws SFException {
    try {
      logger.debug("Starting OAuth authorization code authentication flow...");
      CodeVerifier pkceVerifier = new CodeVerifier();
      AuthorizationCode authorizationCode = requestAuthorizationCode(loginInput, pkceVerifier);
      return exchangeAuthorizationCodeForAccessToken(loginInput, authorizationCode, pkceVerifier);
    } catch (Exception e) {
      logger.error(
          "Error during OAuth authorization code flow. Verify configuration passed to driver and IdP (URLs, grant types, scope, etc.)",
          e);
      throw new SFException(e, ErrorCode.OAUTH_AUTHORIZATION_CODE_FLOW_ERROR, e.getMessage());
    }
  }

  private AuthorizationCode requestAuthorizationCode(
      SFLoginInput loginInput, CodeVerifier pkceVerifier) throws SFException, IOException {
    AuthorizationRequest request = buildAuthorizationRequest(loginInput, pkceVerifier);
    SFOauthLoginInput oauthLoginInput = loginInput.getOauthLoginInput();
    URI authorizeRequestURI = request.toURI();
    HttpServer httpServer = createHttpServer(oauthLoginInput);
    CompletableFuture<String> codeFuture = setupRedirectURIServerForAuthorizationCode(httpServer);
    logger.debug(
        "Waiting for authorization code redirection to {}...", buildRedirectUri(oauthLoginInput));
    return letUserAuthorize(authorizeRequestURI, codeFuture, httpServer);
  }

  private String exchangeAuthorizationCodeForAccessToken(
      SFLoginInput loginInput, AuthorizationCode authorizationCode, CodeVerifier pkceVerifier)
      throws SFException {
    try {
      TokenRequest request = buildTokenRequest(loginInput, authorizationCode, pkceVerifier);
      URI requestUri = request.getEndpointURI();
      logger.debug(
          "Requesting OAuth access token from: {}",
          requestUri.getAuthority() + requestUri.getPath());
      String tokenResponse =
          HttpUtil.executeGeneralRequest(
              OAuthUtil.convertToBaseRequest(request.toHTTPRequest()),
              loginInput.getLoginTimeout(),
              loginInput.getAuthTimeout(),
              loginInput.getSocketTimeoutInMillis(),
              0,
              loginInput.getHttpClientSettingsKey());
      TokenResponseDTO tokenResponseDTO =
          objectMapper.readValue(tokenResponse, TokenResponseDTO.class);
      logger.debug(
          "Received OAuth access token from: {}", requestUri.getAuthority() + requestUri.getPath());
      return tokenResponseDTO.getAccessToken();
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
    } catch (Exception e) {
      if (e instanceof TimeoutException) {
        throw new SFException(
            e,
            ErrorCode.OAUTH_AUTHORIZATION_CODE_FLOW_ERROR,
            "Authorization request timed out. Snowflake driver did not receive authorization code back to the redirect URI. Verify your security integration and driver configuration.");
      }
      throw new SFException(e, ErrorCode.OAUTH_AUTHORIZATION_CODE_FLOW_ERROR, e.getMessage());
    } finally {
      logger.debug("Stopping OAuth redirect URI server @ {}", httpServer.getAddress());
      httpServer.stop(0);
    }
  }

  private static CompletableFuture<String> setupRedirectURIServerForAuthorizationCode(
      HttpServer httpServer) {
    CompletableFuture<String> accessTokenFuture = new CompletableFuture<>();
    httpServer.createContext(
        REDIRECT_URI_ENDPOINT,
        exchange -> {
          Map<String, String> urlParams =
              URLEncodedUtils.parse(exchange.getRequestURI(), StandardCharsets.UTF_8).stream()
                  .collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue));
          if (urlParams.containsKey("error")) {
            accessTokenFuture.completeExceptionally(
                new SFException(
                    ErrorCode.OAUTH_AUTHORIZATION_CODE_FLOW_ERROR,
                    String.format(
                        "Error during authorization: %s, %s",
                        urlParams.get("error"), urlParams.get("error_description"))));
          } else {
            String authorizationCode = urlParams.get("code");
            if (!StringUtils.isNullOrEmpty(authorizationCode)) {
              logger.debug("Received authorization code on redirect URI");
              accessTokenFuture.complete(authorizationCode);
            }
          }
          exchange.sendResponseHeaders(200, -1);
          exchange.getResponseBody().close();
        });
    logger.debug("Starting OAuth redirect URI server @ {}", httpServer.getAddress());
    httpServer.start();
    return accessTokenFuture;
  }

  private static HttpServer createHttpServer(SFOauthLoginInput loginInput) throws IOException {
    URI redirectUri = buildRedirectUri(loginInput);
    return HttpServer.create(
        new InetSocketAddress(redirectUri.getHost(), redirectUri.getPort()), 0);
  }

  private static AuthorizationRequest buildAuthorizationRequest(
      SFLoginInput loginInput, CodeVerifier pkceVerifier) {
    SFOauthLoginInput oauthLoginInput = loginInput.getOauthLoginInput();
    ClientID clientID = new ClientID(oauthLoginInput.getClientId());
    URI callback = buildRedirectUri(oauthLoginInput);
    State state = new State(256);
    String scope = OAuthUtil.getScope(loginInput.getOauthLoginInput(), loginInput.getRole());
    return new AuthorizationRequest.Builder(new ResponseType(ResponseType.Value.CODE), clientID)
        .scope(new Scope(scope))
        .state(state)
        .redirectionURI(callback)
        .codeChallenge(pkceVerifier, CodeChallengeMethod.S256)
        .endpointURI(
            OAuthUtil.getAuthorizationUrl(
                loginInput.getOauthLoginInput(), loginInput.getServerUrl()))
        .build();
  }

  private static TokenRequest buildTokenRequest(
      SFLoginInput loginInput, AuthorizationCode authorizationCode, CodeVerifier pkceVerifier) {
    URI redirectUri = buildRedirectUri(loginInput.getOauthLoginInput());
    AuthorizationGrant codeGrant =
        new AuthorizationCodeGrant(authorizationCode, redirectUri, pkceVerifier);
    ClientAuthentication clientAuthentication =
        new ClientSecretBasic(
            new ClientID(loginInput.getOauthLoginInput().getClientId()),
            new Secret(loginInput.getOauthLoginInput().getClientSecret()));
    Scope scope = new Scope(OAuthUtil.getScope(loginInput.getOauthLoginInput(), loginInput.getRole()));
    return new TokenRequest(
        OAuthUtil.getTokenRequestUrl(loginInput.getOauthLoginInput(), loginInput.getServerUrl()),
        clientAuthentication,
        codeGrant,
        scope);
  }

  private static URI buildRedirectUri(SFOauthLoginInput oauthLoginInput) {
    String redirectUri =
        !StringUtils.isNullOrEmpty(oauthLoginInput.getRedirectUri())
            ? oauthLoginInput.getRedirectUri()
            : DEFAULT_REDIRECT_URI;
    return URI.create(redirectUri);
  }
}
