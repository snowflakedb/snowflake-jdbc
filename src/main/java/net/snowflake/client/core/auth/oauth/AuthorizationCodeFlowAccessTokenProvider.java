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
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.oauth2.sdk.pkce.CodeChallengeMethod;
import com.nimbusds.oauth2.sdk.pkce.CodeVerifier;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

@SnowflakeJdbcInternalApi
public class AuthorizationCodeFlowAccessTokenProvider implements OauthAccessTokenProvider {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(AuthorizationCodeFlowAccessTokenProvider.class);

  private static final String SNOWFLAKE_AUTHORIZE_ENDPOINT = "/oauth/authorize";
  private static final String SNOWFLAKE_TOKEN_REQUEST_ENDPOINT = "/oauth/token-request";

  private static final String REDIRECT_URI_HOST = "localhost";
  private static final int DEFAULT_REDIRECT_URI_PORT = 8001;
  private static final String REDIRECT_URI_ENDPOINT = "/snowflake/oauth-redirect";
  public static final String SESSION_ROLE_SCOPE = "session:role";

  private final AuthExternalBrowserHandlers browserHandler;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final int browserAuthorizationTimeoutSeconds;

  public AuthorizationCodeFlowAccessTokenProvider(
      AuthExternalBrowserHandlers browserHandler, int browserAuthorizationTimeoutSeconds) {
    this.browserHandler = browserHandler;
    this.browserAuthorizationTimeoutSeconds = browserAuthorizationTimeoutSeconds;
  }

  @Override
  public String getAccessToken(SFLoginInput loginInput) throws SFException {
    CodeVerifier pkceVerifier = new CodeVerifier();
    AuthorizationCode authorizationCode = requestAuthorizationCode(loginInput, pkceVerifier);
    return exchangeAuthorizationCodeForAccessToken(loginInput, authorizationCode, pkceVerifier);
  }

  private AuthorizationCode requestAuthorizationCode(
      SFLoginInput loginInput, CodeVerifier pkceVerifier) throws SFException {
    try {
      AuthorizationRequest request = buildAuthorizationRequest(loginInput, pkceVerifier);
      URI authorizeRequestURI = request.toURI();
      CompletableFuture<String> codeFuture =
          setupRedirectURIServerForAuthorizationCode(loginInput.getRedirectUriPort());
      logger.debug(
          "Waiting for authorization code on "
              + buildRedirectURI(loginInput.getRedirectUriPort())
              + "...");
      letUserAuthorizeViaBrowser(authorizeRequestURI);
      String code = codeFuture.get(this.browserAuthorizationTimeoutSeconds, TimeUnit.SECONDS);
      return new AuthorizationCode(code);
    } catch (Exception e) {
      if (e instanceof TimeoutException) {
        throw new RuntimeException(
            "Authorization request timed out. Snowflake driver did not receive authorization code back to the redirect URI. Verify your security integration and driver configuration.",
            e);
      }
      throw new RuntimeException(e);
    }
  }

  private String exchangeAuthorizationCodeForAccessToken(
      SFLoginInput loginInput, AuthorizationCode authorizationCode, CodeVerifier pkceVerifier) {
    try {
      TokenRequest request = buildTokenRequest(loginInput, authorizationCode, pkceVerifier);
      String tokenResponse =
          HttpUtil.executeGeneralRequest(
              convertTokenRequest(request.toHTTPRequest()),
              loginInput.getLoginTimeout(),
              loginInput.getAuthTimeout(),
              loginInput.getSocketTimeoutInMillis(),
              0,
              loginInput.getHttpClientSettingsKey());
      TokenResponseDTO tokenResponseDTO =
          objectMapper.readValue(tokenResponse, TokenResponseDTO.class);
      return tokenResponseDTO.getAccessToken();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void letUserAuthorizeViaBrowser(URI authorizeRequestURI) throws SFException {
    browserHandler.openBrowser(authorizeRequestURI.toString());
  }

  private static CompletableFuture<String> setupRedirectURIServerForAuthorizationCode(
      int redirectUriPort) throws IOException {
    CompletableFuture<String> accessTokenFuture = new CompletableFuture<>();
    int redirectPort = (redirectUriPort != -1) ? redirectUriPort : DEFAULT_REDIRECT_URI_PORT;
    HttpServer httpServer =
        HttpServer.create(new InetSocketAddress(REDIRECT_URI_HOST, redirectPort), 0);
    httpServer.createContext(
        REDIRECT_URI_ENDPOINT,
        exchange -> {
          exchange.sendResponseHeaders(200, 0);
          exchange.getResponseBody().close();
          String authorizationCode =
              extractAuthorizationCodeFromQueryParameters(exchange.getRequestURI().getQuery());
          if (!StringUtils.isNullOrEmpty(authorizationCode)) {
            logger.debug("Received authorization code on redirect URI");
            accessTokenFuture.complete(authorizationCode);
            httpServer.stop(0);
          }
        });
    httpServer.start();
    return accessTokenFuture;
  }

  private static AuthorizationRequest buildAuthorizationRequest(
      SFLoginInput loginInput, CodeVerifier pkceVerifier) throws URISyntaxException {
    URI authorizeEndpoint = new URI(loginInput.getServerUrl() + SNOWFLAKE_AUTHORIZE_ENDPOINT);
    ClientID clientID = new ClientID(loginInput.getClientId());
    Scope scope = new Scope(String.format("%s:%s", SESSION_ROLE_SCOPE, loginInput.getRole()));
    URI callback = buildRedirectURI(loginInput.getRedirectUriPort());
    State state = new State(256);
    return new AuthorizationRequest.Builder(new ResponseType(ResponseType.Value.CODE), clientID)
        .scope(scope)
        .state(state)
        .redirectionURI(callback)
        .codeChallenge(pkceVerifier, CodeChallengeMethod.S256)
        .endpointURI(authorizeEndpoint)
        .build();
  }

  private static TokenRequest buildTokenRequest(
      SFLoginInput loginInput, AuthorizationCode authorizationCode, CodeVerifier pkceVerifier)
      throws URISyntaxException {
    URI redirectURI = buildRedirectURI(loginInput.getRedirectUriPort());
    AuthorizationGrant codeGrant =
        new AuthorizationCodeGrant(authorizationCode, redirectURI, pkceVerifier);
    ClientAuthentication clientAuthentication =
        new ClientSecretBasic(
            new ClientID(loginInput.getClientId()), new Secret(loginInput.getClientSecret()));
    URI tokenEndpoint =
        new URI(String.format(loginInput.getServerUrl() + SNOWFLAKE_TOKEN_REQUEST_ENDPOINT));
    Scope scope = new Scope(SESSION_ROLE_SCOPE, loginInput.getRole());
    return new TokenRequest(tokenEndpoint, clientAuthentication, codeGrant, scope);
  }

  private static URI buildRedirectURI(int redirectUriPort) throws URISyntaxException {
    redirectUriPort = (redirectUriPort != -1) ? redirectUriPort : DEFAULT_REDIRECT_URI_PORT;
    return new URI(
        String.format("http://%s:%s%s", REDIRECT_URI_HOST, redirectUriPort, REDIRECT_URI_ENDPOINT));
  }

  private static String extractAuthorizationCodeFromQueryParameters(String queryParameters) {
    String prefix = "code=";
    String codeSuffix =
        queryParameters.substring(queryParameters.indexOf(prefix) + prefix.length());
    if (codeSuffix.contains("&")) {
      return codeSuffix.substring(0, codeSuffix.indexOf("&"));
    } else {
      return codeSuffix;
    }
  }

  private static HttpRequestBase convertTokenRequest(HTTPRequest nimbusRequest) {
    HttpPost request = new HttpPost(nimbusRequest.getURI());
    request.setEntity(new StringEntity(nimbusRequest.getBody(), StandardCharsets.UTF_8));
    nimbusRequest.getHeaderMap().forEach((key, values) -> request.addHeader(key, values.get(0)));
    return request;
  }
}
