package net.snowflake.client.core.auth.oauth;

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
import com.sun.net.httpserver.HttpServer;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static net.snowflake.client.core.SessionUtilExternalBrowser.DefaultAuthExternalBrowserHandlers;

@SnowflakeJdbcInternalApi
public class AuthorizationCodeFlowAccessTokenProvider implements OauthAccessTokenProvider {

    private static final SFLogger logger = SFLoggerFactory.getLogger(AuthorizationCodeFlowAccessTokenProvider.class);

    private static final String AUTHORIZE_ENDPOINT = "/oauth/authorize";
    private static final String TOKEN_REQUEST_ENDPOINT = "/oauth/token-request";

    private static final String REDIRECT_URI_HOST = "localhost";
    private static final int DEFAULT_REDIRECT_URI_PORT = 8001;
    private static final String REDIRECT_URI_ENDPOINT = "/snowflake/oauth-redirect";
    public static final String SESSION_ROLE_SCOPE = "session:role";

    public static int AUTHORIZE_REDIRECT_TIMEOUT_MINUTES = 2;

    private final DefaultAuthExternalBrowserHandlers browserUtil = new DefaultAuthExternalBrowserHandlers();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String getAccessToken(SFLoginInput loginInput) throws SFException {
        AuthorizationCode authorizationCode = requestAuthorizationCode(loginInput);
        return exchangeAuthorizationCodeForAccessToken(loginInput, authorizationCode);
    }

    private AuthorizationCode requestAuthorizationCode(SFLoginInput loginInput) throws SFException {
        try {
            AuthorizationRequest request = buildAuthorizationRequest(loginInput);
            URI authorizeRequestURI = request.toURI();
            CompletableFuture<String> codeFuture = setupRedirectURIServerForAuthorizationCode(loginInput.getRedirectUriPort());
            letUserAuthorizeViaBrowser(authorizeRequestURI);
            String code = codeFuture.get(AUTHORIZE_REDIRECT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            return new AuthorizationCode(code);
        } catch (Exception e) {
            if (e instanceof TimeoutException) {
                logger.error("Authorization request timed out. Did not receive authorization code back to the redirect URI");
            }
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private String exchangeAuthorizationCodeForAccessToken(SFLoginInput loginInput, AuthorizationCode authorizationCode) throws SFException {
        try {
            TokenRequest request = buildTokenRequest(loginInput, authorizationCode);
            String tokenResponse = HttpUtil.executeGeneralRequest(
                    convertTokenRequest(request.toHTTPRequest()),
                    loginInput.getLoginTimeout(),
                    loginInput.getAuthTimeout(),
                    loginInput.getSocketTimeoutInMillis(),
                    0,
                    loginInput.getHttpClientSettingsKey());
            TokenResponseDTO tokenResponseDTO = objectMapper.readValue(tokenResponse, TokenResponseDTO.class);
            return tokenResponseDTO.getAccessToken();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void letUserAuthorizeViaBrowser(URI authorizeRequestURI) throws SFException {
        browserUtil.openBrowser(authorizeRequestURI.toString());
    }

    private static CompletableFuture<String> setupRedirectURIServerForAuthorizationCode(int redirectUriPort) throws IOException {
        CompletableFuture<String> accessTokenFuture = new CompletableFuture<>();
        int redirectPort = (redirectUriPort != -1) ? redirectUriPort : DEFAULT_REDIRECT_URI_PORT;
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(REDIRECT_URI_HOST, redirectPort), 0);
        httpServer.createContext(REDIRECT_URI_ENDPOINT, exchange -> {
            String authorizationCode = extractAuthorizationCodeFromQueryParameters(exchange.getRequestURI().getQuery());
            if (!StringUtils.isNullOrEmpty(authorizationCode)) {
                accessTokenFuture.complete(authorizationCode);
                httpServer.stop(0);
            }
        });
        httpServer.start();
        return accessTokenFuture;
    }

    private static AuthorizationRequest buildAuthorizationRequest(SFLoginInput loginInput) throws URISyntaxException {
        URI authorizeEndpoint = new URI(loginInput.getServerUrl() + AUTHORIZE_ENDPOINT);
        ClientID clientID = new ClientID(loginInput.getClientId());
        Scope scope = new Scope(String.format("%s:%s", SESSION_ROLE_SCOPE, loginInput.getRole()));
        URI callback = buildRedirectURI(loginInput.getRedirectUriPort());
        State state = new State(256);
        return new AuthorizationRequest.Builder(
                new ResponseType(ResponseType.Value.CODE), clientID)
                .scope(scope)
                .state(state)
                .redirectionURI(callback)
                .endpointURI(authorizeEndpoint)
                .build();
    }

    private static TokenRequest buildTokenRequest(SFLoginInput loginInput, AuthorizationCode authorizationCode) throws URISyntaxException {
        URI callback = buildRedirectURI(loginInput.getRedirectUriPort());
        AuthorizationGrant codeGrant = new AuthorizationCodeGrant(authorizationCode, callback);
        ClientAuthentication clientAuthentication = new ClientSecretBasic(new ClientID(loginInput.getClientId()), new Secret(loginInput.getClientSecret()));
        URI tokenEndpoint = new URI(String.format(loginInput.getServerUrl() + TOKEN_REQUEST_ENDPOINT));
        Scope scope = new Scope(SESSION_ROLE_SCOPE, loginInput.getRole());
        return new TokenRequest(tokenEndpoint, clientAuthentication, codeGrant, scope);
    }

    private static URI buildRedirectURI(int redirectUriPort) throws URISyntaxException {
        redirectUriPort = (redirectUriPort != -1) ? redirectUriPort : DEFAULT_REDIRECT_URI_PORT;
        return new URI(String.format("http://%s:%s%s", REDIRECT_URI_HOST, redirectUriPort, REDIRECT_URI_ENDPOINT));
    }

    private static String extractAuthorizationCodeFromQueryParameters(String queryParameters) {
        String prefix = "code=";
        String codeSuffix = queryParameters.substring(queryParameters.indexOf(prefix) + prefix.length());
        return codeSuffix.substring(0, codeSuffix.indexOf("&"));
    }

    private static HttpRequestBase convertTokenRequest(HTTPRequest nimbusRequest) {
        HttpPost request = new HttpPost(nimbusRequest.getURI());
        request.setEntity(new StringEntity(nimbusRequest.getBody(), StandardCharsets.UTF_8));
        nimbusRequest.getHeaderMap().forEach((key, values) -> request.addHeader(key, values.get(0)));
        return request;
    }
}
