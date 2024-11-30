package net.snowflake.client.core.auth.oauth;

import com.nimbusds.oauth2.sdk.AccessTokenResponse;
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.AuthorizationRequest;
import com.nimbusds.oauth2.sdk.ResponseType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.sun.net.httpserver.HttpServer;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import org.apache.http.client.methods.HttpGet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

@SnowflakeJdbcInternalApi
public class AuthorizationCodeFlowAccessTokenProvider implements OauthAccessTokenProvider {

    private static final String REDIRECT_URI_HOST = "localhost";
    private static final int REDIRECT_URI_PORT = 8001;
    private static final String REDIRECT_URI_PATH = "/oauth-redirect";

    @Override
    public String getAccessToken(SFLoginInput loginInput) throws SFException {
        AuthorizationCode authorizationCode = requestAuthorizationCode(loginInput);
        AccessToken accessToken = exchangeAuthorizationCodeForAccessToken(loginInput, authorizationCode);
        return accessToken.getValue();
    }

    private AuthorizationCode requestAuthorizationCode(SFLoginInput loginInput) throws SFException {
        try {
            AuthorizationRequest request = buildAuthorizationRequest(loginInput);

            URI requestURI = request.toURI();
            HttpUtil.executeGeneralRequest(new HttpGet(requestURI),
                    loginInput.getLoginTimeout(),
                    loginInput.getAuthTimeout(),
                    loginInput.getSocketTimeoutInMillis(),
                    0,
                    loginInput.getHttpClientSettingsKey());
            CompletableFuture<String> f = getAuthorizationCodeFromRedirectURI();
            f.join();
            return new AuthorizationCode(f.get());
        } catch (Exception e) {
            throw new SFException(e, ErrorCode.INTERNAL_ERROR);
        }
    }

    private static AccessToken exchangeAuthorizationCodeForAccessToken(SFLoginInput loginInput, AuthorizationCode authorizationCode) throws SFException {
        try {
            TokenRequest request = buildTokenRequest(loginInput, authorizationCode);
            TokenResponse response = TokenResponse.parse(request.toHTTPRequest().send());
            if (!response.indicatesSuccess()) {
                TokenErrorResponse errorResponse = response.toErrorResponse();
                errorResponse.getErrorObject();
            }
            AccessTokenResponse successResponse = response.toSuccessResponse();
            return successResponse.getTokens().getAccessToken();
        } catch (Exception e) {
            throw new SFException(e, ErrorCode.INTERNAL_ERROR);
        }
    }

    private static CompletableFuture<String> getAuthorizationCodeFromRedirectURI() throws IOException {
        CompletableFuture<String> accessTokenFuture = new CompletableFuture<>();
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(REDIRECT_URI_HOST, REDIRECT_URI_PORT), 0);
        httpServer.createContext(REDIRECT_URI_PATH, exchange -> {
            String authorizationCode = exchange.getRequestURI().getQuery();
            accessTokenFuture.complete(authorizationCode);
            httpServer.stop(0);
        });
        return accessTokenFuture;
    }

    private static AuthorizationRequest buildAuthorizationRequest(SFLoginInput loginInput) throws URISyntaxException {
        URI authorizeEndpoint = new URI(String.format("%s/oauth/authorize", loginInput.getServerUrl()));
        ClientID clientID = new ClientID("123");
        Scope scope = new Scope("read", "write");
        URI callback = buildRedirectURI();
        State state = new State();
        return new AuthorizationRequest.Builder(
                new ResponseType(ResponseType.Value.CODE), clientID)
                .scope(scope)
                .state(state)
                .redirectionURI(callback)
                .endpointURI(authorizeEndpoint)
                .build();
    }

    private static URI buildRedirectURI() throws URISyntaxException {
        return new URI(String.format("https://%s:%s%s", REDIRECT_URI_HOST, REDIRECT_URI_PORT, REDIRECT_URI_PATH));
    }

    private static TokenRequest buildTokenRequest(SFLoginInput loginInput, AuthorizationCode authorizationCode) throws URISyntaxException {
        URI callback = buildRedirectURI();
        AuthorizationGrant codeGrant = new AuthorizationCodeGrant(authorizationCode, callback);
        ClientID clientID = new ClientID("123");
        Secret clientSecret = new Secret("123");
        ClientAuthentication clientAuthentication = new ClientSecretBasic(clientID, clientSecret);
        URI tokenEndpoint = new URI(String.format("%s/oauth/token", loginInput.getServerUrl()));
        return new TokenRequest(tokenEndpoint, clientAuthentication, codeGrant, new Scope());
    }
}
