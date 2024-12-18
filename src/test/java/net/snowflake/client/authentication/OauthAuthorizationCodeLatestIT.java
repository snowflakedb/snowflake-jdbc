package net.snowflake.client.authentication;

import static net.snowflake.client.authentication.AuthConnectionParameters.getOAuthExternalAuthorizationCodeConnectionParameters;
import static net.snowflake.client.authentication.AuthConnectionParameters.getOauthConnectionParameters;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.AUTHENTICATION)
public class OauthAuthorizationCodeLatestIT {
    AuthTestHelper authTestHelper;
    private String idToken;


    @BeforeEach
    public void setUp() throws IOException {
        authTestHelper = new AuthTestHelper();
    }

    @Test
    void shouldAuthenticateUsingOauthAuthorizationCode() throws IOException {
        Properties properties = getOAuthExternalAuthorizationCodeConnectionParameters();
        authTestHelper.connectAndExecuteSimpleQuery(getOAuthExternalAuthorizationCodeConnectionParameters(), null);

        authTestHelper.connectAndExecuteSimpleQuery(properties,null);
    }


//        Thread provideCredentialsThread =
//                new Thread(() -> authTestHelper.provideCredentials("success", login, password));


       // AccessTokenProvider provider = new OAuthClientCredentialsAccessTokenProvider();
//        SFLoginInput loginInput = new SFLoginInput();
//        String accessToken = provider.getAccessToken(loginInput);
//        loginInput.set
//
//        authTestHelper.connectAndExecuteSimpleQuery(getOauthConnectionParameters(accessToken), null);
//        authTestHelper.verifyExceptionIsNotThrown();

    }

//    @Test
//    public void BrowserTimeout() throws SFException {
//        AccessTokenProvider provider = new OAuthClientCredentialsAccessTokenProvider();
//        String accessToken = provider.getAccessToken(loginInput);
//    }
//
//    @Test
//    public void FailedConnection() throws SFException {
//        AccessTokenProvider provider = new OAuthClientCredentialsAccessTokenProvider();
//        String accessToken = provider.getAccessToken(loginInput);
//    }


    //CLIENT CREDENTIALS VS TOKEN REQUEST?

