package net.snowflake.client.authentication;
import static net.snowflake.client.authentication.AuthConnectionParameters.*;
import java.io.IOException;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.TESTING)
public class OauthAuthorizationCodeLatestIT {
    String login = AuthConnectionParameters.SSO_USER;
    String password = AuthConnectionParameters.SSO_PASSWORD;
    AuthTestHelper authTestHelper = new AuthTestHelper();

    private String idToken;


    @BeforeEach
    public void setUp() throws IOException {
        AuthTestHelper.deleteIdToken();
    }

    //AUTHORIZATION CODE - OAUTH BY OKTA

    @Test
    void shouldAuthenticateUsingExternalOauthOktaAuthorizationCode() throws InterruptedException {
        Properties properties = getOAuthExternalAuthorizationCodeConnectionParameters();

        Thread provideCredentialsThread =
                new Thread(() -> authTestHelper.provideCredentials("externalOauthOktaSuccess", login, password));
        Thread connectThread =
                new Thread(() -> authTestHelper.connectAndExecuteSimpleQuery(properties, null));

        authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.verifyExceptionIsNotThrown();
    }


    //AUTHORIZATION CODE - OAUTH BY SNOWFLAKE
    @Test
    void shouldAuthenticateUsingSnowflakeOauthOktaAuthorizationCode() throws InterruptedException {
        Properties properties = getOAuthSnowflakeAuthorizationCodeConnectionParameters();
        Thread provideCredentialsThread =
                new Thread(() -> authTestHelper.provideCredentials("internalOauthSnowflakeSuccess", login, password));
        Thread connectThread =
                new Thread(() -> authTestHelper.connectAndExecuteSimpleQuery(properties, null));

        authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.verifyExceptionIsNotThrown();
    }
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

