package net.snowflake.client.authentication;
import static net.snowflake.client.authentication.AuthConnectionParameters.*;
import java.io.IOException;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.TESTING)
public class OauthSnowflakeAuthorizationCodeLatestIT {
    String login = AuthConnectionParameters.SSO_USER;
    String password = AuthConnectionParameters.SSO_PASSWORD;
    AuthTestHelper authTestHelper = new AuthTestHelper();

    private String idToken;


    @BeforeEach
    public void setUp() throws IOException {
        AuthTestHelper.deleteIdToken();
    }
    @Test
    void shouldAuthenticateUsingSnowflakeOauthAuthorizationCode() throws InterruptedException {
        Properties properties = getOAuthSnowflakeAuthorizationCodeConnectionParameters();
        Thread provideCredentialsThread =
                new Thread(() -> authTestHelper.provideCredentials("internalOauthSnowflakeSuccess", login, password));
        Thread connectThread =
                new Thread(() -> authTestHelper.connectAndExecuteSimpleQuery(properties, null));

        authTestHelper.connectAndProvideCredentials(provideCredentialsThread, connectThread);
        authTestHelper.verifyExceptionIsNotThrown();
    }
}


