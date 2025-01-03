package net.snowflake.client.authentication;
import static net.snowflake.client.authentication.AuthConnectionParameters.*;
import java.io.IOException;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.TESTING)
public class OauthSnowflakeClientCredentialsLatestIT {
    AuthTestHelper authTestHelper = new AuthTestHelper();

    private String idToken;


    @BeforeEach
    public void setUp() throws IOException {
        AuthTestHelper.deleteIdToken();
    }

    @Test
    void shouldAuthenticateUsingSnowflakeOauthClientCredentials() {
        Properties properties = getOAuthSnowflakeClientCredentialParameters();
        authTestHelper.connectAndExecuteSimpleQuery(properties, null);
        authTestHelper.verifyExceptionIsNotThrown();
    }
}
