package net.snowflake.client.authentication;

import static net.snowflake.client.authentication.AuthConnectionParameters.getStoreIDTokenConnectionParameters;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag(TestTags.AUTHENTICATION)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IdTokenIT {

  String login = AuthConnectionParameters.SSO_USER;
  String password = AuthConnectionParameters.SSO_PASSWORD;
  AuthTest authTest;
  private static String firstToken;

  @BeforeAll
  public static void globalSetUp() {
    AuthTest.deleteIdToken();
  }

  @BeforeEach
  public void setUp() throws IOException {
    authTest = new AuthTest();
  }

  @AfterEach
  public void tearDown() {
    authTest.cleanBrowserProcesses();
  }

  @Test
  @Order(1)
  void shouldAuthenticateUsingExternalBrowserAndSaveToken() throws InterruptedException {
    Thread provideCredentialsThread =
        new Thread(() -> authTest.provideCredentials("success", login, password));
    Thread connectThread =
        authTest.getConnectAndExecuteSimpleQueryThread(getStoreIDTokenConnectionParameters());

    authTest.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTest.verifyExceptionIsNotThrown();
    firstToken = authTest.getIdToken();
    verifyFirstTokenWasSaved();
  }

  @Test
  @Order(2)
  void shouldAuthenticateUsingTokenWithoutBrowser() {
    verifyFirstTokenWasSaved();
    authTest.connectAndExecuteSimpleQuery(getStoreIDTokenConnectionParameters(), null);
    authTest.verifyExceptionIsNotThrown();
  }

  @Test
  @Order(3)
  void shouldOpenBrowserAgainWhenTokenIsDeleted() throws InterruptedException {
    verifyFirstTokenWasSaved();
    AuthTest.deleteIdToken();
    Thread provideCredentialsThread =
        new Thread(() -> authTest.provideCredentials("success", login, password));
    Thread connectThread =
        authTest.getConnectAndExecuteSimpleQueryThread(getStoreIDTokenConnectionParameters());

    authTest.connectAndProvideCredentials(provideCredentialsThread, connectThread);
    authTest.verifyExceptionIsNotThrown();
    String secondToken = authTest.getIdToken();
    assertThat("Id token was not saved", secondToken, notNullValue());
    assertThat("Id token was not updated", secondToken, not(firstToken));
  }

  private void verifyFirstTokenWasSaved() {
    assertThat("Id token was not saved", firstToken, notNullValue());
  }
}
