package net.snowflake.client.core.auth.oauth;

import com.nimbusds.oauth2.sdk.id.State;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import net.snowflake.client.core.SFException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class AuthorizationCodeRedirectRequestHandlerTest {

  CompletableFuture<String> authorizationCodeFutureMock = Mockito.mock(CompletableFuture.class);

  @Test
  public void shouldReturnSuccessResponse() {
    Map<String, String> params = new HashMap<>();
    params.put("code", "some authorization code");
    params.put("state", "abc");

    String response =
        AuthorizationCodeRedirectRequestHandler.handleRedirectRequest(
            params, authorizationCodeFutureMock, new State("abc"));
    Mockito.verify(authorizationCodeFutureMock).complete("some authorization code");
    Assertions.assertEquals("Authorization completed successfully.", response);
  }

  @Test
  public void shouldReturnRandomErrorResponse() {
    Map<String, String> params = new HashMap<>();
    params.put("error", "some random error");

    String response =
        AuthorizationCodeRedirectRequestHandler.handleRedirectRequest(
            params, authorizationCodeFutureMock, new State("abc"));
    Mockito.verify(authorizationCodeFutureMock)
        .completeExceptionally(Mockito.any(SFException.class));
    Assertions.assertEquals("Authorization error: some random error", response);
  }

  @Test
  public void shouldReturnEscapedErrorResponse() {
    Map<String, String> params = new HashMap<>();
    params.put("error", "<script>some malicious script<script>");

    String response =
        AuthorizationCodeRedirectRequestHandler.handleRedirectRequest(
            params, authorizationCodeFutureMock, new State("abc"));
    Mockito.verify(authorizationCodeFutureMock)
        .completeExceptionally(Mockito.any(SFException.class));
    Assertions.assertEquals(
        "Authorization error: &lt;script&gt;some malicious script&lt;script&gt;", response);
  }

  @Test
  public void shouldReturnInvalidStateErrorResponse() {
    Map<String, String> params = new HashMap<>();
    params.put("authorization_code", "some authorization code");
    params.put("state", "invalid state");

    String response =
        AuthorizationCodeRedirectRequestHandler.handleRedirectRequest(
            params, authorizationCodeFutureMock, new State("abc"));
    Mockito.verify(authorizationCodeFutureMock)
        .completeExceptionally(Mockito.any(SFException.class));
    Assertions.assertEquals(
        "Authorization error: invalid authorization request redirection state", response);
  }

  @Test
  public void shouldReturnAuthorizationCodeAbsentErrorResponse() {
    Map<String, String> params = new HashMap<>();
    params.put("state", "abc");
    params.put("some-random-param", "some-value");

    String response =
        AuthorizationCodeRedirectRequestHandler.handleRedirectRequest(
            params, authorizationCodeFutureMock, new State("abc"));
    Mockito.verify(authorizationCodeFutureMock)
        .completeExceptionally(Mockito.any(SFException.class));
    Assertions.assertEquals(
        "Authorization error: authorization code has not been returned to the driver.", response);
  }
}
