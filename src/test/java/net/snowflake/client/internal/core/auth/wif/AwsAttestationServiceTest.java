package net.snowflake.client.internal.core.auth.wif;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SFLoginInput;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.GetWebIdentityTokenRequest;
import software.amazon.awssdk.services.sts.model.GetWebIdentityTokenResponse;

public class AwsAttestationServiceTest {

  private static final AwsSessionCredentials CREDENTIALS =
      AwsSessionCredentials.create("akid", "secret", "session-token");

  @Test
  public void shouldReturnJwtFromGetWebIdentityToken() throws SFException {
    AwsAttestationService spy = Mockito.spy(new AwsAttestationService());
    Mockito.doReturn(Region.US_EAST_1).when(spy).getAWSRegion();

    StsClient stsClient = mock(StsClient.class);
    Mockito.when(stsClient.getWebIdentityToken(any(GetWebIdentityTokenRequest.class)))
        .thenReturn(GetWebIdentityTokenResponse.builder().webIdentityToken("jwt-value").build());
    Mockito.doReturn(stsClient).when(spy).createStsClient(any(), anyInt());

    String jwt = spy.getWebIdentityToken(CREDENTIALS);

    assertEquals("jwt-value", jwt);

    ArgumentCaptor<GetWebIdentityTokenRequest> requestCaptor =
        ArgumentCaptor.forClass(GetWebIdentityTokenRequest.class);
    Mockito.verify(stsClient).getWebIdentityToken(requestCaptor.capture());
    GetWebIdentityTokenRequest request = requestCaptor.getValue();
    assertTrue(request.audience().contains(WorkloadIdentityUtil.SNOWFLAKE_AUDIENCE));
    assertEquals("ES384", request.signingAlgorithm());
  }

  @Test
  public void shouldPreserveCauseWhenStsCallFails() throws SFException {
    AwsAttestationService spy = Mockito.spy(new AwsAttestationService());
    Mockito.doReturn(Region.US_EAST_1).when(spy).getAWSRegion();

    StsClient stsClient = mock(StsClient.class);
    RuntimeException original = new RuntimeException("sts boom");
    Mockito.when(stsClient.getWebIdentityToken(any(GetWebIdentityTokenRequest.class)))
        .thenThrow(original);
    Mockito.doReturn(stsClient).when(spy).createStsClient(any(), anyInt());

    SFException thrown =
        assertThrows(SFException.class, () -> spy.getWebIdentityToken(CREDENTIALS));
    // SFException's local override of getCause() returns null in this codebase, so verify
    // the original failure surfaces in the user-facing message instead.
    assertTrue(thrown.getMessage().contains("sts boom"), thrown::getMessage);
  }

  @Test
  public void shouldDisableFipsAndDualstackWhenWorkloadIdentityHostIsSet() throws SFException {
    AwsAttestationService service = new AwsAttestationService();
    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityHost("sts.custom.example.com");
    service.setLoginInput(loginInput);

    StsClientBuilder builder = mock(StsClientBuilder.class);
    Mockito.when(builder.endpointOverride(any())).thenReturn(builder);
    Mockito.when(builder.fipsEnabled(false)).thenReturn(builder);
    Mockito.when(builder.dualstackEnabled(false)).thenReturn(builder);

    service.applyStsEndpointOverride(builder);

    verify(builder).endpointOverride(java.net.URI.create("https://sts.custom.example.com"));
    verify(builder).fipsEnabled(eq(false));
    verify(builder).dualstackEnabled(eq(false));
  }

  @Test
  public void shouldNotOverrideEndpointWhenWorkloadIdentityHostIsUnset() throws SFException {
    AwsAttestationService service = new AwsAttestationService();
    service.setLoginInput(new SFLoginInput());

    StsClientBuilder builder = mock(StsClientBuilder.class);
    service.applyStsEndpointOverride(builder);
    verifyNoInteractions(builder);
  }

  /**
   * Drives the real {@code assumeRole} and {@code getWebIdentityToken} SDK clients against a local
   * server. Those are the only AWS WIF paths that open a socket to STS from the driver, so they are
   * the paths that hard-fail when the regional default does not resolve. Mocking {@code
   * createStsClient} would hide a dropped {@code endpointOverride}.
   */
  @Test
  public void shouldRouteAssumeRoleAndWebIdentityTokenToConfiguredHost()
      throws Exception, SFException {
    String assumeRoleXml =
        "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"
            + "<AssumeRoleResult><Credentials>"
            + "<AccessKeyId>CHAINED_KEY</AccessKeyId>"
            + "<SecretAccessKey>CHAINED_SECRET</SecretAccessKey>"
            + "<SessionToken>CHAINED_TOKEN</SessionToken>"
            + "<Expiration>2035-01-01T00:00:00Z</Expiration>"
            + "</Credentials></AssumeRoleResult></AssumeRoleResponse>";
    String webIdentityXml =
        "<GetWebIdentityTokenResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"
            + "<GetWebIdentityTokenResult>"
            + "<WebIdentityToken>header.payload.signature</WebIdentityToken>"
            + "</GetWebIdentityTokenResult></GetWebIdentityTokenResponse>";

    StsStub assumeStub = StsStub.start(assumeRoleXml);
    try {
      AwsAttestationService service = serviceAimedAt(assumeStub.url());
      AwsCredentials chained =
          service.assumeRole(CREDENTIALS, "arn:aws:iam::123456789012:role/target", null);
      assertEquals("CHAINED_KEY", chained.accessKeyId());
      assertEquals("CHAINED_TOKEN", ((AwsSessionCredentials) chained).sessionToken());
      assertEquals(Collections.singletonList("AssumeRole"), assumeStub.actions);
    } finally {
      assumeStub.stop();
    }

    StsStub hopsStub = StsStub.start(assumeRoleXml);
    try {
      AwsAttestationService spy = serviceAimedAt(hopsStub.url());
      Mockito.doReturn(CREDENTIALS).when(spy).getAWSCredentials();
      SFLoginInput loginInput = new SFLoginInput();
      loginInput.setWorkloadIdentityHost(hopsStub.url());
      loginInput.setWorkloadIdentityImpersonationPath(
          "arn:aws:iam::111111111111:role/first,arn:aws:iam::222222222222:role/second");
      spy.setLoginInput(loginInput);
      AwsCredentials chained = spy.getCredentialsViaRoleChaining(loginInput);
      assertEquals("CHAINED_KEY", chained.accessKeyId());
      assertEquals(2, hopsStub.actions.size());
      assertEquals("AssumeRole", hopsStub.actions.get(0));
      assertEquals("AssumeRole", hopsStub.actions.get(1));
    } finally {
      hopsStub.stop();
    }

    StsStub tokenStub = StsStub.start(webIdentityXml);
    try {
      AwsAttestationService service = serviceAimedAt(tokenStub.url());
      assertEquals("header.payload.signature", service.getWebIdentityToken(CREDENTIALS));
      assertEquals(Collections.singletonList("GetWebIdentityToken"), tokenStub.actions);
    } finally {
      tokenStub.stop();
    }
  }

  private static AwsAttestationService serviceAimedAt(String stsUrl) {
    AwsAttestationService spy = Mockito.spy(new AwsAttestationService());
    Mockito.doReturn(Region.US_EAST_1).when(spy).getAWSRegion();
    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityHost(stsUrl);
    spy.setLoginInput(loginInput);
    return spy;
  }

  private static final class StsStub {
    private final HttpServer server;
    private final List<String> actions = Collections.synchronizedList(new ArrayList<String>());

    private StsStub(HttpServer server) {
      this.server = server;
    }

    static StsStub start(String xmlBody) throws IOException {
      HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      StsStub stub = new StsStub(server);
      byte[] response = xmlBody.getBytes(StandardCharsets.UTF_8);
      server.createContext(
          "/",
          exchange -> {
            try {
              stub.actions.add(
                  actionFrom(
                      readFully(exchange.getRequestBody()),
                      exchange.getRequestURI().getRawQuery()));
              exchange.getResponseHeaders().set("Content-Type", "text/xml");
              exchange.sendResponseHeaders(200, response.length);
              exchange.getResponseBody().write(response);
            } finally {
              exchange.close();
            }
          });
      server.start();
      return stub;
    }

    String url() {
      return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    void stop() {
      server.stop(0);
    }

    private static String readFully(InputStream in) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[1024];
      int n;
      while ((n = in.read(buf)) != -1) {
        out.write(buf, 0, n);
      }
      return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    private static String actionFrom(String body, String query) {
      String source = (body != null && !body.isEmpty()) ? body : query;
      if (source == null) {
        return "";
      }
      String[] parts = source.split("&");
      for (int i = 0; i < parts.length; i++) {
        String part = parts[i];
        int eq = part.indexOf('=');
        if (eq <= 0) {
          continue;
        }
        if (!"Action".equals(urlDecode(part.substring(0, eq)))) {
          continue;
        }
        return urlDecode(part.substring(eq + 1));
      }
      return "";
    }

    private static String urlDecode(String value) {
      try {
        return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
      } catch (Exception e) {
        return value;
      }
    }
  }
}
