package net.snowflake.client.internal.core.auth.wif;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;

import net.snowflake.client.internal.core.SFException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
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
  public void shouldPreserveCauseWhenStsCallFails() {
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
}
