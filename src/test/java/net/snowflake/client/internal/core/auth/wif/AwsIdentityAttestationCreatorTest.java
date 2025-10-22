package net.snowflake.client.internal.core.auth.wif;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SFLoginInput;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;

public class AwsIdentityAttestationCreatorTest {

  @Test
  public void shouldThrowExceptionWhenNoCredentialsFound() {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(null);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(Region.US_EAST_1);
    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, new SFLoginInput());
    assertThrows(SFException.class, attestationCreator::createAttestation);
  }

  @Test
  public void shouldThrowExceptionWhenNoRegion() {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials())
        .thenReturn(AwsBasicCredentials.create("abc", "abc"));
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(null);

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, new SFLoginInput());
    assertThrows(SFException.class, attestationCreator::createAttestation);
  }

  @Test
  public void shouldReturnProperAttestationWithStandardRegion()
      throws JsonProcessingException, SFException {
    shouldReturnProperAttestationWithSignedRequestCredential(
        Region.US_EAST_1, "sts.us-east-1.amazonaws.com");
  }

  @Test
  public void shouldReturnProperAttestationWithCnRegion()
      throws JsonProcessingException, SFException {
    shouldReturnProperAttestationWithSignedRequestCredential(
        Region.CN_NORTHWEST_1, "sts.cn-northwest-1.amazonaws.com.cn");
  }

  private void shouldReturnProperAttestationWithSignedRequestCredential(
      Region region, String expectedStsUrl) throws JsonProcessingException, SFException {
    AwsAttestationService attestationServiceSpy = Mockito.spy(AwsAttestationService.class);
    Mockito.doReturn(AwsSessionCredentials.create("abc", "abc", "aws-session-token"))
        .when(attestationServiceSpy)
        .getAWSCredentials();
    Mockito.doReturn(region).when(attestationServiceSpy).getAWSRegion();

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceSpy, new SFLoginInput());
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.AWS, attestation.getProvider());
    assertNotNull(attestation.getCredential());
    Base64.Decoder decoder = Base64.getDecoder();
    String json = new String(decoder.decode(attestation.getCredential()));
    Map<String, Object> credentialMap = new ObjectMapper().readValue(json, HashMap.class);
    assertEquals(3, credentialMap.size());
    assertEquals(
        String.format("https://%s?Action=GetCallerIdentity&Version=2011-06-15", expectedStsUrl),
        credentialMap.get("url"));
    assertEquals("POST", credentialMap.get("method"));
    assertNotNull(credentialMap.get("headers"));
    Map<String, String> headersMap = (Map<String, String>) credentialMap.get("headers");
    assertEquals(6, headersMap.size());
    assertEquals(expectedStsUrl, headersMap.get("Host"));
    assertEquals("snowflakecomputing.com", headersMap.get("X-Snowflake-Audience"));
    assertNotNull(headersMap.get("X-Amz-Date"));
    assertTrue(headersMap.get("Authorization").matches("^AWS4-HMAC-SHA256 Credential=.*"));
    assertEquals("aws-session-token", headersMap.get("X-Amz-Security-Token"));
    assertNotNull(headersMap.get("x-amz-content-sha256"));
  }

  @Test
  public void shouldCreateAttestationWithoutImpersonationWhenImpersonationPathIsEmpty()
      throws SFException {
    AwsAttestationService attestationServiceSpy = Mockito.spy(AwsAttestationService.class);
    Mockito.doReturn(AwsSessionCredentials.create("abc", "abc", "aws-session-token"))
        .when(attestationServiceSpy)
        .getAWSCredentials();
    Mockito.doReturn(Region.US_EAST_1).when(attestationServiceSpy).getAWSRegion();

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityImpersonationPath(""); // Empty path

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceSpy, loginInput);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.AWS, attestation.getProvider());
    assertNotNull(attestation.getCredential());
  }

  @Test
  public void shouldThrowExceptionWhenImpersonationFailsWithNoInitialCredentials() {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(null);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(Region.US_EAST_1);

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityImpersonationPath("arn:aws:iam::123456789012:role/TestRole");

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, loginInput);

    assertThrows(SFException.class, attestationCreator::createAttestation);
  }

  @Test
  public void shouldCreateAttestationWithImpersonationPath() throws SFException {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    SdkHttpRequest requestMock = mock(SdkHttpRequest.class);
    Mockito.when(requestMock.getUri()).thenReturn(URI.create("https://snowflakecomputing.com"));
    Mockito.when(requestMock.method()).thenReturn(SdkHttpMethod.GET);

    AwsSessionCredentials initialCredentials =
        AwsSessionCredentials.create("initial-key", "initial-secret", "initial-token");
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(initialCredentials);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(Region.US_EAST_1);
    Mockito.when(attestationServiceMock.getCredentialsViaRoleChaining(any())).thenCallRealMethod();

    AwsSessionCredentials assumedCredentials =
        AwsSessionCredentials.create("assumed-key", "assumed-secret", "assumed-token");
    Mockito.when(
            attestationServiceMock.assumeRole(
                initialCredentials, "arn:aws:iam::123456789012:role/TestRole"))
        .thenReturn(assumedCredentials);

    Mockito.doNothing().when(attestationServiceMock).initializeSignerRegion();
    Mockito.when(attestationServiceMock.signRequestWithSigV4(any(), Mockito.eq(assumedCredentials)))
        .thenReturn(requestMock);

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityImpersonationPath("arn:aws:iam::123456789012:role/TestRole");

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, loginInput);

    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.AWS, attestation.getProvider());
    assertNotNull(attestation.getCredential());

    Mockito.verify(attestationServiceMock)
        .assumeRole(initialCredentials, "arn:aws:iam::123456789012:role/TestRole");
  }
}
