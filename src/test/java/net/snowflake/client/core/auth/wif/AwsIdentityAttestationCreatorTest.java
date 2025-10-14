package net.snowflake.client.core.auth.wif;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class AwsIdentityAttestationCreatorTest {

  @Test
  public void shouldThrowExceptionWhenNoCredentialsFound() {
    AwsAttestationService attestationServiceMock = Mockito.mock(AwsAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(null);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn("us-east-1");
    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, new SFLoginInput());
    assertThrows(SFException.class, attestationCreator::createAttestation);
  }

  @Test
  public void shouldThrowExceptionWhenNoRegion() {
    AwsAttestationService attestationServiceMock = Mockito.mock(AwsAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials())
        .thenReturn(new BasicAWSCredentials("abc", "abc"));
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(null);

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, new SFLoginInput());
    assertThrows(SFException.class, attestationCreator::createAttestation);
  }

  @Test
  public void shouldReturnProperAttestationWithStandardRegion()
      throws JsonProcessingException, SFException {
    shouldReturnProperAttestationWithSignedRequestCredential(
        "us-east-1", "sts.us-east-1.amazonaws.com");
  }

  @Test
  public void shouldReturnProperAttestationWithCnRegion()
      throws JsonProcessingException, SFException {
    shouldReturnProperAttestationWithSignedRequestCredential(
        "cn-northwest-1", "sts.cn-northwest-1.amazonaws.com.cn");
  }

  private void shouldReturnProperAttestationWithSignedRequestCredential(
      String region, String expectedStsUrl) throws JsonProcessingException, SFException {
    AwsAttestationService attestationServiceSpy = Mockito.spy(AwsAttestationService.class);
    Mockito.doReturn(new BasicSessionCredentials("abc", "abc", "aws-session-token"))
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
    assertEquals(5, headersMap.size());
    assertEquals(expectedStsUrl, headersMap.get("Host"));
    assertEquals("snowflakecomputing.com", headersMap.get("X-Snowflake-Audience"));
    assertNotNull(headersMap.get("X-Amz-Date"));
    assertTrue(headersMap.get("Authorization").matches("^AWS4-HMAC-SHA256 Credential=.*"));
    assertEquals("aws-session-token", headersMap.get("X-Amz-Security-Token"));
  }

  @Test
  public void shouldCreateAttestationWithoutImpersonationWhenImpersonationPathIsEmpty()
      throws SFException {
    AwsAttestationService attestationServiceSpy = Mockito.spy(AwsAttestationService.class);
    Mockito.doReturn(new BasicSessionCredentials("abc", "abc", "aws-session-token"))
        .when(attestationServiceSpy)
        .getAWSCredentials();
    Mockito.doReturn("us-east-1").when(attestationServiceSpy).getAWSRegion();

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
    AwsAttestationService attestationServiceMock = Mockito.mock(AwsAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(null);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn("us-east-1");

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityImpersonationPath("arn:aws:iam::123456789012:role/TestRole");

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, loginInput);

    assertThrows(SFException.class, attestationCreator::createAttestation);
  }

  @Test
  public void shouldCreateAttestationWithImpersonationPath() throws SFException {
    AwsAttestationService attestationServiceMock = Mockito.mock(AwsAttestationService.class);

    BasicSessionCredentials initialCredentials =
        new BasicSessionCredentials("initial-key", "initial-secret", "initial-token");
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(initialCredentials);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn("us-east-1");
    Mockito.when(attestationServiceMock.getCredentialsViaRoleChaining(any())).thenCallRealMethod();

    BasicSessionCredentials assumedCredentials =
        new BasicSessionCredentials("assumed-key", "assumed-secret", "assumed-token");
    Mockito.when(
            attestationServiceMock.assumeRole(
                initialCredentials, "arn:aws:iam::123456789012:role/TestRole"))
        .thenReturn(assumedCredentials);

    Mockito.doNothing().when(attestationServiceMock).initializeSignerRegion();
    Mockito.doNothing()
        .when(attestationServiceMock)
        .signRequestWithSigV4(any(), Mockito.eq(assumedCredentials));

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
