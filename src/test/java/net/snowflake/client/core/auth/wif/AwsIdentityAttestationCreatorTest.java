package net.snowflake.client.core.auth.wif;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class AwsIdentityAttestationCreatorTest {

  @Test
  public void shouldReturnNullWhenNoCredentialsFound() {
    AwsAttestationService attestationServiceMock = Mockito.mock(AwsAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(null);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn("us-east-1");
    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock);
    assertNull(attestationCreator.createAttestation());
  }

  @Test
  public void shouldReturnNullWhenNoRegion() {
    AwsAttestationService attestationServiceMock = Mockito.mock(AwsAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials())
        .thenReturn(new BasicAWSCredentials("abc", "abc"));
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(null);

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock);
    assertNull(attestationCreator.createAttestation());
  }

  @Test
  public void shouldReturnProperAttestationWithStandardRegion() throws JsonProcessingException {
    shouldReturnProperAttestationWithSignedRequestCredential(
        "us-east-1", "sts.us-east-1.amazonaws.com");
  }

  @Test
  public void shouldReturnProperAttestationWithCnRegion() throws JsonProcessingException {
    shouldReturnProperAttestationWithSignedRequestCredential(
        "cn-northwest-1", "sts.cn-northwest-1.amazonaws.com.cn");
  }

  private void shouldReturnProperAttestationWithSignedRequestCredential(
      String region, String expectedStsUrl) throws JsonProcessingException {
    AwsAttestationService attestationServiceSpy = Mockito.spy(AwsAttestationService.class);
    Mockito.doReturn(new BasicSessionCredentials("abc", "abc", "aws-session-token"))
        .when(attestationServiceSpy)
        .getAWSCredentials();
    Mockito.doReturn(region).when(attestationServiceSpy).getAWSRegion();

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceSpy);
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
}
