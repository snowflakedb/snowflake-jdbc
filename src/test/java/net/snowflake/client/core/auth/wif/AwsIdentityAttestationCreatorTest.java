package net.snowflake.client.core.auth.wif;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.auth.BasicAWSCredentials;
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
    AWSAttestationService attestationServiceMock = Mockito.mock(AWSAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(null);
    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock);
    assertNull(attestationCreator.createAttestation());
  }

  @Test
  public void shouldReturnNullWhenNoRegion() {
    AWSAttestationService attestationServiceMock = Mockito.mock(AWSAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials())
        .thenReturn(new BasicAWSCredentials("abc", "abc"));
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(null);

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock);
    assertNull(attestationCreator.createAttestation());
  }

  @Test
  public void shouldReturnNullWhenNoCallerIdentity() {
    AWSAttestationService attestationServiceMock = Mockito.mock(AWSAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials())
        .thenReturn(new BasicAWSCredentials("abc", "abc"));
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn("eu-west-1");
    Mockito.when(attestationServiceMock.getArn()).thenReturn(null);

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock);
    assertNull(attestationCreator.createAttestation());
  }

  @Test
  public void shouldReturnProperAttestationWithSignedRequestCredential()
      throws JsonProcessingException {
    AWSAttestationService attestationServiceSpy = Mockito.spy(AWSAttestationService.class);
    Mockito.doReturn(new BasicAWSCredentials("abc", "abc"))
        .when(attestationServiceSpy)
        .getAWSCredentials();
    Mockito.doReturn("eu-west-1").when(attestationServiceSpy).getAWSRegion();
    Mockito.doReturn("arn:aws:attestation:abc").when(attestationServiceSpy).getArn();

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceSpy);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.AWS, attestation.getProvider());
    assertEquals("arn:aws:attestation:abc", attestation.getUserIdentifiedComponents().get("arn"));
    assertNotNull(attestation.getCredential());
    Base64.Decoder decoder = Base64.getDecoder();
    String json = new String(decoder.decode(attestation.getCredential()));
    Map<String, Object> credentialMap = new ObjectMapper().readValue(json, HashMap.class);
    assertEquals(3, credentialMap.size());
    assertEquals(
        "https://sts.eu-west-1.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
        credentialMap.get("url"));
    assertEquals("POST", credentialMap.get("method"));
    assertNotNull(credentialMap.get("headers"));
    Map<String, String> headersMap = (Map<String, String>) credentialMap.get("headers");
    assertEquals(4, headersMap.size());
    assertEquals("sts.eu-west-1.amazonaws.com", headersMap.get("Host"));
    assertEquals("snowflakecomputing.com", headersMap.get("X-Snowflake-Audience"));
    assertNotNull(headersMap.get("X-Amz-Date"));
    assertTrue(headersMap.get("Authorization").matches("^AWS4-HMAC-SHA256 Credential=.*"));
  }
}
