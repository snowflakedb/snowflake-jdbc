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

  private static final String FAKE_JWT = "fake.jwt.token-for-testing-only";

  // --- GetCallerIdentity (default) path tests ---

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

  @SuppressWarnings("unchecked")
  private void shouldReturnProperAttestationWithSignedRequestCredential(
      Region region, String expectedStsUrl) throws JsonProcessingException, SFException {
    AwsAttestationService attestationServiceSpy = Mockito.spy(AwsAttestationService.class);
    Mockito.doReturn(AwsSessionCredentials.create("abc", "abc", "aws-session-token"))
        .when(attestationServiceSpy)
        .getAWSCredentials();
    Mockito.doNothing().when(attestationServiceSpy).initializeSignerRegion();
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
    Mockito.doNothing().when(attestationServiceSpy).initializeSignerRegion();
    Mockito.doReturn(Region.US_EAST_1).when(attestationServiceSpy).getAWSRegion();

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityImpersonationPath("");

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
                initialCredentials, "arn:aws:iam::123456789012:role/TestRole", null))
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
        .assumeRole(initialCredentials, "arn:aws:iam::123456789012:role/TestRole", null);
  }

  @Test
  public void shouldPassExternalIdToAssumeRoleWhenProvided() throws SFException {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    SdkHttpRequest requestMock = mock(SdkHttpRequest.class);
    Mockito.when(requestMock.getUri()).thenReturn(URI.create("https://snowflakecomputing.com"));
    Mockito.when(requestMock.method()).thenReturn(SdkHttpMethod.GET);

    AwsSessionCredentials initialCredentials =
        AwsSessionCredentials.create("initial-key", "initial-secret", "initial-token");
    AwsSessionCredentials assumedCredentials =
        AwsSessionCredentials.create("assumed-key", "assumed-secret", "assumed-token");

    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(initialCredentials);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(Region.US_EAST_1);
    Mockito.when(attestationServiceMock.getCredentialsViaRoleChaining(any())).thenCallRealMethod();
    Mockito.when(
            attestationServiceMock.assumeRole(
                initialCredentials, "arn:aws:iam::123456789012:role/TestRole", "my-external-id"))
        .thenReturn(assumedCredentials);
    Mockito.doNothing().when(attestationServiceMock).initializeSignerRegion();
    Mockito.when(attestationServiceMock.signRequestWithSigV4(any(), Mockito.eq(assumedCredentials)))
        .thenReturn(requestMock);

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityImpersonationPath("arn:aws:iam::123456789012:role/TestRole");
    loginInput.setWorkloadIdentityAwsExternalId("my-external-id");

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, loginInput);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.AWS, attestation.getProvider());
    assertNotNull(attestation.getCredential());
    Mockito.verify(attestationServiceMock)
        .assumeRole(
            initialCredentials, "arn:aws:iam::123456789012:role/TestRole", "my-external-id");
  }

  @Test
  public void shouldPassEmptyExternalIdToAssumeRoleWhenSetToEmptyString() throws SFException {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    SdkHttpRequest requestMock = mock(SdkHttpRequest.class);
    Mockito.when(requestMock.getUri()).thenReturn(URI.create("https://snowflakecomputing.com"));
    Mockito.when(requestMock.method()).thenReturn(SdkHttpMethod.GET);

    AwsSessionCredentials initialCredentials =
        AwsSessionCredentials.create("initial-key", "initial-secret", "initial-token");
    AwsSessionCredentials assumedCredentials =
        AwsSessionCredentials.create("assumed-key", "assumed-secret", "assumed-token");

    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(initialCredentials);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(Region.US_EAST_1);
    Mockito.when(attestationServiceMock.getCredentialsViaRoleChaining(any())).thenCallRealMethod();
    Mockito.when(
            attestationServiceMock.assumeRole(
                initialCredentials, "arn:aws:iam::123456789012:role/TestRole", ""))
        .thenReturn(assumedCredentials);
    Mockito.doNothing().when(attestationServiceMock).initializeSignerRegion();
    Mockito.when(attestationServiceMock.signRequestWithSigV4(any(), Mockito.eq(assumedCredentials)))
        .thenReturn(requestMock);

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityImpersonationPath("arn:aws:iam::123456789012:role/TestRole");
    loginInput.setWorkloadIdentityAwsExternalId("");

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, loginInput);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    Mockito.verify(attestationServiceMock)
        .assumeRole(initialCredentials, "arn:aws:iam::123456789012:role/TestRole", "");
  }

  @Test
  public void shouldOnlyPassExternalIdToLastHopInMultiHopChain() throws SFException {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    SdkHttpRequest requestMock = mock(SdkHttpRequest.class);
    Mockito.when(requestMock.getUri()).thenReturn(URI.create("https://snowflakecomputing.com"));
    Mockito.when(requestMock.method()).thenReturn(SdkHttpMethod.GET);

    AwsSessionCredentials initialCredentials =
        AwsSessionCredentials.create("initial-key", "initial-secret", "initial-token");
    AwsSessionCredentials intermediateCredentials =
        AwsSessionCredentials.create(
            "intermediate-key", "intermediate-secret", "intermediate-token");
    AwsSessionCredentials finalCredentials =
        AwsSessionCredentials.create("final-key", "final-secret", "final-token");

    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(initialCredentials);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(Region.US_EAST_1);
    Mockito.when(attestationServiceMock.getCredentialsViaRoleChaining(any())).thenCallRealMethod();
    Mockito.when(
            attestationServiceMock.assumeRole(
                initialCredentials, "arn:aws:iam::111111111111:role/RoleA", null))
        .thenReturn(intermediateCredentials);
    Mockito.when(
            attestationServiceMock.assumeRole(
                intermediateCredentials, "arn:aws:iam::222222222222:role/RoleB", "my-external-id"))
        .thenReturn(finalCredentials);
    Mockito.doNothing().when(attestationServiceMock).initializeSignerRegion();
    Mockito.when(attestationServiceMock.signRequestWithSigV4(any(), Mockito.eq(finalCredentials)))
        .thenReturn(requestMock);

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityImpersonationPath(
        "arn:aws:iam::111111111111:role/RoleA,arn:aws:iam::222222222222:role/RoleB");
    loginInput.setWorkloadIdentityAwsExternalId("my-external-id");

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, loginInput);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    Mockito.verify(attestationServiceMock)
        .assumeRole(initialCredentials, "arn:aws:iam::111111111111:role/RoleA", null);
    Mockito.verify(attestationServiceMock)
        .assumeRole(
            intermediateCredentials, "arn:aws:iam::222222222222:role/RoleB", "my-external-id");
  }

  // --- GetWebIdentityToken (opt-in) path tests ---

  @Test
  public void shouldReturnAttestationWithJwtWhenOutboundTokenEnabled() throws SFException {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    AwsSessionCredentials credentials =
        AwsSessionCredentials.create("abc", "abc", "aws-session-token");
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(credentials);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(Region.US_EAST_1);
    Mockito.when(attestationServiceMock.getWebIdentityToken(Mockito.eq(credentials)))
        .thenReturn(FAKE_JWT);

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityAwsUseOutboundToken(true);

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, loginInput);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.AWS, attestation.getProvider());
    assertEquals(FAKE_JWT, attestation.getCredential());
    assertNotNull(attestation.getUserIdentifierComponents());
    assertEquals(0, attestation.getUserIdentifierComponents().size());
  }

  @Test
  public void shouldPropagateExceptionFromGetWebIdentityToken() throws SFException {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    AwsSessionCredentials credentials =
        AwsSessionCredentials.create("abc", "abc", "aws-session-token");
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(credentials);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(Region.US_EAST_1);
    Mockito.when(attestationServiceMock.getWebIdentityToken(Mockito.eq(credentials)))
        .thenThrow(
            new SFException(
                net.snowflake.client.api.exception.ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "boom"));

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityAwsUseOutboundToken(true);

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, loginInput);
    assertThrows(SFException.class, attestationCreator::createAttestation);
  }

  @Test
  public void shouldCreateAttestationWithImpersonationPathAndOutboundToken() throws SFException {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);

    AwsSessionCredentials initialCredentials =
        AwsSessionCredentials.create("initial-key", "initial-secret", "initial-token");
    AwsSessionCredentials assumedCredentials =
        AwsSessionCredentials.create("assumed-key", "assumed-secret", "assumed-token");

    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(initialCredentials);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(Region.US_EAST_1);
    Mockito.when(attestationServiceMock.getCredentialsViaRoleChaining(any())).thenCallRealMethod();
    Mockito.when(
            attestationServiceMock.assumeRole(
                initialCredentials, "arn:aws:iam::123456789012:role/TestRole", null))
        .thenReturn(assumedCredentials);
    Mockito.when(attestationServiceMock.getWebIdentityToken(Mockito.eq(assumedCredentials)))
        .thenReturn(FAKE_JWT);

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityImpersonationPath("arn:aws:iam::123456789012:role/TestRole");
    loginInput.setWorkloadIdentityAwsUseOutboundToken(true);

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, loginInput);

    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.AWS, attestation.getProvider());
    assertEquals(FAKE_JWT, attestation.getCredential());

    Mockito.verify(attestationServiceMock)
        .assumeRole(initialCredentials, "arn:aws:iam::123456789012:role/TestRole", null);
  }

  @Test
  public void shouldUseGetCallerIdentityWhenOutboundTokenExplicitlyFalse() throws SFException {
    AwsAttestationService attestationServiceSpy = Mockito.spy(AwsAttestationService.class);
    Mockito.doReturn(AwsSessionCredentials.create("abc", "abc", "aws-session-token"))
        .when(attestationServiceSpy)
        .getAWSCredentials();
    Mockito.doNothing().when(attestationServiceSpy).initializeSignerRegion();
    Mockito.doReturn(Region.US_EAST_1).when(attestationServiceSpy).getAWSRegion();

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityAwsUseOutboundToken(false);

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceSpy, loginInput);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    String decoded = new String(Base64.getDecoder().decode(attestation.getCredential()));
    assertTrue(decoded.contains("GetCallerIdentity"));
  }

  @Test
  public void shouldUseGetCallerIdentityByDefault() throws SFException {
    AwsAttestationService attestationServiceSpy = Mockito.spy(AwsAttestationService.class);
    Mockito.doReturn(AwsSessionCredentials.create("abc", "abc", "aws-session-token"))
        .when(attestationServiceSpy)
        .getAWSCredentials();
    Mockito.doNothing().when(attestationServiceSpy).initializeSignerRegion();
    Mockito.doReturn(Region.US_EAST_1).when(attestationServiceSpy).getAWSRegion();

    SFLoginInput loginInput = new SFLoginInput();

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceSpy, loginInput);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.AWS, attestation.getProvider());
    assertNotNull(attestation.getCredential());
    // Credential should be base64-encoded (GetCallerIdentity path), not a plain JWT
    String decoded = new String(Base64.getDecoder().decode(attestation.getCredential()));
    assertTrue(decoded.contains("GetCallerIdentity"));
  }
}
