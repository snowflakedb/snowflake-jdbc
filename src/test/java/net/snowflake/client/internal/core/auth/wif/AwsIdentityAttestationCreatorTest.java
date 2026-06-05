package net.snowflake.client.internal.core.auth.wif;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SFLoginInput;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;

public class AwsIdentityAttestationCreatorTest {

  private static final String FAKE_JWT = "fake.jwt.token-for-testing-only";

  @Test
  public void shouldThrowExceptionWhenNoCredentialsFound() {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(null);
    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, new SFLoginInput());
    assertThrows(SFException.class, attestationCreator::createAttestation);
  }

  @Test
  public void shouldThrowExceptionWhenNoRegion() {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials())
        .thenReturn(AwsSessionCredentials.create("abc", "abc", "aws-session-token"));
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(null);

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, new SFLoginInput());
    assertThrows(SFException.class, attestationCreator::createAttestation);
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

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, new SFLoginInput());
    assertThrows(SFException.class, attestationCreator::createAttestation);
  }

  @Test
  public void shouldReturnAttestationWithJwtCredential() throws SFException {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    AwsSessionCredentials credentials =
        AwsSessionCredentials.create("abc", "abc", "aws-session-token");
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(credentials);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(Region.US_EAST_1);
    Mockito.when(attestationServiceMock.getWebIdentityToken(Mockito.eq(credentials)))
        .thenReturn(FAKE_JWT);

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, new SFLoginInput());
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.AWS, attestation.getProvider());
    assertEquals(FAKE_JWT, attestation.getCredential());
    assertNotNull(attestation.getUserIdentifierComponents());
    assertEquals(0, attestation.getUserIdentifierComponents().size());
  }

  @Test
  public void shouldCreateAttestationWithoutImpersonationWhenImpersonationPathIsEmpty()
      throws SFException {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    AwsSessionCredentials credentials =
        AwsSessionCredentials.create("abc", "abc", "aws-session-token");
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(credentials);
    Mockito.when(attestationServiceMock.getAWSRegion()).thenReturn(Region.US_EAST_1);
    Mockito.when(attestationServiceMock.getWebIdentityToken(Mockito.eq(credentials)))
        .thenReturn(FAKE_JWT);

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityImpersonationPath(""); // Empty path

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, loginInput);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.AWS, attestation.getProvider());
    assertEquals(FAKE_JWT, attestation.getCredential());
    Mockito.verify(attestationServiceMock, Mockito.never()).getCredentialsViaRoleChaining(any());
  }

  @Test
  public void shouldThrowExceptionWhenImpersonationFailsWithNoInitialCredentials() {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);
    Mockito.when(attestationServiceMock.getAWSCredentials()).thenReturn(null);

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityImpersonationPath("arn:aws:iam::123456789012:role/TestRole");

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, loginInput);

    assertThrows(SFException.class, attestationCreator::createAttestation);
  }

  @Test
  public void shouldCreateAttestationWithImpersonationPath() throws SFException {
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
  public void shouldPassExternalIdToAssumeRoleWhenProvided() throws SFException {
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
                initialCredentials, "arn:aws:iam::123456789012:role/TestRole", "my-external-id"))
        .thenReturn(assumedCredentials);
    Mockito.when(attestationServiceMock.getWebIdentityToken(Mockito.eq(assumedCredentials)))
        .thenReturn(FAKE_JWT);

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityImpersonationPath("arn:aws:iam::123456789012:role/TestRole");
    loginInput.setWorkloadIdentityAwsExternalId("my-external-id");

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, loginInput);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.AWS, attestation.getProvider());
    assertEquals(FAKE_JWT, attestation.getCredential());
    Mockito.verify(attestationServiceMock)
        .assumeRole(
            initialCredentials, "arn:aws:iam::123456789012:role/TestRole", "my-external-id");
  }

  @Test
  public void shouldPassEmptyExternalIdToAssumeRoleWhenSetToEmptyString() throws SFException {
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
                initialCredentials, "arn:aws:iam::123456789012:role/TestRole", ""))
        .thenReturn(assumedCredentials);
    Mockito.when(attestationServiceMock.getWebIdentityToken(Mockito.eq(assumedCredentials)))
        .thenReturn(FAKE_JWT);

    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setWorkloadIdentityImpersonationPath("arn:aws:iam::123456789012:role/TestRole");
    loginInput.setWorkloadIdentityAwsExternalId("");

    AwsIdentityAttestationCreator attestationCreator =
        new AwsIdentityAttestationCreator(attestationServiceMock, loginInput);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    assertNotNull(attestation);
    // Empty string is passed through as-is; assumeRole handles it by not setting externalId
    // in the STS AssumeRole request (same effective behaviour as null)
    Mockito.verify(attestationServiceMock)
        .assumeRole(initialCredentials, "arn:aws:iam::123456789012:role/TestRole", "");
  }

  @Test
  public void shouldOnlyPassExternalIdToLastHopInMultiHopChain() throws SFException {
    AwsAttestationService attestationServiceMock = mock(AwsAttestationService.class);

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
    // First hop: no external ID
    Mockito.when(
            attestationServiceMock.assumeRole(
                initialCredentials, "arn:aws:iam::111111111111:role/RoleA", null))
        .thenReturn(intermediateCredentials);
    // Last hop: external ID applied
    Mockito.when(
            attestationServiceMock.assumeRole(
                intermediateCredentials, "arn:aws:iam::222222222222:role/RoleB", "my-external-id"))
        .thenReturn(finalCredentials);
    Mockito.when(attestationServiceMock.getWebIdentityToken(Mockito.eq(finalCredentials)))
        .thenReturn(FAKE_JWT);

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
}
