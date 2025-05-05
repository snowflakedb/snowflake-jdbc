package net.snowflake.client.core.auth.wif;

import static net.snowflake.client.core.auth.wif.WorkloadIdentityProviderType.AWS;
import static net.snowflake.client.core.auth.wif.WorkloadIdentityProviderType.AZURE;
import static net.snowflake.client.core.auth.wif.WorkloadIdentityProviderType.GCP;
import static net.snowflake.client.core.auth.wif.WorkloadIdentityProviderType.OIDC;

import java.util.HashMap;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class WorkloadIdentityAttestationProviderTest {

  @Test
  public void shouldCreateAttestationWithExplicitAWSProvider() throws SFException {
    AwsIdentityAttestationCreator awsCreatorMock =
        Mockito.mock(AwsIdentityAttestationCreator.class);
    Mockito.when(awsCreatorMock.createAttestation())
        .thenReturn(new WorkloadIdentityAttestation(AWS, "credential_abc", new HashMap<>()));
    WorkloadIdentityAttestationProvider provider =
        new WorkloadIdentityAttestationProvider(awsCreatorMock, null, null, null);

    WorkloadIdentityAttestation attestation = provider.getAttestation(AWS.name());
    Assertions.assertNotNull(attestation);
    Assertions.assertEquals(AWS, attestation.getProvider());
    Assertions.assertEquals("credential_abc", attestation.getCredential());
    Assertions.assertEquals(new HashMap<>(), attestation.getUserIdentifierComponents());
  }

  @Test
  public void shouldCreateProperAttestationCreatorByType() throws SFException {
    WorkloadIdentityAttestationProvider provider =
        new WorkloadIdentityAttestationProvider(
            new AwsIdentityAttestationCreator(null),
            new GcpIdentityAttestationCreator(null),
            new AzureIdentityAttestationCreator(null, new SFLoginInput()),
            new OidcIdentityAttestationCreator(null));
    WorkloadIdentityAttestationCreator attestationCreator = provider.getCreator(AWS.name());
    Assertions.assertInstanceOf(AwsIdentityAttestationCreator.class, attestationCreator);

    attestationCreator = provider.getCreator(WorkloadIdentityProviderType.AZURE.name());
    Assertions.assertInstanceOf(AzureIdentityAttestationCreator.class, attestationCreator);

    attestationCreator = provider.getCreator(WorkloadIdentityProviderType.GCP.name());
    Assertions.assertInstanceOf(GcpIdentityAttestationCreator.class, attestationCreator);

    attestationCreator = provider.getCreator(WorkloadIdentityProviderType.OIDC.name());
    Assertions.assertInstanceOf(OidcIdentityAttestationCreator.class, attestationCreator);

    Assertions.assertThrows(
        SFException.class, () -> provider.getCreator("UNKNOWN_IDENTITY_PROVIDER"));
  }

  @Test
  public void shouldAutodetectAwsProvider() throws SFException {
    WorkloadIdentityAttestationProvider provider = createMockProvider(AWS);
    WorkloadIdentityAttestation attestation = provider.getAttestation(null);

    Assertions.assertNotNull(attestation);
    Assertions.assertEquals(AWS, attestation.getProvider());
    Assertions.assertEquals("aws_cred", attestation.getCredential());
  }

  @Test
  public void shouldAutodetectGCPProvider() throws SFException {
    WorkloadIdentityAttestationProvider provider = createMockProvider(GCP);
    WorkloadIdentityAttestation attestation = provider.getAttestation(null);

    Assertions.assertNotNull(attestation);
    Assertions.assertEquals(WorkloadIdentityProviderType.GCP, attestation.getProvider());
    Assertions.assertEquals("gcp_cred", attestation.getCredential());
  }

  @Test
  public void shouldAutodetectAzureProvider() throws SFException {
    WorkloadIdentityAttestationProvider provider = createMockProvider(AZURE);
    WorkloadIdentityAttestation attestation = provider.getAttestation(null);

    Assertions.assertNotNull(attestation);
    Assertions.assertEquals(WorkloadIdentityProviderType.AZURE, attestation.getProvider());
    Assertions.assertEquals("azure_cred", attestation.getCredential());
  }

  @Test
  public void shouldAutodetectOidcProvider() throws SFException {
    WorkloadIdentityAttestationProvider provider = createMockProvider(OIDC);
    WorkloadIdentityAttestation attestation = provider.getAttestation(null);
    Assertions.assertNotNull(attestation);
    Assertions.assertEquals(WorkloadIdentityProviderType.OIDC, attestation.getProvider());
    Assertions.assertEquals("oidc_cred", attestation.getCredential());
  }

  WorkloadIdentityAttestationProvider createMockProvider(
      WorkloadIdentityProviderType actualPresentType) {
    AwsIdentityAttestationCreator aws = Mockito.mock(AwsIdentityAttestationCreator.class);
    Mockito.when(aws.createAttestation())
        .thenReturn(
            actualPresentType == AWS
                ? new WorkloadIdentityAttestation(AWS, "aws_cred", new HashMap<>())
                : null);
    GcpIdentityAttestationCreator gcp = Mockito.mock(GcpIdentityAttestationCreator.class);
    Mockito.when(gcp.createAttestation())
        .thenReturn(
            actualPresentType == GCP
                ? new WorkloadIdentityAttestation(GCP, "gcp_cred", new HashMap<>())
                : null);
    OidcIdentityAttestationCreator oidc = Mockito.mock(OidcIdentityAttestationCreator.class);
    Mockito.when(oidc.createAttestation())
        .thenReturn(
            actualPresentType == OIDC
                ? new WorkloadIdentityAttestation(OIDC, "oidc_cred", new HashMap<>())
                : null);
    AzureIdentityAttestationCreator azure = Mockito.mock(AzureIdentityAttestationCreator.class);
    Mockito.when(azure.createAttestation())
        .thenReturn(
            actualPresentType == AZURE
                ? new WorkloadIdentityAttestation(AZURE, "azure_cred", new HashMap<>())
                : null);
    return new WorkloadIdentityAttestationProvider(aws, gcp, azure, oidc);
  }
}
