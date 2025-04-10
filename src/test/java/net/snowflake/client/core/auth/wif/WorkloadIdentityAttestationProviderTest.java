package net.snowflake.client.core.auth.wif;

import java.util.HashMap;
import net.snowflake.client.core.SFException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class WorkloadIdentityAttestationProviderTest {

  @Test
  public void shouldCreateAttestationWithExplicitAWSProvider() throws SFException {
    AwsIdentityAttestationCreator awsCreatorMock =
        Mockito.mock(AwsIdentityAttestationCreator.class);
    Mockito.when(awsCreatorMock.createAttestation())
        .thenReturn(
            new WorkloadIdentityAttestation(
                WorkloadIdentityProviderType.AWS, "credential_abc", new HashMap<>()));
    WorkloadIdentityAttestationProvider provider =
        new WorkloadIdentityAttestationProvider(awsCreatorMock, null, null, null);

    WorkloadIdentityAttestation attestation =
        provider.getAttestation(WorkloadIdentityProviderType.AWS.name());
    Assertions.assertNotNull(attestation);
    Assertions.assertEquals(WorkloadIdentityProviderType.AWS, attestation.getProvider());
    Assertions.assertEquals("credential_abc", attestation.getCredential());
    Assertions.assertEquals(new HashMap<>(), attestation.getUserIdentifierComponents());
  }

  @Test
  public void shouldCreateProperAttestationCreatorByType() throws SFException {
    WorkloadIdentityAttestationProvider provider =
        new WorkloadIdentityAttestationProvider(
            new AwsIdentityAttestationCreator(null),
            new GcpIdentityAttestationCreator(null),
            new AzureIdentityAttestationCreator(),
            new OidcIdentityAttestationCreator(null));
    WorkloadIdentityAttestationCreator attestationCreator =
        provider.getCreator(WorkloadIdentityProviderType.AWS.name());
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
    OidcIdentityAttestationCreator oidcIdentityAttestationCreator =
        Mockito.mock(OidcIdentityAttestationCreator.class);
    AwsIdentityAttestationCreator awsIdentityAttestationCreatorMock =
        Mockito.mock(AwsIdentityAttestationCreator.class);
    Mockito.when(oidcIdentityAttestationCreator.createAttestation()).thenReturn(null);
    Mockito.when(awsIdentityAttestationCreatorMock.createAttestation())
        .thenReturn(
            new WorkloadIdentityAttestation(WorkloadIdentityProviderType.AWS, "aws_cred", null));
    WorkloadIdentityAttestationProvider provider =
        new WorkloadIdentityAttestationProvider(
            awsIdentityAttestationCreatorMock, null, null, oidcIdentityAttestationCreator);

    WorkloadIdentityAttestation attestation = provider.getAttestation(null);
    Assertions.assertNotNull(attestation);
    Assertions.assertEquals(WorkloadIdentityProviderType.AWS, attestation.getProvider());
    Assertions.assertEquals("aws_cred", attestation.getCredential());
  }

  @Test
  public void shouldAutodetectGCPProvider() throws SFException {
    OidcIdentityAttestationCreator oidcIdentityAttestationCreator =
        Mockito.mock(OidcIdentityAttestationCreator.class);
    AwsIdentityAttestationCreator awsIdentityAttestationCreatorMock =
        Mockito.mock(AwsIdentityAttestationCreator.class);
    GcpIdentityAttestationCreator gcpIdentityAttestationCreatorMock =
        Mockito.mock(GcpIdentityAttestationCreator.class);
    Mockito.when(oidcIdentityAttestationCreator.createAttestation()).thenReturn(null);
    Mockito.when(awsIdentityAttestationCreatorMock.createAttestation()).thenReturn(null);
    Mockito.when(gcpIdentityAttestationCreatorMock.createAttestation())
        .thenReturn(
            new WorkloadIdentityAttestation(WorkloadIdentityProviderType.GCP, "gcp_cred", null));

    WorkloadIdentityAttestationProvider provider =
        new WorkloadIdentityAttestationProvider(
            awsIdentityAttestationCreatorMock,
            gcpIdentityAttestationCreatorMock,
            null,
            oidcIdentityAttestationCreator);

    WorkloadIdentityAttestation attestation = provider.getAttestation(null);
    Assertions.assertNotNull(attestation);
    Assertions.assertEquals(WorkloadIdentityProviderType.GCP, attestation.getProvider());
    Assertions.assertEquals("gcp_cred", attestation.getCredential());
  }

  @Test
  public void shouldAutodetectAzureProvider() throws SFException {
    OidcIdentityAttestationCreator oidcIdentityAttestationCreator =
        Mockito.mock(OidcIdentityAttestationCreator.class);
    AwsIdentityAttestationCreator awsIdentityAttestationCreatorMock =
        Mockito.mock(AwsIdentityAttestationCreator.class);
    GcpIdentityAttestationCreator gcpIdentityAttestationCreatorMock =
        Mockito.mock(GcpIdentityAttestationCreator.class);
    AzureIdentityAttestationCreator azureIdentityAttestationCreatorMock =
        Mockito.mock(AzureIdentityAttestationCreator.class);
    Mockito.when(oidcIdentityAttestationCreator.createAttestation()).thenReturn(null);
    Mockito.when(awsIdentityAttestationCreatorMock.createAttestation()).thenReturn(null);
    Mockito.when(gcpIdentityAttestationCreatorMock.createAttestation()).thenReturn(null);
    Mockito.when(azureIdentityAttestationCreatorMock.createAttestation())
        .thenReturn(
            new WorkloadIdentityAttestation(
                WorkloadIdentityProviderType.AZURE, "azure_cred", null));
    WorkloadIdentityAttestationProvider provider =
        new WorkloadIdentityAttestationProvider(
            awsIdentityAttestationCreatorMock,
            gcpIdentityAttestationCreatorMock,
            azureIdentityAttestationCreatorMock,
            oidcIdentityAttestationCreator);

    WorkloadIdentityAttestation attestation = provider.getAttestation(null);
    Assertions.assertNotNull(attestation);
    Assertions.assertEquals(WorkloadIdentityProviderType.AZURE, attestation.getProvider());
    Assertions.assertEquals("azure_cred", attestation.getCredential());
  }

  @Test
  public void shouldAutodetectOidcProvider() throws SFException {
    OidcIdentityAttestationCreator oidcIdentityAttestationCreator =
        Mockito.mock(OidcIdentityAttestationCreator.class);
    Mockito.when(oidcIdentityAttestationCreator.createAttestation())
        .thenReturn(
            new WorkloadIdentityAttestation(WorkloadIdentityProviderType.OIDC, "oidc_cred", null));
    WorkloadIdentityAttestationProvider provider =
        new WorkloadIdentityAttestationProvider(null, null, null, oidcIdentityAttestationCreator);

    WorkloadIdentityAttestation attestation = provider.getAttestation(null);
    Assertions.assertNotNull(attestation);
    Assertions.assertEquals(WorkloadIdentityProviderType.OIDC, attestation.getProvider());
    Assertions.assertEquals("oidc_cred", attestation.getCredential());
  }
}
