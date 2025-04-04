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
    Assertions.assertEquals(new HashMap<>(), attestation.getUserIdentifiedComponents());
  }

  @Test
  public void shouldCreateProperAttestationCreatorByType() throws SFException {
    WorkloadIdentityAttestationProvider provider =
        new WorkloadIdentityAttestationProvider(
            new AwsIdentityAttestationCreator(null),
            new GcpIdentityAttestationCreator(null),
            new AzureIdentityAttestationCreator(),
            new OidcIdentityAttestationCreator());
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
}
