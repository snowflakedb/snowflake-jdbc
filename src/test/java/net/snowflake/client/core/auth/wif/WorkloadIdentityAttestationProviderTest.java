package net.snowflake.client.core.auth.wif;

import static net.snowflake.client.core.auth.wif.WorkloadIdentityProviderType.AWS;

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
            new AwsIdentityAttestationCreator(null, null),
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

    Assertions.assertThrows(SFException.class, () -> provider.getCreator(null));
  }
}
