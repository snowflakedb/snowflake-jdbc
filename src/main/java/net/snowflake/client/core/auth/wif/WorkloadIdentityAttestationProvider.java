package net.snowflake.client.core.auth.wif;

import java.util.Arrays;
import java.util.stream.Collectors;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class WorkloadIdentityAttestationProvider {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(WorkloadIdentityAttestationProvider.class);

  private final AwsIdentityAttestationCreator awsAttestationCreator;
  private final GcpIdentityAttestationCreator gcpAttestationCreator;
  private final AzureIdentityAttestationCreator azureAttestationCreator;
  private final OidcIdentityAttestationCreator oidcAttestationCreator;

  public WorkloadIdentityAttestationProvider(
      AwsIdentityAttestationCreator awsAttestationCreator,
      GcpIdentityAttestationCreator gcpAttestationCreator,
      AzureIdentityAttestationCreator azureAttestationCreator,
      OidcIdentityAttestationCreator oidcAttestationCreator) {
    this.awsAttestationCreator = awsAttestationCreator;
    this.gcpAttestationCreator = gcpAttestationCreator;
    this.azureAttestationCreator = azureAttestationCreator;
    this.oidcAttestationCreator = oidcAttestationCreator;
  }

  public WorkloadIdentityAttestation getAttestation(String identityProvider) throws SFException {
    return getCreator(identityProvider).createAttestation();
  }

  WorkloadIdentityAttestationCreator getCreator(String identityProvider) throws SFException {
    if (WorkloadIdentityProviderType.AWS.name().equalsIgnoreCase(identityProvider)) {
      return awsAttestationCreator;
    } else if (WorkloadIdentityProviderType.GCP.name().equalsIgnoreCase(identityProvider)) {
      return gcpAttestationCreator;
    } else if (WorkloadIdentityProviderType.AZURE.name().equalsIgnoreCase(identityProvider)) {
      return azureAttestationCreator;
    } else if (WorkloadIdentityProviderType.OIDC.name().equalsIgnoreCase(identityProvider)) {
      return oidcAttestationCreator;
    } else {
      String validValues =
          Arrays.stream(WorkloadIdentityProviderType.values())
              .map(Enum::name)
              .collect(Collectors.joining(", "));
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "Unknown Workload Identity provider specified: "
              + identityProvider
              + ", valid values are: "
              + validValues);
    }
  }
}
