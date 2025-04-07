package net.snowflake.client.core.auth.wif;

import com.google.common.base.Strings;
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
    if (Strings.isNullOrEmpty(identityProvider)) {
      logger.debug("Workload Identity Provider has not been specified. Using autodetect...");
      return createAutodetectAttestation();
    } else {
      return getCreator(identityProvider).createAttestation();
    }
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
      throw new SFException(
          ErrorCode.WORKFLOW_IDENTITY_FLOW_ERROR,
          "Unknown Workload Identity provider specified: " + identityProvider);
    }
  }

  private WorkloadIdentityAttestation createAutodetectAttestation() throws SFException {
    WorkloadIdentityAttestation awsAttestation = awsAttestationCreator.createAttestation();
    if (awsAttestation != null) {
      return awsAttestation;
    }
    WorkloadIdentityAttestation gcpAttestation = gcpAttestationCreator.createAttestation();
    if (gcpAttestation != null) {
      return gcpAttestation;
    }
    WorkloadIdentityAttestation azureAttestation = azureAttestationCreator.createAttestation();
    if (azureAttestation != null) {
      return azureAttestation;
    }
    WorkloadIdentityAttestation oidcAttestation = oidcAttestationCreator.createAttestation();
    if (oidcAttestation != null) {
      return oidcAttestation;
    }
    throw new SFException(
        ErrorCode.WORKFLOW_IDENTITY_FLOW_ERROR,
        "Unable to autodetect Workload Identity. None of supported Workload Identity environments has been identified.");
  }
}
