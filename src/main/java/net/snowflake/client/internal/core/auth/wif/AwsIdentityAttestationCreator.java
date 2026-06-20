package net.snowflake.client.internal.core.auth.wif;

import java.util.Collections;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SFLoginInput;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

public class AwsIdentityAttestationCreator implements WorkloadIdentityAttestationCreator {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(AwsIdentityAttestationCreator.class);

  private final AwsAttestationService attestationService;
  private final SFLoginInput loginInput;

  public AwsIdentityAttestationCreator(
      AwsAttestationService attestationService, SFLoginInput loginInput) {
    this.attestationService = attestationService;
    this.loginInput = loginInput;
  }

  @Override
  public WorkloadIdentityAttestation createAttestation() throws SFException {
    attestationService.initializeSignerRegion();
    AwsCredentials awsCredentials;

    if (loginInput.getWorkloadIdentityImpersonationPath().isEmpty()) {
      logger.debug("Creating AWS identity attestation...");
      awsCredentials = attestationService.getAWSCredentials();
    } else {
      logger.debug("Creating AWS identity attestation with impersonation...");
      awsCredentials = attestationService.getCredentialsViaRoleChaining(loginInput);
    }

    if (awsCredentials == null) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "No AWS credentials were found");
    }
    if (attestationService.getAWSRegion() == null) {
      throw new SFException(ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "No AWS region was found");
    }

    String jwt = attestationService.getWebIdentityToken(awsCredentials);
    return new WorkloadIdentityAttestation(
        WorkloadIdentityProviderType.AWS, jwt, Collections.emptyMap());
  }
}
