package net.snowflake.client.core.auth.wif;

import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class OidcIdentityAttestationCreator implements WorkloadIdentityAttestationCreator {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(OidcIdentityAttestationCreator.class);

  private final String token;

  public OidcIdentityAttestationCreator(String token) {
    this.token = token;
  }

  @Override
  public WorkloadIdentityAttestation createAttestation() throws SFException {
    logger.debug("Creating OIDC identity attestation...");
    if (token == null) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "No OIDC token was specified. Please provide it in `token` property.");
    }
    WorkloadIdentityUtil.SubjectAndIssuer claims =
        WorkloadIdentityUtil.extractClaimsWithoutVerifyingSignature(token);
    return new WorkloadIdentityAttestation(
        WorkloadIdentityProviderType.OIDC, token, claims.toMap());
  }
}
