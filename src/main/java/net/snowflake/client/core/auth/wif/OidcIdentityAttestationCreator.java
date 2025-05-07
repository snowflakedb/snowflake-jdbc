package net.snowflake.client.core.auth.wif;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
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
  public WorkloadIdentityAttestation createAttestation() {
    logger.debug("Creating OIDC identity attestation...");
    if (token == null) {
      logger.debug("No OIDC token was specified");
      return null;
    }
    WorkloadIdentityUtil.SubjectAndIssuer claims =
        WorkloadIdentityUtil.extractClaimsWithoutVerifyingSignature(token);
    if (claims == null) {
      logger.error("Could not extract claims from token");
      return null;
    }
    return new WorkloadIdentityAttestation(
        WorkloadIdentityProviderType.OIDC, token, claims.toMap());
  }
}
