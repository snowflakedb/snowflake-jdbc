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

  @Override
  public WorkloadIdentityAttestation createAttestation() throws SFException {
    throw new SFException(ErrorCode.FEATURE_UNSUPPORTED, "OIDC Workload Identity not supported");
  }
}
