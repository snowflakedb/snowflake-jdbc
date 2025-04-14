package net.snowflake.client.core.auth.wif;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class AzureIdentityAttestationCreator implements WorkloadIdentityAttestationCreator {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(AzureIdentityAttestationCreator.class);

  @Override
  public WorkloadIdentityAttestation createAttestation() {
    throw new RuntimeException("Azure Workload Identity not supported");
  }
}
