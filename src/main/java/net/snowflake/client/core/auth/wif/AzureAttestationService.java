package net.snowflake.client.core.auth.wif;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeUtil;

@SnowflakeJdbcInternalApi
public class AzureAttestationService {

  String getIdentityEndpoint() {
    return SnowflakeUtil.systemGetEnv("IDENTITY_ENDPOINT");
  }

  String getIdentityHeader() {
    return SnowflakeUtil.systemGetEnv("IDENTITY_HEADER");
  }

  String getClientId() {
    return SnowflakeUtil.systemGetEnv("MANAGED_IDENTITY_CLIENT_ID");
  }
}
