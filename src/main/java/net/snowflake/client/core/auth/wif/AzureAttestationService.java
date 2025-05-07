package net.snowflake.client.core.auth.wif;

import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpRequestBase;

@SnowflakeJdbcInternalApi
public class AzureAttestationService {

  private static final SFLogger logger = SFLoggerFactory.getLogger(AzureAttestationService.class);

  // Expected to be set in Azure Functions environment
  String getIdentityEndpoint() {
    return SnowflakeUtil.systemGetEnv("IDENTITY_ENDPOINT");
  }

  // Expected to be set in Azure Functions environment
  String getIdentityHeader() {
    return SnowflakeUtil.systemGetEnv("IDENTITY_HEADER");
  }

  // Expected to be set in Azure Functions environment
  String getClientId() {
    return SnowflakeUtil.systemGetEnv("MANAGED_IDENTITY_CLIENT_ID");
  }

  String fetchTokenFromMetadataService(HttpRequestBase tokenRequest, SFLoginInput loginInput) {
    try {
      return WorkloadIdentityUtil.performIdentityRequest(tokenRequest, loginInput);
    } catch (Exception e) {
      logger.debug("Azure metadata server request was not successful: {}", e);
      return null;
    }
  }
}
