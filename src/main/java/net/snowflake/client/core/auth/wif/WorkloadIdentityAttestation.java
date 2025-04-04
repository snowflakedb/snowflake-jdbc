package net.snowflake.client.core.auth.wif;

import java.util.Map;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class WorkloadIdentityAttestation {

  private final WorkloadIdentityProviderType provider;
  private final String credential;
  private final Map<String, String> userIdentifiedComponents;

  WorkloadIdentityAttestation(
      WorkloadIdentityProviderType provider,
      String credential,
      Map<String, String> userIdentifiedComponents) {
    this.provider = provider;
    this.credential = credential;
    this.userIdentifiedComponents = userIdentifiedComponents;
  }

  public WorkloadIdentityProviderType getProvider() {
    return provider;
  }

  public String getCredential() {
    return credential;
  }

  public Map<String, String> getUserIdentifiedComponents() {
    return userIdentifiedComponents;
  }
}
