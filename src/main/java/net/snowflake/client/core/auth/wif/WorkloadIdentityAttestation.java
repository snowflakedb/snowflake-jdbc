package net.snowflake.client.core.auth.wif;

import java.util.Map;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class WorkloadIdentityAttestation {

  private final WorkloadIdentityProviderType provider;
  private final String credential;
  private final Map<String, String> userIdentifierComponents;

  WorkloadIdentityAttestation(
      WorkloadIdentityProviderType provider,
      String credential,
      Map<String, String> userIdentifierComponents) {
    this.provider = provider;
    this.credential = credential;
    this.userIdentifierComponents = userIdentifierComponents;
  }

  public WorkloadIdentityProviderType getProvider() {
    return provider;
  }

  public String getCredential() {
    return credential;
  }

  public Map<String, String> getUserIdentifierComponents() {
    return userIdentifierComponents;
  }

  @Override
  public String toString() {
    return "WorkloadIdentityAttestation{"
        + "provider="
        + provider
        + ", credential='"
        + credential
        + '\''
        + ", userIdentifierComponents="
        + userIdentifierComponents
        + '}';
  }
}
