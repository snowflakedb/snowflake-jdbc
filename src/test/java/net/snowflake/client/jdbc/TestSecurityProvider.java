package net.snowflake.client.jdbc;

import java.security.Provider;

public class TestSecurityProvider extends Provider {
  public TestSecurityProvider() {
    super(TestSecurityProvider.class.getSimpleName(), 1.0, "Test security provider");
    put("TrustManagerFactory.PKIX", TestTrustManagerSpi.class.getName());
  }
}
