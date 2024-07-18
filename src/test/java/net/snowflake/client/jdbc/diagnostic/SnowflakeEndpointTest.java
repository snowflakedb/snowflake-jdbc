package net.snowflake.client.jdbc.diagnostic;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class SnowflakeEndpointTest {

  @Test
  public void shouldDetectPrivateLinkEndpoint() {
    Map<String, Boolean> hostsToPrivateLinks = new HashMap<>();
    hostsToPrivateLinks.put("snowhouse.snowflakecomputing.com", false);
    hostsToPrivateLinks.put("snowhouse.privatelink.snowflakecomputing.com", true);
    hostsToPrivateLinks.put("snowhouse.snowflakecomputing.cn", false);
    hostsToPrivateLinks.put("snowhouse.PRIVATELINK.snowflakecomputing.cn", true);

    hostsToPrivateLinks.forEach(
        (host, expectedToBePrivateLink) -> {
          SnowflakeEndpoint endpoint = new SnowflakeEndpoint("SNOWFLAKE_DEPLOYMENT", host, 443);
          assertEquals(
              String.format("Expecting %s to be private link: %s", host, expectedToBePrivateLink),
              expectedToBePrivateLink,
              endpoint.isPrivateLink());
        });
  }
}
