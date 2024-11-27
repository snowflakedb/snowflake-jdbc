package net.snowflake.client.jdbc.diagnostic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

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
              expectedToBePrivateLink,
              endpoint.isPrivateLink(),
              String.format("Expecting %s to be private link: %s", host, expectedToBePrivateLink));
        });
  }
}
