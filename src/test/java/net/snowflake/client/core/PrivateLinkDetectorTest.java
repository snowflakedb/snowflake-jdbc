package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PrivateLinkDetectorTest {

  @Parameterized.Parameters(name = "Host {0} is private link: {1}")
  public static Object[][] data() {
    return new Object[][] {
      {"snowhouse.snowflakecomputing.com", false},
      {"snowhouse.privatelink.snowflakecomputing.com", true},
      {"snowhouse.PRIVATELINK.snowflakecomputing.com", true},
      {"snowhouse.snowflakecomputing.cn", false},
      {"snowhouse.privatelink.snowflakecomputing.cn", true},
      {"snowhouse.PRIVATELINK.snowflakecomputing.cn", true},
      {"snowhouse.snowflakecomputing.xyz", false},
      {"snowhouse.privatelink.snowflakecomputing.xyz", true},
      {"snowhouse.PRIVATELINK.snowflakecomputing.xyz", true},
    };
  }

  private final String host;
  private final boolean expectedToBePrivateLink;

  public PrivateLinkDetectorTest(String host, boolean expectedToBePrivateLink) {
    this.host = host;
    this.expectedToBePrivateLink = expectedToBePrivateLink;
  }

  @Test
  public void shouldDetectPrivateLinkHost() {
    assertEquals(
        String.format("Expecting %s to be private link: %s", host, expectedToBePrivateLink),
        expectedToBePrivateLink,
        PrivateLinkDetector.isPrivateLink(host));
  }
}
