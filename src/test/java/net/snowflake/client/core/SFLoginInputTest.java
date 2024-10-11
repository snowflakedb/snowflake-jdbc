package net.snowflake.client.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SFLoginInputTest {

  @Test
  public void testGetHostFromServerUrlWithoutProtocolShouldNotThrow() throws SFException {
    SFLoginInput sfLoginInput = new SFLoginInput();
    sfLoginInput.setServerUrl("host.com:443");
    Assertions.assertEquals("host.com", sfLoginInput.getHostFromServerUrl());
  }

  @Test
  public void testGetHostFromServerUrlWithProtocolShouldNotThrow() throws SFException {
    SFLoginInput sfLoginInput = new SFLoginInput();
    sfLoginInput.setServerUrl("https://host.com");
    Assertions.assertEquals("host.com", sfLoginInput.getHostFromServerUrl());
  }
}
