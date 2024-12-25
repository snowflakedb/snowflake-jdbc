package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SFLoginInputTest {

  @Test
  public void testGetHostFromServerUrlWithoutProtocolShouldNotThrow() throws SFException {
    SFLoginInput sfLoginInput = new SFLoginInput();
    sfLoginInput.setServerUrl("host.com:443");
    assertEquals("host.com", sfLoginInput.getHostFromServerUrl());
  }

  @Test
  public void testGetHostFromServerUrlWithProtocolShouldNotThrow() throws SFException {
    SFLoginInput sfLoginInput = new SFLoginInput();
    sfLoginInput.setServerUrl("https://host.com");
    assertEquals("host.com", sfLoginInput.getHostFromServerUrl());
  }
}
