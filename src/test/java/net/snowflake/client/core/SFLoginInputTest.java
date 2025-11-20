package net.snowflake.client.core;

import static net.snowflake.client.core.SFSession.TELEMETRY_SERVICE_AVAILABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @NullSource
  public void testGetTelemetryServiceAvailableShouldReturnCorrectValue(Boolean value) {
    SFLoginInput sfLoginInput = new SFLoginInput();
    Map<String, Object> sessionParameters = new HashMap<>();
    sessionParameters.put(TELEMETRY_SERVICE_AVAILABLE, value);
    sfLoginInput.setSessionParameters(sessionParameters);
    assertEquals(value, sfLoginInput.getSessionParameters().get(TELEMETRY_SERVICE_AVAILABLE));
  }
}
