package net.snowflake.client.jdbc.telemetryOOB;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TelemetryServiceTest {
  private boolean defaultState;

  @BeforeEach
  public void setUp() {
    TelemetryService service = TelemetryService.getInstance();
    defaultState = service.isEnabled();
    service.enable();
  }

  @AfterEach
  public void tearDown() throws InterruptedException {
    TelemetryService service = TelemetryService.getInstance();
    if (defaultState) {
      service.enable();
    } else {
      service.disable();
    }
  }

  @Test
  public void testTelemetryInitErrors() {
    TelemetryService service = TelemetryService.getInstance();
    assertThat(
        "Telemetry server failure count should be zero at first.",
        service.getServerFailureCount(),
        equalTo(0));
    assertThat(
        "Telemetry client failure count should be zero at first.",
        service.getClientFailureCount(),
        equalTo(0));
  }

  @Test
  public void testTelemetryEnableDisable() {
    TelemetryService service = TelemetryService.getInstance();
    // We already enabled telemetry in setup phase.
    assertThat("Telemetry should be already enabled here.", service.isEnabled(), equalTo(true));
    service.disable();
    assertThat("Telemetry should be already disabled here.", service.isEnabled(), equalTo(false));
    service.enable();
    assertThat("Telemetry should be enabled back", service.isEnabled(), equalTo(true));
  }

  @Test
  public void testTelemetryConnectionString() {
    String INVALID_CONNECTION_STRING =
        "://:-1"; // INVALID_CONNECT_STRING in SnowflakeConnectionString.java
    Map<String, String> param = new HashMap<>();
    param.put("uri", "jdbc:snowflake://snowflake.reg.local:8082");
    param.put("host", "snowflake.reg.local");
    param.put("port", "8082");
    param.put("account", "fakeaccount");
    param.put("user", "fakeuser");
    param.put("password", "fakepassword");
    param.put("database", "fakedatabase");
    param.put("schema", "fakeschema");
    param.put("role", "fakerole");
    TelemetryService service = TelemetryService.getInstance();
    service.updateContextForIT(param);
    assertThat(
        "Telemetry failed to generate sfConnectionStr.",
        service.getSnowflakeConnectionString().toString(),
        not(INVALID_CONNECTION_STRING));
  }
}
