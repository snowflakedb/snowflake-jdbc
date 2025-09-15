package net.snowflake.client.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
class PreSessionTelemetryClientTest {
  private PreSessionTelemetryClient preSessionTelemetryClient;
  private Telemetry mockRealTelemetryClient;
  private TelemetryData mockTelemetryData;

  @BeforeEach
  void setUp() {
    preSessionTelemetryClient = new PreSessionTelemetryClient();
    mockRealTelemetryClient = mock(Telemetry.class);
    mockTelemetryData = mock(TelemetryData.class);
  }

  @Test
  void shouldFlushBufferedDataAndPassThroughAfterRealClientSet() {
    preSessionTelemetryClient.addLogToBatch(mockTelemetryData);
    preSessionTelemetryClient.addLogToBatch(mockTelemetryData);

    preSessionTelemetryClient.setRealTelemetryClient(mockRealTelemetryClient);

    assertTrue(preSessionTelemetryClient.hasRealTelemetryClient());
    verify(mockRealTelemetryClient, times(2)).addLogToBatch(mockTelemetryData);

    preSessionTelemetryClient.addLogToBatch(mockTelemetryData);
    verify(mockRealTelemetryClient, times(3)).addLogToBatch(mockTelemetryData);
  }
}
