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
class BufferedTelemetryClientTest {
  private BufferedTelemetryClient bufferedClient;
  private Telemetry mockRealTelemetryClient;
  private TelemetryData mockTelemetryData;

  @BeforeEach
  void setUp() {
    bufferedClient = new BufferedTelemetryClient();
    mockRealTelemetryClient = mock(Telemetry.class);
    mockTelemetryData = mock(TelemetryData.class);
  }

  @Test
  void shouldFlushBufferedDataAndPassThroughAfterRealClientSet() {
    bufferedClient.addLogToBatch(mockTelemetryData);
    bufferedClient.addLogToBatch(mockTelemetryData);

    bufferedClient.setRealTelemetryClient(mockRealTelemetryClient);

    assertTrue(bufferedClient.hasRealTelemetryClient());
    verify(mockRealTelemetryClient, times(2)).addLogToBatch(mockTelemetryData);

    bufferedClient.addLogToBatch(mockTelemetryData);
    verify(mockRealTelemetryClient, times(3)).addLogToBatch(mockTelemetryData);
  }
}
