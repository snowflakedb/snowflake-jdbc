package net.snowflake.client.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

class NoOpTelemetryClientTest {

  @Test
  void testAddLogToBatch() {
    NoOpTelemetryClient client = new NoOpTelemetryClient();
    assertDoesNotThrow(() -> client.addLogToBatch(null)); // Should not throw an exception
  }

  @Test
  void testClose() {
    NoOpTelemetryClient client = new NoOpTelemetryClient();
    assertDoesNotThrow(client::close); // Should not throw an exception
  }

  @Test
  void testSendBatchAsync() throws Exception {
    NoOpTelemetryClient client = new NoOpTelemetryClient();
    Future<Boolean> result = client.sendBatchAsync();
    assertNotNull(result);
    assertTrue(result.get()); // Should return true
  }

  @Test
  void testPostProcess() {
    NoOpTelemetryClient client = new NoOpTelemetryClient();
    assertDoesNotThrow(
        () ->
            client.postProcess(
                "query123", "SQLSTATE", 1001, new Exception("Test Exception"))); // Should not throw
    assertDoesNotThrow(
        () -> client.postProcess(null, null, 0, null)); // Should handle null values gracefully
  }
}
