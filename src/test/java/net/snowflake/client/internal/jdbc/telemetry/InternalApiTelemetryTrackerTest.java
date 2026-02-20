package net.snowflake.client.internal.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@Tag(TestTags.CORE)
class InternalApiTelemetryTrackerTest {
  private Telemetry mockClient;

  @BeforeEach
  void setUp() {
    InternalApiTelemetryTracker.resetForTesting();
    mockClient = mock(Telemetry.class);
  }

  @Test
  void shouldFlushAggregatedDataThroughProvidedClient() {
    InternalApiTelemetryTracker.record("SFSession", "getDatabase");
    InternalApiTelemetryTracker.record("SFStatement", "execute");

    InternalApiTelemetryTracker.flush(mockClient);

    ArgumentCaptor<TelemetryData> captor = forClass(TelemetryData.class);
    verify(mockClient, times(1)).addLogToBatch(captor.capture());

    ObjectNode message = captor.getValue().getMessage();
    assertEquals("client_internal_api_usage", message.get("type").asText());
    assertEquals("JDBC", message.get("source").asText());

    JsonNode methods = message.get("methods");
    assertNotNull(methods);
    assertEquals(1, methods.get("SFSession#getDatabase").asLong());
    assertEquals(1, methods.get("SFStatement#execute").asLong());
  }

  @Test
  void shouldAggregateMultipleCallsToSameMethod() {
    InternalApiTelemetryTracker.record("SFSession", "getDatabase");
    InternalApiTelemetryTracker.record("SFSession", "getDatabase");
    InternalApiTelemetryTracker.record("SFSession", "getDatabase");

    InternalApiTelemetryTracker.flush(mockClient);

    ArgumentCaptor<TelemetryData> captor = forClass(TelemetryData.class);
    verify(mockClient).addLogToBatch(captor.capture());

    JsonNode methods = captor.getValue().getMessage().get("methods");
    assertEquals(3, methods.get("SFSession#getDatabase").asLong());
  }

  @Test
  void shouldNotFlushWhenNoCallsRecorded() {
    InternalApiTelemetryTracker.flush(mockClient);
    verify(mockClient, never()).addLogToBatch(org.mockito.ArgumentMatchers.any());
  }

  @Test
  void shouldDrainCountsOnFlushAndAccumulateAgain() {
    InternalApiTelemetryTracker.record("SFSession", "getDatabase");
    InternalApiTelemetryTracker.flush(mockClient);

    InternalApiTelemetryTracker.record("SFSession", "getDatabase");
    InternalApiTelemetryTracker.record("SFStatement", "execute");
    InternalApiTelemetryTracker.flush(mockClient);

    ArgumentCaptor<TelemetryData> captor = forClass(TelemetryData.class);
    verify(mockClient, times(2)).addLogToBatch(captor.capture());

    JsonNode firstMethods = captor.getAllValues().get(0).getMessage().get("methods");
    assertEquals(1, firstMethods.get("SFSession#getDatabase").asLong());
    assertNull(firstMethods.get("SFStatement#execute"));

    JsonNode secondMethods = captor.getAllValues().get(1).getMessage().get("methods");
    assertEquals(1, secondMethods.get("SFSession#getDatabase").asLong());
    assertEquals(1, secondMethods.get("SFStatement#execute").asLong());
  }

  @Test
  void concurrentSessionsEachFlushIndependently() {
    Telemetry clientA = mock(Telemetry.class);
    Telemetry clientB = mock(Telemetry.class);

    InternalApiTelemetryTracker.record("SFSession", "getDatabase");
    InternalApiTelemetryTracker.flush(clientA);

    InternalApiTelemetryTracker.record("SFStatement", "execute");
    InternalApiTelemetryTracker.flush(clientB);

    ArgumentCaptor<TelemetryData> captorA = forClass(TelemetryData.class);
    verify(clientA, times(1)).addLogToBatch(captorA.capture());
    assertEquals(
        1, captorA.getValue().getMessage().get("methods").get("SFSession#getDatabase").asLong());

    ArgumentCaptor<TelemetryData> captorB = forClass(TelemetryData.class);
    verify(clientB, times(1)).addLogToBatch(captorB.capture());
    assertEquals(
        1, captorB.getValue().getMessage().get("methods").get("SFStatement#execute").asLong());
  }

  @Test
  void markerAwareRecordingShouldRecordWhenMarkerIsMissing() {
    InternalApiTelemetryTracker.recordIfExternal("SFSession", "getRole", null);

    InternalApiTelemetryTracker.flush(mockClient);

    ArgumentCaptor<TelemetryData> captor = forClass(TelemetryData.class);
    verify(mockClient).addLogToBatch(captor.capture());
    assertEquals(
        1, captor.getValue().getMessage().get("methods").get("SFSession#getRole").asLong());
  }

  @Test
  void markerAwareRecordingShouldNotRecordWhenMarkerIsProvided() {
    InternalApiTelemetryTracker.recordIfExternal(
        "SFSession", "getRole", InternalApiTelemetryTracker.internalCallMarker());

    InternalApiTelemetryTracker.flush(mockClient);

    verify(mockClient, never()).addLogToBatch(org.mockito.ArgumentMatchers.any());
  }

  @Test
  void isExternalClassReturnsTrueForNonDriverPackages() {
    assertTrue(InternalApiTelemetryTracker.isExternalClass("com.customer.app.MyClass"));
    assertTrue(InternalApiTelemetryTracker.isExternalClass("org.apache.spark.sql.Driver"));
    assertTrue(InternalApiTelemetryTracker.isExternalClass("net.snowflake.ingest.SomeClass"));
  }

  @Test
  void isExternalClassReturnsFalseForDriverPackages() {
    assertFalse(
        InternalApiTelemetryTracker.isExternalClass(
            "net.snowflake.client.internal.core.SFSession"));
    assertFalse(
        InternalApiTelemetryTracker.isExternalClass(
            "net.snowflake.client.internal.jdbc.telemetry.InternalApiTelemetryTracker"));
    assertFalse(
        InternalApiTelemetryTracker.isExternalClass(
            "net.snowflake.client.api.driver.SnowflakeDriver"));
  }

  @Test
  void recordIfExternalShouldNotRecordWhenCalledFromDriverPackage() {
    simulateInternalApiCall();
    InternalApiTelemetryTracker.flush(mockClient);
    verify(mockClient, never()).addLogToBatch(org.mockito.ArgumentMatchers.any());
  }

  @Test
  void recordIfExternalShouldRecordWhenCalledFromExternalPackage() {
    InternalApiTelemetryTracker.recordIfCalledExternally(
        "SFSession", "getDatabase", "com.customer.app.MyApp");
    InternalApiTelemetryTracker.flush(mockClient);

    ArgumentCaptor<TelemetryData> captor = forClass(TelemetryData.class);
    verify(mockClient).addLogToBatch(captor.capture());

    JsonNode methods = captor.getValue().getMessage().get("methods");
    assertNotNull(methods);
    assertEquals(1, methods.get("SFSession#getDatabase").asLong());
  }

  @Test
  void recordIfExternalShouldNotRecordWhenCallerIsInDriverPackage() {
    InternalApiTelemetryTracker.recordIfCalledExternally(
        "SFSession", "getDatabase", "net.snowflake.client.internal.jdbc.SomeInternalClass");
    InternalApiTelemetryTracker.flush(mockClient);
    verify(mockClient, never()).addLogToBatch(org.mockito.ArgumentMatchers.any());
  }

  private void simulateInternalApiCall() {
    InternalApiTelemetryTracker.recordIfCalledExternally("SFSession", "getDatabase");
  }
}
