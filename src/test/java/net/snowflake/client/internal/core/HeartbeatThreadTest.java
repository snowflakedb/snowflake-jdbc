package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@Tag(TestTags.CORE)
public class HeartbeatThreadTest {
  private ScheduledExecutorService mockExecutor;
  private Clock fixedClock;
  private SFSession mockSession1;
  private SFSession mockSession2;

  @BeforeEach
  public void setUp() {
    mockExecutor = mock(ScheduledExecutorService.class);
    fixedClock = Clock.fixed(Instant.ofEpochSecond(1000000), ZoneId.systemDefault());
    mockSession1 = mock(SFSession.class);
    mockSession2 = mock(SFSession.class);

    when(mockSession1.getSessionId()).thenReturn("session-1");
    when(mockSession2.getSessionId()).thenReturn("session-2");

    // Mock schedule to return a future
    @SuppressWarnings("unchecked")
    ScheduledFuture<Object> mockFuture = mock(ScheduledFuture.class);
    when(mockExecutor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
        .thenAnswer(invocation -> mockFuture);
  }

  @Test
  public void testConstructor_ValidParameters() {
    HeartbeatThread thread = new HeartbeatThread(10, mockExecutor, fixedClock);

    assertEquals(10, thread.getIntervalSeconds());
    assertEquals(0, thread.getSessionCount());
    assertTrue(thread.isEmpty());
  }

  @Test
  public void testConstructor_InvalidInterval() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HeartbeatThread(0, mockExecutor, fixedClock),
        "Should reject zero interval");

    assertThrows(
        IllegalArgumentException.class,
        () -> new HeartbeatThread(-10, mockExecutor, fixedClock),
        "Should reject negative interval");
  }

  @Test
  public void testConstructor_NullExecutor() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HeartbeatThread(10, null, fixedClock),
        "Should reject null executor");
  }

  @Test
  public void testConstructor_NullClock() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new HeartbeatThread(10, mockExecutor, null),
        "Should reject null clock");
  }

  @Test
  public void testAddSession_FirstSession_StartsThread() {
    HeartbeatThread thread = new HeartbeatThread(10, mockExecutor, fixedClock);

    thread.addSession(mockSession1);

    assertEquals(1, thread.getSessionCount());
    assertFalse(thread.isEmpty());

    // Verify scheduler was called to start heartbeat
    verify(mockExecutor, times(1)).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.SECONDS));
  }

  @Test
  public void testAddSession_MultipleSessionsSameThread_NoReschedule() {
    HeartbeatThread thread = new HeartbeatThread(10, mockExecutor, fixedClock);

    thread.addSession(mockSession1);
    thread.addSession(mockSession2);

    assertEquals(2, thread.getSessionCount());

    // Verify scheduler was called only ONCE (no reschedule on second add)
    verify(mockExecutor, times(1)).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.SECONDS));
  }

  @Test
  public void testRemoveSession() {
    HeartbeatThread thread = new HeartbeatThread(10, mockExecutor, fixedClock);

    thread.addSession(mockSession1);
    thread.addSession(mockSession2);
    assertEquals(2, thread.getSessionCount());

    thread.removeSession(mockSession1);
    assertEquals(1, thread.getSessionCount());
    assertFalse(thread.isEmpty());

    thread.removeSession(mockSession2);
    assertEquals(0, thread.getSessionCount());
    assertTrue(thread.isEmpty());
  }

  @Test
  public void testShutdown() {
    @SuppressWarnings("unchecked")
    ScheduledFuture<Object> mockFuture = mock(ScheduledFuture.class);
    when(mockExecutor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
        .thenAnswer(invocation -> mockFuture);

    HeartbeatThread thread = new HeartbeatThread(10, mockExecutor, fixedClock);
    thread.addSession(mockSession1);

    thread.shutdown();

    // Verify future was cancelled
    verify(mockFuture, times(1)).cancel(false);

    // Thread should be empty after shutdown
    assertEquals(0, thread.getSessionCount());
    assertTrue(thread.isEmpty());
  }

  @Test
  public void testRun_HeartbeatsAllSessions() throws Exception {
    HeartbeatThread thread = new HeartbeatThread(10, mockExecutor, fixedClock);

    thread.addSession(mockSession1);
    thread.addSession(mockSession2);

    // Trigger heartbeat manually
    thread.triggerHeartbeatNow();

    // Verify both sessions were heartbeated
    try {
      verify(mockSession1, times(1)).heartbeat();
      verify(mockSession2, times(1)).heartbeat();
    } catch (SFException | SQLException e) {
      fail("Verification should not throw: " + e.getMessage());
    }
  }

  @Test
  public void testRun_ContinuesOnException() throws Exception {
    HeartbeatThread thread = new HeartbeatThread(10, mockExecutor, fixedClock);

    // Make session1 throw exception
    try {
      doThrow(new RuntimeException("Heartbeat failed")).when(mockSession1).heartbeat();
    } catch (SFException | SQLException e) {
      fail("Stubbing should not throw: " + e.getMessage());
    }

    thread.addSession(mockSession1);
    thread.addSession(mockSession2);

    // Trigger heartbeat - should not throw
    thread.triggerHeartbeatNow();

    // Verify both sessions were called (session1 threw, session2 succeeded)
    try {
      verify(mockSession1, times(1)).heartbeat();
      verify(mockSession2, times(1)).heartbeat();
    } catch (SFException | SQLException e) {
      fail("Verification should not throw: " + e.getMessage());
    }
  }

  @Test
  public void testRun_ReschedulesIfSessionsRemain() throws Exception {
    HeartbeatThread thread = new HeartbeatThread(10, mockExecutor, fixedClock);
    thread.addSession(mockSession1);

    // Clear previous invocations
    clearInvocations(mockExecutor);

    // Run heartbeat
    thread.triggerHeartbeatNow();

    // Should schedule next heartbeat since session still exists
    verify(mockExecutor, times(1)).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.SECONDS));
  }

  @Test
  public void testRun_DoesNotRescheduleAfterShutdown() {
    HeartbeatThread thread = new HeartbeatThread(10, mockExecutor, fixedClock);
    thread.addSession(mockSession1);

    thread.shutdown();
    clearInvocations(mockExecutor);

    // Try to run - should not reschedule
    thread.run();

    verify(mockExecutor, never()).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
  }

  @Test
  public void testScheduleHeartbeat_CalculatesCorrectDelay() {
    HeartbeatThread thread = new HeartbeatThread(10, mockExecutor, fixedClock);

    thread.addSession(mockSession1);

    ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
    verify(mockExecutor).schedule(any(Runnable.class), delayCaptor.capture(), eq(TimeUnit.SECONDS));

    long delay = delayCaptor.getValue();
    assertTrue(delay >= 0, "Delay should be non-negative");
    assertTrue(delay <= 10, "Delay should not exceed interval");
  }

  @Test
  public void testAddSession_AfterShutdown_ReturnsFalse() {
    HeartbeatThread thread = new HeartbeatThread(10, mockExecutor, fixedClock);

    thread.shutdown();

    // Try to add session after shutdown - should return false
    assertFalse(thread.addSession(mockSession1));

    assertEquals(0, thread.getSessionCount());
  }
}
