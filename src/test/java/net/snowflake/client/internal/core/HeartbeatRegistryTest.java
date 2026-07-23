package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class HeartbeatRegistryTest {
  private ScheduledExecutorService mockExecutor;
  private ManualClock manualClock;
  private HeartbeatRegistry registry;
  private SFSession mockSession1;
  private SFSession mockSession2;
  private SFSession mockSession3;

  @BeforeEach
  public void setUp() {
    mockExecutor = mock(ScheduledExecutorService.class);
    manualClock = new ManualClock(Instant.ofEpochSecond(1000000));

    // Mock schedule to return a future
    @SuppressWarnings("unchecked")
    ScheduledFuture<Object> mockFuture = mock(ScheduledFuture.class);
    when(mockExecutor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
        .thenAnswer(invocation -> mockFuture);
    when(mockExecutor.isShutdown()).thenReturn(false);

    registry = new HeartbeatRegistry(mockExecutor, manualClock);

    mockSession1 = mock(SFSession.class);
    mockSession2 = mock(SFSession.class);
    mockSession3 = mock(SFSession.class);

    when(mockSession1.getSessionId()).thenReturn("session-1");
    when(mockSession2.getSessionId()).thenReturn("session-2");
    when(mockSession3.getSessionId()).thenReturn("session-3");
  }

  @AfterEach
  public void tearDown() {
    if (registry != null) {
      registry.shutdown();
    }
  }

  @Test
  public void testAddSession_SingleSession() {
    // interval = min(10, 60/4) = 10, floored to the 900s minimum
    registry.addSession(mockSession1, 60, 10);

    assertEquals(1, registry.getActiveThreadCount());
    assertEquals(1, registry.getSessionCountForInterval(900));
  }

  @Test
  public void testAddSession_MultipleSessionsSameInterval_NoReschedule() {
    registry.addSession(mockSession1, 60, 10);
    registry.addSession(mockSession2, 60, 10);

    // Only ONE thread should exist (both sessions floored to the 900s minimum interval)
    assertEquals(1, registry.getActiveThreadCount());
    assertEquals(2, registry.getSessionCountForInterval(900));

    // Verify scheduler called only ONCE (no reschedule)
    verify(mockExecutor, times(1)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
  }

  @Test
  public void testAddSession_MultipleSessionsDifferentIntervals_Independent() {
    // Session 1: 60s validity, 10s heartbeat (interval = 10s, floored to 900s)
    registry.addSession(mockSession1, 60, 10);

    // Session 2: 4h validity, 1h heartbeat (interval = 3600s)
    registry.addSession(mockSession2, 14400, 3600);

    // TWO independent threads should exist
    assertEquals(2, registry.getActiveThreadCount());
    assertEquals(1, registry.getSessionCountForInterval(900));
    assertEquals(1, registry.getSessionCountForInterval(3600));
  }

  @Test
  public void testAddSession_CalculatesIntervalCorrectly() {
    // Requested 1000s, but validity/4 = 14400/4 = 3600s; both are above the 900s floor,
    // so the interval reflects the min() of the two (1000s).
    registry.addSession(mockSession1, 14400, 1000);

    // Should use 1000s (minimum of 1000 and 14400/4)
    assertEquals(1, registry.getActiveThreadCount());
    assertEquals(1, registry.getSessionCountForInterval(1000));
  }

  @Test
  public void testAddSession_IntervalFlooredToMinimum() {
    // interval = min(10, 60/4) = 10, but is floored to the 900s minimum
    registry.addSession(mockSession1, 60, 10);

    assertEquals(1, registry.getActiveThreadCount());
    assertEquals(1, registry.getSessionCountForInterval(900));
    assertEquals(0, registry.getSessionCountForInterval(10));
  }

  @Test
  public void testAddSession_NullSession_ThrowsException() {
    assertThrows(IllegalArgumentException.class, () -> registry.addSession(null, 60, 10));
  }

  @Test
  public void testAddSession_InvalidValidity_ThrowsException() {
    assertThrows(IllegalArgumentException.class, () -> registry.addSession(mockSession1, 0, 10));

    assertThrows(IllegalArgumentException.class, () -> registry.addSession(mockSession1, -60, 10));
  }

  @Test
  public void testAddSession_InvalidFrequency_ThrowsException() {
    assertThrows(IllegalArgumentException.class, () -> registry.addSession(mockSession1, 60, 0));

    assertThrows(IllegalArgumentException.class, () -> registry.addSession(mockSession1, 60, -10));
  }

  @Test
  public void testRemoveSession_RemovesFromCorrectThread() {
    registry.addSession(mockSession1, 60, 10);
    registry.addSession(mockSession2, 60, 10);

    // both floored to the 900s minimum interval
    assertEquals(2, registry.getSessionCountForInterval(900));

    registry.removeSession(mockSession1);

    assertEquals(1, registry.getSessionCountForInterval(900));
    assertEquals(1, registry.getActiveThreadCount());
  }

  @Test
  public void testRemoveSession_CleansUpEmptyThread() {
    registry.addSession(mockSession1, 60, 10);
    assertEquals(1, registry.getActiveThreadCount());

    registry.removeSession(mockSession1);

    // Thread should be cleaned up
    assertEquals(0, registry.getActiveThreadCount());
    assertEquals(0, registry.getSessionCountForInterval(10));
  }

  @Test
  public void testRemoveSession_DoesNotAffectOtherThreads() {
    registry.addSession(mockSession1, 60, 10);
    registry.addSession(mockSession2, 14400, 3600);

    assertEquals(2, registry.getActiveThreadCount());

    registry.removeSession(mockSession1);

    // Thread for interval 10s should be gone
    assertEquals(1, registry.getActiveThreadCount());
    assertEquals(0, registry.getSessionCountForInterval(10));

    // Thread for interval 3600s should still exist
    assertEquals(1, registry.getSessionCountForInterval(3600));
  }

  @Test
  public void testRemoveSession_NonExistentSession_DoesNotThrow() {
    assertDoesNotThrow(() -> registry.removeSession(mockSession1));
  }

  @Test
  public void testRemoveSession_NullSession_DoesNotThrow() {
    assertDoesNotThrow(() -> registry.removeSession(null));
  }

  @Test
  public void testCriticalBug_ShortSessionNotExpiredByLongSession() throws Exception {
    // This tests the fix for the critical bug: a shorter-interval session must be heartbeated on
    // its own schedule, independently of a longer-interval session.

    // Session A: 1h validity, 15min heartbeat (interval = min(900, 3600/4) = 900s)
    registry.addSession(mockSession1, 3600, 900);

    // Session B: 4h validity, 1h heartbeat (interval = min(3600, 14400/4) = 3600s)
    registry.addSession(mockSession2, 14400, 3600);

    // Verify TWO independent threads exist
    assertEquals(2, registry.getActiveThreadCount());

    // Advance time past session A's interval
    manualClock.advance(Duration.ofSeconds(905));

    // Clear previous invocations
    clearInvocations(mockSession1, mockSession2);

    // Trigger 900s heartbeat (for shorter-interval session)
    registry.triggerHeartbeatForInterval(900);

    // Verify: Session1 got heartbeat (NOT expired)
    try {
      verify(mockSession1, times(1)).heartbeat();
      // Verify: Session2 NOT heartbeated yet (its interval is 3600s)
      verify(mockSession2, never()).heartbeat();
    } catch (SFException | SQLException e) {
      fail("Verification should not throw: " + e.getMessage());
    }
  }

  @Test
  public void testTriggerHeartbeatForInterval_OnlyAffectsSpecifiedInterval() throws Exception {
    registry.addSession(mockSession1, 3600, 900); // interval = 900s
    registry.addSession(mockSession2, 14400, 3600); // interval = 3600s

    // Trigger only 900s interval
    registry.triggerHeartbeatForInterval(900);

    try {
      verify(mockSession1, times(1)).heartbeat();
      verify(mockSession2, never()).heartbeat();
    } catch (SFException | SQLException e) {
      fail("Verification should not throw: " + e.getMessage());
    }

    // Trigger only 3600s interval
    clearInvocations(mockSession1, mockSession2);
    registry.triggerHeartbeatForInterval(3600);

    try {
      verify(mockSession1, never()).heartbeat();
      verify(mockSession2, times(1)).heartbeat();
    } catch (SFException | SQLException e) {
      fail("Verification should not throw: " + e.getMessage());
    }
  }

  @Test
  public void testTriggerHeartbeatForInterval_NonExistentInterval_DoesNotThrow() {
    assertDoesNotThrow(() -> registry.triggerHeartbeatForInterval(999));
  }

  @Test
  public void testAddSession_AfterShutdown_SkipsHeartbeatInsteadOfThrowing() {
    registry.addSession(mockSession1, 60, 10);
    assertEquals(1, registry.getActiveThreadCount());

    registry.shutdown();
    assertEquals(0, registry.getActiveThreadCount());

    assertDoesNotThrow(() -> registry.addSession(mockSession2, 60, 10));
    assertEquals(0, registry.getActiveThreadCount());
    assertEquals(0, registry.getSessionCountForInterval(10));
  }

  @Test
  public void testShutdown_CleansUpAllThreads() {
    registry.addSession(mockSession1, 60, 10);
    registry.addSession(mockSession2, 14400, 3600);

    assertEquals(2, registry.getActiveThreadCount());

    registry.shutdown();

    assertEquals(0, registry.getActiveThreadCount());
    verify(mockExecutor, times(1)).shutdown();
  }

  /** Manual clock implementation for testing. */
  private static class ManualClock extends Clock {
    private Instant currentTime;

    ManualClock(Instant startTime) {
      this.currentTime = startTime;
    }

    void advance(Duration duration) {
      currentTime = currentTime.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneId.systemDefault();
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return currentTime;
    }

    @Override
    public long millis() {
      return currentTime.toEpochMilli();
    }
  }
}
