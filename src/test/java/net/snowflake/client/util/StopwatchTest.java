package net.snowflake.client.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StopwatchTest {
  Stopwatch stopwatch = new Stopwatch();

  @BeforeEach
  public void before() {
    stopwatch = new Stopwatch();
  }

  @Test
  public void testGetMillisWhenStopped() throws InterruptedException {
    stopwatch.start();
    TimeUnit.MILLISECONDS.sleep(20);
    stopwatch.stop();

    assertThat(
        stopwatch.elapsedMillis(), allOf(greaterThanOrEqualTo(10L), lessThanOrEqualTo(500L)));
  }

  @Test
  public void testGetMillisWithoutStopping() throws InterruptedException {
    stopwatch.start();
    TimeUnit.MILLISECONDS.sleep(100);
    assertThat(
        stopwatch.elapsedMillis(), allOf(greaterThanOrEqualTo(10L), lessThanOrEqualTo(500L)));
  }

  @Test
  public void testShouldBeStarted() {
    stopwatch.start();
    assertTrue(stopwatch.isStarted());
  }

  @Test
  public void testShouldBeStopped() {
    assertFalse(stopwatch.isStarted());
  }

  @Test
  public void testThrowsExceptionWhenStartedTwice() {
    stopwatch.start();

    Exception e = assertThrows(IllegalStateException.class, () -> stopwatch.start());

    assertTrue(e.getMessage().contains("Stopwatch is already running"));
  }

  @Test
  public void testThrowsExceptionWhenStoppedTwice() {
    stopwatch.start();
    stopwatch.stop();

    Exception e = assertThrows(IllegalStateException.class, () -> stopwatch.stop());

    assertTrue(e.getMessage().contains("Stopwatch is already stopped"));
  }

  @Test
  public void testThrowsExceptionWhenStoppedWithoutStarting() {
    Exception e = assertThrows(IllegalStateException.class, () -> stopwatch.stop());

    assertTrue(e.getMessage().contains("Stopwatch has not been started"));
  }

  @Test
  public void testThrowsExceptionWhenElapsedMillisWithoutStarting() {
    Exception e = assertThrows(IllegalStateException.class, () -> stopwatch.elapsedMillis());

    assertTrue(e.getMessage().contains("Stopwatch has not been started"));
  }

  @Test
  public void testShouldReset() {
    stopwatch.start();
    assertTrue(stopwatch.isStarted());
    stopwatch.reset();
    assertFalse(stopwatch.isStarted());
  }

  @Test
  public void testShouldRestart() {
    stopwatch.start();
    assertTrue(stopwatch.isStarted());
    stopwatch.stop();
    assertFalse(stopwatch.isStarted());
    stopwatch.restart();
    assertTrue(stopwatch.isStarted());
  }
}
