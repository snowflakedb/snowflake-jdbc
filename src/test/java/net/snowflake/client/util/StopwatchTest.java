package net.snowflake.client.util;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class StopwatchTest {
    Stopwatch stopwatch = new Stopwatch();

    @Before
    public void before() {
        stopwatch = new Stopwatch();
    }

    @Test
    public void testGetMillisWhenStopped() throws InterruptedException {
        stopwatch.start();
        TimeUnit.MILLISECONDS.sleep(20);
        stopwatch.stop();

        assertThat(stopwatch.elapsedMillis(), allOf(greaterThanOrEqualTo(10L), lessThanOrEqualTo(30L)));
    }

    @Test
    public void testGetMillisWithoutStopping() throws InterruptedException {
        stopwatch.start();
        TimeUnit.MILLISECONDS.sleep(20);
        assertThat(stopwatch.elapsedMillis(), allOf(greaterThanOrEqualTo(10L), lessThanOrEqualTo(30L)));
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
