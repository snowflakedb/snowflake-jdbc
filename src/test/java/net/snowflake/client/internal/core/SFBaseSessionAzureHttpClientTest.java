package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class SFBaseSessionAzureHttpClientTest {

  /** Factory runs at most once; subsequent calls return the same instance. */
  @Test
  public void shouldCreateClientLazilyAndReuseIt() {
    SFSession session = new SFSession();
    AtomicInteger calls = new AtomicInteger();
    CountingCloseable first =
        session.getOrCreateAzureHttpClient(
            () -> {
              calls.incrementAndGet();
              return new CountingCloseable();
            });
    CountingCloseable second =
        session.getOrCreateAzureHttpClient(
            () -> {
              calls.incrementAndGet();
              return new CountingCloseable();
            });

    assertSame(first, second);
    assertEquals(1, calls.get(), "factory should be invoked exactly once, got " + calls.get());
    assertFalse(first.closed);
  }

  /** {@code closeAzureHttpClient} closes the held resource and is idempotent. */
  @Test
  public void shouldCloseClientOnceAndBeIdempotent() {
    TestSession session = new TestSession();
    CountingCloseable holder = session.getOrCreateAzureHttpClient(CountingCloseable::new);

    session.invokeCloseAzureHttpClient();
    session.invokeCloseAzureHttpClient();

    assertTrue(holder.closed);
    assertEquals(1, holder.closeCount, "close should run exactly once, got " + holder.closeCount);
  }

  /**
   * After close, attempts to (re)create the client must throw — otherwise we'd resurrect a client
   * that nothing will ever close.
   */
  @Test
  public void shouldRejectGetOrCreateAfterClose() {
    TestSession session = new TestSession();
    session.invokeCloseAzureHttpClient();

    assertThrows(
        IllegalStateException.class,
        () -> session.getOrCreateAzureHttpClient(CountingCloseable::new));
  }

  /**
   * A failure in the holder's close() must not propagate out of {@code closeAzureHttpClient}, and
   * the session must still be poisoned so no caller can resurrect the client. A second close() must
   * be a no-op.
   */
  @Test
  public void shouldSwallowExceptionFromHolderClose() {
    TestSession session = new TestSession();
    session.getOrCreateAzureHttpClient(
        () ->
            () -> {
              throw new RuntimeException("boom");
            });

    session.invokeCloseAzureHttpClient();

    assertThrows(
        IllegalStateException.class,
        () -> session.getOrCreateAzureHttpClient(CountingCloseable::new));
    session.invokeCloseAzureHttpClient();
  }

  /** Test subclass that exposes the protected close hook so we can drive lifecycle directly. */
  private static class TestSession extends SFSession {
    void invokeCloseAzureHttpClient() {
      closeAzureHttpClient();
    }
  }

  private static class CountingCloseable implements AutoCloseable {
    boolean closed;
    int closeCount;

    @Override
    public void close() {
      closed = true;
      closeCount++;
    }
  }
}
