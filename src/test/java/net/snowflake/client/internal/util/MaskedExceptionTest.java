package net.snowflake.client.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class MaskedExceptionTest {

  @Test
  public void testMasksMessage() {
    Throwable inner = new Exception("password=abcdef");
    MaskedException masked = new MaskedException(inner);

    String msg = masked.getMessage();
    assertNotNull(msg);
    assertTrue(msg.contains("****"));
    assertFalse(msg.contains("abcdef"));
  }

  @Test
  public void testMasksToString() {
    Throwable inner = new Exception("password=abcdef");
    MaskedException masked = new MaskedException(inner);

    String rendered = masked.toString();
    assertNotNull(rendered);
    assertTrue(rendered.contains("****"));
    assertFalse(rendered.contains("abcdef"));
  }

  @Test
  public void testPreservesStackFrames() {
    Throwable inner = new Exception("password=abcdef");
    MaskedException masked = new MaskedException(inner);

    assertArrayEquals(inner.getStackTrace(), masked.getStackTrace());
  }
}
