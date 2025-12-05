package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class MinicoreLoaderTest {

  @Test
  public void testLoadLibraryIsCached() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result1 = loader.loadLibrary();
    MinicoreLoadResult result2 = loader.loadLibrary();

    assertSame(result1, result2, "Subsequent calls should return the same cached result");
  }

  @Test
  public void testLoadResultContainsAllFields() {
    MinicoreLoader loader = new MinicoreLoader();
    MinicoreLoadResult result = loader.loadLibrary();
    MinicorePlatform platform = new MinicorePlatform();

    assertNotNull(result.getLibraryFileName(), "Library file name should not be null");
    String expectedFileName = platform.getLibraryFileName();
    assertTrue(
        result.getLibraryFileName().contains("sf_mini_core"),
        "Library file name should contain base name");
    assertEquals(
        result.getLibraryFileName(),
        expectedFileName,
        "Library file name should match platform's expected file name");

    assertNotNull(result.getLogs(), "Logs should not be null");
    assertFalse(result.getLogs().isEmpty(), "Logs should contain entries");

    if (result.isSuccess()) {
      assertNull(result.getErrorMessage(), "Error message should be null on success");
      assertNull(result.getException(), "Exception should be null on success");
    } else {
      assertNotNull(result.getErrorMessage(), "Error message should be set on failure");
    }
  }
}
