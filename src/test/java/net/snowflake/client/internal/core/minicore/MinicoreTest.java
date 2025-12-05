package net.snowflake.client.internal.core.minicore;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class MinicoreTest {

  private static final long MAX_MINICORE_INIT_TIME_MS = 1000;
  private static final int NUM_TIMING_RUNS = 5;

  @Test
  public void testAsyncInitializationCompletesInReasonableTime() {
    long[] times = new long[NUM_TIMING_RUNS];
    MinicoreLoadResult[] results = new MinicoreLoadResult[NUM_TIMING_RUNS];

    for (int run = 0; run < NUM_TIMING_RUNS; run++) {
      Minicore.resetForTesting();
      long startTime = System.currentTimeMillis();
      Minicore.initializeAsync();

      await()
          .atMost(Duration.ofMillis(MAX_MINICORE_INIT_TIME_MS))
          .until(() -> Minicore.getInstance() != null);

      times[run] = System.currentTimeMillis() - startTime;
      results[run] = Minicore.getInstance().getLoadResult();
      assertTrue(results[run].isSuccess());
    }

    System.out.printf(
        "Minicore init (%d runs): firstRun=%dms, avg=%dms%n",
        NUM_TIMING_RUNS, times[0], average(times));

    for (int run = 0; run < NUM_TIMING_RUNS; run++) {
      System.out.printf("Run %d logs:%n", run + 1);
      for (String log : results[run].getLogs()) {
        System.out.printf("  %s%n", log);
      }
    }
  }

  private long average(long[] values) {
    long sum = 0;
    for (long v : values) {
      sum += v;
    }
    return sum / values.length;
  }

  @Test
  public void testMinicoreInitializesSuccessfully() {
    Minicore.resetForTesting();

    Minicore.initialize();
    Minicore instance = Minicore.getInstance();
    MinicorePlatform platform = new MinicorePlatform();

    assertNotNull(instance, "Minicore instance should not be null after initialization");

    MinicoreLoadResult result = instance.getLoadResult();
    assertNotNull(result, "Load result should not be null");

    // Build detailed error message in case of failure
    StringBuilder errorMessage = new StringBuilder();
    errorMessage
        .append("Minicore should load successfully on platform: ")
        .append(platform.getPlatformIdentifier());
    errorMessage.append("\nPlatform supported: ").append(platform.isSupported());
    errorMessage.append("\nOS: ").append(platform.getOsName());
    errorMessage.append("\nArch: ").append(platform.getOsArch());

    if (!result.isSuccess()) {
      errorMessage.append("\n\n=== LOAD FAILURE DETAILS ===");
      errorMessage.append("\nError: ").append(result.getErrorMessage());
      if (result.getException() != null) {
        errorMessage.append("\nException: ").append(result.getException().getClass().getName());
        errorMessage.append(": ").append(result.getException().getMessage());
      }
      errorMessage.append("\nLibrary file: ").append(result.getLibraryFileName());
      errorMessage.append("\n\nLoad Logs:");
      for (String log : result.getLogs()) {
        errorMessage.append("\n  ").append(log);
      }
    }

    // Loading should succeed
    assertTrue(result.isSuccess(), errorMessage.toString());
    assertNull(result.getErrorMessage(), "Error message should be null on success");
    assertNull(result.getException(), "Exception should be null on success");

    MinicoreLibrary library = instance.getLibrary();
    assertNotNull(library, "Library should not be null after successful load");

    String version = library.sf_core_full_version();
    assertNotNull(version, "Version should not be null");
    assertFalse(version.isEmpty(), "Version should not be empty");
    assertTrue(
        version.contains("0.0.1"),
        "Version should contain expected version number, got: " + version);

    assertEquals(version, result.getCoreVersion(), "Version in result should match library call");
  }

  @Test
  public void testMinicoreInitializationIsIdempotent() {
    // Reset to ensure clean state
    Minicore.resetForTesting();

    // GIVEN - Initialize minicore twice
    Minicore.initialize();
    Minicore instance1 = Minicore.getInstance();

    Minicore.initialize();
    Minicore instance2 = Minicore.getInstance();

    // THEN - Should return the same singleton instance
    assertSame(instance1, instance2, "Multiple initializations should return the same instance");

    // AND - Load result should be the same
    assertSame(
        instance1.getLoadResult(),
        instance2.getLoadResult(),
        "Load result should be the same across calls");
  }

  @Test
  public void testMinicoreLibraryFunctionConsistency() {
    // Reset to ensure clean state
    Minicore.resetForTesting();

    // GIVEN - Initialized minicore
    Minicore.initialize();
    Minicore instance = Minicore.getInstance();

    MinicoreLoadResult result = instance.getLoadResult();
    assertTrue(result.isSuccess(), "This test requires successful minicore load");

    MinicoreLibrary library = instance.getLibrary();
    assertNotNull(library, "Library should not be null");

    // WHEN - Calling the same function multiple times
    String version1 = library.sf_core_full_version();
    String version2 = library.sf_core_full_version();
    String version3 = library.sf_core_full_version();

    // THEN - Results should be consistent
    assertEquals(version1, version2, "Version should be consistent across calls");
    assertEquals(version2, version3, "Version should be consistent across calls");
  }
}
