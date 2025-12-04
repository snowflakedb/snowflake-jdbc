package net.snowflake.client.internal.core.minicore;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseJDBCTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Core test suite for Minicore initialization and functionality. */
@Tag(TestTags.CORE)
public class MinicoreTestIT extends BaseJDBCTest {

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

  @Test
  public void testExecuteSelectWithMinicore() throws SQLException {
    int numRuns = 3;

    System.out.println("Warmup runs...");
    for (int i = 0; i < 2; i++) {
      Minicore.resetForTesting();
      Minicore.initialize();
      measureConnectionTime();
      Minicore.resetForTesting();
      measureConnectionTime();
    }

    long[] timesWithMinicore = new long[numRuns];
    long[] timesWithoutMinicore = new long[numRuns];
    long[] minicoreTimes = new long[numRuns];

    System.out.println("\nInterleaved measurement runs:");
    for (int i = 0; i < numRuns; i++) {
      // Run WITHOUT minicore
      Minicore.resetForTesting();
      timesWithoutMinicore[i] = measureConnectionTime();
      System.out.printf("  Run %d - without: %dms%n", i + 1, timesWithoutMinicore[i]);

      // Run WITH minicore (sync init)
      Minicore.resetForTesting();
      long startMinicore = System.currentTimeMillis();
      Minicore.initialize();
      minicoreTimes[i] = System.currentTimeMillis() - startMinicore;
      timesWithMinicore[i] = measureConnectionTime();
      System.out.printf(
          "  Run %d - with (init=%dms): %dms%n", i + 1, minicoreTimes[i], timesWithMinicore[i]);
    }

    // Calculate averages
    long avgWith = average(timesWithMinicore);
    long avgWithout = average(timesWithoutMinicore);
    long avgMinicoreInit = average(minicoreTimes);
    assertTrue(
        (avgWith - avgWithout) < MAX_MINICORE_INIT_TIME_MS,
        "Minicore initialization time difference exceeded");

    System.out.println("\n=== testExecuteSelect with/without Minicore Impact ===");
    System.out.printf("Minicore sync init time: avg=%dms%n", avgMinicoreInit);
    System.out.printf("With sync minicore (%d runs): avg=%dms%n", numRuns, avgWith);
    System.out.printf("Without minicore (%d runs): avg=%dms%n", numRuns, avgWithout);
    System.out.printf("Connection time difference: %dms%n", avgWith - avgWithout);
  }

  private long measureConnectionTime() throws SQLException {
    long start = System.currentTimeMillis();
    try (Connection conn = getConnection()) {
      try (Statement statement = conn.createStatement()) {
        ResultSet rs = statement.executeQuery("select 1;");
        rs.next();
        assertEquals(1, rs.getInt(1));
      }
    }
    return System.currentTimeMillis() - start;
  }

  private long average(long[] values) {
    long sum = 0;
    for (long v : values) sum += v;
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
