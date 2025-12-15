package net.snowflake.client.internal.core.minicore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseJDBCTest;
import org.junit.jupiter.api.Tag;

@Tag(TestTags.CORE)
public class MinicoreTestLatestIT extends BaseJDBCTest {

  private static final long MAX_MINICORE_INIT_TIME_MS = 1000;

  // @Test
  public void testExecuteSelectWithMinicorePerformance() throws SQLException {
    int numRuns = 3;

    // Cold start run (first connection with minicore - simulates customer cold JVM)
    System.out.println("Cold start run (with minicore)...");
    Minicore.resetForTesting();
    long coldStartTotal = System.currentTimeMillis();
    Minicore.initialize();
    long coldMinicoreInitTime = System.currentTimeMillis() - coldStartTotal;
    long coldConnectionTime = measureConnectionTime();
    long coldTotalTime = System.currentTimeMillis() - coldStartTotal;
    for (String log : Minicore.getInstance().getLoadResult().getLogs()) {
      System.out.printf("  %s%n", log);
    }
    System.out.printf(
        "  Cold: minicore=%dms, connection=%dms, total=%dms%n",
        coldMinicoreInitTime, coldConnectionTime, coldTotalTime);

    // Warm interleaved runs
    long[] timesWithMinicore = new long[numRuns];
    long[] timesWithoutMinicore = new long[numRuns];
    long[] minicoreTimes = new long[numRuns];

    System.out.println("\nInterleaved measurement runs (warm JVM):");
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
      for (String log : Minicore.getInstance().getLoadResult().getLogs()) {
        System.out.printf("  %s%n", log);
      }
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
    System.out.println("Cold start (first connection with minicore):");
    System.out.printf("  Minicore init: %dms%n", coldMinicoreInitTime);
    System.out.printf("  Connection:    %dms%n", coldConnectionTime);
    System.out.printf("  Total:         %dms%n", coldTotalTime);
    System.out.println("\nWarm JVM (interleaved runs):");
    System.out.printf("  Minicore sync init time: avg=%dms%n", avgMinicoreInit);
    System.out.printf("  With sync minicore (%d runs): avg=%dms%n", numRuns, avgWith);
    System.out.printf("  Without minicore (%d runs): avg=%dms%n", numRuns, avgWithout);
    System.out.printf("  Connection time difference: %dms%n", avgWith - avgWithout);
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
    for (long v : values) {
      sum += v;
    }
    return sum / values.length;
  }
}
