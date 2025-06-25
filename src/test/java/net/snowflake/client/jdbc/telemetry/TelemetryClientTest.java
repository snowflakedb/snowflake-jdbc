package net.snowflake.client.jdbc.telemetry;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.LogManager;
import net.snowflake.client.AbstractDriverIT;
import org.junit.jupiter.api.Test;

class TelemetryClientTest extends AbstractDriverIT {
  // Test parameters
  private static final int APP_THREAD_COUNT = 16;
  private static final int QUERIES = 1000;
  // Concurrent lists to store timings from multiple threads
  private final ConcurrentHashMap<String, List<Long>> timings = new ConcurrentHashMap<>();

  // Define keys for different operations
  private static final String GET_CONNECTION_TIME = "GetConnection";
  private static final String CREATE_STATEMENT_TIME = "CreateStatement";
  private static final String EXECUTE_QUERY_TIME = "ExecuteQuery";
  private static final String CLOSE_RS_TIME = "CloseResultSet";
  private static final String CLOSE_STMT_TIME = "CloseStatement";
  private static final String CLOSE_CONN_TIME = "CloseConnection";
  private static final String TOTAL_JOB_TIME = "TotalJob";

  @Test
  public void testTelemetryPerformance()
      throws SQLException, InterruptedException { // Initialize timing lists
    timings.put(GET_CONNECTION_TIME, Collections.synchronizedList(new ArrayList<>()));
    timings.put(CREATE_STATEMENT_TIME, Collections.synchronizedList(new ArrayList<>()));
    timings.put(EXECUTE_QUERY_TIME, Collections.synchronizedList(new ArrayList<>()));
    timings.put(CLOSE_RS_TIME, Collections.synchronizedList(new ArrayList<>()));
    timings.put(CLOSE_STMT_TIME, Collections.synchronizedList(new ArrayList<>()));
    timings.put(CLOSE_CONN_TIME, Collections.synchronizedList(new ArrayList<>()));
    timings.put(TOTAL_JOB_TIME, Collections.synchronizedList(new ArrayList<>()));

    try (InputStream inputStream =
        TelemetryClientTest.class.getClassLoader().getResourceAsStream("logging.properties")) {
      if (inputStream != null) {
        LogManager.getLogManager().readConfiguration(inputStream);
      } else {
        System.err.println("logging.properties not found in resources directory.");
      }

    } catch (IOException e) {
      System.err.println("Error reading logging configuration: " + e.getMessage());
      e.printStackTrace();
    }

    ExecutorService applicationSimulator = Executors.newFixedThreadPool(APP_THREAD_COUNT);
    CountDownLatch latch = new CountDownLatch(APP_THREAD_COUNT);
    long startTime = System.currentTimeMillis();

    // 4. Start all application threads to run queries concurrently.
    for (int i = 0; i < QUERIES; i++) {
      applicationSimulator.submit(job());
    }

    // 5. Wait for all threads to finish.
    applicationSimulator.shutdown();
    applicationSimulator.awaitTermination(10 * 60, java.util.concurrent.TimeUnit.SECONDS);
    long duration = System.currentTimeMillis() - startTime;

    System.out.printf("Total Test Duration: %d ms\n", duration);
    System.out.println("-------------------------------------------------");

    // 6. Print statistics
    printTimingStatistics();
  }

  private Runnable job() {
    return () -> {
      long jobStart = System.nanoTime();

      long start, end;

      Connection conn = null;
      Statement stmt = null;
      ResultSet rs = null;

      try {
        // Get Connection
        start = System.nanoTime();
        conn = getConnection();
        end = System.nanoTime();
        addTiming(GET_CONNECTION_TIME, end - start);

        // Create Statement
        start = System.nanoTime();
        stmt = conn.createStatement();
        end = System.nanoTime();
        addTiming(CREATE_STATEMENT_TIME, end - start);

        // Execute Query
        start = System.nanoTime();
        rs = stmt.executeQuery("SELECT 1;");
        end = System.nanoTime();
        addTiming(EXECUTE_QUERY_TIME, end - start);

      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        // Close ResultSet
        try {
          if (rs != null) {
            start = System.nanoTime();
            rs.close();
            end = System.nanoTime();
            addTiming(CLOSE_RS_TIME, end - start);
          }
        } catch (SQLException e) {
          e.printStackTrace();
        }

        // Close Statement
        try {
          if (stmt != null) {
            start = System.nanoTime();
            stmt.close();
            end = System.nanoTime();
            addTiming(CLOSE_STMT_TIME, end - start);
          }
        } catch (SQLException e) {
          e.printStackTrace();
        }

        // Close Connection
        try {
          if (conn != null) {
            start = System.nanoTime();
            conn.close();
            end = System.nanoTime();
            addTiming(CLOSE_CONN_TIME, end - start);
          }
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      long jobEnd = System.nanoTime();
      addTiming(TOTAL_JOB_TIME, jobEnd - jobStart);
    };
  }

  private void addTiming(String operation, long durationNanos) {
    // Add timing to the correct list. ConcurrentHashMap and synchronizedList ensure thread-safety.
    timings.get(operation).add(durationNanos);
  }

  private void printTimingStatistics() {
    System.out.println("JDBC Operation Timings (nanoseconds):");
    timings.forEach(
        (operation, durations) -> {
          if (!durations.isEmpty()) {
            double avg = durations.stream().mapToLong(Long::longValue).average().orElse(0.0);
            long min = durations.stream().mapToLong(Long::longValue).min().orElse(0L);
            long max = durations.stream().mapToLong(Long::longValue).max().orElse(0L);
            long sum = durations.stream().mapToLong(Long::longValue).sum();

            // Convert to milliseconds for display if desired, otherwise keep in nanoseconds
            System.out.printf("  %s (Count: %d):\n", operation, durations.size());
            System.out.printf("    Min: %10d ns (%.3f ms)\n", min, (double) min / 1_000_000.0);
            System.out.printf("    Max: %10d ns (%.3f ms)\n", max, (double) max / 1_000_000.0);
            System.out.printf("    Avg: %10.2f ns (%.3f ms)\n", avg, avg / 1_000_000.0);
            System.out.printf("    Sum: %10d ns (%.3f ms)\n", sum, (double) sum / 1_000_000.0);
            System.out.println();
          } else {
            System.out.printf("  %s: No data collected\n", operation);
          }
        });
  }
}
