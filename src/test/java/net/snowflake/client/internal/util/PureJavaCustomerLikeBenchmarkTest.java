package net.snowflake.client.internal.util;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Pure Java benchmark that mimics the customer's extraction loop:
 *
 * <ul>
 *   <li>Iterate rows and columns
 *   <li>Convert per-cell byte[] payloads to String
 *   <li>Append values into a CSV-like StringBuilder
 *   <li>Periodically flush/reset the buffer
 *   <li>Optionally reproduce the customer's double-getString pattern
 * </ul>
 *
 * <p>This test has no JDBC, Arrow, or Snowflake dependency. It is intended to show whether AIX is
 * generally slower for this Java-side workload shape.
 */
public class PureJavaCustomerLikeBenchmarkTest {
  private static final int DEFAULT_ROWS = 100_000;
  private static final int DEFAULT_TEXT_COLUMNS = 38;
  private static final int DEFAULT_NUMERIC_COLUMNS = 10;
  private static final int DEFAULT_DATE_COLUMNS = 27;
  private static final int DEFAULT_TEXT_LENGTH = 256;
  private static final int DEFAULT_BUFFER_ROWS = 160;
  private static final int DEFAULT_WARMUP = 3;
  private static final int DEFAULT_RUNS = 5;

  @Test
  public void benchmarkCustomerLikeWorkload() {
    int rowCount = Integer.getInteger("benchmark.rows", DEFAULT_ROWS);
    int textColumns = Integer.getInteger("benchmark.textColumns", DEFAULT_TEXT_COLUMNS);
    int numericColumns = Integer.getInteger("benchmark.numericColumns", DEFAULT_NUMERIC_COLUMNS);
    int dateColumns = Integer.getInteger("benchmark.dateColumns", DEFAULT_DATE_COLUMNS);
    int textLength = Integer.getInteger("benchmark.textLength", DEFAULT_TEXT_LENGTH);
    int bufferRows = Integer.getInteger("benchmark.bufferRows", DEFAULT_BUFFER_ROWS);
    int warmupRuns = Integer.getInteger("benchmark.warmup", DEFAULT_WARMUP);
    int measuredRuns = Integer.getInteger("benchmark.runs", DEFAULT_RUNS);

    System.out.println("Pure Java Customer-Like Benchmark");
    System.out.println("=================================");
    System.out.println("Rows: " + rowCount);
    System.out.println("Text columns: " + textColumns);
    System.out.println("Numeric columns: " + numericColumns);
    System.out.println("Date columns: " + dateColumns);
    System.out.println("Target text length: " + textLength);
    System.out.println("Buffer rows: " + bufferRows);
    System.out.println("Warmup runs: " + warmupRuns);
    System.out.println("Measured runs: " + measuredRuns);
    System.out.println("Java version: " + System.getProperty("java.version"));
    System.out.println("VM name: " + System.getProperty("java.vm.name"));
    System.out.println("OS name: " + System.getProperty("os.name"));
    System.out.println("OS arch: " + System.getProperty("os.arch"));

    Dataset dataset =
        createDataset(rowCount, textColumns, numericColumns, dateColumns, textLength, bufferRows);

    System.out.println("Total columns: " + dataset.columnCount);
    System.out.println("Total payload bytes: " + dataset.totalPayloadBytes);

    for (int i = 0; i < warmupRuns; i++) {
      runMode(dataset, false);
      runMode(dataset, true);
    }

    BenchmarkStats singleDecodeStats = measure(dataset, measuredRuns, false);
    BenchmarkStats doubleDecodeStats = measure(dataset, measuredRuns, true);

    printStats("Single decode per cell", singleDecodeStats, dataset);
    printStats("Double decode per non-null cell", doubleDecodeStats, dataset);

    double avgRatio = doubleDecodeStats.averageNs / (double) singleDecodeStats.averageNs;
    double bestRatio = doubleDecodeStats.bestNs / (double) singleDecodeStats.bestNs;
    System.out.printf("Double/single slowdown ratio (avg): %.2fx%n", avgRatio);
    System.out.printf("Double/single slowdown ratio (best): %.2fx%n", bestRatio);
  }

  private static Dataset createDataset(
      int rowCount,
      int textColumns,
      int numericColumns,
      int dateColumns,
      int textLength,
      int bufferRows) {
    int columnCount = textColumns + numericColumns + dateColumns;
    byte[][][] rows = new byte[rowCount][columnCount][];
    int totalPayloadBytes = 0;

    for (int row = 0; row < rowCount; row++) {
      int columnIndex = 0;
      for (int i = 0; i < textColumns; i++, columnIndex++) {
        byte[] value = makeAsciiValue(row, i, textLength + ((row + i) % 17));
        rows[row][columnIndex] = value;
        totalPayloadBytes += value.length;
      }
      for (int i = 0; i < numericColumns; i++, columnIndex++) {
        byte[] value = makeNumericValue(row, i);
        rows[row][columnIndex] = value;
        totalPayloadBytes += value.length;
      }
      for (int i = 0; i < dateColumns; i++, columnIndex++) {
        byte[] value = makeDateValue(row, i);
        rows[row][columnIndex] = value;
        totalPayloadBytes += value.length;
      }
    }

    return new Dataset(rows, rowCount, columnCount, totalPayloadBytes, bufferRows);
  }

  private static byte[] makeAsciiValue(int row, int column, int length) {
    byte[] value = new byte[length];
    for (int i = 0; i < length; i++) {
      value[i] = (byte) ('A' + ((row + column + i) % 26));
    }
    return value;
  }

  private static byte[] makeNumericValue(int row, int column) {
    String value = Integer.toString((row * 31 + column * 17) % 1_000_000);
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] makeDateValue(int row, int column) {
    int year = 2020 + ((row + column) % 5);
    int month = 1 + ((row + column) % 12);
    int day = 1 + ((row + column) % 28);
    String value = String.format("%04d-%02d-%02d", year, month, day);
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static BenchmarkStats measure(Dataset dataset, int runs, boolean doubleDecode) {
    long bestNs = Long.MAX_VALUE;
    long totalNs = 0;
    long checksum = 0;

    for (int run = 0; run < runs; run++) {
      long start = System.nanoTime();
      checksum ^= runMode(dataset, doubleDecode);
      long elapsed = System.nanoTime() - start;
      bestNs = Math.min(bestNs, elapsed);
      totalNs += elapsed;
    }

    return new BenchmarkStats(bestNs, totalNs / runs, checksum);
  }

  private static long runMode(Dataset dataset, boolean doubleDecode) {
    long checksum = 0;
    long decodeCalls = 0;
    StringBuilder raw = new StringBuilder(dataset.bufferRows * dataset.columnCount * 16);
    int bufferedRows = 0;

    for (int row = 0; row < dataset.rowCount; row++) {
      for (int column = 0; column < dataset.columnCount; column++) {
        String firstDecode = new String(dataset.rows[row][column], StandardCharsets.UTF_8);
        decodeCalls++;

        if (doubleDecode) {
          if (firstDecode != null) {
            String secondDecode = new String(dataset.rows[row][column], StandardCharsets.UTF_8);
            decodeCalls++;
            raw.append(secondDecode);
            checksum += secondDecode.length();
            checksum += secondDecode.charAt(0);
          }
        } else {
          raw.append(firstDecode);
          checksum += firstDecode.length();
          checksum += firstDecode.charAt(0);
        }

        if (column < dataset.columnCount - 1) {
          raw.append(',');
          checksum += ',';
        }
      }

      raw.append('\n');
      checksum += '\n';
      bufferedRows++;

      if (bufferedRows >= dataset.bufferRows) {
        checksum += raw.length();
        raw.setLength(0);
        bufferedRows = 0;
      }
    }

    if (raw.length() > 0) {
      checksum += raw.length();
    }
    checksum += decodeCalls;
    return checksum;
  }

  private static void printStats(String label, BenchmarkStats stats, Dataset dataset) {
    System.out.println();
    System.out.println(label);
    System.out.println("-------------------------");
    printLine("Average", stats.averageNs, dataset.rowCount, dataset.columnCount, stats.checksum);
    printLine("Best", stats.bestNs, dataset.rowCount, dataset.columnCount, stats.checksum);
  }

  private static void printLine(
      String label, long elapsedNs, int rows, int columns, long checksum) {
    double totalMs = elapsedNs / 1_000_000.0;
    double nsPerRow = elapsedNs / (double) rows;
    double nsPerCell = elapsedNs / (double) (rows * columns);
    System.out.printf(
        "%-8s total=%.3f ms  ns/row=%.2f  ns/cell=%.2f  checksum=%d%n",
        label, totalMs, nsPerRow, nsPerCell, checksum);
  }

  private static final class Dataset {
    private final byte[][][] rows;
    private final int rowCount;
    private final int columnCount;
    private final int totalPayloadBytes;
    private final int bufferRows;

    private Dataset(
        byte[][][] rows, int rowCount, int columnCount, int totalPayloadBytes, int bufferRows) {
      this.rows = rows;
      this.rowCount = rowCount;
      this.columnCount = columnCount;
      this.totalPayloadBytes = totalPayloadBytes;
      this.bufferRows = bufferRows;
    }
  }

  private static final class BenchmarkStats {
    private final long bestNs;
    private final long averageNs;
    private final long checksum;

    private BenchmarkStats(long bestNs, long averageNs, long checksum) {
      this.bestNs = bestNs;
      this.averageNs = averageNs;
      this.checksum = checksum;
    }
  }
}
