package net.snowflake.client.internal.util;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Pure Java benchmark that mimics the customer's extraction loop:
 *
 * <ul>
 *   <li>Iterate rows and columns
 *   <li>Convert per-cell byte[] payloads to String (UTF-8 decode)
 *   <li>Append values into a CSV-like StringBuilder
 *   <li>Periodically flush/reset the buffer
 * </ul>
 *
 * <p>This test has no JDBC, Arrow, or Snowflake dependency. It is intended to show whether AIX is
 * generally slower for this Java-side workload shape.
 *
 * <p>Data is generated on-the-fly during each iteration using deterministic functions. This avoids
 * pre-allocating all rows in memory, which would cause GC artifacts at large row counts (the
 * pre-allocated byte[][][] for 500k+ rows exceeds several GB and fills tenure space).
 */
public class PureJavaCustomerLikeBenchmarkTest {
  private static final int DEFAULT_TEXT_COLUMNS = 38;
  private static final int DEFAULT_NUMERIC_COLUMNS = 10;
  private static final int DEFAULT_DATE_COLUMNS = 27;
  private static final int DEFAULT_TEXT_LENGTH = 256;
  private static final int DEFAULT_BUFFER_ROWS = 160;
  private static final int DEFAULT_WARMUP = 3;
  private static final int DEFAULT_RUNS = 5;

  @Test
  public void benchmarkCustomerLikeWorkload() {
    int[] rowCounts = {100_000, 500_000};

    for (int rowCount : rowCounts) {
      runForRowCount(rowCount);
    }
  }

  private void runForRowCount(int rowCount) {
    int textColumns = Integer.getInteger("benchmark.textColumns", DEFAULT_TEXT_COLUMNS);
    int numericColumns = Integer.getInteger("benchmark.numericColumns", DEFAULT_NUMERIC_COLUMNS);
    int dateColumns = Integer.getInteger("benchmark.dateColumns", DEFAULT_DATE_COLUMNS);
    int textLength = Integer.getInteger("benchmark.textLength", DEFAULT_TEXT_LENGTH);
    int bufferRows = Integer.getInteger("benchmark.bufferRows", DEFAULT_BUFFER_ROWS);
    int warmupRuns = Integer.getInteger("benchmark.warmup", DEFAULT_WARMUP);
    int measuredRuns = Integer.getInteger("benchmark.runs", DEFAULT_RUNS);

    Config config =
        new Config(rowCount, textColumns, numericColumns, dateColumns, textLength, bufferRows);

    System.out.println();
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
    System.out.println("Total columns: " + config.columnCount);
    System.out.println("Total payload bytes: " + config.totalPayloadBytes);

    for (int i = 0; i < warmupRuns; i++) {
      runBenchmark(config);
    }

    BenchmarkStats stats = measure(config, measuredRuns);
    printStats("Single decode per cell", stats, config);
  }

  private static long calculatePayloadBytes(
      int rowCount, int textColumns, int numericColumns, int dateColumns, int textLength) {
    long total = 0;
    for (int row = 0; row < rowCount; row++) {
      for (int i = 0; i < textColumns; i++) {
        total += textLength + ((row + i) % 17);
      }
      for (int i = 0; i < numericColumns; i++) {
        total += Integer.toString((row * 31 + i * 17) % 1_000_000).length();
      }
      total += (long) dateColumns * 10;
    }
    return total;
  }

  private static BenchmarkStats measure(Config config, int runs) {
    long bestNs = Long.MAX_VALUE;
    long totalNs = 0;
    long checksum = 0;

    for (int run = 0; run < runs; run++) {
      long start = System.nanoTime();
      checksum ^= runBenchmark(config);
      long elapsed = System.nanoTime() - start;
      bestNs = Math.min(bestNs, elapsed);
      totalNs += elapsed;
    }

    return new BenchmarkStats(bestNs, totalNs / runs, checksum);
  }

  private static long runBenchmark(Config config) {
    long checksum = 0;
    StringBuilder raw = new StringBuilder(config.bufferRows * config.columnCount * 16);
    int bufferedRows = 0;

    byte[] textBuf = new byte[config.textLength + 16];
    byte[] dateBuf = new byte[10];

    for (int row = 0; row < config.rowCount; row++) {
      int column = 0;

      for (int i = 0; i < config.textColumns; i++, column++) {
        int len = config.textLength + ((row + i) % 17);
        fillAsciiValue(textBuf, row, i, len);
        String decoded = new String(textBuf, 0, len, StandardCharsets.UTF_8);
        raw.append(decoded);
        checksum += decoded.length();
        checksum += decoded.charAt(0);
        if (column < config.columnCount - 1) {
          raw.append(',');
          checksum += ',';
        }
      }

      for (int i = 0; i < config.numericColumns; i++, column++) {
        byte[] value = makeNumericValue(row, i);
        String decoded = new String(value, StandardCharsets.UTF_8);
        raw.append(decoded);
        checksum += decoded.length();
        checksum += decoded.charAt(0);
        if (column < config.columnCount - 1) {
          raw.append(',');
          checksum += ',';
        }
      }

      for (int i = 0; i < config.dateColumns; i++, column++) {
        fillDateValue(dateBuf, row, i);
        String decoded = new String(dateBuf, 0, 10, StandardCharsets.UTF_8);
        raw.append(decoded);
        checksum += decoded.length();
        checksum += decoded.charAt(0);
        if (column < config.columnCount - 1) {
          raw.append(',');
          checksum += ',';
        }
      }

      raw.append('\n');
      checksum += '\n';
      bufferedRows++;

      if (bufferedRows >= config.bufferRows) {
        checksum += raw.length();
        raw.setLength(0);
        bufferedRows = 0;
      }
    }

    if (raw.length() > 0) {
      checksum += raw.length();
    }
    return checksum;
  }

  private static void fillAsciiValue(byte[] buf, int row, int column, int length) {
    for (int i = 0; i < length; i++) {
      buf[i] = (byte) ('A' + ((row + column + i) % 26));
    }
  }

  private static byte[] makeNumericValue(int row, int column) {
    String value = Integer.toString((row * 31 + column * 17) % 1_000_000);
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static void fillDateValue(byte[] buf, int row, int column) {
    int year = 2020 + ((row + column) % 5);
    int month = 1 + ((row + column) % 12);
    int day = 1 + ((row + column) % 28);
    buf[0] = (byte) ('0' + year / 1000);
    buf[1] = (byte) ('0' + (year / 100) % 10);
    buf[2] = (byte) ('0' + (year / 10) % 10);
    buf[3] = (byte) ('0' + year % 10);
    buf[4] = '-';
    buf[5] = (byte) ('0' + month / 10);
    buf[6] = (byte) ('0' + month % 10);
    buf[7] = '-';
    buf[8] = (byte) ('0' + day / 10);
    buf[9] = (byte) ('0' + day % 10);
  }

  private static void printStats(String label, BenchmarkStats stats, Config config) {
    System.out.println();
    System.out.println(label);
    System.out.println("-------------------------");
    printLine("Average", stats.averageNs, config.rowCount, config.columnCount, stats.checksum);
    printLine("Best", stats.bestNs, config.rowCount, config.columnCount, stats.checksum);
  }

  private static void printLine(
      String label, long elapsedNs, int rows, int columns, long checksum) {
    double totalMs = elapsedNs / 1_000_000.0;
    double nsPerRow = elapsedNs / (double) rows;
    double nsPerCell = elapsedNs / (double) ((long) rows * columns);
    System.out.printf(
        "%-8s total=%.3f ms  ns/row=%.2f  ns/cell=%.2f  checksum=%d%n",
        label, totalMs, nsPerRow, nsPerCell, checksum);
  }

  private static final class Config {
    final int rowCount;
    final int textColumns;
    final int numericColumns;
    final int dateColumns;
    final int textLength;
    final int columnCount;
    final int bufferRows;
    final long totalPayloadBytes;

    Config(
        int rowCount,
        int textColumns,
        int numericColumns,
        int dateColumns,
        int textLength,
        int bufferRows) {
      this.rowCount = rowCount;
      this.textColumns = textColumns;
      this.numericColumns = numericColumns;
      this.dateColumns = dateColumns;
      this.textLength = textLength;
      this.columnCount = textColumns + numericColumns + dateColumns;
      this.bufferRows = bufferRows;
      this.totalPayloadBytes =
          calculatePayloadBytes(rowCount, textColumns, numericColumns, dateColumns, textLength);
    }
  }

  private static final class BenchmarkStats {
    final long bestNs;
    final long averageNs;
    final long checksum;

    BenchmarkStats(long bestNs, long averageNs, long checksum) {
      this.bestNs = bestNs;
      this.averageNs = averageNs;
      this.checksum = checksum;
    }
  }
}
