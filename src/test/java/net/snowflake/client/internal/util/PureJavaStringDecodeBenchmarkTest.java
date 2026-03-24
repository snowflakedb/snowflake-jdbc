package net.snowflake.client.internal.util;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Pure Java benchmark for comparing byte copy and UTF-8 string decoding performance across JVMs
 * and operating systems. This test intentionally has no assertions and is meant to print stable,
 * shareable numbers to CI logs.
 */
public class PureJavaStringDecodeBenchmarkTest {
  private enum PayloadKind {
    ASCII,
    MIXED_UTF8
  }

  private enum BenchmarkMode {
    COPY_ONLY,
    DECODE_ONLY,
    COPY_AND_DECODE
  }

  private static final int DEFAULT_ROWS = 100_000;
  private static final int DEFAULT_LENGTH = 256;
  private static final int DEFAULT_WARMUP = 3;
  private static final int DEFAULT_RUNS = 5;

  @Test
  public void benchmarkPureJavaStringDecoding() {
    int rowCount = Integer.getInteger("benchmark.rows", DEFAULT_ROWS);
    int stringLength = Integer.getInteger("benchmark.length", DEFAULT_LENGTH);
    int warmupRuns = Integer.getInteger("benchmark.warmup", DEFAULT_WARMUP);
    int measuredRuns = Integer.getInteger("benchmark.runs", DEFAULT_RUNS);

    System.out.println("Pure Java String Decode Benchmark");
    System.out.println("================================");
    System.out.println("Rows: " + rowCount);
    System.out.println("Target character length: " + stringLength);
    System.out.println("Warmup runs: " + warmupRuns);
    System.out.println("Measured runs: " + measuredRuns);
    System.out.println("Java version: " + System.getProperty("java.version"));
    System.out.println("VM name: " + System.getProperty("java.vm.name"));
    System.out.println("OS name: " + System.getProperty("os.name"));
    System.out.println("OS arch: " + System.getProperty("os.arch"));

    Dataset asciiDataset = createDataset(rowCount, stringLength, PayloadKind.ASCII);
    Dataset mixedUtf8Dataset = createDataset(rowCount, stringLength, PayloadKind.MIXED_UTF8);

    runSuite(asciiDataset, warmupRuns, measuredRuns);
    runSuite(mixedUtf8Dataset, warmupRuns, measuredRuns);
  }

  private static void runSuite(Dataset dataset, int warmupRuns, int measuredRuns) {
    System.out.println();
    System.out.println("Payload: " + dataset.payloadKind);
    System.out.println("Bytes per row (avg): " + String.format("%.2f", dataset.averageBytesPerRow));
    System.out.println("Total payload bytes: " + dataset.totalBytes);

    for (int i = 0; i < warmupRuns; i++) {
      runMode(dataset, BenchmarkMode.COPY_ONLY);
      runMode(dataset, BenchmarkMode.DECODE_ONLY);
      runMode(dataset, BenchmarkMode.COPY_AND_DECODE);
    }

    printStats(
        "Copy-only", measure(dataset, measuredRuns, BenchmarkMode.COPY_ONLY), dataset.rowCount);
    printStats(
        "Decode-only", measure(dataset, measuredRuns, BenchmarkMode.DECODE_ONLY), dataset.rowCount);
    printStats(
        "Copy+decode",
        measure(dataset, measuredRuns, BenchmarkMode.COPY_AND_DECODE),
        dataset.rowCount);
  }

  private static Dataset createDataset(int rowCount, int stringLength, PayloadKind payloadKind) {
    byte[][] rows = new byte[rowCount][];
    int totalBytes = 0;

    for (int row = 0; row < rowCount; row++) {
      byte[] value =
          payloadKind == PayloadKind.ASCII
              ? makeAsciiValue(row, stringLength)
              : makeMixedUtf8Value(row, stringLength);
      rows[row] = value;
      totalBytes += value.length;
    }

    return new Dataset(rows, rowCount, totalBytes, totalBytes / (double) rowCount, payloadKind);
  }

  private static byte[] makeAsciiValue(int row, int length) {
    byte[] value = new byte[length];
    for (int i = 0; i < length; i++) {
      value[i] = (byte) ('A' + ((row + i) % 26));
    }
    return value;
  }

  private static byte[] makeMixedUtf8Value(int row, int length) {
    String[] alphabet = {"A", "B", "C", "1", "2", "3", "é", "ü", "你", "好", "中", "文", "€", "Ω"};
    StringBuilder sb = new StringBuilder(length * 2);
    while (sb.length() < length) {
      sb.append(alphabet[(row + sb.length()) % alphabet.length]);
    }
    return sb.substring(0, length).getBytes(StandardCharsets.UTF_8);
  }

  private static BenchmarkStats measure(Dataset dataset, int runs, BenchmarkMode mode) {
    long bestNs = Long.MAX_VALUE;
    long totalNs = 0;
    long checksum = 0;

    for (int run = 0; run < runs; run++) {
      long start = System.nanoTime();
      checksum ^= runMode(dataset, mode);
      long elapsed = System.nanoTime() - start;
      bestNs = Math.min(bestNs, elapsed);
      totalNs += elapsed;
    }

    return new BenchmarkStats(bestNs, totalNs / runs, checksum);
  }

  private static long runMode(Dataset dataset, BenchmarkMode mode) {
    switch (mode) {
      case COPY_ONLY:
        return runCopyOnly(dataset);
      case DECODE_ONLY:
        return runDecodeOnly(dataset);
      case COPY_AND_DECODE:
        return runCopyAndDecode(dataset);
      default:
        throw new IllegalArgumentException("Unsupported mode: " + mode);
    }
  }

  private static long runCopyOnly(Dataset dataset) {
    long checksum = 0;

    for (byte[] row : dataset.rows) {
      byte[] copy = row.clone();
      checksum += copy.length;
      checksum += copy[0];
    }

    return checksum;
  }

  private static long runDecodeOnly(Dataset dataset) {
    long checksum = 0;

    for (byte[] row : dataset.rows) {
      String value = new String(row, StandardCharsets.UTF_8);
      checksum += value.length();
      checksum += value.charAt(0);
    }

    return checksum;
  }

  private static long runCopyAndDecode(Dataset dataset) {
    long checksum = 0;

    for (byte[] row : dataset.rows) {
      byte[] copy = row.clone();
      String value = new String(copy, StandardCharsets.UTF_8);
      checksum += value.length();
      checksum += value.charAt(0);
    }

    return checksum;
  }

  private static void printStats(String label, BenchmarkStats stats, int operations) {
    System.out.println();
    System.out.println(label + " benchmark");
    System.out.println("--------------------");
    printLine("Average", stats.averageNs, operations, stats.checksum);
    printLine("Best", stats.bestNs, operations, stats.checksum);
  }

  private static void printLine(String label, long elapsedNs, int operations, long checksum) {
    double totalMs = elapsedNs / 1_000_000.0;
    double nsPerOperation = elapsedNs / (double) operations;
    System.out.printf(
        "%-8s total=%.3f ms  ns/op=%.2f  checksum=%d%n",
        label, totalMs, nsPerOperation, checksum);
  }

  private static final class Dataset {
    private final byte[][] rows;
    private final int rowCount;
    private final int totalBytes;
    private final double averageBytesPerRow;
    private final PayloadKind payloadKind;

    private Dataset(
        byte[][] rows, int rowCount, int totalBytes, double averageBytesPerRow, PayloadKind payloadKind) {
      this.rows = rows;
      this.rowCount = rowCount;
      this.totalBytes = totalBytes;
      this.averageBytesPerRow = averageBytesPerRow;
      this.payloadKind = payloadKind;
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
