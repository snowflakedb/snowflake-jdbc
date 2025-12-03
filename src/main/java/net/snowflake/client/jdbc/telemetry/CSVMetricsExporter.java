package net.snowflake.client.jdbc.telemetry;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

// This class is internal - features implemented in this class should be treated as internal
// features
// and may be changed in the future. You shouldn't depend on these metrics.
@SnowflakeJdbcInternalApi
public class CSVMetricsExporter {
  private static final SFLogger logger = SFLoggerFactory.getLogger(CSVMetricsExporter.class);

  static final String CSV_EXPORTER_FILE_PROPERTY = "metrics.csv.exporter.file";
  static final String CSV_EXPORTER_FLUSH_SIZE_PROPERTY = "metrics.csv.exporter.flush.size";

  private final List<ExecTimeTelemetryData> entries = new ArrayList<>();
  private final String filePath;
  private final Integer flushSize;

  CSVMetricsExporter(String filePath, int flushSize) {
    this.filePath = filePath;
    this.flushSize = flushSize;
    Runtime.getRuntime().addShutdownHook(new Thread(this::flush));
  }

  private static CSVMetricsExporter instance;

  public static synchronized CSVMetricsExporter getDefaultInstance() {
    if (instance == null) {
      String filePath = System.getProperty(CSV_EXPORTER_FILE_PROPERTY);
      int limit =
          Integer.parseInt(
              Optional.ofNullable(System.getProperty(CSV_EXPORTER_FLUSH_SIZE_PROPERTY))
                  .orElse("1"));
      instance = new CSVMetricsExporter(filePath, limit);
    }
    return instance;
  }

  public synchronized void save(ExecTimeTelemetryData data) {
    if (isNullOrEmpty(filePath)) {
      return;
    }
    entries.add(data);
    if (entries.size() >= flushSize) {
      flush();
    }
  }

  private synchronized void flush() {
    if (isNullOrEmpty(filePath)) {
      return;
    }

    if (entries.isEmpty()) {
      return;
    }

    Path path = Paths.get(filePath);

    try {
      Files.createDirectories(path.getParent());

      boolean fileExists = Files.exists(path);

      try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
        if (!fileExists) {
          writer.write(
              "sessionId,requestId,queryId,queryText,executeToSendTime,bindTime,gzipTime,httpClientTime,responseIOStreamTime,processResultChunkTime,createResultSetTime,queryTime");
          writer.newLine();
        }

        for (ExecTimeTelemetryData data : entries) {
          writer.write(formatCsvRow(data));
          writer.newLine();
        }

        writer.flush();
      }

    } catch (IOException e) {
      logger.warn("Failed to write metrics to CSV file: {}", filePath, e);
    } finally {
      // it's better to drop some metrics than have OOM
      entries.clear();
    }
  }

  private String formatCsvRow(ExecTimeTelemetryData data) {
    return String.join(
        ",",
        escapeCsvValue(data.getSessionId()),
        escapeCsvValue(data.getRequestId()),
        escapeCsvValue(data.getQueryId()),
        escapeCsvValue(data.getQueryText()),
        String.valueOf(data.getExecuteToSend().getTime()),
        String.valueOf(data.getBind().getTime()),
        String.valueOf(data.getGzip().getTime()),
        String.valueOf(data.getHttpClient().getTime()),
        String.valueOf(data.getResponseIOStream().getTime()),
        String.valueOf(data.getProcessResultChunk().getTime()),
        String.valueOf(data.getCreateResultSet().getTime()),
        String.valueOf(data.getQuery().getTime()));
  }

  private String escapeCsvValue(String value) {
    if (value == null) {
      return "";
    }
    // If value contains comma, quote, or newline, wrap in quotes and escape internal quotes
    if (value.contains(",")
        || value.contains("\"")
        || value.contains("\n")
        || value.contains("\r")) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
    return value;
  }
}
