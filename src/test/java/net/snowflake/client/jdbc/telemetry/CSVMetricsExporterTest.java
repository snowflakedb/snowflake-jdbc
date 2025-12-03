package net.snowflake.client.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import net.snowflake.client.core.ExecTimeTelemetryData;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

class CSVMetricsExporterTest {
  @Test
  void shouldSaveMetrics() throws Exception {
    Path tempDirectory = Files.createTempDirectory("csv-metrics-exporter-test");
    Path csvPath = tempDirectory.resolve("metrics.csv");
    CSVMetricsExporter exporter = new CSVMetricsExporter(csvPath.toString(), 2);
    saveTelemetryData(exporter, "SELECT 1");
    saveTelemetryData(exporter, "SELECT 2");
    String content = IOUtils.toString(Files.newInputStream(csvPath));
    String[] lines = content.split(System.lineSeparator());
    assertEquals(3, lines.length);
    assertEquals(
        "sessionId,requestId,queryId,queryText,executeToSendTime,bindTime,gzipTime,httpClientTime,responseIOStreamTime,processResultChunkTime,createResultSetTime,queryTime",
        lines[0]);
    assertTrue(lines[1].contains("SELECT 1"));
    assertTrue(lines[2].contains("SELECT 2"));
    // number of fields in a header and a line should be equal
    assertEquals(lines[0].split(",").length, lines[1].split(",").length);
    assertTrue(Pattern.matches("^sessId,reqId,queryId,SELECT 1,([-\\d]+,){7}[-\\d]+$", lines[1]));
  }

  @Test
  void shouldNotSaveAlreadyFlushedMetrics() throws Exception {
    Path tempDirectory = Files.createTempDirectory("csv-metrics-exporter-test");
    Path csvPath = tempDirectory.resolve("metrics.csv");
    CSVMetricsExporter exporter = new CSVMetricsExporter(csvPath.toString(), 2);
    saveTelemetryData(exporter, "SELECT 1");
    saveTelemetryData(exporter, "SELECT 2");
    saveTelemetryData(exporter, "SELECT 3");
    saveTelemetryData(exporter, "SELECT 4");
    saveTelemetryData(exporter, "SELECT 5");
    String content = IOUtils.toString(Files.newInputStream(csvPath));
    String[] lines = content.split(System.lineSeparator());
    assertEquals(5, lines.length);
    assertTrue(lines[1].contains("SELECT 1"));
    assertTrue(lines[2].contains("SELECT 2"));
    assertTrue(lines[3].contains("SELECT 3"));
    assertTrue(lines[4].contains("SELECT 4"));
  }

  private void saveTelemetryData(CSVMetricsExporter exporter, String queryText) {
    ExecTimeTelemetryData data = new ExecTimeTelemetryData();
    data.setQueryText(queryText);
    data.setSessionId("sessId");
    data.setRequestId("reqId");
    data.setQueryId("queryId");
    exporter.save(data);
  }
}
