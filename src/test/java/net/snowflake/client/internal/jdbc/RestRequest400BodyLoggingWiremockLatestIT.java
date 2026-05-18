package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.jdbc.telemetry.ExecTimeTelemetryData;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Verifies that response bodies for non-retryable HTTP 400 responses are logged through {@link
 * SnowflakeUtil#logResponseDetails(org.apache.http.HttpResponse,
 * net.snowflake.client.internal.log.SFLogger)} instead of being silently consumed by {@code
 * RestRequest#checkForDPoPNonceError}.
 *
 * <p>See SNOW-3388174 / GitHub issue #2584.
 */
@Tag(TestTags.OTHERS)
public class RestRequest400BodyLoggingWiremockLatestIT extends BaseWiremockTest {

  private static final String LOGGER_NAME = "net.snowflake.client.internal.jdbc.RestRequest";

  /**
   * Distinctive marker string baked into the stubbed 400 body. The test asserts this exact string
   * shows up in the captured log records, proving the body was actually read and logged.
   */
  private static final String BODY_MARKER = "snow-3388174-marker";

  private static final String BAD_REQUEST_SCENARIO =
      "{\n"
          + "    \"mappings\": [\n"
          + "        {\n"
          + "            \"request\": {\n"
          + "                \"method\": \"GET\",\n"
          + "                \"url\": \"/some-endpoint-returning-400\"\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"status\": 400,\n"
          + "                \"headers\": { \"Content-Type\": \"application/json\" },\n"
          + "                \"body\": \"{\\\"error\\\":\\\"invalid_resource\\\",\\\"error_description\\\":\\\""
          + BODY_MARKER
          + "\\\"}\"\n"
          + "            }\n"
          + "        }\n"
          + "    ],\n"
          + "    \"importOptions\": {\n"
          + "        \"duplicatePolicy\": \"IGNORE\",\n"
          + "        \"deleteAllNotInImport\": true\n"
          + "    }\n"
          + "}";

  private CapturingLogHandler handler;
  private Logger internalLogger;
  private Level previousLevel;
  private boolean previousUseParentHandlers;

  @BeforeEach
  public void attachLogCapture() {
    internalLogger = Logger.getLogger(LOGGER_NAME);
    previousLevel = internalLogger.getLevel();
    previousUseParentHandlers = internalLogger.getUseParentHandlers();

    internalLogger.setLevel(Level.ALL);
    internalLogger.setUseParentHandlers(false);

    handler = new CapturingLogHandler();
    handler.setLevel(Level.ALL);
    internalLogger.addHandler(handler);
  }

  @AfterEach
  public void detachLogCapture() {
    if (internalLogger != null && handler != null) {
      internalLogger.removeHandler(handler);
      internalLogger.setLevel(previousLevel);
      internalLogger.setUseParentHandlers(previousUseParentHandlers);
    }
  }

  @Test
  public void logs400ResponseBodyInsteadOfClosedStreamError() throws Exception {
    importMapping(BAD_REQUEST_SCENARIO);

    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create().disableAutomaticRetries();
    try (CloseableHttpClient httpClient = httpClientBuilder.build()) {
      HttpGet request =
          new HttpGet(
              String.format(
                  "http://%s:%d/some-endpoint-returning-400", WIREMOCK_HOST, wiremockHttpPort));

      // We don't care about the return value - we only care about what got logged while
      // RestRequest processed the 400 response.
      try {
        RestRequest.executeWithRetries(
            httpClient,
            request,
            0,
            0,
            0,
            0,
            0,
            new AtomicBoolean(false),
            false,
            false,
            false,
            false,
            false,
            new ExecTimeTelemetryData(),
            null,
            null,
            null,
            false);
      } catch (Exception ignored) {
        // executeWithRetries may surface the 400 as a SnowflakeSQLException; that's fine.
      }
    }

    List<String> messages = handler.getFormattedMessages();

    assertTrue(
        messages.stream().anyMatch(m -> m.contains(BODY_MARKER)),
        "Expected the 400 response body to be logged (looking for marker '"
            + BODY_MARKER
            + "'); captured messages:\n"
            + String.join("\n----\n", messages));

    assertFalse(
        messages.stream().anyMatch(m -> m.contains("Attempted read from closed stream")),
        "Response body must not have been consumed before logResponseDetails(..) could "
            + "read it. Captured messages:\n"
            + String.join("\n----\n", messages));
  }

  /** Minimal {@link Handler} that records every {@link LogRecord} it receives. */
  private static class CapturingLogHandler extends Handler {
    private final List<LogRecord> records = new ArrayList<>();

    @Override
    public synchronized void publish(LogRecord record) {
      records.add(record);
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    synchronized List<String> getFormattedMessages() {
      List<String> out = new ArrayList<>(records.size());
      for (LogRecord r : records) {
        out.add(formatMessage(r));
      }
      return out;
    }

    /**
     * Best-effort substitution of SLF4J-style {@code {}} placeholders with the record's parameters,
     * so the captured message string contains the resolved body content even though the underlying
     * SFLogger formats lazily.
     */
    private static String formatMessage(LogRecord r) {
      String msg = r.getMessage() == null ? "" : r.getMessage();
      Object[] params = r.getParameters();
      if (params != null) {
        for (Object p : params) {
          int idx = msg.indexOf("{}");
          if (idx < 0) {
            break;
          }
          msg = msg.substring(0, idx) + String.valueOf(p) + msg.substring(idx + 2);
        }
      }
      return msg;
    }
  }
}
