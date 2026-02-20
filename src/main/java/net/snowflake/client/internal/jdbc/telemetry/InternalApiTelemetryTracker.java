package net.snowflake.client.internal.jdbc.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.client.internal.core.ObjectMapperFactory;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

/** Tracks calls to public methods in internal packages and reports them as in-band telemetry. */
public class InternalApiTelemetryTracker {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(InternalApiTelemetryTracker.class);
  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

  static final String DRIVER_PACKAGE_PREFIX = "net.snowflake.client.";

  private static final ConcurrentHashMap<String, AtomicLong> methodCallCounts =
      new ConcurrentHashMap<>();

  /** Marker used by internal call paths to skip external-usage telemetry. */
  public static final class InternalCallMarker {
    private InternalCallMarker() {}
  }

  private static final InternalCallMarker INTERNAL_CALL_MARKER = new InternalCallMarker();

  private InternalApiTelemetryTracker() {}

  public static InternalCallMarker internalCallMarker() {
    return INTERNAL_CALL_MARKER;
  }

  public static void recordIfExternal(
      String className, String methodName, InternalCallMarker internalCallMarker) {
    if (internalCallMarker == null) {
      recordIfCalledExternally(className, methodName);
    }
  }

  /**
   * Records a call only if the caller of the method is outside the driver. Expected stack: [0] this
   * method, [1] the instrumented internal method, [2] the actual caller to inspect.
   */
  public static void recordIfCalledExternally(String className, String methodName) {
    try {
      StackTraceElement[] stack = new Throwable().getStackTrace();
      if (stack.length > 2) {
        recordIfCalledExternally(className, methodName, stack[2].getClassName());
      }
    } catch (Exception e) {
      logger.debug("Failed to record internal API usage: {}", e.getMessage());
    }
  }

  static void recordIfCalledExternally(
      String className, String methodName, String callerClassName) {
    if (isExternalClass(callerClassName)) {
      record(className, methodName);
    }
  }

  static void record(String className, String methodName) {
    methodCallCounts
        .computeIfAbsent(className + "#" + methodName, k -> new AtomicLong(0))
        .incrementAndGet();
  }

  static boolean isExternalClass(String className) {
    return !className.startsWith(DRIVER_PACKAGE_PREFIX);
  }

  public static void flush(Telemetry client) {
    if (client == null || methodCallCounts.isEmpty()) {
      return;
    }

    try {
      ObjectNode message = mapper.createObjectNode();
      message.put(TelemetryField.TYPE.toString(), TelemetryField.INTERNAL_API_USAGE.toString());
      message.put("source", "JDBC");

      ObjectNode methods = mapper.createObjectNode();
      methodCallCounts.forEach(
          (key, count) -> {
            long value = count.getAndSet(0);
            if (value > 0) {
              methods.put(key, value);
            }
          });
      methodCallCounts.entrySet().removeIf(e -> e.getValue().get() == 0);

      if (methods.isEmpty()) {
        return;
      }

      message.set("methods", methods);
      client.addLogToBatch(new TelemetryData(message, System.currentTimeMillis()));
      logger.debug("Flushed internal API usage telemetry with {} distinct methods", methods.size());
    } catch (Exception e) {
      logger.debug("Failed to flush internal API telemetry: {}", e.getMessage());
    }
  }

  static void resetForTesting() {
    methodCallCounts.clear();
  }
}
