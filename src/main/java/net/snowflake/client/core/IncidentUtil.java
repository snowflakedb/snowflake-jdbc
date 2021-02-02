/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.MetricsServlet;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/** @author jrosen + mkeller */
public class IncidentUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(IncidentUtil.class);

  // Json message variables
  public static final String TIMESTAMP = "timestamp";
  public static final String INCIDENT_INFO = "incidentInfo";

  // Determines name of dump file.
  public static final String INC_DUMP_FILE_NAME = "sf_incident_";
  public static final String INC_DUMP_FILE_EXT = ".dmp.gz";

  /**
   * Produce a one line description of the throwable, suitable for error message and log printing.
   *
   * @param thrown thrown object
   * @return description of the thrown object
   */
  public static String oneLiner(Throwable thrown) {
    StackTraceElement[] stack = thrown.getStackTrace();
    String topOfStack = null;
    if (stack.length > 0) {
      topOfStack = " at " + stack[0];
    }
    return thrown.toString() + topOfStack;
  }

  /**
   * Produce a one line description of the throwable, suitable for error message and log printing
   * with a prefix
   *
   * @param prefix String to prefix oneliner summary
   * @param thrown thrown object
   * @return description of the thrown object
   */
  public static String oneLiner(String prefix, Throwable thrown) {
    return prefix + " " + oneLiner(thrown);
  }

  /**
   * Dumps JVM metrics for this process.
   *
   * @param incidentId incident id
   */
  public static void dumpVmMetrics(String incidentId) {
    PrintWriter writer = null;
    try {
      String dumpFile =
          EventUtil.getDumpPathPrefix() + "/" + INC_DUMP_FILE_NAME + incidentId + INC_DUMP_FILE_EXT;

      final OutputStream outStream = new GZIPOutputStream(new FileOutputStream(dumpFile));
      writer = new PrintWriter(outStream, true);

      final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();
      writer.print(
          "\n\n\n---------------------------  METRICS " + "---------------------------\n\n");
      writer.flush();
      JsonFactory jf = new JsonFactory();
      jf.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
      ObjectMapper mapper = new ObjectMapper(jf);

      mapper.setDateFormat(new StdDateFormat());
      mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
      MetricsServlet metrics =
          new MetricsServlet(Clock.defaultClock(), vm, Metrics.defaultRegistry(), jf, true);

      final JsonGenerator json = jf.createGenerator(outStream, JsonEncoding.UTF8);
      json.useDefaultPrettyPrinter();
      json.writeStartObject();

      // JVM metrics
      writeVmMetrics(json, vm);

      // Components metrics
      metrics.writeRegularMetrics(
          json, // json generator
          null, // class prefix
          false); // include full samples

      json.writeEndObject();
      json.close();

      logger.debug("Creating full thread dump in dump file {}", dumpFile);

      // Thread dump next....
      writer.print(
          "\n\n\n---------------------------  THREAD DUMP " + "---------------------------\n\n");
      writer.flush();

      vm.threadDump(outStream);

      logger.debug("Dump file {} is created.", dumpFile);
    } catch (Exception exc) {
      logger.error("Unable to write dump file, exception: {}", exc.getMessage());
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  private static void writeVmMetrics(JsonGenerator json, VirtualMachineMetrics vm)
      throws IOException {
    json.writeFieldName("jvm");
    json.writeStartObject();
    {
      json.writeFieldName("vm");
      json.writeStartObject();
      {
        json.writeStringField("name", vm.name());
        json.writeStringField("version", vm.version());
      }
      json.writeEndObject();

      json.writeFieldName("memory");
      json.writeStartObject();
      {
        json.writeNumberField("totalInit", vm.totalInit());
        json.writeNumberField("totalUsed", vm.totalUsed());
        json.writeNumberField("totalMax", vm.totalMax());
        json.writeNumberField("totalCommitted", vm.totalCommitted());

        json.writeNumberField("heapInit", vm.heapInit());
        json.writeNumberField("heapUsed", vm.heapUsed());
        json.writeNumberField("heapMax", vm.heapMax());
        json.writeNumberField("heapCommitted", vm.heapCommitted());

        json.writeNumberField("heap_usage", vm.heapUsage());
        json.writeNumberField("non_heap_usage", vm.nonHeapUsage());
        json.writeFieldName("memory_pool_usages");
        json.writeStartObject();
        {
          for (Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
            json.writeNumberField(pool.getKey(), pool.getValue());
          }
        }
        json.writeEndObject();
      }
      json.writeEndObject();

      final Map<String, VirtualMachineMetrics.BufferPoolStats> bufferPoolStats =
          vm.getBufferPoolStats();
      if (!bufferPoolStats.isEmpty()) {
        json.writeFieldName("buffers");
        json.writeStartObject();
        {
          json.writeFieldName("direct");
          json.writeStartObject();
          {
            json.writeNumberField("count", bufferPoolStats.get("direct").getCount());
            json.writeNumberField("memoryUsed", bufferPoolStats.get("direct").getMemoryUsed());
            json.writeNumberField(
                "totalCapacity", bufferPoolStats.get("direct").getTotalCapacity());
          }
          json.writeEndObject();

          json.writeFieldName("mapped");
          json.writeStartObject();
          {
            json.writeNumberField("count", bufferPoolStats.get("mapped").getCount());
            json.writeNumberField("memoryUsed", bufferPoolStats.get("mapped").getMemoryUsed());
            json.writeNumberField(
                "totalCapacity", bufferPoolStats.get("mapped").getTotalCapacity());
          }
          json.writeEndObject();
        }
        json.writeEndObject();
      }

      json.writeNumberField("daemon_thread_count", vm.daemonThreadCount());
      json.writeNumberField("thread_count", vm.threadCount());
      json.writeNumberField("current_time", Clock.defaultClock().time());
      json.writeNumberField("uptime", vm.uptime());
      json.writeNumberField("fd_usage", vm.fileDescriptorUsage());

      json.writeFieldName("thread-states");
      json.writeStartObject();
      {
        for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages().entrySet()) {
          json.writeNumberField(entry.getKey().toString().toLowerCase(), entry.getValue());
        }
      }
      json.writeEndObject();

      json.writeFieldName("garbage-collectors");
      json.writeStartObject();
      {
        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry :
            vm.garbageCollectors().entrySet()) {
          json.writeFieldName(entry.getKey());
          json.writeStartObject();
          {
            final VirtualMachineMetrics.GarbageCollectorStats gc = entry.getValue();
            json.writeNumberField("runs", gc.getRuns());
            json.writeNumberField("time", gc.getTime(TimeUnit.MILLISECONDS));
          }
          json.writeEndObject();
        }
      }
      json.writeEndObject();
    }
    json.writeEndObject();
  }

  /**
   * Makes a V2 incident object and triggers ir, effectively reporting the given exception to GS and
   * possibly to crashmanager
   *
   * @param session SFSession object to talk to GS through
   * @param exc the Throwable we should report
   * @param jobId jobId that failed
   * @param requestId requestId that failed
   * @return the given Throwable object
   */
  public static Throwable generateIncidentV2WithException(
      SFBaseSession session, Throwable exc, String jobId, String requestId) {
    // Generate an incident only if the session exists.
    if (session != null) {
      session.raiseError(exc, jobId, requestId);
    }
    return exc;
  }

  /**
   * Makes a V2 incident object and triggers it, effectively reporting the given exception to GS and
   * possibly to crashmanager.
   *
   * <p>This function should be proceeded by a throw as it returns the originally given exception.
   *
   * @param serverUrl url of GS to report incident to
   * @param sessionToken session token to be used to report incident
   * @param exc the Throwable we should report
   * @param jobId jobId that failed
   * @param requestId requestId that failed
   * @return the given Exception object
   */
  public static Throwable generateIncidentV2WithException(
      String serverUrl, String sessionToken, Throwable exc, String jobId, String requestId) {
    new Incident(serverUrl, sessionToken, exc, jobId, requestId).trigger();
    return exc;
  }
}
