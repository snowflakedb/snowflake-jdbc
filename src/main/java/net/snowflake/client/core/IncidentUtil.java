/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Preconditions;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeDriver;
import net.snowflake.common.core.LoginInfoDTO;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.MetricsServlet;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import org.joda.time.DateTime;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import static net.snowflake.client.core.Incident.generateIncidentId;

/**
 *
 * @author jrosen
 */
public class IncidentUtil
{
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(IncidentUtil.class);

  // Determines size of stacktrace to be sent to GS
  private static final int STACK_TRACE_SIZE = 10;

  // Json message variables
  public static final String CLIENT_TYPE = "clientType";
  public static final String CLIENT_VERSION = "clientVersion";
  public static final String CLIENT_BUILD_ID = "clientBuildId";
  public static final String INCIDENT_ID = "incidentId";
  public static final String STACKTRACE = "stackTrace";
  public static final String REQUEST_ID = "requestId";
  public static final String JOB_ID = "jobId";
  public static final String SIGNATURE = "signature";
  public static final String REPORTER = "reporter";
  public static final String DETAIL = "detail";
  public static final String INCIDENT_TYPE = "incidentType";
  public static final String TIMESTAMP = "timestamp";
  public static final String INCIDENT_REPORT = "incidentReport";
  public static final String CLIENT_INFO = "clientInfo";
  public static final String INCIDENT_INFO = "incidentInfo";
  public static final String THROTTLE_SIGNATURE = "throttleSignature";

  // Determines name of dump file.
  public static final String INC_DUMP_FILE_NAME = "sf_incident_";
  public static final String INC_DUMP_FILE_EXT = ".dmp.gz";

   /**
   * Creates an SFException without a cause to be thrown and generates/triggers
   * an incident based on that exception with the specified signature.
   * @param session session that will generate incident
   * @param requestId request id
   * @param jobUuid job uuid
   * @param errorCode error code that caused the incident
   * @param params option parameters
   * @return generated SFException
   */
  public static SFException generateIncidentWithSignatureAndException(
          SFSession session,
          String requestId,
          String jobUuid,
          String signature,
          ErrorCode errorCode,
          Object... params)
  {
    return generateIncidentWithSignatureAndException(
            session != null ?
                session.getSessionToken() : null,
            session != null ?
                session.getServerUrl() : null,
            requestId,
            jobUuid,
            signature,
            errorCode,
            params);
  }


  public static SFException generateIncidentWithSignatureAndException(
          String sessionToken,
          String serverUrl,
          String requestId,
          String jobUuid,
          String signature,
          ErrorCode errorCode,
          Object... params)
  {
    SFException sfe = new SFException(errorCode, params);

    // Figure out who the caller is: that's who should be the reporter.
    StackTraceElement[] stack = sfe.getStackTrace();
    String caller = null;
    for (int i = 1; i < stack.length; i++)
    {
      if (!stack[i].getMethodName().equals("generateIncidentWithException"))
      {
        caller = String.valueOf(stack[i]);
        break;
      }
    }
    String causeStr = sfe.toString() + " at " + caller;

    // Build message for GS Incident reporting
    if (sessionToken != null && serverUrl != null)
    {

      Map<String,Object> incidentInfo =
          IncidentUtil.buildIncidentReport(sessionToken,
                                           serverUrl,
                                           signature,
                                           null,
                                           requestId,
                                           jobUuid,
                                           causeStr);

      // Send Incident message to GS and trigger log dump
      EventUtil.triggerIncident(incidentInfo);
    }
    else
    {
      logger.warn("Failed to generate incident, sessionToken valid={} "
              + "serverUrl={}",
                  sessionToken != null, serverUrl);
    }

    return sfe;
  }

  /**
   * Creates an SFException without a cause to be thrown and generates/triggers
   * an incident based on that exception.
   * @param session session that will generate incident
   * @param requestId request id
   * @param jobUuid job uuid
   * @param errorCode error code that caused the incident
   * @param params option parameters
   * @return generated SFException
   */
  public static SFException generateIncidentWithException(SFSession session,
                                                          String requestId,
                                                          String jobUuid,
                                                          ErrorCode errorCode,
                                                          Object... params)
  {
    return generateIncidentWithException(session != null ?
                                            session.getSessionToken() : null,
                                         session != null ?
                                            session.getServerUrl() : null,
                                         requestId,
                                         jobUuid,
                                         errorCode,
                                         params);
  }

  public static SFException generateIncidentWithException(SFSession session,
                                                          String requestId,
                                                          String jobUuid,
                                                          Throwable cause,
                                                          ErrorCode errorCode,
                                                          Object... params)
  {
    return generateIncidentWithException(session != null ?
                                            session.getSessionToken() : null,
                                         session != null ?
                                            session.getServerUrl() : null,
                                         requestId,
                                         jobUuid,
                                         cause,
                                         errorCode,
                                         params);

  }

  /**
   * Creates an SFException to be thrown and generates/triggers an incident
   * based on that exception.
   * @param sessionToken session token
   * @param serverUrl snowflake url
   * @param requestId request id
   * @param jobUuid job uuid
   * @param cause cause of the incident
   * @param errorCode error code of the incident
   * @param params optional parameters
   * @return generated SFException
   */
  public static SFException generateIncidentWithException(String sessionToken,
                                                          String serverUrl,
                                                          String requestId,
                                                          String jobUuid,
                                                          Throwable cause,
                                                          ErrorCode errorCode,
                                                          Object... params)
  {
    String causeStr = null;

    // Should use generateIncidentWithException without cause argument if
    // there is no cause.
    if (cause != null)
    {
      // Use cause's caller as the reporter
      StackTraceElement[] stack = cause.getStackTrace();
      String topOfStack = null;
      if (stack.length > 0)
      {
        topOfStack = String.valueOf(stack[0]);
      }
      causeStr = cause.toString() + " at " + topOfStack;
    }
    else
    {
      logger.warn("Attempting to generate incident and"
              + " SFException with null cause");
    }

    SFException sfe = new SFException(cause, errorCode, params);

    // Build message for GS Incident reporting
    if (sessionToken != null && serverUrl != null)
    {
      Map<String,Object> incidentInfo =
          IncidentUtil.buildIncidentReport(sessionToken,
                                           serverUrl,
                                           causeStr,
                                           null,
                                           requestId,
                                           jobUuid,
                                           causeStr);

      // Send Incident message to GS and trigger log dump
      EventUtil.triggerIncident(incidentInfo);
    }
    else
    {
      logger.warn("Failed to generate incident, sessionToken valid={} "
              + "serverUrl={}",
                  sessionToken != null, serverUrl);
    }

    return sfe;
  }

  public static SFException generateIncidentWithException(String sessionToken,
                                                          String serverUrl,
                                                          String requestId,
                                                          String jobUuid,
                                                          ErrorCode errorCode,
                                                          Object... params)
  {
    SFException sfe = new SFException(errorCode, params);

    // Figure out who the caller is: that's who should be the reporter.
    StackTraceElement[] stack = sfe.getStackTrace();
    String caller = null;
    for (int i = 1; i < stack.length; i++)
    {
      if (!stack[i].getMethodName().equals("generateIncidentWithException"))
      {
        caller = String.valueOf(stack[i]);
        break;
      }
    }
    String causeStr = sfe.toString() + " at " + caller;

    // Build message for GS Incident reporting
    if (sessionToken != null && serverUrl != null)
    {

      Map<String,Object> incidentInfo =
          IncidentUtil.buildIncidentReport(sessionToken,
                                           serverUrl,
                                           causeStr,
                                           null,
                                           requestId,
                                           jobUuid,
                                           causeStr);

      // Send Incident message to GS and trigger log dump
      EventUtil.triggerIncident(incidentInfo);
    }
    else
    {
      logger.warn("Failed to generate incident, sessionToken valid={} "
              + "serverUrl={}",
                  sessionToken != null, serverUrl);
    }

    return sfe;
  }

  public static void generateIncident(SFSession session,
                                      String signature,
                                      String detail,
                                      String requestId,
                                      String jobUuid,
                                      Throwable cause)
  {
    if (session != null)
    {
      generateIncident(session.getSessionToken(),
                       session.getServerUrl(),
                       signature,
                       detail,
                       requestId,
                       jobUuid,
                       cause);
    }
  }

  /**
   * Generates an incident report based on the parameters passed in and
   * triggers an incident with the EventHandler
   * @param sessionToken session token
   * @param serverUrl server url
   * @param signature incident signature
   * @param detail incident detail
   * @param requestId request id
   * @param jobUuid job uuid
   * @param cause cause of incident
   */
  public static void generateIncident(String sessionToken,
                                      String serverUrl,
                                      String signature,
                                      String detail,
                                      String requestId,
                                      String jobUuid,
                                      Throwable cause)
  {
    String causeStr = null;

    if (cause != null)
    {
      // Whoever is generating the incident is the reporter.
      StackTraceElement[] stack = cause.getStackTrace();
      String topOfStack = null;
      if (stack.length > 0)
      {
        topOfStack = String.valueOf(stack[0]);
      }
      causeStr = cause.toString() + " at " + topOfStack;
    }
    else
    {
      logger.debug("Attempting to generate incident without cause, " +
          "signature={}, detail={}", signature, detail);
    }

    if (sessionToken != null && serverUrl != null)
    {
      // Build message for GS Incident reporting
      Map<String,Object> incidentInfo =
          IncidentUtil.buildIncidentReport(sessionToken,
                                           serverUrl,
                                           signature,
                                           detail,
                                           requestId,
                                           jobUuid,
                                           causeStr);

      // Send Incident message to GS and trigger log dump
      EventUtil.triggerIncident(incidentInfo);
    }
    else
    {
      logger.warn("Failed to generate incident, sessionToken valid={} "
              + "serverUrl={}",
                  sessionToken != null, serverUrl);
    }
  }

  /**
   * Builds an incident report to be serialized for consumption by Global Services.
   * @param signature
   * @param detail
   * @param requestId
   * @param jobUuid
   * @param cause
   * @return
   */
  private static Map<String,Object> buildIncidentReport(String sessionToken,
                                                        String serverUrl,
                                                        String signature,
                                                        String detail,
                                                        String requestId,
                                                        String jobUuid,
                                                        String cause)
  {
    // No session, no incident.
    Preconditions.checkArgument(sessionToken != null && serverUrl != null);

    Map<String,Object> incident = new HashMap<>();
    Map<String,Object> incidentInfo = new HashMap<>();
    Map<String,Object> incidentReport = new HashMap<>();

    // Record environment info
    incidentReport.put(CLIENT_TYPE, LoginInfoDTO.SF_JDBC_APP_ID);
    incidentReport.put(CLIENT_VERSION, SnowflakeDriver.implementVersion);

    // Incident data
    incidentReport.put(INCIDENT_ID, generateIncidentId());

    // The reporter is whoever called this method
    int topOfStack = 2;
    StackTraceElement[] stes = Thread.currentThread().getStackTrace();

    // Find the function that started this incident
    while (stes[topOfStack].getMethodName().startsWith("generateIncident"))
    {
      topOfStack++;
    }

    StackTraceElement caller = stes[topOfStack];

    String reporter = caller.getClassName() + ":" + caller.getMethodName() +
                      ":" + caller.getLineNumber();

    String detailMessage = (detail == null && cause == null) ? null :
                           ((detail == null ? "" : (detail + " : ")) +
                           (cause == null  ? "" : cause));

    // First N lines of the stacktrace
    StackTraceElement[] strace = new StackTraceElement[STACK_TRACE_SIZE];
    for (int i = 0; i < STACK_TRACE_SIZE && (i + topOfStack) < stes.length; i++)
    {
      strace[i] = stes[i + topOfStack];
    }
    incidentReport.put(STACKTRACE, strace);

    // Request ID associated with incident, if any
    incidentReport.put(REQUEST_ID, requestId);

    // Job ID associated with incident, if any
    incidentReport.put(JOB_ID, jobUuid);

    // Type of incident
    incidentReport.put(INCIDENT_TYPE, Event.EventType.INCIDENT.getDescription());

    // Error signature for calculating hash in GS.
    incidentInfo.put(SIGNATURE, signature);

    // What function reported this incident?
    incidentInfo.put(REPORTER, reporter);

    // Incident detail and exception for GS error message generation.
    incidentInfo.put(DETAIL, detailMessage);

    // Other metadata
    incidentInfo.put(TIMESTAMP, DateTime.now().toString());
    incidentInfo.put(INCIDENT_REPORT, incidentReport);

    incident.put(SFSessionProperty.SERVER_URL.getPropertyKey(), serverUrl);
    incident.put(SFSession.SF_HEADER_TOKEN_TAG, sessionToken);
    incident.put(INCIDENT_INFO, incidentInfo);

    // This is the incident string that we'll throttle on
    incident.put(THROTTLE_SIGNATURE, signature);

    return incident;
  }

  /**
   * Produce a one line description of the throwable, suitable for error message
   * and log printing.
   * @param thrown thrown object
   * @return description of the thrown object
   */
  public static String oneLiner(Throwable thrown)
  {
    StackTraceElement[] stack = thrown.getStackTrace();
    String topOfStack = null;
    if (stack.length > 0)
    {
      topOfStack = String.valueOf(stack[0]);
    }
    return thrown.toString() + " at " + topOfStack;
  }

  /**
   * Dumps JVM metrics for this process.
   * @param incidentId incident id
   */
  public static void dumpVmMetrics(String incidentId)
  {
    PrintWriter writer = null;
    try
    {
      String dumpFile = EventUtil.getDumpPathPrefix() + "/" +
                        INC_DUMP_FILE_NAME + incidentId + INC_DUMP_FILE_EXT;

      final OutputStream outStream =
          new GZIPOutputStream(new FileOutputStream(dumpFile));
      writer = new PrintWriter(outStream, true);

      final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();
      writer.print("\n\n\n---------------------------  METRICS "
                   + "---------------------------\n\n");
      writer.flush();
      JsonFactory jf = new JsonFactory();
      jf.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
      ObjectMapper mapper = new ObjectMapper(jf);

      mapper.registerModule(new JodaModule());
      mapper.setDateFormat(new ISO8601DateFormat());
      mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
      MetricsServlet metrics = new MetricsServlet(Clock.defaultClock(),
                                                  vm,
                                                  Metrics.defaultRegistry(),
                                                  jf,
                                                  true);

      final JsonGenerator json = jf.createGenerator(outStream,
                                                    JsonEncoding.UTF8);
      json.useDefaultPrettyPrinter();
      json.writeStartObject();

      // JVM metrics
      writeVmMetrics(json, vm);

      // Components metrics
      metrics.writeRegularMetrics(json, // json generator
                                  null, // class prefix
                                  false); // include full samples

      json.writeEndObject();
      json.close();

      logger.debug("Creating full thread dump in dump file {}", dumpFile);

      // Thread dump next....
      writer.print("\n\n\n---------------------------  THREAD DUMP "
          + "---------------------------\n\n");
      writer.flush();

      vm.threadDump(outStream);

      logger.debug("Dump file {} is created.", dumpFile);
    }
    catch(Exception exc)
    {
      logger.error(
          "Unable to write dump file, exception: {}", exc.getMessage());
    }
    finally
    {
      if (writer != null)
      {
        writer.close();
      }
    }
  }

  private static void writeVmMetrics(JsonGenerator json, VirtualMachineMetrics vm)
          throws IOException
  {
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
          for (Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet())
          {
            json.writeNumberField(pool.getKey(), pool.getValue());
          }
        }
        json.writeEndObject();
      }
      json.writeEndObject();

      final Map<String, VirtualMachineMetrics.BufferPoolStats> bufferPoolStats = vm
              .getBufferPoolStats();
      if (!bufferPoolStats.isEmpty())
      {
        json.writeFieldName("buffers");
        json.writeStartObject();
        {
          json.writeFieldName("direct");
          json.writeStartObject();
          {
            json.writeNumberField("count", bufferPoolStats.get("direct")
                                  .getCount());
            json.writeNumberField("memoryUsed", bufferPoolStats.get("direct")
                                  .getMemoryUsed());
            json.writeNumberField("totalCapacity", bufferPoolStats.get("direct")
                                  .getTotalCapacity());
          }
          json.writeEndObject();

          json.writeFieldName("mapped");
          json.writeStartObject();
          {
            json.writeNumberField("count", bufferPoolStats.get("mapped")
                                  .getCount());
            json.writeNumberField("memoryUsed", bufferPoolStats.get("mapped")
                                  .getMemoryUsed());
            json.writeNumberField("totalCapacity", bufferPoolStats.get("mapped")
                                  .getTotalCapacity());
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
        for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages()
                .entrySet())
        {
          json.writeNumberField(entry.getKey().toString().toLowerCase(),
                                entry.getValue());
        }
      }
      json.writeEndObject();

      json.writeFieldName("garbage-collectors");
      json.writeStartObject();
      {
        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm
                .garbageCollectors()
                .entrySet())
        {
          json.writeFieldName(entry.getKey());
          json.writeStartObject();
          {
            final VirtualMachineMetrics.GarbageCollectorStats gc = entry
                    .getValue();
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
}