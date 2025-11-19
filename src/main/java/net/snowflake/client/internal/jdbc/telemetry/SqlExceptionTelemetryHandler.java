package net.snowflake.client.internal.jdbc.telemetry;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.isNullOrEmpty;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.minidev.json.JSONObject;
import net.snowflake.client.api.driver.SnowflakeDriver;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.jdbc.telemetryOOB.TelemetryEvent;
import net.snowflake.client.internal.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import net.snowflake.common.core.LoginInfoDTO;
import net.snowflake.common.core.SqlState;

/** Handler for SQL exception telemetry reporting. */
public class SqlExceptionTelemetryHandler {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SqlExceptionTelemetryHandler.class);

  /**
   * Send telemetry data for a SQL exception. This method attempts to send data via in-band
   * telemetry if a session is available, falling back to out-of-band telemetry if needed.
   *
   * @param queryId query ID if exists
   * @param sqlState SQL state
   * @param vendorCode vendor code
   * @param session session object (needed for in-band telemetry, may be null)
   * @param ex the SQLException being reported
   */
  public static void sendTelemetry(
      String queryId, String sqlState, int vendorCode, SFBaseSession session, SQLException ex) {
    Telemetry ibInstance = null;
    // if session is not null, try sending data using in-band telemetry
    if (session != null) {
      ibInstance = session.getTelemetryClient();
    }
    // if in-band instance is successfully created, compile sql exception data into an in-band
    // telemetry log
    if (ibInstance != null) {
      ObjectNode ibValue =
          TelemetryUtil.createIBValue(
              queryId, sqlState, vendorCode, TelemetryField.SQL_EXCEPTION, null, null);
      // try to send in-band data asynchronously
      ExecutorService threadExecutor = Executors.newSingleThreadExecutor();
      Telemetry finalIbInstance = ibInstance;
      try {
        threadExecutor.submit(
            () -> {
              boolean inBandSuccess;
              Future<Boolean> sendInBand = sendInBandTelemetryMessage(ibValue, ex, finalIbInstance);
              // record whether in band telemetry message sent with boolean value inBandSuccess
              try {
                inBandSuccess = sendInBand.get(10, TimeUnit.SECONDS);
              } catch (Exception e) {
                inBandSuccess = false;
              }
              // In-band failed so send OOB telemetry instead
              if (!inBandSuccess) {
                logger.debug(
                    "In-band telemetry message failed to send. Sending out-of-band message instead");
                JSONObject oobValue = createOOBValue(queryId, sqlState, vendorCode);
                sendOutOfBandTelemetryMessage(oobValue, ex, TelemetryService.getInstance());
              }
            });
      } finally {
        // Send the shutdown signal to the executor service
        threadExecutor.shutdown();

        // Add an extra hook in the telemetry client, if extra error handling is needed
        ibInstance.postProcess(queryId, sqlState, vendorCode, ex);
      }
    }
    // In-band is not possible so send OOB telemetry instead
    else {
      JSONObject oobValue = createOOBValue(queryId, sqlState, vendorCode);
      sendOutOfBandTelemetryMessage(oobValue, ex, TelemetryService.getInstance());
    }
  }

  /**
   * Create a TelemetryEvent log from the JSONObject and exception and send it via OOB telemetry.
   *
   * @param value JSONObject containing relevant exception information
   * @param ex the SQLException being reported
   * @param oobInstance out-of-band telemetry instance
   */
  private static void sendOutOfBandTelemetryMessage(
      JSONObject value, SQLException ex, TelemetryService oobInstance) {
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ex.printStackTrace(pw);
    String stackTrace = maskStacktrace(sw.toString());
    value.put("Stacktrace", stackTrace);
    value.put("Exception", ex.getClass().getSimpleName());
    TelemetryEvent log =
        logBuilder.withName("Exception: " + ex.getClass().getSimpleName()).withValue(value).build();
    oobInstance.report(log);
  }

  /**
   * Create a TelemetryClient log and send it via in-band telemetry.
   *
   * @param value ObjectNode containing exception information
   * @param ex the SQLException being reported
   * @param ibInstance telemetry instance
   * @return future indicating whether the message was sent successfully
   */
  private static Future<Boolean> sendInBandTelemetryMessage(
      ObjectNode value, SQLException ex, Telemetry ibInstance) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ex.printStackTrace(pw);
    String stackTrace = maskStacktrace(sw.toString());
    value.put("Stacktrace", stackTrace);
    value.put("Exception", ex.getClass().getSimpleName());
    // For SQLFeatureNotSupportedExceptions, add in reason for failure as "<function name> not
    // supported"
    if (value.get("SQLState").toString().contains(SqlState.FEATURE_NOT_SUPPORTED)) {
      String reason = "";
      StackTraceElement[] stackTraceArray = ex.getStackTrace();
      if (stackTraceArray.length >= 1) {
        reason = ex.getStackTrace()[0].getMethodName() + " not supported";
      }
      value.put("reason", reason);
    }
    ibInstance.addLogToBatch(TelemetryUtil.buildJobData(value));
    return ibInstance.sendBatchAsync();
  }

  /**
   * Helper function to remove sensitive data (error message, reason) from the stacktrace.
   *
   * @param stackTrace original stacktrace
   * @return stack trace with sensitive data removed
   */
  public static String maskStacktrace(String stackTrace) {
    Pattern STACKTRACE_BEGINNING =
        Pattern.compile(
            "(com|net)(\\.snowflake\\.client\\.api\\.exception\\.Snowflake|\\.snowflake\\.client\\.internal\\.exception\\.Snowflake|\\.snowflake\\.client\\.jdbc\\.Snowflake)(SQLLogged|LoggedFeatureNotSupported|SQL)(Exception)([\\s\\S]*?)(\\n\\t?at\\snet|com\\.)",
            Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
    Matcher matcher = STACKTRACE_BEGINNING.matcher(stackTrace);
    // Remove the reason from after the stack trace (in group #5 of regex pattern)
    if (matcher.find()) {
      return matcher.replaceAll("$1$2$3$4$6");
    }
    return stackTrace;
  }

  /**
   * Helper function to create JSONObject node for OOB telemetry log.
   *
   * @param queryId query ID
   * @param sqlState the SQL state
   * @param vendorCode the vendor code
   * @return JSONObject with data about SQLException
   */
  public static JSONObject createOOBValue(String queryId, String sqlState, int vendorCode) {
    JSONObject oobValue = new JSONObject();
    oobValue.put(TelemetryField.TYPE.toString(), TelemetryField.SQL_EXCEPTION.toString());
    oobValue.put(TelemetryField.DRIVER_TYPE.toString(), LoginInfoDTO.SF_JDBC_APP_ID);
    oobValue.put(TelemetryField.DRIVER_VERSION.toString(), SnowflakeDriver.implementVersion);
    if (!isNullOrEmpty(queryId)) {
      oobValue.put(TelemetryField.QUERY_ID.toString(), queryId);
    }
    if (!isNullOrEmpty(sqlState)) {
      oobValue.put(TelemetryField.SQL_STATE.toString(), sqlState);
    }
    if (vendorCode != TelemetryUtil.NO_VENDOR_CODE) {
      oobValue.put(TelemetryField.ERROR_NUMBER.toString(), vendorCode);
    }
    return oobValue;
  }
}
