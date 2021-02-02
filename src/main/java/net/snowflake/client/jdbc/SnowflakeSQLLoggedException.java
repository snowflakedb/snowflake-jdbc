/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.util.Strings;
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
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryField;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryEvent;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.common.core.LoginInfoDTO;
import net.snowflake.common.core.SqlState;

/**
 * @author mknister
 *     <p>This SnowflakeSQLLoggedException class extends the SnowflakeSQLException class to add OOB
 *     telemetry data for sql exceptions. Not all sql exceptions require OOB telemetry logging so
 *     the exceptions in this class should only be thrown if there is a need for logging the
 *     exception with OOB telemetry.
 */
public class SnowflakeSQLLoggedException extends SnowflakeSQLException {

  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

  /**
   * Function to create a TelemetryEvent log from the JSONObject and exception and send it via OOB
   * telemetry
   *
   * @param value JSONnode containing relevant information specific to the exception constructor
   *     that should be included in the telemetry data, such as sqlState or vendorCode
   * @param ex The exception being thrown
   * @param oobInstance Out of band telemetry instance through which log will be passed
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
   * Function to create a TelemetryClient log and send it via in-band telemetry
   *
   * @param value ObjectNode containing information specific to the exception constructor that
   *     should be included in the telemetry log, such as SQLState or reason for the error
   * @param ex The exception being thrown
   * @param ibInstance Telemetry instance through which telemetry log will be sent
   * @return true if in-band telemetry log sent successfully or false if it did not
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
   * @return
   */
  static String maskStacktrace(String stackTrace) {
    Pattern STACKTRACE_BEGINNING =
        Pattern.compile(
            "(com|net)(\\.snowflake\\.client\\.jdbc\\.Snowflake)(SQLLogged|LoggedFeatureNotSupported|SQL)(Exception)([\\s\\S]*?)(\\n\\t?at\\snet|com\\.)",
            Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
    Matcher matcher = STACKTRACE_BEGINNING.matcher(stackTrace);
    // Remove the reason from after the stack trace (in group #5 of regex pattern)
    if (matcher.find()) {
      return matcher.replaceAll("$1$2$3$4$6");
    }
    return stackTrace;
  }

  /**
   * Helper function to create JSONObject node for OOB telemetry log
   *
   * @param queryId
   * @param SQLState
   * @param vendorCode
   * @return JSONObject with data about SQLException
   */
  static JSONObject createOOBValue(String queryId, String SQLState, int vendorCode) {
    JSONObject oobValue = new JSONObject();
    oobValue.put("type", TelemetryField.SQL_EXCEPTION.toString());
    oobValue.put("DriverType", LoginInfoDTO.SF_JDBC_APP_ID);
    oobValue.put("DriverVersion", SnowflakeDriver.implementVersion);
    if (!Strings.isNullOrEmpty(queryId)) {
      oobValue.put("QueryID", queryId);
    }
    if (!Strings.isNullOrEmpty(SQLState)) {
      oobValue.put("SQLState", SQLState);
    }
    if (vendorCode != -1) {
      oobValue.put("ErrorNumber", vendorCode);
    }
    return oobValue;
  }

  /**
   * Helper function to create ObjectNode for IB telemetry log
   *
   * @param queryId
   * @param SQLState
   * @param vendorCode
   * @return
   */
  static ObjectNode createIBValue(String queryId, String SQLState, int vendorCode) {
    ObjectNode ibValue = mapper.createObjectNode();
    ibValue.put("type", TelemetryField.SQL_EXCEPTION.toString());
    ibValue.put("DriverType", LoginInfoDTO.SF_JDBC_APP_ID);
    ibValue.put("DriverVersion", SnowflakeDriver.implementVersion);
    if (!Strings.isNullOrEmpty(queryId)) {
      ibValue.put("QueryID", queryId);
    }
    if (!Strings.isNullOrEmpty(SQLState)) {
      ibValue.put("SQLState", SQLState);
    }
    if (vendorCode != -1) {
      ibValue.put("ErrorNumber", vendorCode);
    }
    return ibValue;
  }

  /**
   * Function to construct log data based on possible exception inputs and send data through in-band
   * telemetry, or oob if in-band does not work
   *
   * @param queryId query ID if exists
   * @param SQLState SQLState
   * @param vendorCode vendor code
   * @param session session object, which is needed to send in-band telemetry but not oob. Might be
   *     null, in which case oob is used.
   * @param ex Exception object
   */
  public static void sendTelemetryData(
      String queryId, String SQLState, int vendorCode, SFBaseSession session, SQLException ex) {
    Telemetry ibInstance = null;
    // if session is not null, try sending data using in-band telemetry
    if (session != null) {
      ibInstance = session.getTelemetryClient();
    }
    // if in-band instance is successfully created, compile sql exception data into an in-band
    // telemetry log
    if (ibInstance != null) {
      ObjectNode ibValue = createIBValue(queryId, SQLState, vendorCode);
      // try  to send in-band data asynchronously
      ExecutorService threadExecutor = Executors.newSingleThreadExecutor();
      Telemetry finalIbInstance = ibInstance;
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
              JSONObject oobValue = createOOBValue(queryId, SQLState, vendorCode);
              sendOutOfBandTelemetryMessage(oobValue, ex, TelemetryService.getInstance());
            }
          });
    }
    // In-band is not possible so send OOB telemetry instead
    else {
      JSONObject oobValue = createOOBValue(queryId, SQLState, vendorCode);
      sendOutOfBandTelemetryMessage(oobValue, ex, TelemetryService.getInstance());
    }
  }

  public SnowflakeSQLLoggedException(
      SFBaseSession session, String reason, String SQLState, int vendorCode, String queryId) {
    super(queryId, reason, SQLState, vendorCode);
    sendTelemetryData(queryId, SQLState, vendorCode, session, this);
  }

  public SnowflakeSQLLoggedException(SFBaseSession session, int vendorCode, String SQLState) {
    super(SQLState, vendorCode);
    sendTelemetryData(null, SQLState, vendorCode, session, this);
  }

  public SnowflakeSQLLoggedException(SFBaseSession session, String SQLState, String reason) {
    super(reason, SQLState);
    sendTelemetryData(null, SQLState, -1, session, this);
  }

  public SnowflakeSQLLoggedException(
      SFBaseSession session, int vendorCode, String SQLState, Object... params) {
    super(SQLState, vendorCode, params);
    String reason =
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params);
    sendTelemetryData(null, SQLState, vendorCode, session, this);
  }

  public SnowflakeSQLLoggedException(
      SFBaseSession session, ErrorCode errorCode, Throwable ex, Object... params) {
    super(ex, errorCode, params);
    // add telemetry
    sendTelemetryData(null, errorCode.getSqlState(), errorCode.getMessageCode(), session, this);
  }

  public SnowflakeSQLLoggedException(
      SFBaseSession session, String SQLState, int vendorCode, Throwable ex, Object... params) {
    super(ex, SQLState, vendorCode, params);
    // add telemetry
    String reason =
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params);
    sendTelemetryData(null, SQLState, vendorCode, session, this);
  }

  public SnowflakeSQLLoggedException(SFBaseSession session, ErrorCode errorCode, Object... params) {
    super(errorCode, params);
    // add telemetry
    String reason =
        errorResourceBundleManager.getLocalizedMessage(
            String.valueOf(errorCode.getMessageCode()), params);
    sendTelemetryData(null, null, -1, session, this);
  }

  public SnowflakeSQLLoggedException(SFBaseSession session, SFException e) {
    super(e);
    // add telemetry
    sendTelemetryData(null, null, -1, session, this);
  }

  public SnowflakeSQLLoggedException(SFBaseSession session, String reason) {
    super(reason);
    sendTelemetryData(null, null, -1, session, this);
  }
}
