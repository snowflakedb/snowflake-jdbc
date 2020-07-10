/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.minidev.json.JSONObject;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import net.snowflake.client.jdbc.telemetry.TelemetryField;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryEvent;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author USER
 * <p>
 * This SnowflakeSQLLoggedException class extends the SnowflakeSQLException class to add OOB telemetry data for sql
 * exceptions. Not all sql exceptions require OOB telemetry logging so the exceptions in this class should only be
 * thrown if there is a need for logging the exception with OOB telemetry.
 */

public class SnowflakeSQLLoggedException extends SnowflakeSQLException
{
  /*
  OOB telemetry instance
   */
  public TelemetryService oobInstance = TelemetryService.getInstance();

  /*
  IB telemtry instance
   */
  public Telemetry ibInstance;

  private final static ObjectMapper mapper =
          ObjectMapperFactory.getObjectMapper();

  /**
   * @param value JSONnode containing relevant information specific to the exception constructor that
   *              should be included in the telemetry data, such as sqlState or vendorCode
   * @param ex    The exception being thrown
   */
  private void buildExceptionTelemetryLog(JSONObject value, SnowflakeSQLLoggedException ex)
  {
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ex.printStackTrace(pw);
    String stackTrace = sw.toString();
    value.put("Stacktrace", stackTrace);
    TelemetryEvent log = logBuilder
        .withName("Exception: " + ex.getMessage())
        .withValue(value)
        .build();
    oobInstance.report(log);
  }

  private boolean sendInBandTelemetryMessage(ObjectNode value, SnowflakeSQLLoggedException ex) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ex.printStackTrace(pw);
    String stackTrace = sw.toString();
    value.put("Stacktrace", stackTrace);
    ibInstance.addLogToBatch(new TelemetryData(value, System.currentTimeMillis()));
    Future<Boolean> success = ibInstance.sendBatchAsync();
    try
    {
      // wait for 10 seconds at most before giving up
      return success.get(10, TimeUnit.SECONDS);
    }
    catch (Exception e)
    {
      success.cancel(true);
      return false;
    }
  }


  public SnowflakeSQLLoggedException(String queryId, String reason, String SQLState, int vendorCode, SFSession session)
  {
    super(queryId, reason, SQLState, vendorCode);
    // add telemetry
    ibInstance = session.getTelemetryClient();
    boolean success = false;
    if (ibInstance != null)
    {
      ObjectNode value = mapper.createObjectNode();
      value.put("type", TelemetryField.SQL_EXCEPTION.toString());
      value.put("Query ID", queryId);
      value.put("Vendor Code", vendorCode);
      value.put("reason", reason);
      success = sendInBandTelemetryMessage(value, this);
    }
    if (!success)
    {
      JSONObject value = new JSONObject();
      value.put("SQLState", SQLState);
      value.put("Query ID", queryId);
      value.put("Vendor Code", vendorCode);
      value.put("reason", reason);
      buildExceptionTelemetryLog(value, this);
    }
  }

  public SnowflakeSQLLoggedException(String SQLState, int vendorCode)
  {
    super(SQLState, vendorCode);
    // add telemetry
    JSONObject value = new JSONObject();
    value.put("SQLState", SQLState);
    value.put("Vendor Code", vendorCode);
    buildExceptionTelemetryLog(value, this);
  }

  public SnowflakeSQLLoggedException(String reason, String SQLState)
  {
    super(reason, SQLState);
    // add telemetry
    JSONObject value = new JSONObject();
    value.put("SQLState", SQLState);
    value.put("reason", reason);
    buildExceptionTelemetryLog(value, this);
  }

  public SnowflakeSQLLoggedException(String SQLState, int vendorCode, Object... params)
  {
    super(SQLState, vendorCode, params);
    // add telemetry
    String errorMessage = errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params);
    JSONObject value = new JSONObject();
    value.put("SQLState", SQLState);
    value.put("error message", errorMessage);
    value.put("vendorCode", vendorCode);
    buildExceptionTelemetryLog(value, this);
  }

  public SnowflakeSQLLoggedException(Throwable ex, ErrorCode errorCode, Object... params)
  {
    super(ex, errorCode, params);
    // add telemetry
    String errorMessage =
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(errorCode.getMessageCode()), params);
    JSONObject value = new JSONObject();
    value.put("SQLState", errorCode.getSqlState());
    value.put("error message", errorMessage);
    value.put("VendorCode", errorCode.getMessageCode());
    value.put("Error code", errorCode);
    buildExceptionTelemetryLog(value, this);
  }

  public SnowflakeSQLLoggedException(Throwable ex,
                                     String SQLState,
                                     int vendorCode,
                                     Object... params)
  {
    super(ex, SQLState, vendorCode, params);
    // add telemetry
    String errorMessage = errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params);
    JSONObject value = new JSONObject();
    value.put("error message", errorMessage);
    value.put("VendorCode", vendorCode);
    buildExceptionTelemetryLog(value, this);
  }

  public SnowflakeSQLLoggedException(ErrorCode errorCode, Object... params)
  {
    super(errorCode, params);
    // add telemetry
    String errorMessage =
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(errorCode.getMessageCode()), params);
    JSONObject value = new JSONObject();
    value.put("error message", errorMessage);
    value.put("errorCode", errorCode);
    buildExceptionTelemetryLog(value, this);
  }

  public SnowflakeSQLLoggedException(SFException e)
  {
    super(e);
    // add telemetry
    buildExceptionTelemetryLog(null, this);
  }

  public SnowflakeSQLLoggedException(String reason)
  {
    super(reason);
    // add telemetry
    JSONObject value = new JSONObject();
    value.put("reason", reason);
    buildExceptionTelemetryLog(value, this);
  }
}
