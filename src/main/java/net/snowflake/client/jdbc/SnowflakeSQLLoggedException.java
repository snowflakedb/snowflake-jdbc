/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.util.Strings;
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
import java.util.concurrent.*;

/**
 * @author mknister
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
  private TelemetryService oobInstance = TelemetryService.getInstance();

  /*
  IB telemtry instance
   */
  private Telemetry ibInstance = null;

  private final static ObjectMapper mapper =
      ObjectMapperFactory.getObjectMapper();

  /**
   * Function to create a TelemetryEvent log from the JSONObject and exception and send it via OOB telemetry
   *
   * @param value JSONnode containing relevant information specific to the exception constructor that
   *              should be included in the telemetry data, such as sqlState or vendorCode
   * @param ex    The exception being thrown
   */
  private void sendOutOfBandTelemetryMessage(JSONObject value, SnowflakeSQLLoggedException ex)
  {
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ex.printStackTrace(pw);
    String stackTrace = sw.toString();
    value.put("Stacktrace", stackTrace);
    TelemetryEvent log = logBuilder
        .withName("Exception: " + ex.getClass().getSimpleName())
        .withValue(value)
        .build();
    oobInstance.report(log);
  }

  /**
   * Function to create a TelemetryClient log and send it via in-band telemetry
   *
   * @param value ObjectNode containing information specific to the exception constructor that should be included in
   *              the telemetry log, such as SQLState or reason for the error
   * @param ex    The exception being thrown
   * @return true if in-band telemetry log sent successfully or false if it did not
   */
  private Future<Boolean> sendInBandTelemetryMessage(ObjectNode value, SnowflakeSQLLoggedException ex)
  {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ex.printStackTrace(pw);
    String stackTrace = sw.toString();
    value.put("Stacktrace", stackTrace);
    value.put("Exception", ex.getClass().getSimpleName());
    ibInstance.addLogToBatch(new TelemetryData(value, System.currentTimeMillis()));
    return ibInstance.sendBatchAsync();
  }

  /**
   * Helper function to create JSONObject node for OOB telemetry log
   *
   * @param queryId
   * @param reason
   * @param SQLState
   * @param vendorCode
   * @param errorCode
   * @return JSONObject with data about SQLException
   */
  private JSONObject createOOBValue(String queryId, String reason, String SQLState, int vendorCode, ErrorCode errorCode)
  {
    JSONObject oobValue = new JSONObject();
    if (!Strings.isNullOrEmpty(queryId))
    {
      oobValue.put("Query ID", queryId);
    }
    if (!Strings.isNullOrEmpty(SQLState))
    {
      oobValue.put("SQLState", SQLState);
    }
    if (vendorCode != -1)
    {
      oobValue.put("Vendor Code", vendorCode);
    }
    if (errorCode != null)
    {
      oobValue.put("Error Code", errorCode.toString());
    }
    if (!Strings.isNullOrEmpty(reason))
    {
      oobValue.put("reason", reason);
    }
    return oobValue;
  }

  /**
   * Function to construct log data based on possible exception inputs and send data through in-band telemetry, or oob
   * if in-band does not work
   *
   * @param queryId    query ID if exists
   * @param reason     reason for the exception
   * @param SQLState   SQLState
   * @param vendorCode vendor code
   * @param errorCode  error code
   * @param session    session object, which is needed to send in-band telemetry but not oob. Might be null, in which case
   *                   oob is used.
   */
  public void sendTelemetryData(String queryId, String reason, String SQLState, int vendorCode, ErrorCode errorCode, SFSession session)
  {
    // if session is not null, try sending data using in-band telemetry
    if (session != null)
    {
      ibInstance = session.getTelemetryClient();
    }
    // if in-band instance is successfully created, compile sql exception data into an in-band telemetry log
    if (ibInstance != null)
    {
      ObjectNode ibValue = mapper.createObjectNode();
      ibValue.put("type", TelemetryField.SQL_EXCEPTION.toString());
      if (!Strings.isNullOrEmpty(queryId))
      {
        ibValue.put("Query ID", queryId);
      }
      if (!Strings.isNullOrEmpty(SQLState))
      {
        ibValue.put("SQLState", SQLState);
      }
      if (vendorCode != -1)
      {
        ibValue.put("Vendor Code", vendorCode);
      }
      if (errorCode != null)
      {
        ibValue.put("Error Code", errorCode.toString());
      }
      if (!Strings.isNullOrEmpty(reason))
      {
        ibValue.put("reason", reason);
      }
      // try  to send in-band data asynchronously
      ExecutorService threadExecutor = Executors.newSingleThreadExecutor();
      threadExecutor.submit(() ->
                            {
                              boolean inBandSuccess;
                              Future<Boolean> sendInBand = sendInBandTelemetryMessage(ibValue, this);
                              // record whether in band telemetry message sent with boolean value inBandSuccess
                              try
                              {
                                inBandSuccess = sendInBand.get(10, TimeUnit.SECONDS);
                              }
                              catch (Exception e)
                              {
                                inBandSuccess = false;
                              }
                              // In-band failed so send OOB telemetry instead
                              if (!inBandSuccess)
                              {
                                logger.debug("In-band telemetry message failed to send. Sending out-of-band message instead");
                                JSONObject oobValue = createOOBValue(queryId, reason, SQLState, vendorCode, errorCode);
                                sendOutOfBandTelemetryMessage(oobValue, this);
                              }
                            }
      );
    }
    // In-band is not possible so send OOB telemetry instead
    else
    {
      JSONObject oobValue = createOOBValue(queryId, reason, SQLState, vendorCode, errorCode);
      sendOutOfBandTelemetryMessage(oobValue, this);
    }
  }

  public SnowflakeSQLLoggedException(String queryId, String reason, String SQLState, int vendorCode, SFSession session)
  {
    super(queryId, reason, SQLState, vendorCode);
    sendTelemetryData(queryId, reason, SQLState, vendorCode, null, session);
  }

  public SnowflakeSQLLoggedException(String SQLState, int vendorCode, SFSession session)
  {
    super(SQLState, vendorCode);
    sendTelemetryData(null, null, SQLState, vendorCode, null, session);
  }

  public SnowflakeSQLLoggedException(String reason, String SQLState, SFSession session)
  {
    super(reason, SQLState);
    sendTelemetryData(null, reason, SQLState, -1, null, session);
  }

  public SnowflakeSQLLoggedException(String SQLState, int vendorCode, SFSession session, Object... params)
  {
    super(SQLState, vendorCode, params);
    String reason = errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params);
    sendTelemetryData(null, reason, SQLState, vendorCode, null, session);
  }

  public SnowflakeSQLLoggedException(Throwable ex, ErrorCode errorCode, SFSession session, Object... params)
  {
    super(ex, errorCode, params);
    // add telemetry
    String reason =
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(errorCode.getMessageCode()), params);
    sendTelemetryData(null, reason, errorCode.getSqlState(), errorCode.getMessageCode(), errorCode, session);
  }

  public SnowflakeSQLLoggedException(Throwable ex,
                                     String SQLState,
                                     int vendorCode,
                                     SFSession session,
                                     Object... params)
  {
    super(ex, SQLState, vendorCode, params);
    // add telemetry
    String reason = errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params);
    sendTelemetryData(null, reason, SQLState, vendorCode, null, session);
  }

  public SnowflakeSQLLoggedException(ErrorCode errorCode, SFSession session, Object... params)
  {
    super(errorCode, params);
    // add telemetry
    String reason =
        errorResourceBundleManager.getLocalizedMessage(String.valueOf(errorCode.getMessageCode()), params);
    sendTelemetryData(null, reason, null, -1, errorCode, session);
  }

  public SnowflakeSQLLoggedException(SFException e, SFSession session)
  {
    super(e);
    // add telemetry
    sendTelemetryData(null, null, null, -1, null, session);
  }

  public SnowflakeSQLLoggedException(String reason, SFSession session)
  {
    super(reason);
    sendTelemetryData(null, reason, null, -1, null, session);
  }
}
