package net.snowflake.client.jdbc.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.rmi.UnexpectedException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;


/**
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 * <p>
 * Telemetry Service Interface
 */
public class Telemetry
{
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SFSession.class);

  private static final String SF_PATH_TELEMETRY = "/telemetry/send";

  // if the number of cached logs is larger than this threshold,
  // the telemetry connector will flush the buffer automatically.
  private final int forceFlushSize;

  private static final int DEFAULT_FORCE_FLUSH_SIZE = 100;

  private final String serverUrl;
  private final String telemetryUrl;


  private final SFSession session;
  private LinkedList<TelemetryData> logBatch;
  private static final ObjectMapper mapper =
      ObjectMapperFactory.getObjectMapper();

  private boolean isClosed;

  private Object locker = new Object();

  //false if meet any error when sending metrics
  private boolean isTelemetryServiceAvailable = true;

  private Telemetry(SFSession session, int flushSize)
  {
    this.session = session;
    this.serverUrl = session.getUrl();

    if (this.serverUrl.endsWith("/"))
    {
      this.telemetryUrl =
          this.serverUrl.substring(0, this.serverUrl.length() - 1)
          + SF_PATH_TELEMETRY;
    }
    else
    {
      this.telemetryUrl = this.serverUrl + SF_PATH_TELEMETRY;
    }

    this.logBatch = new LinkedList<>();
    this.isClosed = false;
    this.forceFlushSize = flushSize;

  }

  /**
   * Return whether the client can be used to add/send metrics
   *
   * @return whether client is enabled
   */
  public boolean isTelemetryEnabled()
  {
    return this.session.isClientTelemetryEnabled() && this.isTelemetryServiceAvailable;
  }

  /**
   * Disable any use of the client to add/send metrics
   */
  public void disableTelemetry()
  {
    this.isTelemetryServiceAvailable = false;
  }


  /**
   * Initialize the telemetry connector
   *
   * @param conn      connection with the session to use for the connector
   * @param flushSize maximum size of telemetry batch before flush
   * @return a telemetry connector
   */
  public static Telemetry createTelemetry(Connection conn, int flushSize)
  {
    try
    {
      return createTelemetry(conn.unwrap(SnowflakeConnectionV1.class).getSfSession(), flushSize);
    }
    catch (SQLException ex)
    {
      logger.debug("input connection is not a SnowflakeConnection");
      return null;
    }
  }

  /**
   * Initialize the telemetry connector
   *
   * @param conn connection with the session to use for the connector
   * @return a telemetry connector
   */
  public static Telemetry createTelemetry(Connection conn)
  {
    return createTelemetry(conn, DEFAULT_FORCE_FLUSH_SIZE);
  }


  /**
   * Initialize the telemetry connector
   *
   * @param session session to use for telemetry dumps
   * @return a telemetry connector
   */
  public static Telemetry createTelemetry(SFSession session)
  {
    return createTelemetry(session, DEFAULT_FORCE_FLUSH_SIZE);
  }

  /**
   * Initialize the telemetry connector
   *
   * @param session   session to use for telemetry dumps
   * @param flushSize maximum size of telemetry batch before flush
   * @return a telemetry connector
   */
  public static Telemetry createTelemetry(SFSession session, int flushSize)
  {
    return new Telemetry(session, flushSize);
  }

  /**
   * Add log to batch to be submitted to telemetry. Send batch if forceFlushSize
   * reached
   *
   * @param log entry to add
   * @throws IOException if closed or uploading batch fails
   */
  public void addLogToBatch(TelemetryData log) throws IOException
  {
    if (isClosed)
    {
      throw new IOException("Telemetry connector is closed");
    }
    if (!isTelemetryEnabled())
    {
      return; // if disable, do nothing
    }

    synchronized (locker)
    {
      this.logBatch.add(log);
    }

    if (this.logBatch.size() >= this.forceFlushSize)
    {
      this.sendBatch();
    }
  }

  /**
   * Add log to batch to be submitted to telemetry. Send batch if forceFlushSize
   * reached
   *
   * @param message   json node of log
   * @param timeStamp timestamp to use for log
   * @throws IOException if closed or uploading batch fails
   */
  public void addLogToBatch(ObjectNode message, long timeStamp) throws
                                                                IOException
  {
    this.addLogToBatch(new TelemetryData(message, timeStamp));
  }

  /**
   * Attempt to add log to batch, and suppress exceptions thrown in case of
   * failure
   *
   * @param log entry to add
   */
  public void tryAddLogToBatch(TelemetryData log)
  {
    try
    {
      addLogToBatch(log);
    }
    catch (IOException ex)
    {
      logger.debug("Exception encountered while sending metrics to telemetry endpoint.", ex);
    }
  }

  /**
   * Close telemetry connector and send any unsubmitted logs
   *
   * @throws IOException if closed or uploading batch fails
   */
  public void close() throws IOException
  {
    if (isClosed)
    {
      throw new IOException("Telemetry connector is closed");
    }
    try
    {
      this.sendBatch();
    }
    catch (IOException e)
    {
      logger.error("Send logs failed on closing", e);
    }
    finally
    {
      this.isClosed = true;
    }
  }

  /**
   * Return whether the client has been closed
   *
   * @return whether client is closed
   */
  public boolean isClosed()
  {
    return this.isClosed;
  }

  /**
   * Send all cached logs to server
   *
   * @return whether the logs were sent successfully
   * @throws IOException if closed or uploading batch fails
   */
  public boolean sendBatch() throws IOException
  {
    if (isClosed)
    {
      throw new IOException("Telemetry connector is closed");
    }
    if (!isTelemetryEnabled())
    {
      return false;
    }

    LinkedList<TelemetryData> tmpList;
    synchronized (locker)
    {
      tmpList = this.logBatch;
      this.logBatch = new LinkedList<>();
    }

    if (session.isClosed())
    {
      throw new UnexpectedException("Session is closed when sending log");
    }

    if (!tmpList.isEmpty())
    {
      //session shared with JDBC
      String sessionToken = this.session.getSessionToken();

      HttpPost post = new HttpPost(this.telemetryUrl);
      post.setEntity(new StringEntity(logsToString(tmpList)));
      post.setHeader("Content-type", "application/json");
      post.setHeader("Authorization", "Snowflake Token=\"" + sessionToken +
                                      "\"");

      String response = null;

      try
      {
        response = HttpUtil.executeRequest(post, 1000, 0, null);
      }
      catch (SnowflakeSQLException e)
      {
        disableTelemetry(); // when got error like 404 or bad request, disable telemetry in this telemetry instance
        logger.error(
            "Telemetry request failed, " +
            "response: {}, exception: {}", response, e.getMessage());
        return false;
      }
    }
    return true;
  }


  /**
   * Send a log to the server, along with any existing logs waiting to be sent
   *
   * @param log entry to send
   * @return whether the logs were sent successfully
   * @throws IOException if closed or uploading batch fails
   */
  public boolean sendLog(TelemetryData log) throws IOException
  {
    addLogToBatch(log);
    return sendBatch();
  }

  /**
   * Send a log to the server, along with any existing logs waiting to be sent
   *
   * @param message   json node of log
   * @param timeStamp timestamp to use for log
   * @return whether the logs were sent successfully
   * @throws IOException if closed or uploading batch fails
   */
  public boolean sendLog(ObjectNode message, long timeStamp) throws IOException
  {
    return this.sendLog(new TelemetryData(message, timeStamp));
  }

  /**
   * convert a list of log to a JSON object
   *
   * @param telemetryData a list of log
   * @return the result json string
   */
  static ObjectNode logsToJson(LinkedList<TelemetryData> telemetryData)
  {
    ObjectNode node = mapper.createObjectNode();
    ArrayNode logs = mapper.createArrayNode();
    for (TelemetryData data : telemetryData)
    {
      logs.add(data.toJson());
    }
    node.set("logs", logs);

    return node;
  }

  /**
   * convert a list of log to a JSON String
   *
   * @param telemetryData a list of log
   * @return the result json string
   */
  static String logsToString(LinkedList<TelemetryData> telemetryData)
  {
    return logsToJson(telemetryData).toString();
  }

  /**
   * For test use only
   *
   * @return the number of cached logs
   */
  public int bufferSize()
  {
    return this.logBatch.size();
  }

  /**
   * For test use only
   *
   * @return a copy of the logs currently in the buffer
   */
  public LinkedList<TelemetryData> logBuffer()
  {
    return new LinkedList<>(this.logBatch);
  }
}
