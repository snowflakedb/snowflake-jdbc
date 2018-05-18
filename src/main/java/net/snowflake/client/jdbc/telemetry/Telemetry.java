package net.snowflake.client.jdbc.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.rmi.UnexpectedException;
import java.sql.Connection;
import java.util.LinkedList;


/**
 * Copyright (c) 2018 Snowflake Computing Inc. All rights reserved.
 *
 * Telemetry Service Interface
 *
 */
public class Telemetry
{
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SFSession.class);

  private static final String SF_PATH_TELEMETRY = "/telemetry/send";
  private final String serverUrl;
  private final String telemetryUrl;


  private final SFSession session;
  private LinkedList<TelemetryData> logBatch;
  private static final ObjectMapper mapper = new ObjectMapper();

  private Object locker = new Object();

  private Telemetry(SFSession session)
  {
    this.session = session;

    this.serverUrl = session.getUrl();
    this.telemetryUrl = this.serverUrl + SF_PATH_TELEMETRY;
    this.logBatch = new LinkedList<>();

  }


  /**
   * Initialize the telemetry connector
   * @param conn
   * @return
   */
  public static Telemetry createTelemetry(Connection conn)
  {
    if(conn instanceof SnowflakeConnectionV1){
      return new Telemetry(((SnowflakeConnectionV1) conn).getSfSession());
    }
    logger.debug("input connection is not a SnowflakeConnection");
    return null;

  }

  /**
   * Add a log data to batch
   */
  public void addLogToBatch(TelemetryData log)
  {
    synchronized (locker)
    {
      this.logBatch.add(log);
    }
  }

  public void addLogToBatch(ObjectNode message, long timeStamp)
  {
    this.addLogToBatch(new TelemetryData(message,timeStamp));
  }

  /**
   * Send all cached logs to server
   * @throws IOException
   */
  public boolean sendBatch() throws IOException
  {
    LinkedList<TelemetryData> tmpList;
    synchronized (locker)
    {
      tmpList = this.logBatch;
      this.logBatch = new LinkedList<>();
    }

    if(session.isClosed()){
      throw new UnexpectedException("Session is closed when sending log");
    }

    //session shared with JDBC
    String sessionToken = this.session.getSessionToken();
    HttpClient httpClient = HttpUtil.getHttpClient();

    HttpPost post = new HttpPost(this.telemetryUrl);
    post.setEntity(new StringEntity(logsToString(tmpList)));
    post.setHeader("Content-type", "application/json");
    post.setHeader("Authorization","Snowflake Token=\""+sessionToken+"\"");

    String response = null;

    try
    {
      response = HttpUtil.executeRequest(post,httpClient,1000,0,null);
    } catch (SnowflakeSQLException e)
    {
      logger.error(
          "Telemetry request failed, " +
              "response: {}, exception: {}", response, e.getMessage());

      //if failed, add telemetry data back to list
      for (TelemetryData data: tmpList)
      {
        this.addLogToBatch(data);
      }
      return false;
    }

    return true;
  }


  /**
   * Send a log data to server
   *
   * @throws IOException
   */
  public boolean sendLog(TelemetryData log) throws IOException
  {
    addLogToBatch(log);
    return sendBatch();
  }

  public boolean sendLog(ObjectNode message, long timeStamp) throws IOException
  {
    return this.sendLog(new TelemetryData(message, timeStamp));
  }

  /**
   * convert a list of log to a JSON object
   * @param telemetryData a list of log
   * @return the result json string
   */
  static ObjectNode logsToJson(LinkedList<TelemetryData> telemetryData)
  {
    ObjectNode node = mapper.createObjectNode();
    ArrayNode logs = mapper.createArrayNode();
    for (TelemetryData data: telemetryData)
    {
      logs.add(data.toJson());
    }
    node.set("logs",logs);

    return node;
  }

  /**
   * convert a list of log to a JSON String
   * @param telemetryData a list of log
   * @return the result json string
   */
  static String logsToString(LinkedList<TelemetryData> telemetryData)
  {
    return logsToJson(telemetryData).toString();
  }
}
