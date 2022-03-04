/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.concurrent.Future;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryThreadPool;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 *
 * <p>Telemetry Service Interface
 */
public class TelemetryClient implements Telemetry {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFBaseSession.class);

  private static final String SF_PATH_TELEMETRY = "/telemetry/send";
  private static final String SF_PATH_TELEMETRY_SESSIONLESS = "/telemetry/send/sessionless";

  // if the number of cached logs is larger than this threshold,
  // the telemetry connector will flush the buffer automatically.
  private final int forceFlushSize;

  private static final int DEFAULT_FORCE_FLUSH_SIZE = 100;

  private final String serverUrl;
  private final String telemetryUrl;

  private final SFSession session;
  private LinkedList<TelemetryData> logBatch;
  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

  private boolean isClosed;

  // HTTP client object used to communicate with other machine
  private final CloseableHttpClient httpClient;

  // JWT token
  private String token;

  private Object locker = new Object();

  // false if meet any error when sending metrics
  private boolean isTelemetryServiceAvailable = true;

  // Retry timeout for the HTTP request
  private static final int TELEMETRY_HTTP_RETRY_TIMEOUT_IN_SEC = 1000;

  private TelemetryClient(SFSession session, int flushSize) {
    this.session = session;
    this.serverUrl = session.getUrl();
    this.httpClient = null;

    if (this.serverUrl.endsWith("/")) {
      this.telemetryUrl =
          this.serverUrl.substring(0, this.serverUrl.length() - 1) + SF_PATH_TELEMETRY;
    } else {
      this.telemetryUrl = this.serverUrl + SF_PATH_TELEMETRY;
    }

    this.logBatch = new LinkedList<>();
    this.isClosed = false;
    this.forceFlushSize = flushSize;
  }

  /**
   * Constructor for creating a sessionless telemetry client
   *
   * @param httpClient client object used to communicate with other machine
   * @param serverUrl server url
   * @param flushSize maximum size of telemetry batch before flush
   */
  private TelemetryClient(CloseableHttpClient httpClient, String serverUrl, int flushSize) {
    this.session = null;
    this.serverUrl = serverUrl;
    this.httpClient = httpClient;

    if (this.serverUrl.endsWith("/")) {
      this.telemetryUrl =
          this.serverUrl.substring(0, this.serverUrl.length() - 1) + SF_PATH_TELEMETRY_SESSIONLESS;
    } else {
      this.telemetryUrl = this.serverUrl + SF_PATH_TELEMETRY_SESSIONLESS;
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
  public boolean isTelemetryEnabled() {
    return (this.session == null || this.session.isClientTelemetryEnabled())
        && this.isTelemetryServiceAvailable;
  }

  /** Disable any use of the client to add/send metrics */
  public void disableTelemetry() {
    this.isTelemetryServiceAvailable = false;
  }

  /**
   * Initialize the telemetry connector
   *
   * @param conn connection with the session to use for the connector
   * @param flushSize maximum size of telemetry batch before flush
   * @return a telemetry connector
   */
  public static Telemetry createTelemetry(Connection conn, int flushSize) {
    try {
      return createTelemetry(
          (SFSession) conn.unwrap(SnowflakeConnectionV1.class).getSFBaseSession(), flushSize);
    } catch (SQLException ex) {
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
  public static Telemetry createTelemetry(Connection conn) {
    return createTelemetry(conn, DEFAULT_FORCE_FLUSH_SIZE);
  }

  /**
   * Initialize the telemetry connector
   *
   * @param session session to use for telemetry dumps
   * @return a telemetry connector
   */
  public static Telemetry createTelemetry(SFSession session) {
    return createTelemetry(session, DEFAULT_FORCE_FLUSH_SIZE);
  }

  /**
   * Initialize the telemetry connector
   *
   * @param session session to use for telemetry dumps
   * @param flushSize maximum size of telemetry batch before flush
   * @return a telemetry connector
   */
  public static Telemetry createTelemetry(SFSession session, int flushSize) {
    return new TelemetryClient(session, flushSize);
  }

  /**
   * Initialize the sessionless telemetry connector
   *
   * @param httpClient client object used to communicate with other machine
   * @param serverUrl server url
   * @return a telemetry connector
   */
  public static Telemetry createSessionlessTelemetry(
      CloseableHttpClient httpClient, String serverUrl) {
    return createSessionlessTelemetry(httpClient, serverUrl, DEFAULT_FORCE_FLUSH_SIZE);
  }

  /**
   * Initialize the sessionless telemetry connector
   *
   * @param httpClient client object used to communicate with other machine
   * @param serverUrl server url
   * @param flushSize maximum size of telemetry batch before flush
   * @return a telemetry connector
   */
  public static Telemetry createSessionlessTelemetry(
      CloseableHttpClient httpClient, String serverUrl, int flushSize) {
    return new TelemetryClient(httpClient, serverUrl, flushSize);
  }

  /**
   * Add log to batch to be submitted to telemetry. Send batch if forceFlushSize reached
   *
   * @param log entry to add
   */
  @Override
  public void addLogToBatch(TelemetryData log) {
    if (isClosed) {
      logger.debug("Telemetry already closed");
      return;
    }

    if (!isTelemetryEnabled()) {
      return; // if disable, do nothing
    }

    synchronized (locker) {
      this.logBatch.add(log);
    }

    if (this.logBatch.size() >= this.forceFlushSize) {
      this.sendBatchAsync();
    }
  }

  /**
   * Add log to batch to be submitted to telemetry. Send batch if forceFlushSize reached
   *
   * @param message json node of log
   * @param timeStamp timestamp to use for log
   */
  public void addLogToBatch(ObjectNode message, long timeStamp) {
    this.addLogToBatch(new TelemetryData(message, timeStamp));
  }

  /** Close telemetry connector and send any unsubmitted logs */
  @Override
  public void close() {
    if (isClosed) {
      logger.debug("Telemetry client already closed");
      return;
    }

    try {
      // sendBatch when close is synchronous, otherwise client might be closed
      // before data was sent.
      sendBatchAsync().get();
    } catch (Throwable e) {
      logger.debug("Error when sending batch data, {}", e);
    } finally {
      this.isClosed = true;
    }
  }

  /**
   * Return whether the client has been closed
   *
   * @return whether client is closed
   */
  public boolean isClosed() {
    return this.isClosed;
  }

  @Override
  public Future<Boolean> sendBatchAsync() {
    return TelemetryThreadPool.getInstance()
        .submit(
            () -> {
              try {
                return this.sendBatch();
              } catch (Throwable e) {
                logger.debug("Failed to send telemetry data, {}", e);
                return false;
              }
            });
  }

  @Override
  public void postProcess(String queryId, String sqlState, int vendorCode, Throwable ex) {
    // This is a no-op.
  }

  /**
   * Send all cached logs to server
   *
   * @return whether the logs were sent successfully
   * @throws IOException if closed or uploading batch fails
   */
  private boolean sendBatch() throws IOException {
    if (isClosed) {
      throw new IOException("Telemetry connector is closed");
    }
    if (!isTelemetryEnabled()) {
      return false;
    }

    LinkedList<TelemetryData> tmpList;
    synchronized (locker) {
      tmpList = this.logBatch;
      this.logBatch = new LinkedList<>();
    }

    if (this.session != null && this.session.isClosed()) {
      throw new UnexpectedException("Session is closed when sending log");
    }

    if (!tmpList.isEmpty()) {
      // session shared with JDBC
      String payload = logsToString(tmpList);

      logger.debugNoMask("Payload of telemetry is : " + payload);

      HttpPost post = new HttpPost(this.telemetryUrl);
      post.setEntity(new StringEntity(payload));
      post.setHeader("Content-type", "application/json");

      if (this.session == null) {
        post.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + this.token);
        post.setHeader("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT");
        post.setHeader(HttpHeaders.ACCEPT, "application/json");
      } else {
        post.setHeader(
            HttpHeaders.AUTHORIZATION,
            "Snowflake Token=\"" + this.session.getSessionToken() + "\"");
      }

      String response = null;

      try {
        response =
            this.session == null
                ? HttpUtil.executeGeneralRequest(
                    post, TELEMETRY_HTTP_RETRY_TIMEOUT_IN_SEC, this.session.getAuthTimeout(), 0, this.httpClient)
                : HttpUtil.executeGeneralRequest(
                    post, TELEMETRY_HTTP_RETRY_TIMEOUT_IN_SEC, this.session.getAuthTimeout(), 0, this.session.getHttpClientKey());
      } catch (SnowflakeSQLException e) {
        disableTelemetry(); // when got error like 404 or bad request, disable telemetry in this
        // telemetry instance
        logger.error(
            "Telemetry request failed, " + "response: {}, exception: {}", response, e.getMessage());
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
  public boolean sendLog(TelemetryData log) throws IOException {
    addLogToBatch(log);
    return sendBatch();
  }

  /**
   * Send a log to the server, along with any existing logs waiting to be sent
   *
   * @param message json node of log
   * @param timeStamp timestamp to use for log
   * @return whether the logs were sent successfully
   * @throws IOException if closed or uploading batch fails
   */
  public boolean sendLog(ObjectNode message, long timeStamp) throws IOException {
    return this.sendLog(new TelemetryData(message, timeStamp));
  }

  /**
   * convert a list of log to a JSON object
   *
   * @param telemetryData a list of log
   * @return the result json string
   */
  static ObjectNode logsToJson(LinkedList<TelemetryData> telemetryData) {
    ObjectNode node = mapper.createObjectNode();
    ArrayNode logs = mapper.createArrayNode();
    for (TelemetryData data : telemetryData) {
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
  static String logsToString(LinkedList<TelemetryData> telemetryData) {
    return logsToJson(telemetryData).toString();
  }

  /**
   * For test use only
   *
   * @return the number of cached logs
   */
  public int bufferSize() {
    return this.logBatch.size();
  }

  /**
   * For test use only
   *
   * @return a copy of the logs currently in the buffer
   */
  public LinkedList<TelemetryData> logBuffer() {
    return new LinkedList<>(this.logBatch);
  }

  /**
   * Refresh the JWT token
   *
   * @param token latest JWT token
   */
  public void refreshToken(String token) {
    this.token = token;
  }
}
