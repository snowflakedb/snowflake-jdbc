/*
 * Copyright (c) 2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static net.snowflake.client.util.SFTimestamp.getUTCNow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;
import net.snowflake.client.jdbc.SnowflakeDriver;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.tika.utils.ExceptionUtils;

public class Incident extends Event {
  private static final SFLogger logger = SFLoggerFactory.getLogger(Incident.class);
  private static final String SF_PATH_CREATE_INCIDENT_V2 = "/incidents/v2/create-incident";

  // Incident Fields
  final String serverUrl;
  final String sessionToken;
  final String driverName = "jdbc";
  final String driverVersion = SnowflakeDriver.implementVersion;
  final String signature;
  final String errorMessage;
  final String errorStackTrace;
  final String osName;
  final String osVersion;
  final String jobId;
  final String requestId;
  final String timestamp = getUTCNow() + ".000";
  final String uuid = UUID.randomUUID().toString();

  /**
   * Submit an exception to GS from a Throwable, this is the only constructor that should be used to
   * report incidents
   *
   * @param exc a Throwable we want to report
   * @param jobId job id String
   * @param requestId request id string
   */
  public Incident(SFSession session, Throwable exc, String jobId, String requestId) {
    this(
        session,
        jobId,
        requestId,
        exc.getMessage(),
        ExceptionUtils.getStackTrace(exc),
        String.valueOf(exc.getStackTrace()[0]));
  }

  /**
   * Submit an exception to GS from a Throwable, this is the only constructor that should be used to
   * report incidents
   *
   * @param serverUrl GS's url
   * @param sessionToken GS's session token
   * @param exc an Throwable we want to report
   * @param jobId job id String
   * @param requestId request id string
   */
  public Incident(
      String serverUrl, String sessionToken, Throwable exc, String jobId, String requestId) {
    this(
        serverUrl,
        sessionToken,
        jobId,
        requestId,
        exc.getMessage(),
        ExceptionUtils.getStackTrace(exc),
        String.valueOf(exc.getStackTrace()[0]));
  }

  /**
   * A constructor for a V2 client incident with all fields explicitly given
   *
   * @param session current session's SFSession object
   * @param jobId offending job id
   * @param requestId offending request id
   * @param errorMessage error message to be reported
   * @param errorStackTrace stack trace to be reported
   * @param raiser string representation of top of stack trace
   */
  Incident(
      SFSession session,
      String jobId,
      String requestId,
      String errorMessage,
      String errorStackTrace,
      String raiser) {
    this(
        session.getServerUrl(),
        session.getSessionToken(),
        jobId,
        requestId,
        errorMessage,
        errorStackTrace,
        raiser);
  }

  /**
   * A constructor for a V2 client incident with all fields explicitly given
   *
   * @param serverUrl GS server's URL
   * @param sessionToken current session token
   * @param jobId offending job id
   * @param requestId offending request id
   * @param errorMessage error message of exception to be reported
   * @param errorStackTrace stack trace to be reported
   * @param raiser String representation of top of the stack
   */
  Incident(
      String serverUrl,
      String sessionToken,
      String jobId,
      String requestId,
      String errorMessage,
      String errorStackTrace,
      String raiser) {
    super(Event.EventType.INCIDENT, errorMessage);
    this.serverUrl = serverUrl;
    this.sessionToken = sessionToken;
    this.jobId = jobId;
    this.requestId = requestId;
    this.signature = generateSignature(errorMessage, raiser);
    this.errorMessage = errorMessage;
    this.errorStackTrace = errorStackTrace;
    // Generated fields
    this.osName = systemGetProperty("os.name");
    this.osVersion = systemGetProperty("os.version");
  }

  /**
   * Generate Signature of the incident automatically, signature should not be written manually
   *
   * @param errorMessage error message of exception to be reported
   * @param raiser String representation of top of stack
   * @return String of signature created
   */
  private static String generateSignature(String errorMessage, String raiser) {
    return errorMessage + " at " + raiser;
  }

  /** Sends incident to GS to log */
  @Override
  public void flush() {
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    String dtoDump;
    URI incidentURI;
    try {
      dtoDump = mapper.writeValueAsString(new IncidentV2DTO(this));
    } catch (JsonProcessingException ex) {

      logger.error(
          "Incident registration failed, could not map "
              + "incident report to json string. Exception: {}",
          ex.getMessage());
      return;
    }

    // Sanity check...
    Preconditions.checkNotNull(dtoDump);

    try {
      URIBuilder uriBuilder = new URIBuilder(this.serverUrl);
      uriBuilder.setPath(SF_PATH_CREATE_INCIDENT_V2);
      incidentURI = uriBuilder.build();
    } catch (URISyntaxException ex) {
      logger.error(
          "Incident registration failed, " + "URI could not be built. Exception: {}",
          ex.getMessage());
      return;
    }

    HttpPost postRequest = new HttpPost(incidentURI);
    postRequest.setHeader(
        SFSession.SF_HEADER_AUTHORIZATION,
        SFSession.SF_HEADER_SNOWFLAKE_AUTHTYPE
            + " "
            + SFSession.SF_HEADER_TOKEN_TAG
            + "=\""
            + this.sessionToken
            + "\"");

    // Compress the payload.
    ByteArrayEntity input = null;
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      GZIPOutputStream gzos = new GZIPOutputStream(baos);
      byte[] bytes = dtoDump.getBytes(StandardCharsets.UTF_8);
      gzos.write(bytes);
      gzos.finish();
      input = new ByteArrayEntity(baos.toByteArray());
      input.setContentType("application/json");
    } catch (IOException exc) {
      logger.debug(
          "Incident registration failed, could not compress" + " payload. Exception: {}",
          exc.getMessage());
    }

    postRequest.setEntity(input);
    postRequest.addHeader("content-encoding", "gzip");

    try {
      String response = HttpUtil.executeGeneralRequest(postRequest, 1000, OCSPMode.FAIL_OPEN);
      logger.debug("Incident registration was successful. Response: '{}'", response);
    } catch (Exception ex) {
      // No much we can do here besides complain.
      logger.error("Incident registration request failed, exception: {}", ex.getMessage());
    }
  }

  void trigger() {
    EventUtil.triggerIncident(this);
  }
}
