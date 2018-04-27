/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Random;
import java.util.zip.GZIPOutputStream;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * Incident Event class for triggering and registering Incidents with GS
 * @author jrosen
 */
public class Incident extends Event
{
  private static final SFLogger logger = SFLoggerFactory.getLogger(Incident.class);

  private static final String SF_PATH_CREATE_INCIDENT = "/incidents/create-incident";

  private static final Random randomGenerator = new Random();

  private final Map<String,Object> incident;

  public Incident(Event.EventType type, Map<String,Object> incident)
  {
    super(type, "");

    this.incident = incident;
  }

  /**
   * Generates the Incident Identifier. This should be reasonably unique within
   * the time instant generated. I.e. in case there happens to be a collision
   * (unlikely), then the id together with the timestamp can be used to
   * disambiguate the two events. (Stolen from GS)
   * @return incident id
   */
  public static String generateIncidentId()
  {
    return Integer.toString(randomGenerator.nextInt(8999999) + 1000000);
  }

  /**
   * Registers the incident with Global Services
   */
  @Override
  public void flush()
  {
    ObjectMapper mapper = new ObjectMapper();
    String json;
    String response;
    CloseableHttpClient httpClient;
    URI incidentURI;
    HttpPost postRequest;

    logger.debug("Flushing incident, type={}, msg={}",
          this.getType().getDescription(), this.getMessage());

    // Get session token and incident info
    String sessionToken =
        (String)incident.get(SFSession.SF_HEADER_TOKEN_TAG);
    String serverUrl =
        (String)incident.get(SFSessionProperty.SERVER_URL.getPropertyKey());
    Map<String,Object> incidentInfo =
        (Map<String,Object>)incident.get(IncidentUtil.INCIDENT_INFO);

    if (sessionToken == null || serverUrl == null)
    {
      logger.debug("Incident registration failed, sessionToken or "
              + "serverUrl not specified");
      return;
    }

    // Map the incident to a json payload
    try
    {
      json = mapper.writeValueAsString(incidentInfo);
    }
    catch (JsonProcessingException ex)
    {
      logger.error("Incident registration failed, could not map "
              + "incident report to json string. Exception: {}", ex.getMessage());
      return;
    }

    // Sanity check...
    Preconditions.checkNotNull(json);

    httpClient = HttpUtil.getHttpClient();

    try
    {
      URIBuilder uriBuilder = new URIBuilder(serverUrl);
      uriBuilder.setPath(SF_PATH_CREATE_INCIDENT);
      incidentURI = uriBuilder.build();
    }
    catch (URISyntaxException ex)
    {
      logger.error("Incident registration failed, "
              + "URI could not be built. Exception: {}", ex.getMessage());
      return;
    }

    logger.debug("Creating new HTTP request, token={}, URL={}",
        sessionToken, incidentURI.toString());

    postRequest = new HttpPost(incidentURI);
    postRequest.setHeader(SFSession.SF_HEADER_AUTHORIZATION,
          SFSession.SF_HEADER_SNOWFLAKE_AUTHTYPE + " "
              + SFSession.SF_HEADER_TOKEN_TAG + "=\""
              + sessionToken + "\"");

    // Compress the payload.
    ByteArrayEntity input = null;
    try
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      GZIPOutputStream gzos = new GZIPOutputStream(baos);
      byte[] bytes = json.getBytes("UTF-8");
      gzos.write(bytes);
      gzos.finish();
      input = new ByteArrayEntity(baos.toByteArray());
      input.setContentType("application/json");
    }
    catch(IOException exc)
    {
      logger.warn("Incident registration failed, could not compress"
              + " payload. Exception: {}", exc.getMessage());
    }

    postRequest.setEntity(input);
    postRequest.addHeader("content-encoding", "gzip");

    response = null;

    try
    {
      response = HttpUtil.executeRequest(postRequest,
                                         httpClient,
                                         1000, 0,
                                         null);
    }
    catch (Exception ex)
    {
      // No much we can do here besides complain.
      logger.error(
          "Incident registration request failed, " +
              "response: {}, exception: {}", response, ex.getMessage());
    }
  }
}
