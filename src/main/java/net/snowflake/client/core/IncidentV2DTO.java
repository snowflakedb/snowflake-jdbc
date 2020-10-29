/*
 * Copyright (c) 2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import java.util.LinkedList;
import java.util.List;

public class IncidentV2DTO
{
  // Helper DTOs
  public static class TagDTO
  {
    public final String Name;
    public final String Value;


    public TagDTO(String name,
                  String value)
    {
      this.Name = name;
      this.Value = value;
    }
  }

  public static class ValuesDTO
  {
    public final String requestId;
    public final String jobId;
    public final String exceptionMessage;
    public final String exceptionStackTrace;

    public ValuesDTO(String requestId,
                     String jobId,
                     String exceptionMessage,
                     String exceptionStackTrace)
    {
      this.requestId = requestId;
      this.jobId = jobId;
      this.exceptionMessage = exceptionMessage;
      this.exceptionStackTrace = exceptionStackTrace;
    }
  }

  // ============= Main DTO ================

  /* Signature of the incident */
  public final String Name;

  /* UUID of incident creation */
  public final String UUID;

  /* Created_On time */
  public final String Created_On;

  /* Value object */
  public final ValuesDTO Value;

  /* tags */
  public final List<TagDTO> Tags;

  public IncidentV2DTO(Incident incident)
  {
    this.Name = incident.signature;
    this.UUID = incident.uuid;
    this.Created_On = incident.timestamp;
    this.Value = new ValuesDTO(incident.requestId,
                               incident.jobId,
                               incident.errorMessage,
                               incident.errorStackTrace);
    this.Tags = new LinkedList<>();
    this.Tags.add(new TagDTO("driver", incident.driverName));
    this.Tags.add(new TagDTO("version", incident.driverVersion));
    this.Tags.add(new TagDTO("os", incident.osName));
    this.Tags.add(new TagDTO("osVersion", incident.osVersion));
  }
}
