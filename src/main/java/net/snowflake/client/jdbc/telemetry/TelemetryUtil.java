package net.snowflake.client.jdbc.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TelemetryUtil
{
  private final static ObjectMapper mapper = new ObjectMapper();

  public final static String TYPE = "type";
  public final static String QUERY_ID = "query_id";
  public final static String VALUE = "value";

  /**
   * Create a simple TelemetryData instance for Job metrics using given parameters
   * @param queryId the id of the query
   * @param field the field to log (represents the "type" field in telemetry)
   * @param value the value to log for the field
   * @return TelemetryData instance constructed from parameters
   */
  public static TelemetryData buildJobData(String queryId, TelemetryField field, long value)
  {
    ObjectNode obj = mapper.createObjectNode();
    obj.put(TYPE, field.toString());
    obj.put(QUERY_ID, queryId);
    obj.put(VALUE, value);
    return new TelemetryData(obj, System.currentTimeMillis());
  }
}
