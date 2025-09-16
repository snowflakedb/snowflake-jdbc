package net.snowflake.client.jdbc.telemetry;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeDriver;
import net.snowflake.common.core.LoginInfoDTO;

public class TelemetryUtil {
  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

  public static final int NO_VENDOR_CODE = -1;
  @Deprecated public static final String TYPE = "type";
  @Deprecated public static final String QUERY_ID = "query_id";
  @Deprecated public static final String VALUE = "value";

  /**
   * Create a simple TelemetryData instance for Job metrics using given parameters
   *
   * @param queryId the id of the query
   * @param field the field to log (represents the "type" field in telemetry)
   * @param value the value to log for the field
   * @return TelemetryData instance constructed from parameters
   */
  public static TelemetryData buildJobData(String queryId, TelemetryField field, long value) {
    ObjectNode obj = mapper.createObjectNode();
    obj.put(TelemetryField.TYPE.toString(), field.toString());
    obj.put(TelemetryField.QUERY_ID.toString(), queryId);
    obj.put(TelemetryField.VALUE.toString(), value);
    return new TelemetryData(obj, System.currentTimeMillis());
  }

  public static TelemetryData buildJobData(ObjectNode obj) {
    return new TelemetryData(obj, System.currentTimeMillis());
  }

  /**
   * Helper function to create ObjectNode for IB telemetry log
   *
   * @param queryId query ID
   * @param sqlState the SQL state
   * @param errorNumber the error number
   * @return ObjectNode for IB telemetry log
   */
  public static ObjectNode createIBValue(
      String queryId,
      String sqlState,
      int errorNumber,
      TelemetryField type,
      String errorMessage,
      String reason) {
    ObjectNode ibValue = mapper.createObjectNode();
    ibValue.put(TelemetryField.TYPE.toString(), type.toString());
    ibValue.put(TelemetryField.DRIVER_TYPE.toString(), LoginInfoDTO.SF_JDBC_APP_ID);
    ibValue.put(TelemetryField.DRIVER_VERSION.toString(), SnowflakeDriver.implementVersion);
    if (!isNullOrEmpty(queryId)) {
      ibValue.put(TelemetryField.QUERY_ID.toString(), queryId);
    }
    if (!isNullOrEmpty(sqlState)) {
      ibValue.put(TelemetryField.SQL_STATE.toString(), sqlState);
    }
    if (errorNumber != NO_VENDOR_CODE) {
      ibValue.put(TelemetryField.ERROR_NUMBER.toString(), errorNumber);
    }
    if (!isNullOrEmpty(errorMessage)) {
      ibValue.put(TelemetryField.ERROR_MESSAGE.toString(), errorMessage);
    }
    if (!isNullOrEmpty(reason)) {
      ibValue.put(TelemetryField.REASON.toString(), reason);
    }
    return ibValue;
  }

  @SnowflakeJdbcInternalApi
  public static TelemetryData buildCrlData(
      String crlUrl, long crlBytes, int revokedCertificates, long downloadTime, long parseTime) {
    ObjectNode obj = mapper.createObjectNode();
    obj.put(TelemetryField.TYPE.toString(), TelemetryField.CLIENT_CRL_STATS.toString());
    obj.put(TelemetryField.CRL_URL.toString(), crlUrl);
    obj.put(TelemetryField.CRL_BYTES.toString(), crlBytes);
    obj.put(TelemetryField.CRL_REVOKED_CERTIFICATES.toString(), revokedCertificates);
    obj.put(TelemetryField.TIME_DOWNLOADING_CRL.toString(), downloadTime);
    obj.put(TelemetryField.TIME_PARSING_CRL.toString(), parseTime);
    return new TelemetryData(obj, System.currentTimeMillis());
  }
}
