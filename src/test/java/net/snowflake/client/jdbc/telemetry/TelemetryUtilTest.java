package net.snowflake.client.jdbc.telemetry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.SnowflakeDriver;
import net.snowflake.common.core.LoginInfoDTO;
import org.junit.Test;

public class TelemetryUtilTest {
  String queryId = "testQueryId";
  String sqlState = "00000";
  int errorNumber = 1000;
  TelemetryField type = TelemetryField.HTTP_EXCEPTION;
  String errorMessage = "Test error message";
  String reason = "reason";

  @Test
  public void testCreateIBValueWithAllValues() {
    ObjectNode on =
        TelemetryUtil.createIBValue(queryId, sqlState, errorNumber, type, errorMessage, reason);

    assertEquals(type.toString(), on.get(TelemetryField.TYPE.toString()).asText());
    assertEquals(
        LoginInfoDTO.SF_JDBC_APP_ID, on.get(TelemetryField.DRIVER_TYPE.toString()).asText());
    assertEquals(
        SnowflakeDriver.implementVersion,
        on.get(TelemetryField.DRIVER_VERSION.toString()).asText());
    assertEquals(queryId, on.get(TelemetryField.QUERY_ID.toString()).asText());
    assertEquals(sqlState, on.get(TelemetryField.SQL_STATE.toString()).asText());
    assertEquals(errorNumber, on.get(TelemetryField.ERROR_NUMBER.toString()).asInt());
    assertEquals(errorMessage, on.get(TelemetryField.ERROR_MESSAGE.toString()).asText());
    assertEquals(reason, on.get(TelemetryField.REASON.toString()).asText());
  }

  @Test
  public void testCreateIBValueWithNullValue() {
    ObjectNode on =
        TelemetryUtil.createIBValue(null, sqlState, errorNumber, type, errorMessage, null);
    assertNull(on.get(TelemetryField.QUERY_ID.toString()));
    assertNull(on.get(TelemetryField.REASON.toString()));
  }

  @Test
  public void testBuildCrlData() {
    TelemetryData telemetryData = TelemetryUtil.buildCrlData("url", 123, 1, 100, 25);

    ObjectNode message = telemetryData.getMessage();
    assertNotNull(message);
    assertEquals(
        TelemetryField.CLIENT_CRL_STATS.toString(),
        message.get(TelemetryField.TYPE.toString()).asText());
    assertEquals("url", message.get(TelemetryField.CRL_URL.toString()).asText());
    assertEquals(123, message.get(TelemetryField.CRL_BYTES.toString()).asInt());
    assertEquals(1, message.get(TelemetryField.CRL_REVOKED_CERTIFICATES.toString()).asInt());
    assertEquals(100, message.get(TelemetryField.TIME_DOWNLOADING_CRL.toString()).asInt());
    assertEquals(25, message.get(TelemetryField.TIME_PARSING_CRL.toString()).asInt());
  }
}
