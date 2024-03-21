package net.snowflake.client.core;

import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class SqlInputTimestampUtilTest {

  private static final String TIMESTAMP_IN_FORMAT_1 = "2021-12-22 09:43:44.000 +0100";
  private static final String TIMESTAMP_IN_FORMAT_2 = "Wed, 22 Dec 2021 09:43:44 +0100";
  private static final Map<String, Object> CONNECTION_PARAMS = new HashMap<>();
  private static final Timestamp EXPECTED_TIMESTAMP = Timestamp.valueOf("2021-12-22 09:43:44.0");

  private static SFBaseSession mockSession;

  @BeforeClass
  public static void setup() {
    CONNECTION_PARAMS.put("TIMESTAMP_OUTPUT_FORMAT", "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM");
    CONNECTION_PARAMS.put("TIMESTAMP_TZ_OUTPUT_FORMAT", "DY, DD MON YYYY HH24:MI:SS TZHTZM");
    mockSession = Mockito.mock(SFBaseSession.class);
    Mockito.when(mockSession.getCommonParameters()).thenReturn(CONNECTION_PARAMS);
  }

  @Test
  public void shouldGetTimestampForDifferentType() {
    // when
    Timestamp resultLtz =
        getFromType(SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ, TIMESTAMP_IN_FORMAT_1, null);
    Timestamp resultTz =
        getFromType(SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ, TIMESTAMP_IN_FORMAT_2, null);
    Timestamp resultNtz =
        getFromType(
            SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_NTZ,
            TIMESTAMP_IN_FORMAT_1,
            TimeZone.getTimeZone("EST"));

    assertEquals(EXPECTED_TIMESTAMP, resultLtz);
    System.out.println("LTZ = " + resultLtz);
    assertEquals(EXPECTED_TIMESTAMP, resultTz);
    System.out.println("TZ = " + resultLtz);
    assertEquals(EXPECTED_TIMESTAMP, resultNtz);
    System.out.println("NTZ = " + resultLtz);
  }

  private Timestamp getFromType(int type, String value, TimeZone explicitTimezone) {
    return SqlInputTimestampUtil.getTimestampFromType(
        type, value, mockSession, TimeZone.getTimeZone("UTC"), explicitTimezone);
  }
}
