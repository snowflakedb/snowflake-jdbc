package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Disabled
public class SqlInputTimestampUtilTest {

  private static final String TIMESTAMP_IN_FORMAT_1 = "2021-12-22 09:43:44.000 +0100";
  private static final String TIMESTAMP_IN_FORMAT_2 = "Wed, 22 Dec 2021 09:43:44 +0100";
  private static final Map<String, Object> CONNECTION_PARAMS = new HashMap<>();
  private static final Timestamp EXPECTED_TIMESTAMP =
      Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44));

  private static SFBaseSession mockSession;

  @BeforeAll
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
        getFromType(SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_NTZ, TIMESTAMP_IN_FORMAT_1, null);

    assertEquals(EXPECTED_TIMESTAMP, resultLtz);
    assertEquals(EXPECTED_TIMESTAMP, resultTz);
    assertEquals(EXPECTED_TIMESTAMP, resultNtz);
  }

  private Timestamp getFromType(int type, String value, TimeZone explicitTimezone) {
    return SfTimestampUtil.getTimestampFromType(
        type, value, mockSession, TimeZone.getTimeZone("GMT"), explicitTimezone);
  }
}
